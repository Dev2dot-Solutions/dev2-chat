package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/llm"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/nats"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/repository"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/tools"
	"github.com/go-chi/chi/v5"
)

type AgentHandler struct {
	sessionRepo    *repository.SessionRepo
	messageRepo    *repository.MessageRepo
	knowledgeRepo  *repository.KnowledgeRepo
	llmClient      *llm.Client
	natsClient     *nats.Client
	toolExecutor   *tools.Executor
	llmModel       string
	llmProvider    string
}

func NewAgentHandler(
	sr *repository.SessionRepo,
	mr *repository.MessageRepo,
	kr *repository.KnowledgeRepo,
	lc *llm.Client,
	nc *nats.Client,
	te *tools.Executor,
	llmModel, llmProvider string,
) *AgentHandler {
	return &AgentHandler{
		sessionRepo:    sr,
		messageRepo:    mr,
		knowledgeRepo:  kr,
		llmClient:      lc,
		natsClient:     nc,
		toolExecutor:   te,
		llmModel:       llmModel,
		llmProvider:    llmProvider,
	}
}

func (h *AgentHandler) Routes(r chi.Router) {
	r.Post("/agent/ask", h.Ask)
}

func (h *AgentHandler) Ask(w http.ResponseWriter, r *http.Request) {
	var req models.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.CompanyID == "" || req.Question == "" {
		respondError(w, http.StatusBadRequest, "company_id and question are required")
		return
	}

	session, err := h.resolveSession(r, req)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to create session")
		return
	}

	knowledgeContext := h.searchKnowledge(r, req)
	systemPrompt := "You are a helpful AI assistant for the Dev2Knowledge platform. " +
		"Answer questions based on the knowledge context provided. " +
		"When you need more information, use the available tools to search knowledge, " +
		"look up tickets, or interact with the Project Tracker." +
		knowledgeContext

	history, _ := h.messageRepo.ListBySession(r.Context(), session.ID, 10)

	h.saveMessage(r, session.ID, "user", req.Question, "", "")

	llmReq := h.buildLLMRequest(systemPrompt, history, req)
	finalAnswer, toolCallResults := h.processLLMResponse(r, session, llmReq, req)

	h.saveMessage(r, session.ID, "assistant", finalAnswer, "", "")

	if llmResp, _ := h.callLLM(llmReq); llmResp != nil && llmResp.Usage != nil {
		h.sessionRepo.UpdateTokenCount(r.Context(), session.ID, llmResp.Usage.TotalTokens)
	}

	go h.natsClient.PublishMessageSent(session.ID, req.CompanyID, req.UserID, "user", req.Question)
	go h.natsClient.PublishMessageSent(session.ID, req.CompanyID, req.UserID, "assistant", finalAnswer)

	var sources []models.Source
	if knowledgeContext != "" {
		sources = []models.Source{{Type: "knowledge_graph", Label: "Context from knowledge graph"}}
	}

	respondJSON(w, http.StatusOK, models.ChatResponse{
		Answer:         finalAnswer,
		ConversationID: session.ID,
		ToolCalls:      toolCallResults,
		Sources:        sources,
	})
}

func (h *AgentHandler) resolveSession(r *http.Request, req models.ChatRequest) (*models.ChatSession, error) {
	if req.ConversationID != "" {
		s, err := h.sessionRepo.GetByID(r.Context(), req.ConversationID)
		if err == nil && s != nil {
			return s, nil
		}
	}
	title := req.Question
	if len(title) > 100 {
		title = title[:100]
	}
	s, err := h.sessionRepo.Create(r.Context(), models.ChatSessionInput{
		CompanyID: req.CompanyID,
		UserID:    req.UserID,
		Title:     title,
		Model:     h.llmModel,
		Provider:  h.llmProvider,
	})
	if err == nil {
		go h.natsClient.PublishSessionCreated(s)
	}
	return s, err
}

func (h *AgentHandler) searchKnowledge(r *http.Request, req models.ChatRequest) string {
	if h.knowledgeRepo == nil {
		return ""
	}
	result, err := h.knowledgeRepo.SearchKnowledgeGraph(r.Context(), req.Question, req.CompanyID, 5)
	if err != nil || result.TotalMatches == 0 {
		return ""
	}
	var entries []string
	for entityType, results := range result.Results {
		for _, res := range results {
			snippet := res.Snippet
			if len(snippet) > 200 {
				snippet = snippet[:200]
			}
			entries = append(entries, "["+entityType+"] "+res.Name+": "+snippet)
		}
	}
	if len(entries) == 0 {
		return ""
	}
	return "\n\nRelevant knowledge context:\n" + strings.Join(entries, "\n")
}

func (h *AgentHandler) buildLLMRequest(systemPrompt string, history []models.ChatMessage, req models.ChatRequest) *models.LLMRequest {
	llmReq := &models.LLMRequest{
		Model:     h.llmModel,
		MaxTokens: 4096,
		Tools:     h.toolExecutor.ToolDefinitions(),
	}
	llmReq.Messages = append(llmReq.Messages, models.LLMMessage{Role: "system", Content: systemPrompt})
	for _, msg := range history {
		m := models.LLMMessage{Role: msg.Role, Content: msg.Content}
		if msg.ToolCallID != "" {
			m.ToolCallID = msg.ToolCallID
			m.Name = msg.Name
		}
		llmReq.Messages = append(llmReq.Messages, m)
	}
	llmReq.Messages = append(llmReq.Messages, models.LLMMessage{Role: "user", Content: req.Question})
	return llmReq
}

func (h *AgentHandler) processLLMResponse(r *http.Request, session *models.ChatSession, llmReq *models.LLMRequest, req models.ChatRequest) (string, []models.ToolCallDisplay) {
	llmResp, err := h.callLLM(llmReq)
	if err != nil {
		log.Printf("[agent] LLM call failed: %v", err)
		return "AI service unavailable. Please try again.", nil
	}

	finalAnswer := llmResp.Content
	var toolCallResults []models.ToolCallDisplay

	if len(llmResp.ToolCalls) > 0 {
		assistantMsg := &models.ChatMessage{
			SessionID: session.ID, Role: "assistant", Content: llmResp.Content,
		}
		for _, tc := range llmResp.ToolCalls {
			assistantMsg.ToolCalls = append(assistantMsg.ToolCalls, models.ToolCallResult{
				ID: tc.ID, Type: tc.Type, Name: tc.Function.Name, Arguments: tc.Function.Arguments,
			})
		}
		h.messageRepo.Create(r.Context(), assistantMsg)

		for _, tc := range llmResp.ToolCalls {
			result := h.toolExecutor.Execute(r.Context(), tc, req.CompanyID, req.UserID)
			toolCallResults = append(toolCallResults, models.ToolCallDisplay{Name: tc.Function.Name, Result: result})
			llmReq.Messages = append(llmReq.Messages, models.LLMMessage{
				Role: "tool", Content: result, ToolCallID: tc.ID, Name: tc.Function.Name,
			})
		}

		followUpResp, err := h.callLLM(llmReq)
		if err == nil && followUpResp.Content != "" {
			finalAnswer = followUpResp.Content
		}
	}

	return finalAnswer, toolCallResults
}

func (h *AgentHandler) callLLM(req *models.LLMRequest) (*models.LLMResponse, error) {
	if h.natsClient != nil {
		resp, err := h.natsClient.RequestLLM(req)
		if err == nil {
			return resp, nil
		}
		log.Printf("[agent] NATS LLM failed, fallback to HTTP: %v", err)
	}
	if h.llmClient != nil {
		return h.llmClient.ChatCompletion(req)
	}
	return nil, nil
}

func (h *AgentHandler) saveMessage(r *http.Request, sessionID, role, content, toolCallID, name string) {
	msg := &models.ChatMessage{
		SessionID:  sessionID,
		Role:       role,
		Content:    content,
		ToolCallID: toolCallID,
		Name:       name,
	}
	if err := h.messageRepo.Create(r.Context(), msg); err != nil {
		log.Printf("[agent] Save message error: %v", err)
	}
}
