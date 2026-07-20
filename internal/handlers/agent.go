package handlers

import (
	"encoding/json"
	"fmt"
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
	sessionRepo   *repository.SessionRepo
	messageRepo   *repository.MessageRepo
	knowledgeRepo *repository.KnowledgeRepo
	llmClient     *llm.Client
	natsClient    *nats.Client
	toolExecutor  *tools.Executor
	llmModel      string
	llmProvider   string
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
		sessionRepo:   sr,
		messageRepo:   mr,
		knowledgeRepo: kr,
		llmClient:     lc,
		natsClient:    nc,
		toolExecutor:  te,
		llmModel:      llmModel,
		llmProvider:   llmProvider,
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
		respondError(w, http.StatusBadRequest, "companyId and question are required")
		return
	}

	session, project, status, errMsg := h.resolveSession(r, req)
	if errMsg != "" {
		respondError(w, status, errMsg)
		return
	}
	profile := models.NormalizeAccessProfile(session.AccessProfile)

	knowledgeContext := h.searchKnowledge(r, req)
	systemPrompt := "You are a helpful AI assistant for the Dev2Knowledge platform. " +
		"Answer questions based on the knowledge context provided. " +
		"When you need more information, use the available tools to search knowledge, " +
		"look up tickets, or interact with the Project Tracker." +
		knowledgeContext

	history, _ := h.messageRepo.ListBySession(r.Context(), session.ID, 10)

	h.saveMessage(r, session.ID, "user", req.Question, "", "")

	llmReq := h.buildLLMRequest(systemPrompt, history, req, profile, project)
	finalAnswer, toolCallResults := h.processLLMResponse(r, session, llmReq, req, profile, project)

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

	if req.Stream || r.URL.Query().Get("stream") == "true" {
		h.streamAnswer(w, session, finalAnswer, toolCallResults, sources)
		return
	}

	respondJSON(w, http.StatusOK, models.ChatResponse{
		Answer:         finalAnswer,
		ConversationID: session.ID,
		ToolCalls:      toolCallResults,
		Sources:        sources,
	})
}

// resolveSession loads or creates the chat session for a request and enforces
// the access-profile authorization rules:
//   - developer sessions require an admin user (JWT dev2-admins group or the
//     service API key);
//   - a project-bound session requires the project's visibility flag for the
//     profile (visibility.client / visibility.developer);
//   - an existing session must belong to the request's company, and a
//     non-admin may only continue sessions they own.
//
// On failure it returns an HTTP status and message for the caller to send.
func (h *AgentHandler) resolveSession(r *http.Request, req models.ChatRequest) (*models.ChatSession, *models.CompanyProject, int, string) {
	isAdmin := GetIsAdmin(r)
	authUserID := GetUserID(r)

	if req.ConversationID != "" {
		s, err := h.sessionRepo.GetByID(r.Context(), req.ConversationID)
		if err != nil {
			log.Printf("[agent] GetSession error: %v", err)
			return nil, nil, http.StatusInternalServerError, "failed to load session"
		}
		if s != nil {
			if s.CompanyID != req.CompanyID {
				return nil, nil, http.StatusForbidden, "session belongs to a different company"
			}
			if models.NormalizeAccessProfile(s.AccessProfile) == models.AccessProfileDeveloper && !isAdmin {
				return nil, nil, http.StatusForbidden, "developer sessions require an admin user"
			}
			if !isAdmin && authUserID != "" && s.UserID != "" && s.UserID != authUserID {
				return nil, nil, http.StatusForbidden, "session belongs to a different user"
			}
			var project *models.CompanyProject
			if s.ProjectID != "" {
				// Best-effort: used for PT project key + workspace scoping.
				if p, err := h.lookupProject(r, s.CompanyID, s.ProjectID); err == nil {
					project = p
				} else {
					log.Printf("[agent] project lookup failed for session %s: %v", s.ID, err)
				}
			}
			return s, project, 0, ""
		}
		// Unknown conversationId — fall through and create a new session
		// (legacy behaviour for stale client-side IDs).
	}

	// New session — validate the requested profile and project binding.
	profile := models.AccessProfileClient
	if req.AccessProfile != "" {
		if !models.IsValidAccessProfile(req.AccessProfile) {
			return nil, nil, http.StatusBadRequest, "accessProfile must be \"client\" or \"developer\""
		}
		profile = req.AccessProfile
	}
	if profile == models.AccessProfileDeveloper && !isAdmin {
		return nil, nil, http.StatusForbidden, "developer profile requires an admin user"
	}

	var project *models.CompanyProject
	if req.ProjectID != "" {
		p, err := h.lookupProject(r, req.CompanyID, req.ProjectID)
		if err != nil {
			log.Printf("[agent] project lookup error: %v", err)
			return nil, nil, http.StatusBadGateway, "failed to resolve project"
		}
		if p == nil {
			return nil, nil, http.StatusBadRequest, "unknown projectId for this company"
		}
		switch profile {
		case models.AccessProfileClient:
			if !p.Visibility.Client {
				return nil, nil, http.StatusForbidden, "project is not visible to client chat"
			}
		case models.AccessProfileDeveloper:
			if !p.Visibility.Developer {
				return nil, nil, http.StatusForbidden, "project is not visible to developer chat"
			}
		}
		project = p
	}

	userID := req.UserID
	if authUserID != "" {
		// Bind the session to the authenticated identity rather than trusting
		// the client-supplied userId.
		userID = authUserID
	}

	title := req.Question
	if len(title) > 100 {
		title = title[:100]
	}
	s, err := h.sessionRepo.Create(r.Context(), models.ChatSessionInput{
		CompanyID:     req.CompanyID,
		UserID:        userID,
		Title:         title,
		Model:         h.llmModel,
		Provider:      h.llmProvider,
		AccessProfile: profile,
		ProjectID:     req.ProjectID,
	})
	if err != nil {
		log.Printf("[agent] Create session error: %v", err)
		return nil, nil, http.StatusInternalServerError, "failed to create session"
	}
	go h.natsClient.PublishSessionCreated(s)
	return s, project, 0, ""
}

// lookupProject resolves a Dev2Project via company.projects.get (cached).
func (h *AgentHandler) lookupProject(r *http.Request, companyID, projectID string) (*models.CompanyProject, error) {
	if h.natsClient == nil {
		return nil, fmt.Errorf("project lookup unavailable")
	}
	projects, err := h.natsClient.RequestCompanyProjects(companyID)
	if err != nil {
		return nil, err
	}
	for i := range projects {
		if projects[i].ID == projectID {
			return &projects[i], nil
		}
	}
	return nil, nil
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

func (h *AgentHandler) buildLLMRequest(systemPrompt string, history []models.ChatMessage, req models.ChatRequest, profile string, project *models.CompanyProject) *models.LLMRequest {
	llmReq := &models.LLMRequest{
		Model:         h.llmModel,
		MaxTokens:     4096,
		Tools:         h.toolExecutor.ToolDefinitions(profile),
		AccessProfile: profile,
	}
	if project != nil {
		llmReq.WorkspaceCompanyID = project.CompanyID
		llmReq.WorkspaceProjectID = project.ID
		llmReq.WorkspacePTProjectKey = project.ProjectTrackerKey
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

func (h *AgentHandler) processLLMResponse(r *http.Request, session *models.ChatSession, llmReq *models.LLMRequest, req models.ChatRequest, profile string, project *models.CompanyProject) (string, []models.ToolCallDisplay) {
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

		execCtx := tools.ExecContext{
			CompanyID:     req.CompanyID,
			UserID:        session.UserID,
			AccessProfile: profile,
		}
		if project != nil {
			execCtx.PTProjectKey = project.ProjectTrackerKey
		}

		for _, tc := range llmResp.ToolCalls {
			result := h.toolExecutor.Execute(r.Context(), tc, execCtx)
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

// streamAnswer writes the completed answer as Server-Sent Events. The NATS
// llm.request exchange is single-shot request-reply, so the final content is
// chunked here rather than token-streamed from the LLM — token-level
// streaming and tool-call trace visibility are DEV2-100 follow-ups.
func (h *AgentHandler) streamAnswer(w http.ResponseWriter, session *models.ChatSession, answer string, toolCalls []models.ToolCallDisplay, sources []models.Source) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		respondError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	for _, chunk := range chunkText(answer, 200) {
		data, _ := json.Marshal(map[string]string{"content": chunk})
		fmt.Fprintf(w, "event: chunk\ndata: %s\n\n", data)
		flusher.Flush()
	}

	meta, _ := json.Marshal(models.ChatResponse{
		ConversationID: session.ID,
		ToolCalls:      toolCalls,
		Sources:        sources,
	})
	fmt.Fprintf(w, "event: meta\ndata: %s\n\n", meta)
	fmt.Fprintf(w, "event: done\ndata: [DONE]\n\n")
	flusher.Flush()
}

// chunkText splits s into rune chunks of at most size, breaking on
// whitespace where possible.
func chunkText(s string, size int) []string {
	runes := []rune(s)
	if len(runes) == 0 {
		return []string{""}
	}
	var chunks []string
	for len(runes) > 0 {
		n := size
		if n > len(runes) {
			n = len(runes)
		}
		// Prefer breaking at a space near the chunk boundary.
		if n < len(runes) {
			for i := n - 1; i > n/2; i-- {
				if runes[i] == ' ' || runes[i] == '\n' {
					n = i + 1
					break
				}
			}
		}
		chunks = append(chunks, string(runes[:n]))
		runes = runes[n:]
	}
	return chunks
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
