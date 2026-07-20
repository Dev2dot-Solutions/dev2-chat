package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/llm"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/nats"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/repository"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/tools"
	"github.com/go-chi/chi/v5"
)

type AgentHandler struct {
	sessionRepo           *repository.SessionRepo
	messageRepo           *repository.MessageRepo
	approvalRepo          *repository.ApprovalRepo
	knowledgeRepo         *repository.KnowledgeRepo
	llmClient             *llm.Client
	natsClient            *nats.Client
	toolExecutor          *tools.Executor
	llmModel              string
	llmProvider           string
	legacyActiveTransport bool
	legacyDeveloperMaxAge time.Duration
}

func (h *AgentHandler) SetLegacyActiveTransportEnabled(enabled bool) {
	h.legacyActiveTransport = enabled
}

func (h *AgentHandler) SetLegacyDeveloperMaxAge(maxAge time.Duration) {
	h.legacyDeveloperMaxAge = maxAge
}

type agentResult struct {
	answer           string
	toolCalls        []models.ToolCallDisplay
	pendingApprovals []models.PendingApproval
	totalTokens      int
	cancelled        bool
}

type preparedAgentRequest struct {
	session     *models.ChatSession
	project     *models.CompanyProject
	req         models.ChatRequest
	profile     string
	actorUserID string
	llmReq      *models.LLMRequest
	sources     []models.Source
}

type completedAgentRequest struct {
	result    agentResult
	toolTrace []models.ToolTraceEvent
}

func NewAgentHandler(
	sr *repository.SessionRepo,
	mr *repository.MessageRepo,
	ar *repository.ApprovalRepo,
	kr *repository.KnowledgeRepo,
	lc *llm.Client,
	nc *nats.Client,
	te *tools.Executor,
	llmModel, llmProvider string,
) *AgentHandler {
	return &AgentHandler{
		sessionRepo:   sr,
		messageRepo:   mr,
		approvalRepo:  ar,
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
	if !h.legacyActiveTransport {
		respondError(w, http.StatusGone, "legacy active chat transport is disabled; use WebSocket")
		return
	}
	var req models.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if companyID := GetCompanyID(r); companyID != "" {
		// Authenticated company identity wins whenever the token carries it.
		req.CompanyID = companyID
	}
	if req.CompanyID == "" || req.Question == "" {
		respondError(w, http.StatusBadRequest, "companyId and question are required")
		return
	}
	if status, errMsg := h.validateLegacyRequestAuthorization(r, req); errMsg != "" {
		respondError(w, status, errMsg)
		return
	}

	prepared, status, errMsg := h.prepareAgentRequest(r, req)
	if errMsg != "" {
		respondError(w, status, errMsg)
		return
	}

	if req.Stream || r.URL.Query().Get("stream") == "true" {
		h.streamAnswer(w, r, prepared)
		return
	}

	completed := h.completeAgentRequest(r, prepared, nil)
	if completed.result.cancelled {
		respondError(w, http.StatusRequestTimeout, "generation cancelled")
		return
	}

	respondJSON(w, http.StatusOK, models.ChatResponse{
		Answer:           completed.result.answer,
		ConversationID:   prepared.session.ID,
		ToolCalls:        completed.result.toolCalls,
		ToolTrace:        completed.toolTrace,
		PendingApprovals: completed.result.pendingApprovals,
		Sources:          prepared.sources,
	})
}

func (h *AgentHandler) validateLegacyRequestAuthorization(r *http.Request, req models.ChatRequest) (int, string) {
	if req.ConversationID != "" {
		session, err := h.sessionRepo.GetByID(r.Context(), req.ConversationID)
		if err != nil {
			return http.StatusInternalServerError, "failed to load session"
		}
		if session != nil {
			return h.validateCurrentSessionAuthorization(r, session, h.legacyDeveloperMaxAge)
		}
	}
	profile := models.NormalizeAccessProfile(req.AccessProfile)
	return h.validateCurrentSessionAuthorization(r, &models.ChatSession{
		CompanyID: req.CompanyID, UserID: GetUserID(r), AccessProfile: profile, ProjectID: req.ProjectID,
	}, h.legacyDeveloperMaxAge)
}

func (h *AgentHandler) validateCurrentSessionAuthorization(r *http.Request, session *models.ChatSession, developerMaxAge time.Duration) (int, string) {
	if session == nil || session.ProjectID == "" {
		return http.StatusForbidden, "project-bound session is required"
	}
	if companyID := GetCompanyID(r); companyID != "" && companyID != session.CompanyID {
		return http.StatusForbidden, "session belongs to a different company"
	}
	profile := models.NormalizeAccessProfile(session.AccessProfile)
	if profile == models.AccessProfileDeveloper {
		if !GetIsAdmin(r) {
			return http.StatusForbidden, "developer sessions require an admin user"
		}
		issuedAt := GetAuthIssuedAt(r)
		if developerMaxAge > 0 && (issuedAt.IsZero() || time.Since(issuedAt) > developerMaxAge) {
			return http.StatusUnauthorized, "developer authentication must be refreshed"
		}
	}
	project, err := h.lookupProjectFresh(r, session.CompanyID, session.ProjectID)
	if err != nil {
		return http.StatusServiceUnavailable, "project authorization unavailable"
	}
	if project == nil || (project.CompanyID != "" && project.CompanyID != session.CompanyID) {
		return http.StatusForbidden, "project authorization revoked"
	}
	if profile == models.AccessProfileClient && !project.Visibility.Client {
		return http.StatusForbidden, "client project access revoked"
	}
	if profile == models.AccessProfileDeveloper && !project.Visibility.Developer {
		return http.StatusForbidden, "developer project access revoked"
	}
	return 0, ""
}

// prepareAgentRequest is shared by REST/SSE and WebSocket transports. It owns
// all session creation, identity binding, project visibility, history and
// prompt preparation so transport handlers cannot fork chat business logic.
func (h *AgentHandler) prepareAgentRequest(r *http.Request, req models.ChatRequest) (*preparedAgentRequest, int, string) {
	session, project, status, errMsg := h.resolveSession(r, req)
	if errMsg != "" {
		return nil, status, errMsg
	}
	profile := models.NormalizeAccessProfile(session.AccessProfile)
	actorUserID := GetUserID(r)
	if actorUserID == "" {
		actorUserID = session.UserID
	}
	req.CompanyID = session.CompanyID
	req.UserID = actorUserID
	req.ConversationID = session.ID
	req.ProjectID = session.ProjectID
	req.AccessProfile = profile

	knowledgeContext := h.searchKnowledge(r, req)
	systemPrompt := "You are a helpful AI assistant for the Dev2Knowledge platform. " +
		"Answer questions based on the knowledge context provided. " +
		"When you need more information, use the available tools to search knowledge, " +
		"look up tickets, or interact with the Project Tracker." + knowledgeContext
	history, _ := h.messageRepo.ListBySession(r.Context(), session.ID, 10)
	h.saveMessage(r, session.ID, req.RequestID, "user", req.Question, "", "")

	prepared := &preparedAgentRequest{
		session: session, project: project, req: req, profile: profile,
		actorUserID: actorUserID,
		llmReq:      h.buildLLMRequest(systemPrompt, history, req, profile, project),
	}
	if knowledgeContext != "" {
		prepared.sources = []models.Source{{Type: "knowledge_graph", Label: "Context from knowledge graph"}}
	}
	return prepared, 0, ""
}

func (h *AgentHandler) completeAgentRequest(r *http.Request, prepared *preparedAgentRequest, onProgress func(models.ToolTraceEvent)) completedAgentRequest {
	var progress []models.ToolTraceEvent
	result := h.processLLMResponse(r, prepared.session, prepared.llmReq, prepared.req, prepared.profile, prepared.project, func(event models.ToolTraceEvent) {
		if !models.IsToolTraceEvent(event) {
			return
		}
		progress = append(progress, event)
		if onProgress != nil {
			onProgress(event)
		}
	})
	if prepared.profile != models.AccessProfileDeveloper {
		result.pendingApprovals = nil
	}
	toolTrace := models.NormalizeToolTrace(progress)
	if !result.cancelled {
		h.finishAsk(r, prepared.session, prepared.req, prepared.actorUserID, result, toolTrace)
	}
	return completedAgentRequest{result: result, toolTrace: toolTrace}
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
	project, status, errMsg := h.validateNewSessionScope(r, req.CompanyID, profile, req.ProjectID)
	if errMsg != "" {
		return nil, nil, status, errMsg
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

// validateNewSessionScope is used both when creating a session and when
// issuing a socket ticket, keeping profile/admin/project policy identical.
func (h *AgentHandler) validateNewSessionScope(r *http.Request, companyID, profile, projectID string) (*models.CompanyProject, int, string) {
	if !models.IsValidAccessProfile(profile) {
		return nil, http.StatusBadRequest, "accessProfile must be \"client\" or \"developer\""
	}
	if profile == models.AccessProfileDeveloper && !GetIsAdmin(r) {
		return nil, http.StatusForbidden, "developer profile requires an admin user"
	}
	if projectID == "" {
		return nil, 0, ""
	}
	p, err := h.lookupProject(r, companyID, projectID)
	if err != nil {
		log.Printf("[agent] project lookup error: %v", err)
		return nil, http.StatusBadGateway, "failed to resolve project"
	}
	if p == nil {
		return nil, http.StatusBadRequest, "unknown projectId for this company"
	}
	if profile == models.AccessProfileClient && !p.Visibility.Client {
		return nil, http.StatusForbidden, "project is not visible to client chat"
	}
	if profile == models.AccessProfileDeveloper && !p.Visibility.Developer {
		return nil, http.StatusForbidden, "project is not visible to developer chat"
	}
	return p, 0, ""
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

func (h *AgentHandler) lookupProjectFresh(r *http.Request, companyID, projectID string) (*models.CompanyProject, error) {
	if h.natsClient == nil {
		return nil, fmt.Errorf("project lookup unavailable")
	}
	projects, err := h.natsClient.RequestCompanyProjectsFresh(companyID)
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
		Model:              h.llmModel,
		MaxTokens:          4096,
		Tools:              h.toolExecutor.ToolDefinitions(profile),
		AccessProfile:      profile,
		SessionID:          req.ConversationID,
		UserID:             req.UserID,
		WorkspaceCompanyID: req.CompanyID,
		WorkspaceProjectID: req.ProjectID,
	}
	if project != nil {
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

func (h *AgentHandler) processLLMResponse(r *http.Request, session *models.ChatSession, llmReq *models.LLMRequest, req models.ChatRequest, profile string, project *models.CompanyProject, onProgress func(models.ToolTraceEvent)) agentResult {
	llmResp, err := h.callLLM(r.Context(), llmReq, onProgress)
	if err != nil {
		if r.Context().Err() != nil {
			return agentResult{cancelled: true}
		}
		log.Printf("[agent] LLM call failed: %v", err)
		return agentResult{answer: "AI service unavailable. Please try again."}
	}
	if llmResp == nil {
		return agentResult{answer: "AI service unavailable. Please try again."}
	}

	result := agentResult{answer: llmResp.Content}
	if llmResp.Usage != nil {
		result.totalTokens = llmResp.Usage.TotalTokens
	}

	// Approval-gated tools executed inside dev2-llm-service report back as
	// tool results with a pending_approval payload (DEV2-108).
	result.pendingApprovals = parsePendingApprovals(llmResp.ToolResults)

	if len(llmResp.ToolCalls) > 0 {
		assistantMsg := &models.ChatMessage{
			SessionID: session.ID, RequestID: req.RequestID, Role: "assistant", Content: llmResp.Content,
		}
		for _, tc := range llmResp.ToolCalls {
			assistantMsg.ToolCalls = append(assistantMsg.ToolCalls, models.ToolCallResult{
				ID: tc.ID, Type: tc.Type, Name: tc.Function.Name, Arguments: tc.Function.Arguments,
			})
		}
		h.messageRepo.Create(r.Context(), assistantMsg)

		execCtx := tools.ExecContext{
			CompanyID:     req.CompanyID,
			UserID:        req.UserID,
			SessionID:     session.ID,
			ProjectID:     session.ProjectID,
			AccessProfile: profile,
		}
		if project != nil {
			execCtx.PTProjectKey = project.ProjectTrackerKey
		}

		for _, tc := range llmResp.ToolCalls {
			toolResult := h.toolExecutor.Execute(r.Context(), tc, execCtx)
			result.toolCalls = append(result.toolCalls, models.ToolCallDisplay{Name: tc.Function.Name, Result: toolResult})
			llmReq.Messages = append(llmReq.Messages, models.LLMMessage{
				Role: "tool", Content: toolResult, ToolCallID: tc.ID, Name: tc.Function.Name,
			})
		}

		followUpResp, err := h.callLLM(r.Context(), llmReq, onProgress)
		if err == nil && followUpResp != nil && followUpResp.Content != "" {
			result.answer = followUpResp.Content
		}
		if err == nil && followUpResp != nil {
			result.pendingApprovals = append(result.pendingApprovals, parsePendingApprovals(followUpResp.ToolResults)...)
			if followUpResp.Usage != nil {
				result.totalTokens += followUpResp.Usage.TotalTokens
			}
		}
	}

	return result
}

// parsePendingApprovals extracts approval requests from dev2-llm-service
// tool results. Approval-gated tools return an Output payload shaped
// {"status":"pending_approval","approvalId":...,"summary":...,"preview":...,
// "expiresAt":...} (camelCase); anything else is ignored.
func parsePendingApprovals(results []models.LLMToolResult) []models.PendingApproval {
	var out []models.PendingApproval
	for _, tr := range results {
		if tr.Output == "" {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(tr.Output), &payload); err != nil {
			continue
		}
		if status, _ := payload["status"].(string); status != "pending_approval" {
			continue
		}
		approvalID, _ := payload["approvalId"].(string)
		if approvalID == "" {
			continue
		}
		pa := models.PendingApproval{
			ApprovalID: approvalID,
			Tool:       tr.ToolName,
			Status:     models.ApprovalStatusPending,
		}
		if t, _ := payload["tool"].(string); t != "" {
			pa.Tool = t
		}
		pa.Summary, _ = payload["summary"].(string)
		pa.Preview, _ = payload["preview"].(string)
		if pa.Summary == "" {
			pa.Summary = pa.Preview
		}
		if e, _ := payload["expiresAt"].(string); e != "" {
			if ts, err := time.Parse(time.RFC3339, e); err == nil {
				pa.ExpiresAt = ts
			}
		}
		out = append(out, pa)
	}
	return out
}

// recordPendingApprovals persists the approvalId → session mapping used by
// the decision endpoint to resolve ownership (DEV2-108). Best-effort: a
// persistence failure is logged but does not fail the chat response.
func (h *AgentHandler) recordPendingApprovals(r *http.Request, session *models.ChatSession, actorUserID string, approvals []models.PendingApproval) {
	if h.approvalRepo == nil {
		return
	}
	for _, pa := range approvals {
		rec := &models.ApprovalRecord{
			ID:        pa.ApprovalID,
			SessionID: session.ID,
			CompanyID: session.CompanyID,
			UserID:    actorUserID,
			Tool:      pa.Tool,
			Summary:   pa.Summary,
			Preview:   pa.Preview,
			ExpiresAt: pa.ExpiresAt,
		}
		if err := h.approvalRepo.RecordPending(r.Context(), rec); err != nil {
			log.Printf("[agent] Record approval %s error: %v", pa.ApprovalID, err)
		}
	}
}

// streamAnswer owns all ResponseWriter access. The worker reports progress and
// its final result over channels so NATS callbacks can never write SSE data
// concurrently.
func (h *AgentHandler) streamAnswer(w http.ResponseWriter, r *http.Request, prepared *preparedAgentRequest) {
	if r.Context().Err() != nil {
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		respondError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	flusher.Flush()

	progressCh := make(chan models.ToolTraceEvent, 64)
	resultCh := make(chan agentResult, 1)
	go func() {
		completed := h.completeAgentRequest(r, prepared, func(event models.ToolTraceEvent) {
			if !models.IsToolTraceEvent(event) {
				return
			}
			if r.Context().Err() != nil {
				return
			}
			select {
			case progressCh <- event:
			case <-r.Context().Done():
			}
		})
		select {
		case resultCh <- completed.result:
		case <-r.Context().Done():
		}
	}()

	var progress []models.ToolTraceEvent
	for {
		select {
		case event := <-progressCh:
			if r.Context().Err() != nil {
				return
			}
			progress = append(progress, event)
			if err := writeSSEJSON(w, flusher, "trace", event); err != nil {
				return
			}
		case result := <-resultCh:
			if r.Context().Err() != nil {
				return
			}
			// processLLMResponse sends its result only after every progress
			// callback has completed, so all remaining events are now buffered.
		drainProgress:
			for {
				select {
				case event := <-progressCh:
					if r.Context().Err() != nil {
						return
					}
					progress = append(progress, event)
					if err := writeSSEJSON(w, flusher, "trace", event); err != nil {
						return
					}
				default:
					break drainProgress
				}
			}

			if result.cancelled {
				return
			}
			toolTrace := models.NormalizeToolTrace(progress)
			for _, chunk := range chunkText(result.answer, 200) {
				if r.Context().Err() != nil {
					return
				}
				if err := writeSSEJSON(w, flusher, "chunk", map[string]string{"content": chunk}); err != nil {
					return
				}
			}
			if r.Context().Err() != nil {
				return
			}
			if err := writeSSEJSON(w, flusher, "meta", buildStreamMeta(prepared.session.ID, result, toolTrace, prepared.sources)); err != nil {
				return
			}
			if r.Context().Err() != nil {
				return
			}
			if _, err := fmt.Fprint(w, "event: done\ndata: [DONE]\n\n"); err != nil {
				return
			}
			flusher.Flush()
			return
		case <-r.Context().Done():
			return
		}
	}
}

func buildStreamMeta(sessionID string, result agentResult, toolTrace []models.ToolTraceEvent, sources []models.Source) models.ChatResponse {
	return models.ChatResponse{
		Answer:           result.answer,
		ConversationID:   sessionID,
		ToolCalls:        result.toolCalls,
		ToolTrace:        toolTrace,
		PendingApprovals: result.pendingApprovals,
		Sources:          sources,
	}
}

func writeSSEJSON(w http.ResponseWriter, flusher http.Flusher, event string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
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

func (h *AgentHandler) callLLM(ctx context.Context, req *models.LLMRequest, onProgress func(models.ToolTraceEvent)) (*models.LLMResponse, error) {
	if h.natsClient != nil {
		resp, err := h.natsClient.RequestLLMWithProgress(ctx, req, onProgress)
		if err == nil {
			return resp, nil
		}
		log.Printf("[agent] NATS LLM failed, fallback to HTTP: %v", err)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if h.llmClient != nil {
		return h.llmClient.ChatCompletionWithContext(ctx, req)
	}
	return nil, fmt.Errorf("LLM service unavailable")
}

func (h *AgentHandler) finishAsk(r *http.Request, session *models.ChatSession, req models.ChatRequest, actorUserID string, result agentResult, toolTrace []models.ToolTraceEvent) {
	h.recordPendingApprovals(r, session, actorUserID, result.pendingApprovals)
	assistantMsg := &models.ChatMessage{
		SessionID:        session.ID,
		RequestID:        req.RequestID,
		Role:             "assistant",
		Content:          result.answer,
		PendingApprovals: result.pendingApprovals,
		ToolTrace:        toolTrace,
	}
	if err := h.messageRepo.Create(r.Context(), assistantMsg); err != nil {
		log.Printf("[agent] Save message error: %v", err)
	}
	if result.totalTokens > 0 {
		if err := h.sessionRepo.UpdateTokenCount(r.Context(), session.ID, result.totalTokens); err != nil {
			log.Printf("[agent] Update token count error: %v", err)
		}
	}
	if h.natsClient != nil {
		go h.natsClient.PublishMessageSent(session.ID, req.CompanyID, req.UserID, "user", req.Question)
		go h.natsClient.PublishMessageSent(session.ID, req.CompanyID, req.UserID, "assistant", result.answer)
	}
}

func (h *AgentHandler) saveMessage(r *http.Request, sessionID, requestID, role, content, toolCallID, name string) {
	msg := &models.ChatMessage{
		SessionID:  sessionID,
		RequestID:  requestID,
		Role:       role,
		Content:    content,
		ToolCallID: toolCallID,
		Name:       name,
	}
	if err := h.messageRepo.Create(r.Context(), msg); err != nil {
		log.Printf("[agent] Save message error: %v", err)
	}
}
