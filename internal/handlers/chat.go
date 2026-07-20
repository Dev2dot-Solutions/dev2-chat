package handlers

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/nats"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/repository"
	"github.com/go-chi/chi/v5"
)

type ChatHandler struct {
	sessionRepo     *repository.SessionRepo
	messageRepo     *repository.MessageRepo
	approvalRepo    *repository.ApprovalRepo
	natsClient      *nats.Client
	projectResolver func(string) ([]models.CompanyProject, error)
}

func (h *ChatHandler) SetAgentHandler(agent *AgentHandler) {
	if agent != nil && agent.natsClient != nil {
		h.projectResolver = agent.natsClient.RequestCompanyProjectsFresh
	}
}

func NewChatHandler(sr *repository.SessionRepo, mr *repository.MessageRepo, ar *repository.ApprovalRepo, nc *nats.Client) *ChatHandler {
	return &ChatHandler{sessionRepo: sr, messageRepo: mr, approvalRepo: ar, natsClient: nc}
}

func (h *ChatHandler) Routes(r chi.Router) {
	r.Route("/chat", func(r chi.Router) {
		r.Get("/sessions", h.ListSessions)    // GET /chat/sessions — list sessions
		r.Get("/sessions/{id}", h.GetSession) // GET /chat/sessions/{id} — get session with messages
	})
}

// ListSessions handles GET /chat/sessions
// Admins deliberately choose companyId/userId/profile query scope. Non-admin
// scope always comes from authenticated context and never from query input.
func (h *ChatHandler) ListSessions(w http.ResponseWriter, r *http.Request) {
	isAdmin := GetIsAdmin(r)
	requestedCompanyID := r.URL.Query().Get("companyId")
	companyID, userID := requestedCompanyID, r.URL.Query().Get("userId")
	accessProfile := r.URL.Query().Get("accessProfile")
	if accessProfile != "" && !models.IsValidAccessProfile(accessProfile) {
		respondError(w, http.StatusBadRequest, "accessProfile must be \"client\" or \"developer\"")
		return
	}
	if !isAdmin {
		companyID, userID = GetCompanyID(r), GetUserID(r)
		if !isValidUUID(companyID) || userID == "" {
			respondError(w, http.StatusForbidden, "authenticated company and user identity are required")
			return
		}
		if requestedCompanyID != "" && requestedCompanyID != companyID {
			respondError(w, http.StatusForbidden, "company query scope does not match authenticated company")
			return
		}
		if accessProfile == models.AccessProfileDeveloper {
			respondError(w, http.StatusForbidden, "developer sessions require an admin user")
			return
		}
		if accessProfile == "" {
			accessProfile = models.AccessProfileClient
		}
	} else if !isValidUUID(companyID) {
		respondError(w, http.StatusBadRequest, "missing or invalid companyId")
		return
	}
	projects, err := h.resolveHistoryProjects(companyID)
	if err != nil {
		log.Printf("[chat] project authorization unavailable for session list: %v", err)
		respondError(w, http.StatusServiceUnavailable, "project authorization unavailable")
		return
	}
	clientProjectIDs, developerProjectIDs := visibleProjectIDs(projects, companyID)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	result, err := h.sessionRepo.ListByCompany(r.Context(), companyID, userID, accessProfile, clientProjectIDs, developerProjectIDs, limit, offset)
	if err != nil {
		log.Printf("[chat] ListSessions error: %v", err)
		respondError(w, http.StatusInternalServerError, "failed to list sessions")
		return
	}

	respondJSON(w, http.StatusOK, result)
}

// GetSession handles GET /chat/sessions/{id}
func (h *ChatHandler) GetSession(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if !isValidUUID(id) {
		respondError(w, http.StatusBadRequest, "invalid session id")
		return
	}

	session, err := h.sessionRepo.GetByID(r.Context(), id)
	if err != nil {
		log.Printf("[chat] GetSession error: %v", err)
		respondError(w, http.StatusInternalServerError, "failed to get session")
		return
	}
	if session == nil {
		respondError(w, http.StatusNotFound, "session not found")
		return
	}

	if !GetIsAdmin(r) {
		if errMsg := nonAdminHistoryAuthorizationError(session, GetCompanyID(r), GetUserID(r)); errMsg != "" {
			respondError(w, http.StatusForbidden, errMsg)
			return
		}
	}
	projects, err := h.resolveHistoryProjects(session.CompanyID)
	if err != nil {
		log.Printf("[chat] project authorization unavailable for session %s: %v", id, err)
		respondError(w, http.StatusServiceUnavailable, "project authorization unavailable")
		return
	}
	if !historySessionVisible(session, projects) {
		respondError(w, http.StatusForbidden, "session project access revoked")
		return
	}

	messages, err := h.messageRepo.ListBySession(r.Context(), id, 50)
	if err != nil {
		log.Printf("[chat] ListMessages error: %v", err)
		respondError(w, http.StatusInternalServerError, "failed to get messages")
		return
	}
	if models.NormalizeAccessProfile(session.AccessProfile) == models.AccessProfileClient {
		stripActionableApprovals(messages)
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"session":  session,
		"messages": messages,
	})
}

func nonAdminHistoryAuthorizationError(session *models.ChatSession, companyID, userID string) string {
	if session == nil || companyID == "" || session.CompanyID != companyID {
		return "session belongs to a different company"
	}
	if models.NormalizeAccessProfile(session.AccessProfile) == models.AccessProfileDeveloper {
		return "developer sessions require an admin user"
	}
	if userID == "" || session.UserID == "" || session.UserID != userID {
		return "session belongs to a different user"
	}
	return ""
}

func (h *ChatHandler) resolveHistoryProjects(companyID string) ([]models.CompanyProject, error) {
	if h.projectResolver == nil {
		return nil, fmt.Errorf("project resolver unavailable")
	}
	return h.projectResolver(companyID)
}

func visibleProjectIDs(projects []models.CompanyProject, companyID string) ([]string, []string) {
	var clientIDs, developerIDs []string
	for _, project := range projects {
		if project.CompanyID != "" && project.CompanyID != companyID {
			continue
		}
		if project.Visibility.Client {
			clientIDs = append(clientIDs, project.ID)
		}
		if project.Visibility.Developer {
			developerIDs = append(developerIDs, project.ID)
		}
	}
	return clientIDs, developerIDs
}

func historySessionVisible(session *models.ChatSession, projects []models.CompanyProject) bool {
	profile := models.NormalizeAccessProfile(session.AccessProfile)
	if session.ProjectID == "" {
		return profile == models.AccessProfileClient
	}
	for _, project := range projects {
		if project.ID != session.ProjectID || (project.CompanyID != "" && project.CompanyID != session.CompanyID) {
			continue
		}
		if profile == models.AccessProfileDeveloper {
			return project.Visibility.Developer
		}
		return project.Visibility.Client
	}
	return false
}

func stripActionableApprovals(messages []models.ChatMessage) {
	for i := range messages {
		messages[i].PendingApprovals = nil
	}
}

// decideApproval contains the authorization and business transaction used by
// WebSocket approval decisions.
func (h *ChatHandler) decideApproval(r *http.Request, approvalID, decision string) (int, map[string]any, string) {
	rec, err := h.approvalRepo.GetByID(r.Context(), approvalID)
	if err != nil {
		log.Printf("[chat] DecideApproval lookup error: %v", err)
		return http.StatusInternalServerError, nil, "failed to load approval"
	}
	if rec == nil {
		return http.StatusNotFound, nil, "approval not found"
	}

	session, err := h.sessionRepo.GetByID(r.Context(), rec.SessionID)
	if err != nil {
		log.Printf("[chat] DecideApproval session error: %v", err)
		return http.StatusInternalServerError, nil, "failed to load session"
	}
	if session == nil {
		return http.StatusNotFound, nil, "approval not found"
	}

	if errMsg := approvalAuthorizationError(session, GetIsAdmin(r)); errMsg != "" {
		return http.StatusForbidden, nil, errMsg
	}
	if companyID := GetCompanyID(r); companyID != "" && session.CompanyID != companyID {
		return http.StatusForbidden, nil, "session belongs to a different company"
	}

	if rec.Status != models.ApprovalStatusPending {
		return http.StatusConflict, nil, "approval already decided"
	}

	// Identity is taken from the persisted approval/session, not the request body.
	approvalUserID := rec.UserID
	if approvalUserID == "" {
		approvalUserID = session.UserID
	}
	toolResp, err := h.natsClient.RequestToolApproval(&models.ToolApprovalRequest{
		ApprovalID: rec.ID,
		Decision:   decision,
		SessionID:  session.ID,
		UserID:     approvalUserID,
	})
	if err != nil {
		log.Printf("[chat] tool.approve failed for %s: %v", approvalID, err)
		return http.StatusBadGateway, nil, "approval service unavailable"
	}

	switch toolResp.Status {
	case models.ApprovalStatusExecuted, models.ApprovalStatusRejected:
		if ok, err := h.approvalRepo.MarkDecided(r.Context(), rec.ID, decision, toolResp.Status); err != nil {
			log.Printf("[chat] MarkDecided error for %s: %v", approvalID, err)
		} else if !ok {
			// Lost a race with a concurrent decision.
			return http.StatusConflict, nil, "approval already decided"
		}
		if err := h.messageRepo.UpdateApprovalStatus(r.Context(), session.ID, rec.ID, toolResp.Status); err != nil {
			log.Printf("[chat] UpdateApprovalStatus error for %s: %v", approvalID, err)
		}
		h.appendDecisionMessage(r, session, rec, decision, toolResp.Status)
		return http.StatusOK, map[string]any{
			"approvalId": rec.ID,
			"decision":   decision,
			"status":     toolResp.Status,
			"result":     toolResp.Result,
		}, ""
	case models.ApprovalStatusExpired:
		// Terminal: the card is dead, block repeat actions.
		if _, err := h.approvalRepo.MarkDecided(r.Context(), rec.ID, decision, models.ApprovalStatusExpired); err != nil {
			log.Printf("[chat] MarkDecided error for %s: %v", approvalID, err)
		}
		if err := h.messageRepo.UpdateApprovalStatus(r.Context(), session.ID, rec.ID, models.ApprovalStatusExpired); err != nil {
			log.Printf("[chat] UpdateApprovalStatus error for %s: %v", approvalID, err)
		}
		return http.StatusConflict, nil, "approval expired"
	case "not_found":
		if _, err := h.approvalRepo.MarkDecided(r.Context(), rec.ID, decision, models.ApprovalStatusExpired); err != nil {
			log.Printf("[chat] MarkDecided error for %s: %v", approvalID, err)
		}
		return http.StatusNotFound, nil, "approval not found or expired"
	case "unauthorized":
		return http.StatusForbidden, nil, "approval decision rejected by approval service"
	default:
		log.Printf("[chat] tool.approve unexpected status for %s: %q (error=%s)", approvalID, toolResp.Status, toolResp.Error)
		return http.StatusBadGateway, nil, "approval decision failed"
	}
}

func approvalAuthorizationError(session *models.ChatSession, isAdmin bool) string {
	if session == nil || models.NormalizeAccessProfile(session.AccessProfile) != models.AccessProfileDeveloper {
		return "approvals require a developer session"
	}
	if !isAdmin {
		return "approvals require an admin user"
	}
	return ""
}

// appendDecisionMessage records the outcome in the conversation so the
// session history reflects the user's decision.
func (h *ChatHandler) appendDecisionMessage(r *http.Request, session *models.ChatSession, rec *models.ApprovalRecord, decision, status string) {
	verb := "Approved"
	if decision == models.ApprovalDecisionReject {
		verb = "Rejected"
	}
	summary := rec.Summary
	if summary == "" {
		summary = rec.Preview
	}
	content := fmt.Sprintf("%s: %s %s — %s", verb, rec.Tool, summary, status)
	msg := &models.ChatMessage{
		SessionID: session.ID,
		Role:      "assistant",
		Content:   content,
		Name:      rec.Tool,
	}
	if err := h.messageRepo.Create(r.Context(), msg); err != nil {
		log.Printf("[chat] Save decision message error: %v", err)
	}
	go h.natsClient.PublishMessageSent(session.ID, session.CompanyID, session.UserID, "assistant", content)
}
