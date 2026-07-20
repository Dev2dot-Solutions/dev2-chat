package handlers

import (
	"encoding/json"
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
	sessionRepo           *repository.SessionRepo
	messageRepo           *repository.MessageRepo
	approvalRepo          *repository.ApprovalRepo
	natsClient            *nats.Client
	legacyActiveTransport bool
	agentHandler          *AgentHandler
}

func (h *ChatHandler) SetLegacyActiveTransportEnabled(enabled bool) {
	h.legacyActiveTransport = enabled
}

func (h *ChatHandler) SetAgentHandler(agent *AgentHandler) {
	h.agentHandler = agent
}

func NewChatHandler(sr *repository.SessionRepo, mr *repository.MessageRepo, ar *repository.ApprovalRepo, nc *nats.Client) *ChatHandler {
	return &ChatHandler{sessionRepo: sr, messageRepo: mr, approvalRepo: ar, natsClient: nc}
}

func (h *ChatHandler) Routes(r chi.Router) {
	r.Route("/chat", func(r chi.Router) {
		r.Post("/", h.SendMessage)                          // POST /chat — send message, get response
		r.Get("/sessions", h.ListSessions)                  // GET /chat/sessions — list sessions
		r.Get("/sessions/{id}", h.GetSession)               // GET /chat/sessions/{id} — get session with messages
		r.Post("/approvals/{approvalId}", h.DecideApproval) // POST /chat/approvals/{approvalId} — approve/reject a pending tool call
	})
}

// SendMessage handles POST /chat (alias kept for compatibility, logic is in AgentHandler.Ask)
func (h *ChatHandler) SendMessage(w http.ResponseWriter, r *http.Request) {
	if !h.legacyActiveTransport {
		respondError(w, http.StatusGone, "legacy active chat transport is disabled; use WebSocket")
		return
	}
	// Chat messages are now handled by the /agent/ask endpoint
	// This is a lightweight wrapper that creates a session if needed
	respondJSON(w, http.StatusOK, map[string]string{
		"message": "Use POST /agent/ask for Q&A. This endpoint creates sessions.",
	})
}

// ListSessions handles GET /chat/sessions
// Query params: companyId (required), userId, accessProfile (client|developer),
// limit, offset. Non-admin callers are restricted to their own sessions and
// can never see developer-profile sessions.
func (h *ChatHandler) ListSessions(w http.ResponseWriter, r *http.Request) {
	companyID := r.URL.Query().Get("companyId")
	if !isValidUUID(companyID) {
		respondError(w, http.StatusBadRequest, "missing or invalid companyId")
		return
	}
	userID := r.URL.Query().Get("userId")
	accessProfile := r.URL.Query().Get("accessProfile")
	if accessProfile != "" && !models.IsValidAccessProfile(accessProfile) {
		respondError(w, http.StatusBadRequest, "accessProfile must be \"client\" or \"developer\"")
		return
	}
	excludeDeveloper := false
	if !GetIsAdmin(r) {
		if accessProfile == models.AccessProfileDeveloper {
			respondError(w, http.StatusForbidden, "developer sessions require an admin user")
			return
		}
		// Non-admins only ever list their own sessions, and developer
		// sessions are hidden unless explicitly filtered (admin-only above).
		userID = GetUserID(r)
		if userID == "" {
			respondError(w, http.StatusForbidden, "user identity unavailable")
			return
		}
		if accessProfile == "" {
			excludeDeveloper = true
		}
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	result, err := h.sessionRepo.ListByCompany(r.Context(), companyID, userID, accessProfile, excludeDeveloper, limit, offset)
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
		// Non-admins: developer sessions are admin-only, and client sessions
		// are readable only by their owner — which also blocks reading
		// another company's sessions.
		if models.NormalizeAccessProfile(session.AccessProfile) == models.AccessProfileDeveloper {
			respondError(w, http.StatusForbidden, "developer sessions require an admin user")
			return
		}
		if uid := GetUserID(r); uid == "" || session.UserID == "" || session.UserID != uid {
			respondError(w, http.StatusForbidden, "session belongs to a different user")
			return
		}
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

func stripActionableApprovals(messages []models.ChatMessage) {
	for i := range messages {
		messages[i].PendingApprovals = nil
	}
}

// DecideApproval handles POST /chat/approvals/{approvalId} (DEV2-108).
//
// The approval is resolved to its session via the chat_approvals mapping
// recorded when the pending card was surfaced. Authorization mirrors
// GetSession: non-admins may only decide approvals on sessions they own, and
// developer-profile sessions additionally require an admin. The sessionId
// and userId forwarded on tool.approve come from the resolved session —
// never from the request body.
func (h *ChatHandler) DecideApproval(w http.ResponseWriter, r *http.Request) {
	if !h.legacyActiveTransport {
		respondError(w, http.StatusGone, "legacy approval transport is disabled; use WebSocket")
		return
	}
	approvalID := chi.URLParam(r, "approvalId")
	if approvalID == "" {
		respondError(w, http.StatusBadRequest, "missing approval id")
		return
	}

	var req struct {
		Decision string `json:"decision"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Decision != models.ApprovalDecisionApprove && req.Decision != models.ApprovalDecisionReject {
		respondError(w, http.StatusBadRequest, "decision must be \"approve\" or \"reject\"")
		return
	}
	status, payload, errMsg := h.decideApproval(r, approvalID, req.Decision)
	if errMsg != "" {
		respondError(w, status, errMsg)
		return
	}
	respondJSON(w, status, payload)
}

// decideApproval contains the authorization and business transaction shared
// by REST and WebSocket approval decisions.
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
	if h.legacyActiveTransport && h.agentHandler != nil {
		if status, errMsg := h.agentHandler.validateCurrentSessionAuthorization(r, session, h.agentHandler.legacyDeveloperMaxAge); errMsg != "" {
			return status, nil, errMsg
		}
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
