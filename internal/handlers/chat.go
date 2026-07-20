package handlers

import (
	"log"
	"net/http"
	"strconv"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/repository"
	"github.com/go-chi/chi/v5"
)

type ChatHandler struct {
	sessionRepo *repository.SessionRepo
	messageRepo *repository.MessageRepo
}

func NewChatHandler(sr *repository.SessionRepo, mr *repository.MessageRepo) *ChatHandler {
	return &ChatHandler{sessionRepo: sr, messageRepo: mr}
}

func (h *ChatHandler) Routes(r chi.Router) {
	r.Route("/chat", func(r chi.Router) {
		r.Post("/", h.SendMessage)            // POST /chat — send message, get response
		r.Get("/sessions", h.ListSessions)    // GET /chat/sessions — list sessions
		r.Get("/sessions/{id}", h.GetSession) // GET /chat/sessions/{id} — get session with messages
	})
}

// SendMessage handles POST /chat (alias kept for compatibility, logic is in AgentHandler.Ask)
func (h *ChatHandler) SendMessage(w http.ResponseWriter, r *http.Request) {
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

	respondJSON(w, http.StatusOK, map[string]any{
		"session":  session,
		"messages": messages,
	})
}
