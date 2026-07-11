package handlers

import (
	"log"
	"net/http"
	"strconv"

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
		r.Post("/", h.SendMessage)           // POST /chat — send message, get response
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
func (h *ChatHandler) ListSessions(w http.ResponseWriter, r *http.Request) {
	companyID := r.URL.Query().Get("company_id")
	if !isValidUUID(companyID) {
		respondError(w, http.StatusBadRequest, "missing or invalid company_id")
		return
	}
	userID := r.URL.Query().Get("user_id")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	result, err := h.sessionRepo.ListByCompany(r.Context(), companyID, userID, limit, offset)
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
