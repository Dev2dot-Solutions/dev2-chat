package handlers

import (
	"log"
	"net/http"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/repository"
	"github.com/go-chi/chi/v5"
)

type SettingsHandler struct {
	settingsRepo *repository.SettingsRepo
}

func NewSettingsHandler(sr *repository.SettingsRepo) *SettingsHandler {
	return &SettingsHandler{settingsRepo: sr}
}

func (h *SettingsHandler) Routes(r chi.Router) {
	r.Route("/settings", func(r chi.Router) {
		r.Get("/llm", h.GetLLMConfig)
		r.Get("/pt", h.GetPTConfig)
	})
}

func (h *SettingsHandler) GetLLMConfig(w http.ResponseWriter, r *http.Request) {
	companyID := r.URL.Query().Get("companyId")
	if !isValidUUID(companyID) {
		respondError(w, http.StatusBadRequest, "missing or invalid companyId")
		return
	}

	config, err := h.settingsRepo.GetLLMConfig(r.Context(), companyID)
	if err != nil {
		log.Printf("[settings] GetLLM error: %v", err)
		respondError(w, http.StatusInternalServerError, "failed to get LLM config")
		return
	}

	respondJSON(w, http.StatusOK, config)
}

func (h *SettingsHandler) GetPTConfig(w http.ResponseWriter, r *http.Request) {
	companyID := r.URL.Query().Get("companyId")
	if !isValidUUID(companyID) {
		respondError(w, http.StatusBadRequest, "missing or invalid companyId")
		return
	}

	config, err := h.settingsRepo.GetPTConfig(r.Context(), companyID)
	if err != nil {
		log.Printf("[settings] GetPT error: %v", err)
		respondError(w, http.StatusInternalServerError, "failed to get PT config")
		return
	}

	respondJSON(w, http.StatusOK, config)
}
