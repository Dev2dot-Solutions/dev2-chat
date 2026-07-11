package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/nats-io/nats.go"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/config"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/database"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/handlers"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/llm"
	chatNats "github.com/Dev2dot-Solutions/dev2-chat/internal/nats"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/pt"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/repository"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/tickets"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/tools"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// MongoDB
	mongoClient, err := database.NewMongoClient(ctx, cfg.MongoURI)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(ctx)
	mongoDB := mongoClient.Database(cfg.MongoDatabase)
	log.Printf("Connected to MongoDB: %s/%s", cfg.MongoURI, cfg.MongoDatabase)

	// Repositories
	sessionRepo := repository.NewSessionRepo(mongoDB)
	messageRepo := repository.NewMessageRepo(mongoDB)
	knowledgeRepo := repository.NewKnowledgeRepo(mongoDB)
	settingsRepo := repository.NewSettingsRepo(mongoDB)

	// NATS (optional)
	var nc *nats.Conn
	natsClient := chatNats.NewClient(nil)
	if ncConn, err := nats.Connect(cfg.NATSURL); err != nil {
		log.Printf("NATS not available at %s — continuing without: %v", cfg.NATSURL, err)
	} else {
		nc = ncConn
		natsClient = chatNats.NewClient(nc)
		log.Printf("Connected to NATS: %s", cfg.NATSURL)
	}
	if nc != nil {
		defer natsClient.Close()
	}

	// LLM client (direct HTTP fallback)
	llmClient := llm.NewClient(cfg.LLMApiKey, cfg.LLMBaseURL)

	// External service clients
	ticketsClient := tickets.NewClient(cfg.TicketsSvcURL)
	ptClient := pt.NewClient(cfg.PTSvcURL)

	// Tool executor
	toolExecutor := tools.NewExecutor(
		knowledgeRepo,
		ticketsClient,
		ptClient,
		settingsRepo.GetPTConfig,
	)

	// Handlers
	chatHandler := handlers.NewChatHandler(sessionRepo, messageRepo)
	agentHandler := handlers.NewAgentHandler(
		sessionRepo, messageRepo, knowledgeRepo,
		llmClient, natsClient, toolExecutor,
		cfg.LLMModel, cfg.LLMProvider,
	)
	settingsHandler := handlers.NewSettingsHandler(settingsRepo)

	// Router
	r := chi.NewRouter()
	r.Use(chimw.RequestID)
	r.Use(chimw.RealIP)
	r.Use(chimw.Logger)
	r.Use(chimw.Recoverer)
	r.Use(chimw.Timeout(30 * time.Second))
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// Health
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","service":"dev2-chat"}`))
	})

	// Register routes
	chatHandler.Routes(r)
	agentHandler.Routes(r)
	settingsHandler.Routes(r)

	// Server
	addr := fmt.Sprintf(":%d", cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 120 * time.Second, // Long timeout for LLM inference
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		srv.Shutdown(shutdownCtx)
		cancel()
	}()

	log.Printf("dev2-chat starting on %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
	log.Println("Server stopped")
}
