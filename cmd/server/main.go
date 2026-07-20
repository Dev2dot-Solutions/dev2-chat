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

	// Startup migration: snake_case → lowerCamelCase document keys (idempotent)
	repository.MigrateSnakeToCamel(ctx, mongoDB,
		[]string{
			"chat_sessions", "chat_messages", "companies",
			"conventions", "business_rules", "architecture_decisions", "domain_terms", "processes",
		},
		map[string]string{
			"company_id":      "companyId",
			"user_id":         "userId",
			"session_id":      "sessionId",
			"conversation_id": "conversationId",
			"access_profile":  "accessProfile",
			"project_id":      "projectId",
			"token_count":     "tokenCount",
			"message_count":   "messageCount",
			"tool_calls":      "toolCalls",
			"tool_call_id":    "toolCallId",
			"project_key":     "projectKey",
			"api_key":         "apiKey",
			"base_url":        "baseUrl",
			"ticket_id":       "ticketId",
			"created_by":      "createdBy",
			"assigned_to":     "assignedTo",
			"created_at":      "createdAt",
			"updated_at":      "updatedAt",
		})

	// Repositories
	sessionRepo := repository.NewSessionRepo(mongoDB)
	messageRepo := repository.NewMessageRepo(mongoDB)
	approvalRepo := repository.NewApprovalRepo(mongoDB)
	socketRepo := repository.NewSocketRepo(mongoDB)
	if err := socketRepo.EnsureIndexes(ctx); err != nil {
		log.Fatalf("Failed to create socket indexes: %v", err)
	}
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
		natsClient,
	)

	// Handlers
	chatHandler := handlers.NewChatHandler(sessionRepo, messageRepo, approvalRepo, natsClient)
	agentHandler := handlers.NewAgentHandler(
		sessionRepo, messageRepo, approvalRepo, knowledgeRepo,
		llmClient, natsClient, toolExecutor,
		cfg.LLMModel, cfg.LLMProvider,
	)
	settingsHandler := handlers.NewSettingsHandler(settingsRepo)
	socketHandler := handlers.NewSocketHandler(socketRepo, agentHandler, chatHandler, handlers.SocketOptions{
		AllowedOrigins: cfg.SocketAllowedOrigins, SendQueue: cfg.SocketSendQueue, ReadLimit: cfg.SocketReadLimit,
		PingInterval: cfg.SocketPingInterval, IdleTimeout: cfg.SocketIdleTimeout,
		MaxLifetime: cfg.SocketMaxLifetime, DeveloperMaxLifetime: cfg.SocketDeveloperMaxLifetime,
		ServiceMaxLifetime: cfg.SocketServiceMaxLifetime,
		TicketPolicy:       repository.TicketPolicy{IssuePerMinute: cfg.SocketTicketRate, MaxOutstanding: cfg.SocketOutstandingTickets},
		ConnectionPolicy: repository.ConnectionPolicy{
			GlobalLimit: cfg.SocketConnectionsGlobal, CompanyLimit: cfg.SocketConnectionsCompany,
			UserLimit: cfg.SocketConnectionsUser, IPLimit: cfg.SocketConnectionsIP, LeaseTTL: cfg.SocketConnectionLeaseTTL,
		},
		GenerationPolicy: repository.GenerationPolicy{
			CompanyLimit: cfg.SocketGenerationsCompany, UserLimit: cfg.SocketGenerationsUser, LeaseTTL: cfg.SocketGenerationLeaseTTL,
		},
		MessagesPerMinute: cfg.SocketMessagesPerMinute, MessageBurst: cfg.SocketMessageBurst,
	})

	// Router
	r := chi.NewRouter()
	r.Use(chimw.RequestID)
	r.Use(chimw.RealIP)
	r.Use(chimw.Logger)
	r.Use(chimw.Recoverer)
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/chat/ws" {
				next.ServeHTTP(w, r)
				return
			}
			chimw.Timeout(30*time.Second)(next).ServeHTTP(w, r)
		})
	})
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   cfg.AllowedOrigins,
		AllowedMethods:   []string{"GET", "POST", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))
	r.Use(handlers.AuthMiddlewareWithOptions(handlers.AuthOptions{
		Issuer: cfg.AuthentikIssuer, Audience: cfg.AuthentikAudience,
		ServiceMaxLifetime: cfg.SocketServiceMaxLifetime,
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
	socketHandler.Routes(r)

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
