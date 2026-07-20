package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for dev2-chat.
type Config struct {
	Port                 int
	MongoURI             string
	MongoDatabase        string
	NATSURL              string
	LLMApiKey            string
	LLMBaseURL           string
	LLMProvider          string
	LLMModel             string
	KnowledgeSvcURL      string
	TicketsSvcURL        string
	LLMServiceURL        string
	PTSvcURL             string
	SocketAllowedOrigins []string
	SocketSendQueue      int
	SocketReadLimit      int64
	SocketPingInterval   time.Duration
	SocketIdleTimeout    time.Duration
}

func Load() (*Config, error) {
	port, err := getEnvInt("PORT", 8080)
	if err != nil {
		return nil, fmt.Errorf("PORT: %w", err)
	}
	sendQueue, err := getEnvInt("CHAT_SOCKET_SEND_QUEUE", 128)
	if err != nil {
		return nil, err
	}
	readLimit, err := getEnvInt("CHAT_SOCKET_READ_LIMIT_BYTES", 65536)
	if err != nil {
		return nil, err
	}
	pingInterval, err := getEnvDuration("CHAT_SOCKET_PING_INTERVAL", 25*time.Second)
	if err != nil {
		return nil, err
	}
	idleTimeout, err := getEnvDuration("CHAT_SOCKET_IDLE_TIMEOUT", 60*time.Second)
	if err != nil {
		return nil, err
	}
	return &Config{
		Port:                 port,
		MongoURI:             getEnv("MONGO_URI", "mongodb://root:dev2@mongodb:27017/dev2knowledge?authSource=admin"),
		MongoDatabase:        getEnv("MONGO_DATABASE", "dev2knowledge"),
		NATSURL:              getEnv("NATS_URL", "nats://localhost:4223"),
		LLMApiKey:            getEnv("LLM_API_KEY", ""),
		LLMBaseURL:           getEnv("LLM_BASE_URL", "https://api.openai.com/v1"),
		LLMProvider:          getEnv("LLM_PROVIDER", "openai"),
		LLMModel:             getEnv("LLM_MODEL", "gpt-4o"),
		KnowledgeSvcURL:      getEnv("KNOWLEDGE_SVC_URL", "http://dev2-knowledge:8080"),
		TicketsSvcURL:        getEnv("TICKETS_SVC_URL", "http://dev2-tickets:8080"),
		LLMServiceURL:        getEnv("LLM_SERVICE_URL", ""),
		PTSvcURL:             getEnv("PT_SVC_URL", "https://app.project-tracker.ai/api"),
		SocketAllowedOrigins: splitCSV(getEnv("CHAT_SOCKET_ALLOWED_ORIGINS", "https://dev2.solutions,http://localhost:3000")),
		SocketSendQueue:      sendQueue,
		SocketReadLimit:      int64(readLimit),
		SocketPingInterval:   pingInterval,
		SocketIdleTimeout:    idleTimeout,
	}, nil
}

func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid %s duration", key)
	}
	return d, nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	val := os.Getenv(key)
	if val == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return n, nil
}
