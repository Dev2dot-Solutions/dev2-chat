package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all configuration for dev2-chat.
type Config struct {
	Port          int
	MongoURI      string
	MongoDatabase string
	NATSURL       string
	LLMApiKey     string
	LLMBaseURL    string
	LLMProvider   string
	LLMModel      string
	KnowledgeSvcURL string
	TicketsSvcURL   string
	LLMServiceURL   string
	PTSvcURL        string
}

func Load() (*Config, error) {
	port, err := getEnvInt("PORT", 8080)
	if err != nil {
		return nil, fmt.Errorf("PORT: %w", err)
	}
	return &Config{
		Port:            port,
		MongoURI:        getEnv("MONGO_URI", "mongodb://root:dev2@mongodb:27017/dev2knowledge?authSource=admin"),
		MongoDatabase:   getEnv("MONGO_DATABASE", "dev2knowledge"),
		NATSURL:         getEnv("NATS_URL", "nats://localhost:4223"),
		LLMApiKey:       getEnv("LLM_API_KEY", ""),
		LLMBaseURL:      getEnv("LLM_BASE_URL", "https://api.openai.com/v1"),
		LLMProvider:     getEnv("LLM_PROVIDER", "openai"),
		LLMModel:        getEnv("LLM_MODEL", "gpt-4o"),
		KnowledgeSvcURL: getEnv("KNOWLEDGE_SVC_URL", "http://dev2-knowledge:8080"),
		TicketsSvcURL:   getEnv("TICKETS_SVC_URL", "http://dev2-tickets:8080"),
		LLMServiceURL:   getEnv("LLM_SERVICE_URL", ""),
		PTSvcURL:        getEnv("PT_SVC_URL", "https://app.project-tracker.ai/api"),
	}, nil
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
