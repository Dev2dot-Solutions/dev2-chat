package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for dev2-chat.
type Config struct {
	Environment                string
	LegacyActiveTransport      bool
	Port                       int
	MongoURI                   string
	MongoDatabase              string
	NATSURL                    string
	LLMApiKey                  string
	LLMBaseURL                 string
	LLMProvider                string
	LLMModel                   string
	KnowledgeSvcURL            string
	TicketsSvcURL              string
	LLMServiceURL              string
	PTSvcURL                   string
	AuthentikIssuer            string
	AuthentikAudience          string
	AllowedOrigins             []string
	SocketAllowedOrigins       []string
	SocketTrustedProxyCIDRs    []string
	SocketRequireTrustedProxy  bool
	SocketSendQueue            int
	SocketReadLimit            int64
	SocketPingInterval         time.Duration
	SocketIdleTimeout          time.Duration
	SocketMaxLifetime          time.Duration
	SocketDeveloperMaxLifetime time.Duration
	SocketServiceMaxLifetime   time.Duration
	SocketTicketRate           int
	SocketOutstandingTickets   int
	SocketConnectionsGlobal    int
	SocketConnectionsCompany   int
	SocketConnectionsUser      int
	SocketConnectionsIP        int
	SocketConnectionLeaseTTL   time.Duration
	SocketGenerationsCompany   int
	SocketGenerationsUser      int
	SocketGenerationLeaseTTL   time.Duration
	SocketMessagesPerMinute    int
	SocketMessageBurst         int
	SocketMessagesUser         int
	SocketMessagesCompany      int
	SocketMessagesIP           int
	SocketHandshakeRate        int
	SocketHandshakeBurst       int
}

func Load() (*Config, error) {
	environment := strings.ToLower(getEnv("ENVIRONMENT", "production"))
	legacyTransport, err := getEnvBool("CHAT_LEGACY_ACTIVE_TRANSPORT_ENABLED", environment == "development")
	if err != nil {
		return nil, err
	}
	if environment == "production" && legacyTransport {
		return nil, fmt.Errorf("CHAT_LEGACY_ACTIVE_TRANSPORT_ENABLED cannot be enabled in production")
	}
	requireTrustedProxy, err := getEnvBool("CHAT_SOCKET_REQUIRE_TRUSTED_PROXY", environment == "production")
	if err != nil {
		return nil, err
	}
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
	maxLifetime, err := getEnvDuration("CHAT_SOCKET_MAX_LIFETIME", 30*time.Minute)
	if err != nil {
		return nil, err
	}
	developerLifetime, err := getEnvDuration("CHAT_SOCKET_DEVELOPER_MAX_LIFETIME", 5*time.Minute)
	if err != nil {
		return nil, err
	}
	serviceLifetime, err := getEnvDuration("CHAT_SOCKET_SERVICE_MAX_LIFETIME", 5*time.Minute)
	if err != nil {
		return nil, err
	}
	connectionLeaseTTL, err := getEnvDuration("CHAT_SOCKET_CONNECTION_LEASE_TTL", 75*time.Second)
	if err != nil {
		return nil, err
	}
	generationLeaseTTL, err := getEnvDuration("CHAT_SOCKET_GENERATION_LEASE_TTL", 3*time.Minute)
	if err != nil {
		return nil, err
	}
	intValue := func(key string, fallback int) (int, error) {
		value, parseErr := getEnvInt(key, fallback)
		if parseErr != nil || value <= 0 {
			if parseErr != nil {
				return 0, parseErr
			}
			return 0, fmt.Errorf("%s must be positive", key)
		}
		return value, nil
	}
	ticketRate, err := intValue("CHAT_SOCKET_TICKET_RATE_PER_MINUTE", 10)
	if err != nil {
		return nil, err
	}
	outstanding, err := intValue("CHAT_SOCKET_MAX_OUTSTANDING_TICKETS", 3)
	if err != nil {
		return nil, err
	}
	connGlobal, err := intValue("CHAT_SOCKET_CONNECTIONS_GLOBAL", 500)
	if err != nil {
		return nil, err
	}
	connCompany, err := intValue("CHAT_SOCKET_CONNECTIONS_PER_COMPANY", 50)
	if err != nil {
		return nil, err
	}
	connUser, err := intValue("CHAT_SOCKET_CONNECTIONS_PER_USER", 3)
	if err != nil {
		return nil, err
	}
	connIP, err := intValue("CHAT_SOCKET_CONNECTIONS_PER_IP", 20)
	if err != nil {
		return nil, err
	}
	genCompany, err := intValue("CHAT_SOCKET_GENERATIONS_PER_COMPANY", 20)
	if err != nil {
		return nil, err
	}
	genUser, err := intValue("CHAT_SOCKET_GENERATIONS_PER_USER", 2)
	if err != nil {
		return nil, err
	}
	messagesMinute, err := intValue("CHAT_SOCKET_MESSAGES_PER_MINUTE", 60)
	if err != nil {
		return nil, err
	}
	messageBurst, err := intValue("CHAT_SOCKET_MESSAGE_BURST", 20)
	if err != nil {
		return nil, err
	}
	messageUser, err := intValue("CHAT_SOCKET_MESSAGES_PER_MINUTE_PER_USER", 120)
	if err != nil {
		return nil, err
	}
	messageCompany, err := intValue("CHAT_SOCKET_MESSAGES_PER_MINUTE_PER_COMPANY", 1200)
	if err != nil {
		return nil, err
	}
	messageIP, err := intValue("CHAT_SOCKET_MESSAGES_PER_MINUTE_PER_IP", 600)
	if err != nil {
		return nil, err
	}
	handshakeRate, err := intValue("CHAT_SOCKET_HANDSHAKE_RATE_PER_MINUTE", 30)
	if err != nil {
		return nil, err
	}
	handshakeBurst, err := intValue("CHAT_SOCKET_HANDSHAKE_BURST", 10)
	if err != nil {
		return nil, err
	}
	defaultOrigins := "https://dev2.solutions"
	if environment == "development" {
		defaultOrigins += ",http://localhost:3000"
	}
	trustedProxyCIDRs := splitCSV(getEnv("CHAT_SOCKET_TRUSTED_PROXY_CIDRS", ""))
	for _, cidr := range trustedProxyCIDRs {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return nil, fmt.Errorf("invalid CHAT_SOCKET_TRUSTED_PROXY_CIDRS entry %q", cidr)
		}
	}
	if requireTrustedProxy && len(trustedProxyCIDRs) == 0 {
		return nil, fmt.Errorf("CHAT_SOCKET_TRUSTED_PROXY_CIDRS is required when CHAT_SOCKET_REQUIRE_TRUSTED_PROXY=true")
	}
	return &Config{
		Environment:                environment,
		LegacyActiveTransport:      legacyTransport,
		Port:                       port,
		MongoURI:                   getEnv("MONGO_URI", "mongodb://root:dev2@mongodb:27017/dev2knowledge?authSource=admin"),
		MongoDatabase:              getEnv("MONGO_DATABASE", "dev2knowledge"),
		NATSURL:                    getEnv("NATS_URL", "nats://localhost:4223"),
		LLMApiKey:                  getEnv("LLM_API_KEY", ""),
		LLMBaseURL:                 getEnv("LLM_BASE_URL", "https://api.openai.com/v1"),
		LLMProvider:                getEnv("LLM_PROVIDER", "openai"),
		LLMModel:                   getEnv("LLM_MODEL", "gpt-4o"),
		KnowledgeSvcURL:            getEnv("KNOWLEDGE_SVC_URL", "http://dev2-knowledge:8080"),
		TicketsSvcURL:              getEnv("TICKETS_SVC_URL", "http://dev2-tickets:8080"),
		LLMServiceURL:              getEnv("LLM_SERVICE_URL", ""),
		PTSvcURL:                   getEnv("PT_SVC_URL", "https://app.project-tracker.ai/api"),
		AuthentikIssuer:            getEnv("AUTHENTIK_ISSUER", ""),
		AuthentikAudience:          getEnv("AUTHENTIK_AUDIENCE", ""),
		AllowedOrigins:             splitCSV(getEnv("CHAT_ALLOWED_ORIGINS", defaultOrigins)),
		SocketAllowedOrigins:       splitCSV(getEnv("CHAT_SOCKET_ALLOWED_ORIGINS", defaultOrigins)),
		SocketTrustedProxyCIDRs:    trustedProxyCIDRs,
		SocketRequireTrustedProxy:  requireTrustedProxy,
		SocketSendQueue:            sendQueue,
		SocketReadLimit:            int64(readLimit),
		SocketPingInterval:         pingInterval,
		SocketIdleTimeout:          idleTimeout,
		SocketMaxLifetime:          maxLifetime,
		SocketDeveloperMaxLifetime: developerLifetime,
		SocketServiceMaxLifetime:   serviceLifetime,
		SocketTicketRate:           ticketRate,
		SocketOutstandingTickets:   outstanding,
		SocketConnectionsGlobal:    connGlobal,
		SocketConnectionsCompany:   connCompany,
		SocketConnectionsUser:      connUser,
		SocketConnectionsIP:        connIP,
		SocketConnectionLeaseTTL:   connectionLeaseTTL,
		SocketGenerationsCompany:   genCompany,
		SocketGenerationsUser:      genUser,
		SocketGenerationLeaseTTL:   generationLeaseTTL,
		SocketMessagesPerMinute:    messagesMinute,
		SocketMessageBurst:         messageBurst,
		SocketMessagesUser:         messageUser,
		SocketMessagesCompany:      messageCompany,
		SocketMessagesIP:           messageIP,
		SocketHandshakeRate:        handshakeRate,
		SocketHandshakeBurst:       handshakeBurst,
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

func getEnvBool(key string, fallback bool) (bool, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}
