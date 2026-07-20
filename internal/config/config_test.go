package config

import (
	"slices"
	"testing"
)

func TestProductionOriginsExcludeLocalhostAndRESTIsSeparate(t *testing.T) {
	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("CHAT_ALLOWED_ORIGINS", "https://rest.example")
	t.Setenv("CHAT_SOCKET_ALLOWED_ORIGINS", "https://socket.example")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.AllowedOrigins, []string{"https://rest.example"}) ||
		!slices.Equal(cfg.SocketAllowedOrigins, []string{"https://socket.example"}) {
		t.Fatalf("origins were not separated: REST=%v socket=%v", cfg.AllowedOrigins, cfg.SocketAllowedOrigins)
	}
}

func TestLocalhostDefaultRequiresDevelopmentEnvironment(t *testing.T) {
	for _, environment := range []string{"production", "development"} {
		t.Run(environment, func(t *testing.T) {
			t.Setenv("ENVIRONMENT", environment)
			t.Setenv("CHAT_ALLOWED_ORIGINS", "")
			t.Setenv("CHAT_SOCKET_ALLOWED_ORIGINS", "")
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			hasLocalhost := slices.Contains(cfg.SocketAllowedOrigins, "http://localhost:3000")
			if hasLocalhost != (environment == "development") {
				t.Fatalf("environment %s localhost=%v", environment, hasLocalhost)
			}
		})
	}
}
