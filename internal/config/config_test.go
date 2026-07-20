package config

import (
	"slices"
	"testing"
)

func TestProductionOriginsExcludeLocalhostAndRESTIsSeparate(t *testing.T) {
	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("CHAT_ALLOWED_ORIGINS", "https://rest.example")
	t.Setenv("CHAT_SOCKET_ALLOWED_ORIGINS", "https://socket.example")
	t.Setenv("CHAT_SOCKET_REQUIRE_TRUSTED_PROXY", "false")
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
			t.Setenv("CHAT_SOCKET_REQUIRE_TRUSTED_PROXY", "false")
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

func TestTrustedProxyCIDRsAreValidated(t *testing.T) {
	t.Setenv("CHAT_SOCKET_REQUIRE_TRUSTED_PROXY", "false")
	t.Setenv("CHAT_SOCKET_TRUSTED_PROXY_CIDRS", "not-a-cidr")
	if _, err := Load(); err == nil {
		t.Fatal("invalid trusted proxy CIDR accepted")
	}
}

func TestProductionRequiresNarrowTrustedProxyConfiguration(t *testing.T) {
	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("CHAT_SOCKET_REQUIRE_TRUSTED_PROXY", "true")
	t.Setenv("CHAT_SOCKET_TRUSTED_PROXY_CIDRS", "")
	if _, err := Load(); err == nil {
		t.Fatal("production proxy requirement allowed an empty trusted CIDR list")
	}
	t.Setenv("CHAT_SOCKET_TRUSTED_PROXY_CIDRS", "192.0.2.10/32")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("narrow trusted proxy CIDR rejected: %v", err)
	}
	if cfg.SocketGenerationsGlobal != 100 {
		t.Fatalf("unexpected global generation default: %d", cfg.SocketGenerationsGlobal)
	}
}

func TestLegacyTransportDefaultsOffOutsideDevelopment(t *testing.T) {
	for _, test := range []struct {
		environment string
		enabled     bool
	}{{"production", false}, {"development", true}} {
		t.Run(test.environment, func(t *testing.T) {
			t.Setenv("ENVIRONMENT", test.environment)
			t.Setenv("CHAT_SOCKET_REQUIRE_TRUSTED_PROXY", "false")
			t.Setenv("CHAT_LEGACY_ACTIVE_TRANSPORT_ENABLED", "")
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.LegacyActiveTransport != test.enabled {
				t.Fatalf("legacy transport enabled=%v", cfg.LegacyActiveTransport)
			}
		})
	}
}

func TestProductionCannotEnableLegacyActiveTransport(t *testing.T) {
	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("CHAT_LEGACY_ACTIVE_TRANSPORT_ENABLED", "true")
	t.Setenv("CHAT_SOCKET_REQUIRE_TRUSTED_PROXY", "false")
	if _, err := Load(); err == nil {
		t.Fatal("production enabled the legacy active transport")
	}
}
