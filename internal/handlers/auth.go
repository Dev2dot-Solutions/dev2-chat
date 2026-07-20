package handlers

import (
	"context"
	"crypto/rsa"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/golang-jwt/jwt/v5"
)

// contextKey type to avoid collisions
type contextKey string

const (
	ContextUserID       contextKey = "userId"
	ContextCompanyID    contextKey = "companyId"
	ContextIsAdmin      contextKey = "isAdmin"
	ContextUserEmail    contextKey = "userEmail"
	ContextUserName     contextKey = "userName"
	contextSocketTicket contextKey = "socketTicket"
)

// jwksCache holds the fetched JWKS keys with a TTL
var (
	jwksKeys   []jwkKey
	jwksExpiry time.Time
	jwksMu     sync.Mutex
)

type jwkKey struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
	Use string `json:"use"`
}

type jwksResponse struct {
	Keys []jwkKey `json:"keys"`
}

func getJwksURL() string {
	issuer := os.Getenv("AUTHENTIK_ISSUER")
	if issuer == "" {
		return ""
	}
	// Use the OIDC discovery document to find the JWKS URI
	// Fallback: construct from issuer
	if strings.HasSuffix(issuer, "/") {
		return issuer + ".well-known/openid-configuration"
	}
	return issuer + "/.well-known/openid-configuration"
}

func fetchJWKS() ([]jwkKey, error) {
	discoveryURL := getJwksURL()
	if discoveryURL == "" {
		return nil, fmt.Errorf("AUTHENTIK_ISSUER not configured")
	}

	client := &http.Client{Timeout: 10 * time.Second}

	// Fetch OIDC discovery document
	resp, err := client.Get(discoveryURL)
	if err != nil {
		return nil, fmt.Errorf("fetch discovery: %w", err)
	}
	defer resp.Body.Close()

	var discovery struct {
		JWKSUri string `json:"jwks_uri"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&discovery); err != nil {
		return nil, fmt.Errorf("decode discovery: %w", err)
	}

	if discovery.JWKSUri == "" {
		return nil, fmt.Errorf("no jwks_uri in discovery document")
	}

	// Fetch JWKS
	jwksResp, err := client.Get(discovery.JWKSUri)
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS: %w", err)
	}
	defer jwksResp.Body.Close()

	var jwks jwksResponse
	if err := json.NewDecoder(jwksResp.Body).Decode(&jwks); err != nil {
		return nil, fmt.Errorf("decode JWKS: %w", err)
	}

	return jwks.Keys, nil
}

func getKeys() ([]jwkKey, error) {
	jwksMu.Lock()
	defer jwksMu.Unlock()

	if time.Now().Before(jwksExpiry) && len(jwksKeys) > 0 {
		return jwksKeys, nil
	}

	keys, err := fetchJWKS()
	if err != nil {
		if len(jwksKeys) > 0 {
			return jwksKeys, nil
		}
		return nil, err
	}

	jwksKeys = keys
	jwksExpiry = time.Now().Add(5 * time.Minute)
	return jwksKeys, nil
}

func rsaPublicKeyFromJWK(key jwkKey) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(key.N)
	if err != nil {
		return nil, fmt.Errorf("decode n: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
	if err != nil {
		return nil, fmt.Errorf("decode e: %w", err)
	}

	n := new(big.Int).SetBytes(nBytes)
	e := 0
	for _, b := range eBytes {
		e = e*256 + int(b)
	}

	return &rsa.PublicKey{N: n, E: e}, nil
}

// AuthMiddleware validates the Authorization Bearer token against Authentik JWKS.
func AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A WebSocket upgrade authenticates with the one-time ticket consumed by
		// its handler. Ticket issuance remains behind JWT middleware.
		if r.URL.Path == "/health" || r.URL.Path == "/chat/ws" {
			next.ServeHTTP(w, r)
			return
		}

		// API key path for service-to-service and tooling access.
		// Checked before JWT auth; only active when SERVICE_API_KEY is configured.
		if apiKey := os.Getenv("SERVICE_API_KEY"); apiKey != "" {
			if provided := r.Header.Get("X-API-Key"); provided != "" &&
				subtle.ConstantTimeCompare([]byte(provided), []byte(apiKey)) == 1 {
				log.Printf("[auth] API key authentication (prefix %.4s...) from %s %s", provided, r.Method, r.URL.Path)
				ctx := context.WithValue(r.Context(), ContextUserID, "service:api-key")
				ctx = context.WithValue(ctx, ContextUserEmail, "service@internal")
				ctx = context.WithValue(ctx, ContextUserName, "Service (API key)")
				ctx = context.WithValue(ctx, ContextIsAdmin, true)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, `{"error":"missing authorization header"}`, http.StatusUnauthorized)
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		// Parse token without validation first to get the kid
		token, _, err := new(jwt.Parser).ParseUnverified(tokenStr, jwt.MapClaims{})
		if err != nil {
			http.Error(w, `{"error":"invalid token"}`, http.StatusUnauthorized)
			return
		}

		kid, ok := token.Header["kid"].(string)
		if !ok {
			http.Error(w, `{"error":"no kid in token"}`, http.StatusUnauthorized)
			return
		}

		// Get JWKS keys
		keys, err := getKeys()
		if err != nil {
			log.Printf("[auth] failed to get JWKS keys: %v", err)
			http.Error(w, `{"error":"auth service unavailable"}`, http.StatusInternalServerError)
			return
		}

		// Find the matching key
		var matchingKey *jwkKey
		for _, k := range keys {
			if k.Kid == kid {
				matchingKey = &k
				break
			}
		}

		if matchingKey == nil {
			http.Error(w, `{"error":"unknown key"}`, http.StatusUnauthorized)
			return
		}

		// Build RSA public key from JWK
		pubKey, err := rsaPublicKeyFromJWK(*matchingKey)
		if err != nil {
			log.Printf("[auth] failed to build RSA key: %v", err)
			http.Error(w, `{"error":"auth error"}`, http.StatusInternalServerError)
			return
		}

		// Verify the token
		parsed, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return pubKey, nil
		})
		if err != nil {
			http.Error(w, `{"error":"invalid token signature"}`, http.StatusUnauthorized)
			return
		}

		claims, ok := parsed.Claims.(jwt.MapClaims)
		if !ok || !parsed.Valid {
			http.Error(w, `{"error":"invalid claims"}`, http.StatusUnauthorized)
			return
		}

		// Extract claims
		sub, _ := claims["sub"].(string)
		email, _ := claims["email"].(string)
		name, _ := claims["name"].(string)
		companyID, _ := claims["companyId"].(string)
		if companyID == "" {
			companyID, _ = claims["company_id"].(string)
		}

		// Check admin group
		isAdmin := false
		if groups, ok := claims["groups"].([]interface{}); ok {
			for _, g := range groups {
				if gs, ok := g.(string); ok && gs == "dev2-admins" {
					isAdmin = true
					break
				}
			}
		}

		// Set claims on context
		ctx := context.WithValue(r.Context(), ContextUserID, sub)
		if companyID != "" {
			ctx = context.WithValue(ctx, ContextCompanyID, companyID)
		}
		ctx = context.WithValue(ctx, ContextUserEmail, email)
		ctx = context.WithValue(ctx, ContextUserName, name)
		ctx = context.WithValue(ctx, ContextIsAdmin, isAdmin)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// SocketTicketRedactionMiddleware removes the raw socket ticket from the URL
// before request logging while retaining it only in request context.
func SocketTicketRedactionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/chat/ws" {
			ticket := r.URL.Query().Get("ticket")
			clone := r.Clone(context.WithValue(r.Context(), contextSocketTicket, ticket))
			urlCopy := *r.URL
			urlCopy.RawQuery = ""
			clone.URL = &urlCopy
			clone.RequestURI = urlCopy.Path
			r = clone
		}
		next.ServeHTTP(w, r)
	})
}

func socketTicketFromRequest(r *http.Request) string {
	ticket, _ := r.Context().Value(contextSocketTicket).(string)
	return ticket
}

func requestWithSocketIdentity(ctx context.Context, identity models.SocketIdentity) *http.Request {
	ctx = context.WithValue(ctx, ContextUserID, identity.UserID)
	ctx = context.WithValue(ctx, ContextCompanyID, identity.CompanyID)
	ctx = context.WithValue(ctx, ContextIsAdmin, identity.IsAdmin)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://socket.internal/", nil)
	return req
}

// GetUserID extracts the authenticated user ID from the request context.
func GetUserID(r *http.Request) string {
	if v, ok := r.Context().Value(ContextUserID).(string); ok {
		return v
	}
	return ""
}

// GetCompanyID extracts a company identity when the authentication provider
// includes one. Not all service/admin tokens are bound to a single company.
func GetCompanyID(r *http.Request) string {
	if v, ok := r.Context().Value(ContextCompanyID).(string); ok {
		return v
	}
	return ""
}

// GetIsAdmin extracts the admin status from the request context.
func GetIsAdmin(r *http.Request) bool {
	if v, ok := r.Context().Value(ContextIsAdmin).(bool); ok {
		return v
	}
	return false
}
