package handlers

import (
	"context"
	"crypto/rsa"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	"github.com/google/uuid"
)

// contextKey type to avoid collisions
type contextKey string

const (
	ContextUserID      contextKey = "userId"
	ContextCompanyID   contextKey = "companyId"
	ContextIsAdmin     contextKey = "isAdmin"
	ContextUserEmail   contextKey = "userEmail"
	ContextUserName    contextKey = "userName"
	ContextAuthExpires contextKey = "authExpiresAt"
	ContextAuthIssued  contextKey = "authIssuedAt"
	ContextAuthSource  contextKey = "authSource"
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

type AuthOptions struct {
	Issuer             string
	Audience           string
	ServiceMaxLifetime time.Duration
}

func getJwksURL(issuer string) string {
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

func fetchJWKS(issuer string) ([]jwkKey, error) {
	discoveryURL := getJwksURL(issuer)
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
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("discovery returned status %d", resp.StatusCode)
	}

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
	if jwksResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JWKS returned status %d", jwksResp.StatusCode)
	}

	var jwks jwksResponse
	if err := json.NewDecoder(jwksResp.Body).Decode(&jwks); err != nil {
		return nil, fmt.Errorf("decode JWKS: %w", err)
	}

	return jwks.Keys, nil
}

func getKeys(issuer string) ([]jwkKey, error) {
	jwksMu.Lock()
	defer jwksMu.Unlock()

	if time.Now().Before(jwksExpiry) && len(jwksKeys) > 0 {
		return jwksKeys, nil
	}

	keys, err := fetchJWKS(issuer)
	if err != nil {
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

// AuthMiddleware preserves the existing entry point for tests/embedders while
// main uses AuthMiddlewareWithOptions with validated configuration.
func AuthMiddleware(next http.Handler) http.Handler {
	return AuthMiddlewareWithOptions(AuthOptions{
		Issuer: os.Getenv("AUTHENTIK_ISSUER"), Audience: os.Getenv("AUTHENTIK_AUDIENCE"),
		ServiceMaxLifetime: 5 * time.Minute,
	})(next)
}

// AuthMiddlewareWithOptions validates JWT signature, RS256 method, issuer,
// audience, expiry and required identity claims.
func AuthMiddlewareWithOptions(options AuthOptions) func(http.Handler) http.Handler {
	if options.ServiceMaxLifetime <= 0 {
		options.ServiceMaxLifetime = 5 * time.Minute
	}
	return func(next http.Handler) http.Handler {
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
					log.Printf("[auth] service API key authentication from %s %s", r.Method, r.URL.Path)
					ctx := context.WithValue(r.Context(), ContextUserID, "service:api-key")
					if companyID := r.Header.Get("X-Company-ID"); companyID != "" {
						if !isValidCompanyID(companyID) {
							http.Error(w, `{"error":"invalid company identity"}`, http.StatusUnauthorized)
							return
						}
						ctx = context.WithValue(ctx, ContextCompanyID, companyID)
					}
					ctx = context.WithValue(ctx, ContextUserEmail, "service@internal")
					ctx = context.WithValue(ctx, ContextUserName, "Service (API key)")
					ctx = context.WithValue(ctx, ContextIsAdmin, true)
					ctx = context.WithValue(ctx, ContextAuthSource, "service")
					ctx = context.WithValue(ctx, ContextAuthIssued, time.Now().UTC())
					ctx = context.WithValue(ctx, ContextAuthExpires, time.Now().UTC().Add(options.ServiceMaxLifetime))
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}
			if options.Issuer == "" || options.Audience == "" {
				http.Error(w, `{"error":"JWT authentication is not configured"}`, http.StatusServiceUnavailable)
				return
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
			if token.Method.Alg() != jwt.SigningMethodRS256.Alg() {
				http.Error(w, `{"error":"invalid token method"}`, http.StatusUnauthorized)
				return
			}

			kid, ok := token.Header["kid"].(string)
			if !ok {
				http.Error(w, `{"error":"no kid in token"}`, http.StatusUnauthorized)
				return
			}

			// Get JWKS keys
			keys, err := getKeys(options.Issuer)
			if err != nil {
				log.Printf("[auth] failed to get JWKS keys: %v", err)
				http.Error(w, `{"error":"auth service unavailable"}`, http.StatusServiceUnavailable)
				return
			}

			// Find the matching key
			var matchingKey *jwkKey
			for _, k := range keys {
				if k.Kid == kid && k.Kty == "RSA" && k.Alg == "RS256" && (k.Use == "" || k.Use == "sig") {
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
			parsed, claims, err := validateJWT(tokenStr, pubKey, options.Issuer, options.Audience, time.Now())
			if err != nil {
				http.Error(w, `{"error":"invalid token"}`, http.StatusUnauthorized)
				return
			}
			if !parsed.Valid {
				http.Error(w, `{"error":"invalid claims"}`, http.StatusUnauthorized)
				return
			}

			// Extract claims
			sub, _ := claims["sub"].(string)
			if strings.TrimSpace(sub) == "" {
				http.Error(w, `{"error":"missing subject"}`, http.StatusUnauthorized)
				return
			}
			email, _ := claims["email"].(string)
			name, _ := claims["name"].(string)
			companyID, _ := claims["companyId"].(string)
			if companyID == "" {
				companyID, _ = claims["company_id"].(string)
			}
			if companyID != "" {
				if !isValidCompanyID(companyID) {
					http.Error(w, `{"error":"invalid company identity"}`, http.StatusUnauthorized)
					return
				}
			}
			expiresAt, err := claims.GetExpirationTime()
			if err != nil || expiresAt == nil {
				http.Error(w, `{"error":"missing token expiry"}`, http.StatusUnauthorized)
				return
			}
			issuedAt, _ := claims.GetIssuedAt()

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
			ctx = context.WithValue(ctx, ContextAuthSource, "jwt")
			ctx = context.WithValue(ctx, ContextAuthExpires, expiresAt.Time.UTC())
			if issuedAt != nil {
				ctx = context.WithValue(ctx, ContextAuthIssued, issuedAt.Time.UTC())
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func validateJWT(token string, key any, issuer, audience string, now time.Time) (*jwt.Token, jwt.MapClaims, error) {
	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodRS256 {
			return nil, fmt.Errorf("unexpected signing method %s", t.Method.Alg())
		}
		return key, nil
	}, jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuer(issuer), jwt.WithAudience(audience),
		jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithTimeFunc(func() time.Time { return now }))
	if err != nil {
		return nil, nil, err
	}
	subject, err := claims.GetSubject()
	if err != nil || strings.TrimSpace(subject) == "" {
		return nil, nil, errors.New("missing subject")
	}
	return parsed, claims, nil
}

func isValidCompanyID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == strings.ToLower(value)
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

func GetAuthExpiresAt(r *http.Request) time.Time {
	value, _ := r.Context().Value(ContextAuthExpires).(time.Time)
	return value
}

func GetAuthSource(r *http.Request) string {
	value, _ := r.Context().Value(ContextAuthSource).(string)
	return value
}

func GetAuthIssuedAt(r *http.Request) time.Time {
	value, _ := r.Context().Value(ContextAuthIssued).(time.Time)
	return value
}
