package handlers

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestValidateJWTRequiresMethodIssuerAudienceExpiryAndSubject(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	issuer, audience := "https://auth.example/application/o/chat/", "dev2-chat"
	validClaims := jwt.MapClaims{
		"iss": issuer, "aud": audience, "sub": "user-1",
		"exp": now.Add(time.Minute).Unix(),
	}
	sign := func(method jwt.SigningMethod, claims jwt.MapClaims, key any) string {
		t.Helper()
		token, signErr := jwt.NewWithClaims(method, claims).SignedString(key)
		if signErr != nil {
			t.Fatal(signErr)
		}
		return token
	}
	valid := sign(jwt.SigningMethodRS256, validClaims, privateKey)
	if _, _, err := validateJWT(valid, &privateKey.PublicKey, issuer, audience, now); err != nil {
		t.Fatalf("valid JWT rejected: %v", err)
	}

	tests := []struct {
		name     string
		claims   jwt.MapClaims
		issuer   string
		audience string
	}{
		{"wrong issuer", validClaims, "https://other.example/", audience},
		{"wrong audience", validClaims, issuer, "other"},
		{"expired", jwt.MapClaims{"iss": issuer, "aud": audience, "sub": "user-1", "exp": now.Add(-time.Second).Unix()}, issuer, audience},
		{"missing expiry", jwt.MapClaims{"iss": issuer, "aud": audience, "sub": "user-1"}, issuer, audience},
		{"missing subject", jwt.MapClaims{"iss": issuer, "aud": audience, "exp": now.Add(time.Minute).Unix()}, issuer, audience},
		{"future issued at", jwt.MapClaims{"iss": issuer, "aud": audience, "sub": "user-1", "iat": now.Add(time.Minute).Unix(), "exp": now.Add(2 * time.Minute).Unix()}, issuer, audience},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			token := sign(jwt.SigningMethodRS256, test.claims, privateKey)
			if _, _, err := validateJWT(token, &privateKey.PublicKey, test.issuer, test.audience, now); err == nil {
				t.Fatal("invalid JWT accepted")
			}
		})
	}

	hsToken := sign(jwt.SigningMethodHS256, validClaims, []byte("not-an-rsa-key"))
	if _, _, err := validateJWT(hsToken, []byte("not-an-rsa-key"), issuer, audience, now); err == nil {
		t.Fatal("non-RS256 JWT accepted")
	}
}

func TestCompanyIdentityMustBeUUID(t *testing.T) {
	if !isValidCompanyID("550e8400-e29b-41d4-a716-446655440000") {
		t.Fatal("valid company UUID rejected")
	}
	if isValidCompanyID("company-1") {
		t.Fatal("invalid company ID accepted")
	}
}

func TestServiceAuthenticationGetsShortExpiryWithoutJWTConfig(t *testing.T) {
	t.Setenv("SERVICE_API_KEY", "test-service-key")
	var expiry time.Time
	handler := AuthMiddlewareWithOptions(AuthOptions{ServiceMaxLifetime: time.Minute})(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		expiry = GetAuthExpiresAt(r)
	}))
	request := httptest.NewRequest(http.MethodGet, "/chat/sessions", nil)
	request.Header.Set("X-API-Key", "test-service-key")
	request.Header.Set("X-Company-ID", "550e8400-e29b-41d4-a716-446655440000")
	response := httptest.NewRecorder()
	before := time.Now()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("service authentication failed: %d", response.Code)
	}
	if expiry.Before(before.Add(59*time.Second)) || expiry.After(before.Add(61*time.Second)) {
		t.Fatalf("unexpected service auth expiry: %v", expiry)
	}
}

func TestJWTPathFailsSecureWhenIssuerOrAudienceMissing(t *testing.T) {
	t.Setenv("SERVICE_API_KEY", "")
	handler := AuthMiddlewareWithOptions(AuthOptions{})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("unconfigured JWT request reached handler")
	}))
	request := httptest.NewRequest(http.MethodGet, "/chat/sessions", nil)
	request.Header.Set("Authorization", "Bearer ignored")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected fail-secure 503, got %d", response.Code)
	}
}
