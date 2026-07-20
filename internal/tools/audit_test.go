package tools

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRedactAuditArguments(t *testing.T) {
	raw := `{"projectKey":"DEV2","apiToken":"top-secret","nested":{"password":"pw","privateKey":"-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----"},"note":"Bearer abc.def"}`
	got := redactAuditArguments(raw)

	for _, secret := range []string{"top-secret", `"pw"`, "abc.def", "BEGIN PRIVATE KEY"} {
		if strings.Contains(got, secret) {
			t.Fatalf("redacted arguments still contain %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, `"projectKey":"DEV2"`) {
		t.Fatalf("non-secret project key was unexpectedly redacted: %s", got)
	}
	if strings.Count(got, `"***"`) < 3 || !strings.Contains(got, "Bearer ***") {
		t.Fatalf("expected all secret values to be replaced: %s", got)
	}
}

func TestRedactAuditArgumentsHidesUnparseableInput(t *testing.T) {
	got := redactAuditArguments(`{"password":"leaked"`)
	if got != "[unparseable arguments redacted]" {
		t.Fatalf("unexpected malformed argument audit value: %q", got)
	}
}

func TestTruncateAuditValueCapsBytesAndPreservesUTF8(t *testing.T) {
	got := truncateAuditValue(strings.Repeat("🙂", 20), 32)
	if len(got) > 32 {
		t.Fatalf("value is %d bytes, want at most 32", len(got))
	}
	if !utf8.ValidString(got) {
		t.Fatalf("truncated value is not valid UTF-8: %q", got)
	}
	if !strings.HasSuffix(got, "...[truncated]") {
		t.Fatalf("missing truncation marker: %q", got)
	}
}

func TestAuditResultError(t *testing.T) {
	if got := auditResultError(`{"error":"denied"}`); got != "denied" {
		t.Fatalf("got %q, want denied", got)
	}
	if got := auditResultError(`{"ok":true}`); got != "" {
		t.Fatalf("got unexpected error %q", got)
	}
}
