package tools

import (
	"encoding/json"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxAuditNameBytes      = 256
	maxAuditIdentityBytes  = 256
	maxAuditArgumentsBytes = 8 * 1024
	maxAuditResultBytes    = 8 * 1024
	maxAuditErrorBytes     = 2 * 1024
)

var (
	bearerSecretPattern   = regexp.MustCompile(`(?i)bearer\s+[a-z0-9._~+/=-]+`)
	privateKeyPattern     = regexp.MustCompile(`(?s)-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----`)
	assignedSecretPattern = regexp.MustCompile(`(?i)(token|secret|password|passwd|api[_-]?key|private[_-]?key|authorization)\s*[:=]\s*[^\s,;]+`)
)

func redactAuditArguments(raw string) string {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return "[unparseable arguments redacted]"
	}
	redacted, err := json.Marshal(redactAuditValue(value))
	if err != nil {
		return "[arguments redacted]"
	}
	return truncateAuditValue(string(redacted), maxAuditArgumentsBytes)
}

func redactAuditResult(raw string) string {
	var value any
	if json.Unmarshal([]byte(raw), &value) == nil {
		if redacted, err := json.Marshal(redactAuditValue(value)); err == nil {
			return string(redacted)
		}
	}
	return redactAuditText(raw)
}

func redactAuditValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if isSensitiveAuditKey(key) {
				v[key] = "***"
			} else {
				v[key] = redactAuditValue(child)
			}
		}
		return v
	case []any:
		for i := range v {
			v[i] = redactAuditValue(v[i])
		}
		return v
	case string:
		return redactAuditText(v)
	default:
		return value
	}
}

func isSensitiveAuditKey(key string) bool {
	var normalized strings.Builder
	for _, r := range strings.ToLower(key) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			normalized.WriteRune(r)
		}
	}
	k := normalized.String()
	return strings.Contains(k, "token") || strings.Contains(k, "secret") ||
		strings.Contains(k, "password") || strings.Contains(k, "passwd") ||
		strings.Contains(k, "privatekey") || strings.Contains(k, "apikey") ||
		strings.Contains(k, "credential") || k == "pat" || k == "authorization"
}

func redactAuditText(value string) string {
	value = privateKeyPattern.ReplaceAllString(value, "***")
	value = bearerSecretPattern.ReplaceAllString(value, "Bearer ***")
	return assignedSecretPattern.ReplaceAllStringFunc(value, func(match string) string {
		if i := strings.IndexAny(match, ":="); i >= 0 {
			return match[:i+1] + "***"
		}
		return "***"
	})
}

func truncateAuditValue(value string, maxBytes int) string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	const suffix = "...[truncated]"
	limit := maxBytes - len(suffix)
	if limit <= 0 {
		return suffix[:maxBytes]
	}
	for limit > 0 && !utf8.ValidString(value[:limit]) {
		limit--
	}
	return value[:limit] + suffix
}

func auditResultError(result string) string {
	var payload map[string]any
	if err := json.Unmarshal([]byte(result), &payload); err != nil {
		return ""
	}
	if msg, ok := payload["error"].(string); ok {
		return msg
	}
	return ""
}

func attributionProjectID(projectID string) string {
	if projectID == "" {
		return "none"
	}
	return projectID
}
