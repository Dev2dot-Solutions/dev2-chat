package models

// Access profiles for chat sessions (DEV2-106 client flow, DEV2-107 developer
// flow). The profile is stamped on the session at creation time and governs
// which local tools are advertised/executed and which accessProfile is
// forwarded to dev2-llm-service on llm.request.
const (
	// AccessProfileClient is the client portal profile: knowledge search,
	// own-company tickets, PT create/read in the bound project.
	AccessProfileClient = "client"
	// AccessProfileDeveloper is the admin portal profile: everything in
	// client plus the full PT workflow and (via dev2-llm-service) persona
	// orchestration.
	AccessProfileDeveloper = "developer"
)

// IsValidAccessProfile reports whether p is an explicitly supported profile.
func IsValidAccessProfile(p string) bool {
	return p == AccessProfileClient || p == AccessProfileDeveloper
}

// NormalizeAccessProfile maps empty (legacy sessions) or unknown values to the
// most restrictive chat profile (client) — fail closed.
func NormalizeAccessProfile(p string) string {
	if p == AccessProfileDeveloper {
		return p
	}
	return AccessProfileClient
}
