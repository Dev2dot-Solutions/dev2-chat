package models

import "time"

// TicketAttribution is appended to ticket creation requests. Older ticket
// service versions safely ignore these additive fields.
type TicketAttribution struct {
	Origin          string `json:"origin,omitempty"`
	SourceUserID    string `json:"sourceUserId,omitempty"`
	SourceSessionID string `json:"sourceSessionId,omitempty"`
	SourceProjectID string `json:"sourceProjectId,omitempty"`
}

// ToolAuditEvent is published for every tool executed locally by dev2-chat.
// The field names align with dev2-llm-service's audit.tool.invocation events;
// EventID and Result are additive attribution/detail fields.
type ToolAuditEvent struct {
	EventID       string    `json:"eventId"`
	Timestamp     time.Time `json:"timestamp"`
	CompanyID     string    `json:"companyId"`
	ProjectID     string    `json:"projectId"`
	UserID        string    `json:"userId,omitempty"`
	SessionID     string    `json:"sessionId,omitempty"`
	AccessProfile string    `json:"accessProfile,omitempty"`
	PersonaName   string    `json:"personaName,omitempty"`
	ToolName      string    `json:"toolName"`
	Arguments     string    `json:"arguments"`
	Result        string    `json:"result"`
	Error         string    `json:"error,omitempty"`
	LatencyMS     string    `json:"latencyMs"`
	Success       bool      `json:"success"`
}
