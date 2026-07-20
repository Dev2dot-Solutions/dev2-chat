package models

import "time"

// ChatSession represents a conversation session.
type ChatSession struct {
	ID        string `bson:"_id" json:"id"`
	CompanyID string `bson:"companyId" json:"companyId"`
	UserID    string `bson:"userId" json:"userId"`
	Title     string `bson:"title" json:"title"`
	Model     string `bson:"model" json:"model"`
	Provider  string `bson:"provider" json:"provider"`
	Status    string `bson:"status" json:"status"`
	// AccessProfile is "client" or "developer" (empty on legacy sessions —
	// treated as client). See models/profile.go.
	AccessProfile string `bson:"accessProfile,omitempty" json:"accessProfile,omitempty"`
	// ProjectID optionally binds the session to a Dev2Project (dev2-company-config).
	ProjectID  string    `bson:"projectId,omitempty" json:"projectId,omitempty"`
	TokenCount int       `bson:"tokenCount" json:"tokenCount"`
	CreatedAt  time.Time `bson:"createdAt" json:"createdAt"`
	UpdatedAt  time.Time `bson:"updatedAt" json:"updatedAt"`
}

// ChatMessage represents a single message within a session.
type ChatMessage struct {
	ID         string           `bson:"_id" json:"id"`
	SessionID  string           `bson:"sessionId" json:"sessionId"`
	Role       string           `bson:"role" json:"role"` // "user", "assistant", "system", "tool"
	Content    string           `bson:"content" json:"content"`
	ToolCalls  []ToolCallResult `bson:"toolCalls,omitempty" json:"toolCalls,omitempty"`
	ToolCallID string           `bson:"toolCallId,omitempty" json:"toolCallId,omitempty"`
	Name       string           `bson:"name,omitempty" json:"name,omitempty"`
	// PendingApprovals carries the approval cards surfaced with this message
	// (DEV2-108) so session reloads re-render them.
	PendingApprovals []PendingApproval `bson:"pendingApprovals,omitempty" json:"pendingApprovals,omitempty"`
	ToolTrace        []ToolTraceEvent  `bson:"toolTrace,omitempty" json:"toolTrace,omitempty"`
	TokenCount       int               `bson:"tokenCount,omitempty" json:"tokenCount,omitempty"`
	CreatedAt        time.Time         `bson:"createdAt" json:"createdAt"`
}

// PendingApproval describes a write/execute tool call awaiting a user
// decision, surfaced from dev2-llm-service tool results whose payload has
// status "pending_approval". Status starts as "pending" and is updated to
// the decision outcome ("executed", "rejected", "expired") once decided via
// POST /chat/approvals/{approvalId}.
type PendingApproval struct {
	ApprovalID string    `bson:"approvalId" json:"approvalId"`
	Tool       string    `bson:"tool" json:"tool"`
	Summary    string    `bson:"summary,omitempty" json:"summary,omitempty"`
	Preview    string    `bson:"preview,omitempty" json:"preview,omitempty"`
	ExpiresAt  time.Time `bson:"expiresAt,omitempty" json:"expiresAt,omitempty"`
	Status     string    `bson:"status,omitempty" json:"status,omitempty"`
}

// ToolCallResult captures a tool invocation for persistence.
type ToolCallResult struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
	Result    string `json:"result"`
}

// ChatSessionInput is the request body for creating a chat session.
type ChatSessionInput struct {
	CompanyID     string `json:"companyId"`
	UserID        string `json:"userId"`
	Title         string `json:"title"`
	Model         string `json:"model,omitempty"`
	Provider      string `json:"provider,omitempty"`
	AccessProfile string `json:"accessProfile,omitempty"`
	ProjectID     string `json:"projectId,omitempty"`
}

// ChatRequest is the request body for sending a chat message.
type ChatRequest struct {
	CompanyID      string `json:"companyId"`
	UserID         string `json:"userId"`
	ConversationID string `json:"conversationId,omitempty"`
	Question       string `json:"question"`
	Stream         bool   `json:"stream,omitempty"`
	// AccessProfile and ProjectID are only used when a new session is
	// created by this request (no conversationId). Existing sessions keep
	// the profile/project they were created with.
	AccessProfile string `json:"accessProfile,omitempty"`
	ProjectID     string `json:"projectId,omitempty"`
}

// ChatResponse is the response from the chat endpoint.
type ChatResponse struct {
	Answer           string            `json:"answer"`
	ConversationID   string            `json:"conversationId"`
	ToolCalls        []ToolCallDisplay `json:"toolCalls,omitempty"`
	ToolTrace        []ToolTraceEvent  `json:"toolTrace,omitempty"`
	PendingApprovals []PendingApproval `json:"pendingApprovals,omitempty"`
	Sources          []Source          `json:"sources,omitempty"`
}

// ToolTraceEvent is safe progress metadata emitted by dev2-llm-service. It
// intentionally has no fields for tool arguments, output, prompts, or other
// potentially sensitive payloads.
type ToolTraceEvent struct {
	EventID          string `bson:"eventId" json:"eventId"`
	Type             string `bson:"type" json:"type"`
	SessionID        string `bson:"sessionId" json:"sessionId"`
	ToolCallID       string `bson:"toolCallId,omitempty" json:"toolCallId,omitempty"`
	ParentToolCallID string `bson:"parentToolCallId,omitempty" json:"parentToolCallId,omitempty"`
	ToolName         string `bson:"toolName,omitempty" json:"toolName,omitempty"`
	PersonaName      string `bson:"personaName,omitempty" json:"personaName,omitempty"`
	PersonaScope     string `bson:"personaScope,omitempty" json:"personaScope,omitempty"`
	DelegationDepth  int    `bson:"delegationDepth" json:"delegationDepth"`
	Summary          string `bson:"summary" json:"summary"`
	Status           string `bson:"status" json:"status"`
	Timestamp        string `bson:"timestamp" json:"timestamp"`
	DurationMS       *int64 `bson:"durationMs,omitempty" json:"durationMs,omitempty"`
	Success          *bool  `bson:"success,omitempty" json:"success,omitempty"`
}

// ToolCallDisplay represents a tool call shown in the response.
type ToolCallDisplay struct {
	Name   string `json:"name"`
	Result string `json:"result,omitempty"`
}

// Source represents a knowledge graph source used in the answer.
type Source struct {
	Type  string `json:"type"`
	Label string `json:"label"`
	ID    string `json:"id,omitempty"`
}

// SessionListItem is a summary for listing sessions.
type SessionListItem struct {
	ID            string    `json:"id"`
	Title         string    `json:"title"`
	LastMessage   string    `json:"lastMessage,omitempty"`
	MessageCount  int       `json:"messageCount"`
	Model         string    `json:"model"`
	AccessProfile string    `json:"accessProfile,omitempty"`
	ProjectID     string    `json:"projectId,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// SessionListResponse wraps a list of sessions.
type SessionListResponse struct {
	Sessions []SessionListItem `json:"sessions"`
	Total    int               `json:"total"`
}
