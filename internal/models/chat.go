package models

import "time"

// ChatSession represents a conversation session.
type ChatSession struct {
	ID        string    `bson:"_id" json:"id"`
	CompanyID string    `bson:"company_id" json:"company_id"`
	UserID    string    `bson:"user_id" json:"user_id"`
	Title     string    `bson:"title" json:"title"`
	Model     string    `bson:"model" json:"model"`
	Provider  string    `bson:"provider" json:"provider"`
	Status    string    `bson:"status" json:"status"`
	TokenCount int      `bson:"token_count" json:"token_count"`
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

// ChatMessage represents a single message within a session.
type ChatMessage struct {
	ID        string    `bson:"_id" json:"id"`
	SessionID string    `bson:"session_id" json:"session_id"`
	Role      string    `bson:"role" json:"role"`       // "user", "assistant", "system", "tool"
	Content   string    `bson:"content" json:"content"`
	ToolCalls []ToolCallResult `bson:"tool_calls,omitempty" json:"tool_calls,omitempty"`
	ToolCallID string   `bson:"tool_call_id,omitempty" json:"tool_call_id,omitempty"`
	Name      string    `bson:"name,omitempty" json:"name,omitempty"`
	TokenCount int      `bson:"token_count,omitempty" json:"token_count,omitempty"`
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

// ToolCallResult captures a tool invocation for persistence.
type ToolCallResult struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Name     string `json:"name"`
	Arguments string `json:"arguments"`
	Result   string `json:"result"`
}

// ChatSessionInput is the request body for creating a chat session.
type ChatSessionInput struct {
	CompanyID string `json:"company_id"`
	UserID    string `json:"user_id"`
	Title     string `json:"title"`
	Model     string `json:"model,omitempty"`
	Provider  string `json:"provider,omitempty"`
}

// ChatRequest is the request body for sending a chat message.
type ChatRequest struct {
	CompanyID      string `json:"company_id"`
	UserID         string `json:"user_id"`
	ConversationID string `json:"conversation_id,omitempty"`
	Question       string `json:"question"`
	Stream         bool   `json:"stream,omitempty"`
}

// ChatResponse is the response from the chat endpoint.
type ChatResponse struct {
	Answer         string            `json:"answer"`
	ConversationID string            `json:"conversation_id"`
	ToolCalls      []ToolCallDisplay `json:"tool_calls,omitempty"`
	Sources        []Source          `json:"sources,omitempty"`
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
	ID             string    `json:"id"`
	Title          string    `json:"title"`
	LastMessage    string    `json:"last_message,omitempty"`
	MessageCount   int       `json:"message_count"`
	Model          string    `json:"model"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// SessionListResponse wraps a list of sessions.
type SessionListResponse struct {
	Sessions []SessionListItem `json:"sessions"`
	Total    int               `json:"total"`
}
