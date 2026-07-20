package models

import "time"

// ChatSession represents a conversation session.
type ChatSession struct {
	ID        string    `bson:"_id" json:"id"`
	CompanyID string    `bson:"companyId" json:"companyId"`
	UserID    string    `bson:"userId" json:"userId"`
	Title     string    `bson:"title" json:"title"`
	Model     string    `bson:"model" json:"model"`
	Provider  string    `bson:"provider" json:"provider"`
	Status    string    `bson:"status" json:"status"`
	TokenCount int      `bson:"tokenCount" json:"tokenCount"`
	CreatedAt time.Time `bson:"createdAt" json:"createdAt"`
	UpdatedAt time.Time `bson:"updatedAt" json:"updatedAt"`
}

// ChatMessage represents a single message within a session.
type ChatMessage struct {
	ID        string    `bson:"_id" json:"id"`
	SessionID string    `bson:"sessionId" json:"sessionId"`
	Role      string    `bson:"role" json:"role"`       // "user", "assistant", "system", "tool"
	Content   string    `bson:"content" json:"content"`
	ToolCalls []ToolCallResult `bson:"toolCalls,omitempty" json:"toolCalls,omitempty"`
	ToolCallID string   `bson:"toolCallId,omitempty" json:"toolCallId,omitempty"`
	Name      string    `bson:"name,omitempty" json:"name,omitempty"`
	TokenCount int      `bson:"tokenCount,omitempty" json:"tokenCount,omitempty"`
	CreatedAt time.Time `bson:"createdAt" json:"createdAt"`
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
	CompanyID string `json:"companyId"`
	UserID    string `json:"userId"`
	Title     string `json:"title"`
	Model     string `json:"model,omitempty"`
	Provider  string `json:"provider,omitempty"`
}

// ChatRequest is the request body for sending a chat message.
type ChatRequest struct {
	CompanyID      string `json:"companyId"`
	UserID         string `json:"userId"`
	ConversationID string `json:"conversationId,omitempty"`
	Question       string `json:"question"`
	Stream         bool   `json:"stream,omitempty"`
}

// ChatResponse is the response from the chat endpoint.
type ChatResponse struct {
	Answer         string            `json:"answer"`
	ConversationID string            `json:"conversationId"`
	ToolCalls      []ToolCallDisplay `json:"toolCalls,omitempty"`
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
	LastMessage    string    `json:"lastMessage,omitempty"`
	MessageCount   int       `json:"messageCount"`
	Model          string    `json:"model"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

// SessionListResponse wraps a list of sessions.
type SessionListResponse struct {
	Sessions []SessionListItem `json:"sessions"`
	Total    int               `json:"total"`
}
