package nats

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/nats-io/nats.go"
)

const (
	SubjectKnowledgeSearch    = "knowledge.search"
	SubjectKnowledgeEntityGet = "knowledge.entity.get"
	SubjectLLMRequest         = "llm.request"
	SubjectChatSessionCreated = "chat.session.created"
	SubjectChatMessageSent    = "chat.message.sent"
)

// Client manages NATS connections for dev2-chat.
type Client struct {
	nc  *nats.Conn
	enc *nats.EncodedConn
}

// NewClient creates a new NATS client. If nc is nil, all operations are no-ops.
func NewClient(nc *nats.Conn) *Client {
	if nc == nil {
		return &Client{}
	}
	enc, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		log.Printf("[nats] Failed to create encoded connection: %v", err)
		return &Client{}
	}
	return &Client{nc: nc, enc: enc}
}

// RequestKnowledgeSearch sends a NATS request-reply to dev2-knowledge for context search.
func (c *Client) RequestKnowledgeSearch(req *models.KnowledgeSearchRequest) (*models.KnowledgeSearchResponse, error) {
	if c.enc == nil {
		return nil, fmt.Errorf("NATS not connected")
	}
	var resp models.KnowledgeSearchResponse
	err := c.enc.Request(SubjectKnowledgeSearch, req, &resp, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("knowledge.search request failed: %w", err)
	}
	return &resp, nil
}

// RequestLLM sends a NATS request-reply to dev2-llm-service for completion.
func (c *Client) RequestLLM(req *models.LLMRequest) (*models.LLMResponse, error) {
	if c.enc == nil {
		return nil, fmt.Errorf("NATS not connected")
	}
	sessionID := fmt.Sprintf("chat-%d", time.Now().UnixNano())

	// Build a NATSRequest-compatible payload
	// Extract system prompt from messages
	systemPrompt := ""
	var history []map[string]interface{}
	for _, msg := range req.Messages {
		if msg.Role == "system" {
			systemPrompt = msg.Content
			continue
		}
		history = append(history, map[string]interface{}{
			"role":    msg.Role,
			"content": msg.Content,
		})
	}

	// Find the latest user message
	latestMessage := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			latestMessage = req.Messages[i].Content
			break
		}
	}

	natsReq := map[string]interface{}{
		"sessionId":           sessionID,
		"systemPrompt":        systemPrompt,
		"conversationHistory": history,
		"latestMessage":       latestMessage,
		"modelOverride":       req.Model,
	}

	var resp models.LLMResponse
	err := c.enc.Request(fmt.Sprintf("%s.%s", SubjectLLMRequest, sessionID), natsReq, &resp, 120*time.Second)
	if err != nil {
		return nil, fmt.Errorf("llm.request failed: %w", err)
	}
	return &resp, nil
}

// PublishSessionCreated publishes a chat.session.created event.
func (c *Client) PublishSessionCreated(session *models.ChatSession) {
	if c.enc == nil {
		return
	}
	event := map[string]any{
		"sessionId": session.ID,
		"companyId": session.CompanyID,
		"userId":    session.UserID,
		"title":      session.Title,
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}
	if err := c.enc.Publish(SubjectChatSessionCreated, event); err != nil {
		log.Printf("[nats] Failed to publish session.created: %v", err)
	}
}

// PublishMessageSent publishes a chat.message.sent event.
func (c *Client) PublishMessageSent(sessionID, companyID, userID, role, content string) {
	if c.enc == nil {
		return
	}
	event := map[string]any{
		"sessionId":      sessionID,
		"companyId":      companyID,
		"userId":         userID,
		"role":            role,
		"contentPreview": truncate(content, 200),
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
	}
	if err := c.enc.Publish(SubjectChatMessageSent, event); err != nil {
		log.Printf("[nats] Failed to publish message.sent: %v", err)
	}
}

// Close drains the NATS connection.
func (c *Client) Close() {
	if c.nc != nil {
		c.nc.Drain()
	}
}

func truncate(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen])
}

// RawPublish publishes raw JSON to a subject for arbitrary events.
func (c *Client) RawPublish(subject string, data any) {
	if c.enc == nil {
		return
	}
	payload, err := json.Marshal(data)
	if err != nil {
		log.Printf("[nats] Failed to marshal event for %s: %v", subject, err)
		return
	}
	if err := c.nc.Publish(subject, payload); err != nil {
		log.Printf("[nats] Failed to publish %s: %v", subject, err)
	}
}
