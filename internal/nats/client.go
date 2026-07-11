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
	var resp models.LLMResponse
	err := c.enc.Request(fmt.Sprintf("%s.%s", SubjectLLMRequest, sessionID), req, &resp, 120*time.Second)
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
		"session_id": session.ID,
		"company_id": session.CompanyID,
		"user_id":    session.UserID,
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
		"session_id":      sessionID,
		"company_id":      companyID,
		"user_id":         userID,
		"role":            role,
		"content_preview": truncate(content, 200),
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
