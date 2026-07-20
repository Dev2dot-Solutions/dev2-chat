package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
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
	SubjectCompanyProjectsGet = "company.projects.get"
	SubjectToolApprove        = "tool.approve"
	SubjectToolAudit          = "audit.tool.invocation"
	companyProjectsCacheTTL   = 60 * time.Second
	toolApproveTimeout        = 10 * time.Second
)

// Client manages NATS connections for dev2-chat.
type Client struct {
	nc  *nats.Conn
	enc *nats.EncodedConn

	projectsMu    sync.Mutex
	projectsCache map[string]cachedProjects
}

type cachedProjects struct {
	projects []models.CompanyProject
	expires  time.Time
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
	return c.RequestLLMWithProgress(context.Background(), req, nil)
}

// RequestLLMWithProgress sends an LLM request while forwarding safe progress
// metadata from a unique inbox. The progress subscription is active before
// the request is published and is always removed when the final reply,
// cancellation, or timeout is observed.
func (c *Client) RequestLLMWithProgress(ctx context.Context, req *models.LLMRequest, onProgress func(models.ToolTraceEvent)) (*models.LLMResponse, error) {
	if c.nc == nil {
		return nil, fmt.Errorf("NATS not connected")
	}
	sessionID := req.SessionID
	if sessionID == "" {
		sessionID = fmt.Sprintf("chat-%d", time.Now().UnixNano())
	}

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

	progressSubject := nats.NewInbox()
	progressMessages := make(chan *nats.Msg, 64)
	progressSub, err := c.nc.ChanSubscribe(progressSubject, progressMessages)
	if err != nil {
		return nil, fmt.Errorf("subscribe to llm progress: %w", err)
	}
	defer progressSub.Unsubscribe()
	// Flush guarantees the server has registered the inbox before llm.request
	// can emit its first progress event.
	if err := c.nc.FlushTimeout(5 * time.Second); err != nil {
		return nil, fmt.Errorf("subscribe to llm progress: %w", err)
	}

	natsReq := map[string]interface{}{
		"sessionId":           sessionID,
		"userId":              req.UserID,
		"systemPrompt":        systemPrompt,
		"conversationHistory": history,
		"latestMessage":       latestMessage,
		"modelOverride":       req.Model,
		"accessProfile":       req.AccessProfile,
		"workspaceCompanyId":  req.WorkspaceCompanyID,
		"workspaceProjectId":  req.WorkspaceProjectID,
		"progressSubject":     progressSubject,
	}
	// Forward the session's access profile and workspace scoping so
	// dev2-llm-service can gate its own tools and resolve personas.
	if req.WorkspacePTProjectKey != "" {
		natsReq["workspacePtProjectKey"] = req.WorkspacePTProjectKey
	}

	payload, err := json.Marshal(natsReq)
	if err != nil {
		return nil, fmt.Errorf("marshal llm.request: %w", err)
	}

	requestCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	type requestResult struct {
		msg *nats.Msg
		err error
	}
	resultCh := make(chan requestResult, 1)
	go func() {
		msg, requestErr := c.nc.RequestWithContext(requestCtx, fmt.Sprintf("%s.%s", SubjectLLMRequest, sessionID), payload)
		resultCh <- requestResult{msg: msg, err: requestErr}
	}()

	forwardProgress := func(msg *nats.Msg) {
		if onProgress == nil {
			return
		}
		var event models.ToolTraceEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			log.Printf("[nats] Ignoring invalid llm progress event: %v", err)
			return
		}
		onProgress(event)
	}

	for {
		select {
		case msg := <-progressMessages:
			forwardProgress(msg)
		case result := <-resultCh:
			// The NATS connection dispatches messages in wire order. Drain any
			// progress already delivered before processing the final reply.
			for {
				select {
				case msg := <-progressMessages:
					forwardProgress(msg)
				default:
					if result.err != nil {
						return nil, fmt.Errorf("llm.request failed: %w", result.err)
					}
					var resp models.LLMResponse
					if err := json.Unmarshal(result.msg.Data, &resp); err != nil {
						return nil, fmt.Errorf("decode llm.request response: %w", err)
					}
					return &resp, nil
				}
			}
		case <-requestCtx.Done():
			return nil, fmt.Errorf("llm.request failed: %w", requestCtx.Err())
		}
	}
}

// RequestCompanyProjects fetches the company's Dev2Projects from
// dev2-company-config via company.projects.get request-reply. Results are
// cached briefly (60s) since project/visibility changes are infrequent.
func (c *Client) RequestCompanyProjects(companyID string) ([]models.CompanyProject, error) {
	if c.enc == nil {
		return nil, fmt.Errorf("NATS not connected")
	}

	c.projectsMu.Lock()
	if cp, ok := c.projectsCache[companyID]; ok && time.Now().Before(cp.expires) {
		c.projectsMu.Unlock()
		return cp.projects, nil
	}
	c.projectsMu.Unlock()

	var resp struct {
		CompanyID string                  `json:"companyId"`
		Projects  []models.CompanyProject `json:"projects"`
		Error     string                  `json:"error,omitempty"`
	}
	err := c.enc.Request(SubjectCompanyProjectsGet,
		map[string]string{"companyId": companyID}, &resp, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("company.projects.get request failed: %w", err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("company.projects.get: %s", resp.Error)
	}

	c.projectsMu.Lock()
	if c.projectsCache == nil {
		c.projectsCache = make(map[string]cachedProjects)
	}
	c.projectsCache[companyID] = cachedProjects{
		projects: resp.Projects,
		expires:  time.Now().Add(companyProjectsCacheTTL),
	}
	c.projectsMu.Unlock()
	return resp.Projects, nil
}

// RequestToolApproval forwards an approval decision to dev2-llm-service via
// tool.approve request-reply (DEV2-108). Session and user identity must
// already be resolved from the authenticated session — never from the HTTP
// request body.
func (c *Client) RequestToolApproval(req *models.ToolApprovalRequest) (*models.ToolApprovalResponse, error) {
	if c.enc == nil {
		return nil, fmt.Errorf("NATS not connected")
	}
	var resp models.ToolApprovalResponse
	err := c.enc.Request(SubjectToolApprove, req, &resp, toolApproveTimeout)
	if err != nil {
		return nil, fmt.Errorf("tool.approve request failed: %w", err)
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
		"title":     session.Title,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
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
		"role":           role,
		"contentPreview": truncate(content, 200),
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	}
	if err := c.enc.Publish(SubjectChatMessageSent, event); err != nil {
		log.Printf("[nats] Failed to publish message.sent: %v", err)
	}
}

// PublishToolInvocation publishes a local-tool audit event. NATS Publish is
// asynchronous; callers also invoke this method in a goroutine so auditing can
// never delay or fail a chat tool execution.
func (c *Client) PublishToolInvocation(event models.ToolAuditEvent) {
	if c.nc == nil {
		return
	}
	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("[nats] Failed to marshal tool audit event: %v", err)
		return
	}
	if err := c.nc.Publish(SubjectToolAudit, payload); err != nil {
		log.Printf("[nats] Failed to publish tool audit event: %v", err)
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
