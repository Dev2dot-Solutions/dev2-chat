package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/gorilla/websocket"
)

const (
	socketWriteTimeout = 10 * time.Second
	socketCloseSlow    = "client is not consuming events"
)

type socketStore interface {
	IssueTicket(context.Context, models.SocketIdentity, time.Time) (string, time.Time, error)
	ConsumeTicket(context.Context, string, time.Time) (*models.SocketTicket, error)
	AppendEvent(context.Context, models.SocketIdentity, models.SocketServerEvent) (*models.SocketServerEvent, error)
	ReplayEvents(context.Context, models.SocketIdentity, string, int64) ([]models.SocketServerEvent, error)
	BeginReceipt(context.Context, models.SocketIdentity, string, string, string, time.Time) (*models.SocketActionReceipt, bool, error)
	UpdateReceipt(context.Context, models.SocketIdentity, string, string, string, string, map[string]any) error
}

type SocketOptions struct {
	AllowedOrigins []string
	SendQueue      int
	ReadLimit      int64
	PingInterval   time.Duration
	IdleTimeout    time.Duration
}

type SocketHandler struct {
	store    socketStore
	agent    *AgentHandler
	chat     *ChatHandler
	options  SocketOptions
	upgrader websocket.Upgrader
}

func NewSocketHandler(store socketStore, agent *AgentHandler, chat *ChatHandler, options SocketOptions) *SocketHandler {
	if options.SendQueue <= 0 {
		options.SendQueue = 128
	}
	if options.ReadLimit <= 0 {
		options.ReadLimit = 64 << 10
	}
	if options.PingInterval <= 0 {
		options.PingInterval = 25 * time.Second
	}
	if options.IdleTimeout <= 0 {
		options.IdleTimeout = 60 * time.Second
	}
	h := &SocketHandler{store: store, agent: agent, chat: chat, options: options}
	h.upgrader = websocket.Upgrader{
		HandshakeTimeout: 10 * time.Second,
		CheckOrigin: func(r *http.Request) bool {
			return originAllowed(r.Header.Get("Origin"), options.AllowedOrigins)
		},
	}
	return h
}

func (h *SocketHandler) Routes(r interface {
	Post(string, http.HandlerFunc)
	Get(string, http.HandlerFunc)
}) {
	r.Post("/chat/socket-ticket", h.IssueTicket)
	r.Get("/chat/ws", h.Connect)
}

func (h *SocketHandler) IssueTicket(w http.ResponseWriter, r *http.Request) {
	var body struct {
		AccessProfile string `json:"accessProfile"`
		ProjectID     string `json:"projectId"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	userID, companyID := GetUserID(r), GetCompanyID(r)
	if userID == "" || companyID == "" {
		respondError(w, http.StatusForbidden, "authenticated user and company identity are required")
		return
	}
	if body.ProjectID == "" {
		respondError(w, http.StatusBadRequest, "projectId is required")
		return
	}
	if _, status, errMsg := h.agent.validateNewSessionScope(r, companyID, body.AccessProfile, body.ProjectID); errMsg != "" {
		respondError(w, status, errMsg)
		return
	}
	identity := models.SocketIdentity{
		UserID: userID, CompanyID: companyID, IsAdmin: GetIsAdmin(r),
		AccessProfile: body.AccessProfile, ProjectID: body.ProjectID,
	}
	ticket, expiresAt, err := h.store.IssueTicket(r.Context(), identity, time.Now())
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to issue socket ticket")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"ticket": ticket, "expiresAt": expiresAt})
}

func (h *SocketHandler) Connect(w http.ResponseWriter, r *http.Request) {
	if !originAllowed(r.Header.Get("Origin"), h.options.AllowedOrigins) {
		respondError(w, http.StatusForbidden, "origin not allowed")
		return
	}
	ticket, err := h.store.ConsumeTicket(r.Context(), socketTicketFromRequest(r), time.Now())
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to consume socket ticket")
		return
	}
	if ticket == nil {
		respondError(w, http.StatusUnauthorized, "invalid or expired socket ticket")
		return
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	identity := ticket.SocketIdentity
	ctx, cancel := context.WithCancel(r.Context())
	c := &socketConnection{
		handler: h, conn: conn, identity: identity,
		ctx: ctx, cancel: cancel, send: make(chan models.SocketServerEvent, h.options.SendQueue),
		generations: make(map[string]context.CancelFunc), actions: make(chan struct{}, 16),
	}
	c.run()
}

type socketConnection struct {
	handler     *SocketHandler
	conn        *websocket.Conn
	identity    models.SocketIdentity
	ctx         context.Context
	cancel      context.CancelFunc
	send        chan models.SocketServerEvent
	actions     chan struct{}
	eventMu     sync.Mutex
	genMu       sync.Mutex
	generations map[string]context.CancelFunc
	closeOnce   sync.Once
	closeCode   int
	closeText   string
}

func (c *socketConnection) run() {
	c.closeCode = websocket.CloseNormalClosure
	c.closeText = "connection closed"
	c.conn.SetReadLimit(c.handler.options.ReadLimit)
	_ = c.conn.SetReadDeadline(time.Now().Add(c.handler.options.IdleTimeout))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(c.handler.options.IdleTimeout))
	})
	writerDone := make(chan struct{})
	go func() { defer close(writerDone); c.writeLoop() }()
	c.emit("connection.ready", "", "", map[string]any{
		"accessProfile": c.identity.AccessProfile, "projectId": c.identity.ProjectID,
	})
	c.readLoop()
	c.shutdown(websocket.CloseNormalClosure, "connection closed")
	<-writerDone
}

func (c *socketConnection) readLoop() {
	for {
		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(c.handler.options.IdleTimeout))
		msg, err := parseSocketMessage(payload)
		if err != nil {
			c.emitError("", "", "invalid_message", err.Error())
			continue
		}
		select {
		case c.actions <- struct{}{}:
			go func() {
				defer func() { <-c.actions }()
				c.handleMessage(msg)
			}()
		default:
			c.shutdown(websocket.ClosePolicyViolation, "too many concurrent actions")
			return
		}
	}
}

func (c *socketConnection) writeLoop() {
	ticker := time.NewTicker(c.handler.options.PingInterval)
	defer ticker.Stop()
	defer func() {
		deadline := time.Now().Add(socketWriteTimeout)
		_ = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(c.closeCode, c.closeText), deadline)
		_ = c.conn.Close()
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		select {
		case event := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(socketWriteTimeout))
			if err := c.conn.WriteJSON(event); err != nil {
				c.shutdown(websocket.CloseGoingAway, "write failed")
				return
			}
		case <-ticker.C:
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(socketWriteTimeout)); err != nil {
				c.shutdown(websocket.CloseGoingAway, "heartbeat failed")
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *socketConnection) handleMessage(msg models.SocketClientMessage) {
	switch msg.Type {
	case "chat.send":
		c.handleChatSend(msg)
	case "approval.decide":
		c.handleApproval(msg)
	case "generation.cancel":
		c.handleCancel(msg)
	case "session.resume":
		c.handleResume(msg)
	case "ping":
		c.emit("pong", msg.RequestID, msg.SessionID, map[string]any{})
	default:
		c.emitError(msg.RequestID, msg.SessionID, "unsupported_type", "unsupported message type")
	}
}

func (c *socketConnection) handleChatSend(msg models.SocketClientMessage) {
	var data struct {
		Message       string `json:"message"`
		ProjectID     string `json:"projectId"`
		AccessProfile string `json:"accessProfile"`
	}
	if err := decodeSocketData(msg, &data); err != nil || strings.TrimSpace(data.Message) == "" {
		c.emitError(msg.RequestID, msg.SessionID, "invalid_chat_send", "message, projectId and accessProfile are required")
		return
	}
	if msg.RequestID == "" || msg.IdempotencyKey == "" {
		c.emitError(msg.RequestID, msg.SessionID, "invalid_chat_send", "requestId and idempotencyKey are required")
		return
	}
	if data.ProjectID != c.identity.ProjectID || data.AccessProfile != c.identity.AccessProfile {
		c.emitError(msg.RequestID, msg.SessionID, "scope_mismatch", "chat scope does not match socket ticket")
		return
	}
	if msg.SessionID != "" {
		if _, err := c.authorizeSession(msg.SessionID); err != nil {
			c.emitError(msg.RequestID, msg.SessionID, "session_forbidden", err.Error())
			return
		}
	}
	receipt, created, err := c.handler.store.BeginReceipt(c.ctx, c.identity, msg.IdempotencyKey, models.SocketActionChatSend, msg.RequestID, time.Now())
	if err != nil {
		c.emitError(msg.RequestID, msg.SessionID, "idempotency_unavailable", "could not record action")
		return
	}
	if !created {
		c.replayReceipt(msg, receipt, models.SocketActionChatSend)
		return
	}
	genCtx, cancel := context.WithCancel(c.ctx)
	c.genMu.Lock()
	c.generations[msg.RequestID] = cancel
	c.genMu.Unlock()
	defer func() {
		cancel()
		c.genMu.Lock()
		delete(c.generations, msg.RequestID)
		c.genMu.Unlock()
	}()

	req := models.ChatRequest{
		CompanyID: c.identity.CompanyID, UserID: c.identity.UserID,
		ConversationID: msg.SessionID, Question: data.Message,
		AccessProfile: data.AccessProfile, ProjectID: data.ProjectID,
	}
	request := requestWithSocketIdentity(genCtx, c.identity)
	prepared, status, errMsg := c.handler.agent.prepareAgentRequest(request, req)
	if errMsg != "" {
		data := map[string]any{"code": "chat_rejected", "message": errMsg, "status": status}
		_ = c.handler.store.UpdateReceipt(context.Background(), c.identity, msg.IdempotencyKey, "failed", msg.SessionID, "error", data)
		c.emit("error", msg.RequestID, msg.SessionID, data)
		return
	}
	sessionID := prepared.session.ID
	_ = c.handler.store.UpdateReceipt(c.ctx, c.identity, msg.IdempotencyKey, "accepted", sessionID, "", nil)
	c.emit("chat.accepted", msg.RequestID, sessionID, map[string]any{"idempotencyKey": msg.IdempotencyKey})
	completed := c.handler.agent.completeAgentRequest(request, prepared, func(event models.ToolTraceEvent) {
		c.emit("trace", msg.RequestID, sessionID, objectData(event))
	})
	if completed.result.cancelled {
		data := map[string]any{"targetRequestId": msg.RequestID}
		_ = c.handler.store.UpdateReceipt(context.Background(), c.identity, msg.IdempotencyKey, "cancelled", sessionID, "generation.cancelled", data)
		c.emit("generation.cancelled", msg.RequestID, sessionID, data)
		return
	}
	for _, chunk := range chunkText(completed.result.answer, 200) {
		c.emit("content.delta", msg.RequestID, sessionID, map[string]any{"content": chunk})
	}
	meta := buildStreamMeta(sessionID, completed.result, completed.toolTrace, prepared.sources)
	c.emit("chat.meta", msg.RequestID, sessionID, objectData(meta))
	for _, approval := range completed.result.pendingApprovals {
		c.emit("approval.requested", msg.RequestID, sessionID, objectData(approval))
	}
	finalData := objectData(meta)
	finalData["status"] = "completed"
	_ = c.handler.store.UpdateReceipt(context.Background(), c.identity, msg.IdempotencyKey, "completed", sessionID, "generation.completed", finalData)
	c.emit("generation.completed", msg.RequestID, sessionID, finalData)
}

func (c *socketConnection) handleApproval(msg models.SocketClientMessage) {
	var data struct {
		ApprovalID string `json:"approvalId"`
		Decision   string `json:"decision"`
	}
	if err := decodeSocketData(msg, &data); err != nil || data.ApprovalID == "" ||
		(data.Decision != models.ApprovalDecisionApprove && data.Decision != models.ApprovalDecisionReject) {
		c.emitError(msg.RequestID, msg.SessionID, "invalid_approval", "approvalId and a valid decision are required")
		return
	}
	if msg.RequestID == "" || msg.IdempotencyKey == "" {
		c.emitError(msg.RequestID, msg.SessionID, "invalid_approval", "requestId and idempotencyKey are required")
		return
	}
	rec, err := c.handler.chat.approvalRepo.GetByID(c.ctx, data.ApprovalID)
	if err != nil || rec == nil {
		c.emitError(msg.RequestID, msg.SessionID, "approval_not_found", "approval not found")
		return
	}
	if msg.SessionID != "" && msg.SessionID != rec.SessionID {
		c.emitError(msg.RequestID, msg.SessionID, "scope_mismatch", "approval belongs to a different session")
		return
	}
	if _, err := c.authorizeSession(rec.SessionID); err != nil {
		c.emitError(msg.RequestID, rec.SessionID, "session_forbidden", err.Error())
		return
	}
	receipt, created, err := c.handler.store.BeginReceipt(c.ctx, c.identity, msg.IdempotencyKey, models.SocketActionApprovalDecide, msg.RequestID, time.Now())
	if err != nil {
		c.emitError(msg.RequestID, rec.SessionID, "idempotency_unavailable", "could not record action")
		return
	}
	if !created {
		c.replayReceipt(msg, receipt, models.SocketActionApprovalDecide)
		return
	}
	status, result, errMsg := c.handler.chat.decideApproval(requestWithSocketIdentity(c.ctx, c.identity), data.ApprovalID, data.Decision)
	if errMsg != "" {
		final := map[string]any{"code": "approval_failed", "message": errMsg, "status": status}
		_ = c.handler.store.UpdateReceipt(context.Background(), c.identity, msg.IdempotencyKey, "failed", rec.SessionID, "error", final)
		c.emit("error", msg.RequestID, rec.SessionID, final)
		return
	}
	_ = c.handler.store.UpdateReceipt(context.Background(), c.identity, msg.IdempotencyKey, "completed", rec.SessionID, "approval.resolved", result)
	c.emit("approval.resolved", msg.RequestID, rec.SessionID, result)
}

func (c *socketConnection) handleCancel(msg models.SocketClientMessage) {
	var data struct {
		TargetRequestID string `json:"targetRequestId"`
	}
	if err := decodeSocketData(msg, &data); err != nil || data.TargetRequestID == "" {
		c.emitError(msg.RequestID, msg.SessionID, "invalid_cancel", "targetRequestId is required")
		return
	}
	c.genMu.Lock()
	cancel := c.generations[data.TargetRequestID]
	c.genMu.Unlock()
	if cancel == nil {
		c.emitError(msg.RequestID, msg.SessionID, "generation_not_found", "active generation not found")
		return
	}
	cancel()
}

func (c *socketConnection) handleResume(msg models.SocketClientMessage) {
	var data struct {
		SessionID string `json:"sessionId"`
		LastSeq   int64  `json:"lastSeq"`
	}
	if err := decodeSocketData(msg, &data); err != nil || data.SessionID == "" || data.LastSeq < 0 {
		c.emitError(msg.RequestID, msg.SessionID, "invalid_resume", "sessionId and non-negative lastSeq are required")
		return
	}
	if msg.SessionID != "" && msg.SessionID != data.SessionID {
		c.emitError(msg.RequestID, msg.SessionID, "scope_mismatch", "sessionId values do not match")
		return
	}
	if _, err := c.authorizeSession(data.SessionID); err != nil {
		c.emitError(msg.RequestID, data.SessionID, "session_forbidden", err.Error())
		return
	}
	c.eventMu.Lock()
	events, err := c.handler.store.ReplayEvents(c.ctx, c.identity, data.SessionID, data.LastSeq)
	if err == nil {
		for _, event := range events {
			if !c.enqueue(event) {
				break
			}
		}
	}
	c.eventMu.Unlock()
	if err != nil {
		c.emitError(msg.RequestID, data.SessionID, "replay_unavailable", "could not load replay")
		return
	}
	c.emit("replay.completed", msg.RequestID, data.SessionID, map[string]any{"replayed": len(events), "afterSeq": data.LastSeq})
}

func (c *socketConnection) authorizeSession(sessionID string) (*models.ChatSession, error) {
	session, err := c.handler.chat.sessionRepo.GetByID(c.ctx, sessionID)
	if err != nil {
		return nil, errors.New("failed to load session")
	}
	if session == nil {
		return nil, errors.New("session not found")
	}
	if session.CompanyID != c.identity.CompanyID || session.UserID != c.identity.UserID ||
		models.NormalizeAccessProfile(session.AccessProfile) != c.identity.AccessProfile || session.ProjectID != c.identity.ProjectID {
		return nil, errors.New("session does not match socket ticket")
	}
	return session, nil
}

func (c *socketConnection) replayReceipt(msg models.SocketClientMessage, receipt *models.SocketActionReceipt, actionType string) {
	if receipt.ActionType != actionType {
		c.emitError(msg.RequestID, msg.SessionID, "idempotency_conflict", "idempotency key was used for another action")
		return
	}
	if receipt.SessionID != "" && actionType == models.SocketActionChatSend {
		c.emit("chat.accepted", msg.RequestID, receipt.SessionID, map[string]any{"duplicate": true})
	}
	if receipt.FinalEventType != "" {
		c.emit(receipt.FinalEventType, msg.RequestID, receipt.SessionID, receipt.FinalData)
		return
	}
	c.emitError(msg.RequestID, receipt.SessionID, "action_in_progress", "action is already in progress")
}

func (c *socketConnection) emit(eventType, requestID, sessionID string, data map[string]any) {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()
	event, err := c.handler.store.AppendEvent(c.ctx, c.identity, models.SocketServerEvent{
		Type: eventType, RequestID: requestID, SessionID: sessionID,
		Timestamp: time.Now().UTC(), Data: data,
	})
	if err != nil {
		c.shutdown(websocket.CloseInternalServerErr, "event persistence failed")
		return
	}
	c.enqueue(*event)
}

func (c *socketConnection) emitError(requestID, sessionID, code, message string) {
	c.emit("error", requestID, sessionID, map[string]any{"code": code, "message": message})
}

func (c *socketConnection) enqueue(event models.SocketServerEvent) bool {
	select {
	case c.send <- event:
		return true
	default:
		c.shutdown(websocket.ClosePolicyViolation, socketCloseSlow)
		return false
	}
}

func (c *socketConnection) shutdown(code int, text string) {
	c.closeOnce.Do(func() {
		c.closeCode, c.closeText = code, text
		c.cancel()
		c.genMu.Lock()
		for _, cancel := range c.generations {
			cancel()
		}
		c.genMu.Unlock()
	})
}

func parseSocketMessage(payload []byte) (models.SocketClientMessage, error) {
	var msg models.SocketClientMessage
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&msg); err != nil {
		return msg, fmt.Errorf("invalid envelope: %w", err)
	}
	if msg.Type == "" || len(msg.Data) == 0 || string(msg.Data) == "null" {
		return msg, errors.New("type and data object are required")
	}
	var object map[string]any
	if err := json.Unmarshal(msg.Data, &object); err != nil || object == nil {
		return msg, errors.New("data must be an object")
	}
	return msg, nil
}

func decodeSocketData(msg models.SocketClientMessage, target any) error {
	dec := json.NewDecoder(strings.NewReader(string(msg.Data)))
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}

func originAllowed(origin string, allowed []string) bool {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		return false
	}
	want := strings.ToLower(parsed.Scheme + "://" + parsed.Host)
	for _, candidate := range allowed {
		u, err := url.Parse(strings.TrimSpace(candidate))
		if err == nil && u.Scheme != "" && u.Host != "" && strings.ToLower(u.Scheme+"://"+u.Host) == want {
			return true
		}
	}
	return false
}

func objectData(value any) map[string]any {
	payload, _ := json.Marshal(value)
	var data map[string]any
	_ = json.Unmarshal(payload, &data)
	if data == nil {
		data = map[string]any{}
	}
	return data
}
