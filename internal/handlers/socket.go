package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/repository"
	"github.com/gorilla/websocket"
)

const (
	socketWriteTimeout    = 10 * time.Second
	socketTerminalTimeout = 5 * time.Second
	socketCloseAuth       = 4401
	socketCloseForbidden  = 4403
	socketCloseRate       = 4429
	socketCloseBusy       = 1013
	socketCloseSlow       = "client is not consuming events"
	ticketProtocolPrefix  = "dev2-ticket."
	baseProtocol          = "dev2-chat"
)

var socketTicketPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
var errAuthorizationUnavailable = errors.New("authorization service unavailable")

type socketStore interface {
	IssueTicket(context.Context, models.SocketIdentity, time.Time, repository.TicketPolicy, time.Time) (string, time.Time, error)
	ConsumeTicket(context.Context, string, time.Time) (*models.SocketTicket, error)
	AcquireConnection(context.Context, models.SocketIdentity, string, repository.ConnectionPolicy, time.Time) (*models.SocketLease, error)
	AcquireGeneration(context.Context, models.SocketIdentity, repository.GenerationPolicy, time.Time) (*models.SocketLease, error)
	RenewLease(context.Context, *models.SocketLease, time.Duration, time.Time) error
	ReleaseLease(context.Context, *models.SocketLease)
	RecordEvent(context.Context, models.SocketIdentity, models.SocketServerEvent) (*models.SocketServerEvent, bool, error)
	ReplayEvents(context.Context, models.SocketIdentity, string, int64) (*models.SocketReplay, error)
	BeginReceipt(context.Context, models.SocketActionBinding, string, time.Time) (*models.SocketActionReceipt, bool, error)
	UpdateReceipt(context.Context, models.SocketActionBinding, string, string, string, map[string]any) error
}

type SocketOptions struct {
	AllowedOrigins       []string
	TrustedProxyCIDRs    []string
	SendQueue            int
	ReadLimit            int64
	PingInterval         time.Duration
	IdleTimeout          time.Duration
	MaxLifetime          time.Duration
	DeveloperMaxLifetime time.Duration
	ServiceMaxLifetime   time.Duration
	TicketPolicy         repository.TicketPolicy
	ConnectionPolicy     repository.ConnectionPolicy
	GenerationPolicy     repository.GenerationPolicy
	MessagesPerMinute    int
	MessageBurst         int
}

type SocketHandler struct {
	store          socketStore
	agent          *AgentHandler
	chat           *ChatHandler
	options        SocketOptions
	trustedProxies []*net.IPNet
	upgrader       websocket.Upgrader
}

func NewSocketHandler(store socketStore, agent *AgentHandler, chat *ChatHandler, options SocketOptions) *SocketHandler {
	applySocketDefaults(&options)
	h := &SocketHandler{store: store, agent: agent, chat: chat, options: options}
	for _, cidr := range options.TrustedProxyCIDRs {
		if _, network, err := net.ParseCIDR(strings.TrimSpace(cidr)); err == nil {
			h.trustedProxies = append(h.trustedProxies, network)
		}
	}
	h.upgrader = websocket.Upgrader{
		HandshakeTimeout: 10 * time.Second,
		Subprotocols:     []string{baseProtocol},
		CheckOrigin: func(r *http.Request) bool {
			return originAllowed(r.Header.Get("Origin"), options.AllowedOrigins)
		},
	}
	return h
}

func applySocketDefaults(options *SocketOptions) {
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
	if options.MaxLifetime <= 0 {
		options.MaxLifetime = 30 * time.Minute
	}
	if options.DeveloperMaxLifetime <= 0 {
		options.DeveloperMaxLifetime = 5 * time.Minute
	}
	if options.ServiceMaxLifetime <= 0 {
		options.ServiceMaxLifetime = 5 * time.Minute
	}
	if options.TicketPolicy.IssuePerMinute <= 0 {
		options.TicketPolicy.IssuePerMinute = 10
	}
	if options.TicketPolicy.MaxOutstanding <= 0 {
		options.TicketPolicy.MaxOutstanding = 3
	}
	if options.ConnectionPolicy.GlobalLimit <= 0 {
		options.ConnectionPolicy.GlobalLimit = 500
	}
	if options.ConnectionPolicy.CompanyLimit <= 0 {
		options.ConnectionPolicy.CompanyLimit = 50
	}
	if options.ConnectionPolicy.UserLimit <= 0 {
		options.ConnectionPolicy.UserLimit = 3
	}
	if options.ConnectionPolicy.IPLimit <= 0 {
		options.ConnectionPolicy.IPLimit = 20
	}
	if options.ConnectionPolicy.LeaseTTL <= 0 {
		options.ConnectionPolicy.LeaseTTL = 75 * time.Second
	}
	if options.GenerationPolicy.CompanyLimit <= 0 {
		options.GenerationPolicy.CompanyLimit = 20
	}
	if options.GenerationPolicy.UserLimit <= 0 {
		options.GenerationPolicy.UserLimit = 2
	}
	if options.GenerationPolicy.LeaseTTL <= 0 {
		options.GenerationPolicy.LeaseTTL = 3 * time.Minute
	}
	if options.MessagesPerMinute <= 0 {
		options.MessagesPerMinute = 60
	}
	if options.MessageBurst <= 0 {
		options.MessageBurst = 20
	}
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
	now := time.Now().UTC()
	authExpiresAt := GetAuthExpiresAt(r)
	if authExpiresAt.IsZero() || !authExpiresAt.After(now) {
		respondError(w, http.StatusUnauthorized, "authentication is expired")
		return
	}
	maxLifetime := h.options.MaxLifetime
	developerExpiry := time.Time{}
	if body.AccessProfile == models.AccessProfileDeveloper {
		issuedAt := GetAuthIssuedAt(r)
		if issuedAt.IsZero() {
			respondError(w, http.StatusUnauthorized, "developer authentication must include issued-at time")
			return
		}
		developerExpiry = issuedAt.Add(h.options.DeveloperMaxLifetime)
		if !developerExpiry.After(now) {
			respondError(w, http.StatusUnauthorized, "developer authentication must be refreshed")
			return
		}
	}
	if GetAuthSource(r) == "service" && h.options.ServiceMaxLifetime < maxLifetime {
		maxLifetime = h.options.ServiceMaxLifetime
	}
	socketExpiresAt := minTime(authExpiresAt, now.Add(maxLifetime))
	if !developerExpiry.IsZero() {
		socketExpiresAt = minTime(socketExpiresAt, developerExpiry)
	}
	identity := models.SocketIdentity{
		UserID: userID, CompanyID: companyID, IsAdmin: GetIsAdmin(r),
		AccessProfile: body.AccessProfile, ProjectID: body.ProjectID,
		AuthSource: GetAuthSource(r), AuthIssuedAt: GetAuthIssuedAt(r), AuthExpiresAt: authExpiresAt,
	}
	ticket, expiresAt, err := h.store.IssueTicket(r.Context(), identity, socketExpiresAt, h.options.TicketPolicy, now)
	if errors.Is(err, repository.ErrSocketRateLimited) || errors.Is(err, repository.ErrSocketCapacity) {
		respondError(w, http.StatusTooManyRequests, "socket ticket limit reached")
		return
	}
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to issue socket ticket")
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"ticket": ticket, "expiresAt": expiresAt})
}

func (h *SocketHandler) Connect(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("ticket") {
		respondError(w, http.StatusBadRequest, "query tickets are not supported")
		return
	}
	if !originAllowed(r.Header.Get("Origin"), h.options.AllowedOrigins) {
		respondError(w, http.StatusForbidden, "origin not allowed")
		return
	}
	token, err := extractSocketTicketProtocol(r.Header.Values("Sec-WebSocket-Protocol"))
	if err != nil {
		respondError(w, http.StatusUnauthorized, "invalid socket subprotocol")
		return
	}
	now := time.Now().UTC()
	ticket, err := h.store.ConsumeTicket(r.Context(), token, now)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to consume socket ticket")
		return
	}
	if ticket == nil || !ticket.SocketExpiresAt.After(now) {
		respondError(w, http.StatusUnauthorized, "invalid or expired socket ticket")
		return
	}
	lease, err := h.store.AcquireConnection(r.Context(), ticket.SocketIdentity, h.remoteIP(r), h.options.ConnectionPolicy, now)
	if errors.Is(err, repository.ErrSocketCapacity) || errors.Is(err, repository.ErrSocketRateLimited) {
		respondError(w, http.StatusTooManyRequests, "socket connection limit reached")
		return
	}
	if err != nil {
		respondError(w, http.StatusServiceUnavailable, "socket capacity unavailable")
		return
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), socketTerminalTimeout)
		h.store.ReleaseLease(ctx, lease)
		cancel()
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	c := &socketConnection{
		handler: h, conn: conn, identity: ticket.SocketIdentity, socketExpiresAt: ticket.SocketExpiresAt,
		ctx: ctx, cancel: cancel, send: make(chan models.SocketServerEvent, h.options.SendQueue),
		generations: make(map[string]context.CancelFunc), actions: make(chan struct{}, 16), connectionLease: lease,
		bucket: newTokenBucket(h.options.MessageBurst, h.options.MessagesPerMinute, now),
	}
	c.run()
}

type socketConnection struct {
	handler         *SocketHandler
	conn            *websocket.Conn
	identity        models.SocketIdentity
	socketExpiresAt time.Time
	connectionLease *models.SocketLease
	ctx             context.Context
	cancel          context.CancelFunc
	send            chan models.SocketServerEvent
	actions         chan struct{}
	eventMu         sync.Mutex
	genMu           sync.Mutex
	generations     map[string]context.CancelFunc
	closeOnce       sync.Once
	closeCode       int
	closeText       string
	localSeq        atomic.Int64
	bucket          *tokenBucket
}

func (c *socketConnection) run() {
	c.closeCode, c.closeText = websocket.CloseNormalClosure, "connection closed"
	c.conn.SetReadLimit(c.handler.options.ReadLimit)
	_ = c.conn.SetReadDeadline(time.Now().Add(c.handler.options.IdleTimeout))
	c.conn.SetPongHandler(func(string) error { return c.conn.SetReadDeadline(time.Now().Add(c.handler.options.IdleTimeout)) })
	writerDone := make(chan struct{})
	go func() { defer close(writerDone); c.writeLoop() }()
	go c.connectionGuards()
	c.sendEphemeral("connection.ready", "", "", map[string]any{
		"accessProfile": c.identity.AccessProfile, "projectId": c.identity.ProjectID,
		"authExpiresAt": c.socketExpiresAt,
	})
	c.readLoop()
	c.shutdown(websocket.CloseNormalClosure, "connection closed")
	<-writerDone
	c.releaseLease(c.connectionLease)
}

func (c *socketConnection) connectionGuards() {
	authTimer := time.NewTimer(time.Until(c.socketExpiresAt))
	leaseTicker := time.NewTicker(c.handler.options.PingInterval)
	defer authTimer.Stop()
	defer leaseTicker.Stop()
	for {
		select {
		case <-authTimer.C:
			c.shutdown(socketCloseAuth, "authentication expired")
			return
		case now := <-leaseTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), socketWriteTimeout)
			err := c.handler.store.RenewLease(ctx, c.connectionLease, c.handler.options.ConnectionPolicy.LeaseTTL, now)
			cancel()
			if err != nil {
				c.shutdown(socketCloseBusy, "connection lease unavailable")
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *socketConnection) readLoop() {
	for {
		messageType, payload, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(c.handler.options.IdleTimeout))
		if messageType != websocket.TextMessage {
			c.sendEphemeral("error", "", "", errorData("invalid_message", "text messages are required"))
			continue
		}
		msg, err := parseSocketMessage(payload)
		if err != nil {
			c.sendEphemeral("error", "", "", errorData("invalid_message", err.Error()))
			continue
		}
		switch msg.Type {
		case "ping":
			c.sendEphemeral("pong", msg.RequestID, msg.SessionID, map[string]any{})
			continue
		case "session.resume":
			if !c.bucket.Allow(time.Now()) {
				c.shutdown(socketCloseRate, "message rate exceeded")
				return
			}
			c.handleResume(msg) // synchronous: no later action can overtake replay
			continue
		case "generation.cancel":
			if !c.bucket.Allow(time.Now()) {
				c.shutdown(socketCloseRate, "message rate exceeded")
				return
			}
			c.handleCancel(msg)
			continue
		case "chat.send", "approval.decide":
			if !c.bucket.Allow(time.Now()) {
				c.shutdown(socketCloseRate, "message rate exceeded")
				return
			}
		default:
			c.sendEphemeral("error", msg.RequestID, msg.SessionID, errorData("unsupported_type", "unsupported message type"))
			continue
		}
		select {
		case c.actions <- struct{}{}:
			go func() { defer func() { <-c.actions }(); c.handleAction(msg) }()
		default:
			c.shutdown(socketCloseBusy, "too many concurrent actions")
			return
		}
	}
}

func (c *socketConnection) writeLoop() {
	ticker := time.NewTicker(c.handler.options.PingInterval)
	defer ticker.Stop()
	defer func() {
		_ = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(c.closeCode, c.closeText), time.Now().Add(socketWriteTimeout))
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

func (c *socketConnection) handleAction(msg models.SocketClientMessage) {
	if msg.Type == "chat.send" {
		c.handleChatSend(msg)
	} else {
		c.handleApproval(msg)
	}
}

func (c *socketConnection) handleChatSend(msg models.SocketClientMessage) {
	var data struct{ Message, ProjectID, AccessProfile string }
	if err := decodeSocketData(msg, &data); err != nil || strings.TrimSpace(data.Message) == "" {
		c.sendError(msg, "invalid_chat_send", "message, projectId and accessProfile are required")
		return
	}
	if !validActionIdentifiers(msg.RequestID, msg.IdempotencyKey) {
		c.sendError(msg, "invalid_chat_send", "requestId and idempotencyKey are required")
		return
	}
	if data.ProjectID != c.identity.ProjectID || data.AccessProfile != c.identity.AccessProfile {
		c.sendError(msg, "scope_mismatch", "chat scope does not match socket ticket")
		return
	}
	if msg.SessionID != "" {
		if _, err := c.authorizeSession(msg.SessionID); err != nil {
			c.sendError(msg, "session_forbidden", err.Error())
			return
		}
	}
	if c.identity.AccessProfile == models.AccessProfileDeveloper {
		if err := c.revalidateCurrentAuthorization(); err != nil {
			c.closeAuthorizationError(err)
			return
		}
	}
	binding := c.actionBinding(msg, repository.CanonicalPayloadHash(struct {
		Message       string `json:"message"`
		ProjectID     string `json:"projectId"`
		AccessProfile string `json:"accessProfile"`
	}{strings.TrimSpace(data.Message), data.ProjectID, data.AccessProfile}))
	receipt, created, err := c.handler.store.BeginReceipt(c.ctx, binding, msg.RequestID, time.Now())
	if errors.Is(err, repository.ErrSocketReceiptMismatch) {
		c.sendError(msg, "idempotency_conflict", err.Error())
		return
	}
	if err != nil {
		c.sendError(msg, "idempotency_unavailable", "could not record action")
		return
	}
	if !created {
		c.replayReceipt(msg, binding, receipt)
		return
	}
	genLease, err := c.handler.store.AcquireGeneration(c.ctx, c.identity, c.handler.options.GenerationPolicy, time.Now())
	if err != nil {
		c.updateReceiptBackground(binding, "failed", msg.SessionID, "error", errorData("generation_capacity", "generation limit reached"))
		c.shutdown(socketCloseRate, "generation limit reached")
		return
	}
	defer c.releaseLease(genLease)
	genCtx, cancel := context.WithCancel(c.ctx)
	if !c.registerGeneration(msg.RequestID, cancel) {
		cancel()
		c.updateReceiptBackground(binding, "failed", msg.SessionID, "error", errorData("request_conflict", "requestId is already active"))
		c.sendError(msg, "request_conflict", "requestId is already active")
		return
	}
	go c.renewGenerationLease(genCtx, genLease, cancel)
	defer func() { cancel(); c.unregisterGeneration(msg.RequestID) }()
	req := models.ChatRequest{
		CompanyID: c.identity.CompanyID, UserID: c.identity.UserID, ConversationID: msg.SessionID,
		Question: strings.TrimSpace(data.Message), AccessProfile: data.AccessProfile, ProjectID: data.ProjectID,
	}
	request := requestWithSocketIdentity(genCtx, c.identity)
	prepared, status, errMsg := c.handler.agent.prepareAgentRequest(request, req)
	if errMsg != "" {
		failed := map[string]any{"code": "chat_rejected", "message": errMsg, "status": status}
		c.updateReceiptBackground(binding, "failed", msg.SessionID, "error", failed)
		c.sendEvent("error", msg.RequestID, msg.SessionID, failed)
		return
	}
	sessionID := prepared.session.ID
	if err := c.handler.store.UpdateReceipt(c.ctx, binding, "pending", sessionID, "", nil); err != nil {
		c.shutdown(socketCloseBusy, "receipt persistence unavailable")
		return
	}
	c.sendEvent("chat.accepted", msg.RequestID, sessionID, map[string]any{"idempotencyKey": msg.IdempotencyKey})
	completed := c.handler.agent.completeAgentRequest(request, prepared, func(event models.ToolTraceEvent) {
		c.sendEvent("trace", msg.RequestID, sessionID, objectData(event))
	})
	if completed.result.cancelled || genCtx.Err() != nil {
		data := map[string]any{"targetRequestId": msg.RequestID, "status": "cancelled"}
		c.persistTerminal(binding, "cancelled", "generation.cancelled", msg.RequestID, sessionID, data)
		return
	}
	for _, chunk := range chunkText(completed.result.answer, 200) {
		c.sendEvent("content.delta", msg.RequestID, sessionID, map[string]any{"content": chunk})
	}
	meta := buildStreamMeta(sessionID, completed.result, completed.toolTrace, prepared.sources)
	c.sendEvent("chat.meta", msg.RequestID, sessionID, objectData(meta))
	for _, approval := range completed.result.pendingApprovals {
		c.sendEvent("approval.requested", msg.RequestID, sessionID, objectData(approval))
	}
	c.persistTerminal(binding, "completed", "generation.completed", msg.RequestID, sessionID, map[string]any{"status": "completed"})
}

func (c *socketConnection) handleApproval(msg models.SocketClientMessage) {
	var data struct{ ApprovalID, Decision string }
	if err := decodeSocketData(msg, &data); err != nil || data.ApprovalID == "" ||
		(data.Decision != models.ApprovalDecisionApprove && data.Decision != models.ApprovalDecisionReject) {
		c.sendError(msg, "invalid_approval", "approvalId and a valid decision are required")
		return
	}
	if !validActionIdentifiers(msg.RequestID, msg.IdempotencyKey) {
		c.sendError(msg, "invalid_approval", "requestId and idempotencyKey are required")
		return
	}
	rec, err := c.handler.chat.approvalRepo.GetByID(c.ctx, data.ApprovalID)
	if err != nil || rec == nil {
		c.sendError(msg, "approval_not_found", "approval not found")
		return
	}
	if msg.SessionID != "" && msg.SessionID != rec.SessionID {
		c.sendError(msg, "scope_mismatch", "approval belongs to a different session")
		return
	}
	if _, err := c.authorizeSession(rec.SessionID); err != nil {
		c.sendError(msg, "session_forbidden", err.Error())
		return
	}
	if err := c.revalidateCurrentAuthorization(); err != nil {
		c.closeAuthorizationError(err)
		return
	}
	msg.SessionID = rec.SessionID
	binding := c.actionBinding(msg, repository.CanonicalPayloadHash(struct {
		ApprovalID string `json:"approvalId"`
		Decision   string `json:"decision"`
	}{data.ApprovalID, data.Decision}))
	receipt, created, err := c.handler.store.BeginReceipt(c.ctx, binding, msg.RequestID, time.Now())
	if errors.Is(err, repository.ErrSocketReceiptMismatch) {
		c.sendError(msg, "idempotency_conflict", err.Error())
		return
	}
	if err != nil {
		c.sendError(msg, "idempotency_unavailable", "could not record action")
		return
	}
	if !created {
		c.replayReceipt(msg, binding, receipt)
		return
	}
	status, result, errMsg := c.handler.chat.decideApproval(requestWithSocketIdentity(c.ctx, c.identity), data.ApprovalID, data.Decision)
	if errMsg != "" {
		failed := map[string]any{"code": "approval_failed", "message": errMsg, "status": status}
		c.updateReceiptBackground(binding, "failed", rec.SessionID, "error", failed)
		c.sendEvent("error", msg.RequestID, rec.SessionID, failed)
		return
	}
	safeResult := map[string]any{"approvalId": data.ApprovalID, "decision": data.Decision, "status": result["status"]}
	c.persistTerminalWithLive(binding, "completed", "approval.resolved", msg.RequestID, rec.SessionID, safeResult, result)
}

func (c *socketConnection) handleCancel(msg models.SocketClientMessage) {
	var data struct {
		TargetRequestID string `json:"targetRequestId"`
	}
	if err := decodeSocketData(msg, &data); err != nil || data.TargetRequestID == "" {
		c.sendError(msg, "invalid_cancel", "targetRequestId is required")
		return
	}
	c.genMu.Lock()
	cancel := c.generations[data.TargetRequestID]
	c.genMu.Unlock()
	if cancel == nil {
		c.sendError(msg, "generation_not_found", "active generation not found")
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
		c.sendError(msg, "invalid_resume", "sessionId and non-negative lastSeq are required")
		return
	}
	if msg.SessionID != "" && msg.SessionID != data.SessionID {
		c.sendError(msg, "scope_mismatch", "sessionId values do not match")
		return
	}
	if _, err := c.authorizeSession(data.SessionID); err != nil {
		c.sendError(msg, "session_forbidden", err.Error())
		return
	}
	c.eventMu.Lock()
	window, err := c.handler.store.ReplayEvents(c.ctx, c.identity, data.SessionID, data.LastSeq)
	if err == nil {
		for _, event := range window.Events {
			if !c.enqueue(event) {
				break
			}
		}
		c.sendEphemeral("replay.completed", msg.RequestID, data.SessionID, map[string]any{
			"replayed": len(window.Events), "afterSeq": data.LastSeq,
			"earliestAvailableSeq": window.EarliestAvailableSeq, "latestSeq": window.LatestSeq, "gapDetected": window.GapDetected,
		})
	}
	c.eventMu.Unlock()
	if err != nil {
		c.sendError(msg, "replay_unavailable", "could not load replay")
		return
	}
}

func (c *socketConnection) authorizeSession(sessionID string) (*models.ChatSession, error) {
	session, err := c.handler.chat.sessionRepo.GetByID(c.ctx, sessionID)
	if err != nil {
		return nil, errors.New("failed to load session")
	}
	if session == nil {
		return nil, errors.New("session not found")
	}
	if session.CompanyID != c.identity.CompanyID || session.UserID != c.identity.UserID || models.NormalizeAccessProfile(session.AccessProfile) != c.identity.AccessProfile {
		return nil, errors.New("session does not match socket ticket")
	}
	if session.ProjectID == "" {
		bound, err := c.handler.chat.sessionRepo.BindLegacyProject(c.ctx, session.ID, c.identity.CompanyID, c.identity.UserID, c.identity.AccessProfile, c.identity.ProjectID)
		if err != nil {
			return nil, errors.New("failed to bind legacy session")
		}
		session = bound
	}
	if session == nil || session.ProjectID != c.identity.ProjectID {
		return nil, errors.New("session does not match socket ticket")
	}
	return session, nil
}

func (c *socketConnection) revalidateCurrentAuthorization() error {
	if time.Now().After(c.socketExpiresAt) {
		return errors.New("authentication expired")
	}
	if c.identity.AccessProfile == models.AccessProfileDeveloper && !c.identity.IsAdmin {
		return errors.New("developer authorization expired")
	}
	request := requestWithSocketIdentity(c.ctx, c.identity)
	project, err := c.handler.agent.lookupProjectFresh(request, c.identity.CompanyID, c.identity.ProjectID)
	if err != nil {
		return errAuthorizationUnavailable
	}
	if project == nil || (project.CompanyID != "" && project.CompanyID != c.identity.CompanyID) {
		return errors.New("project authorization unavailable")
	}
	if c.identity.AccessProfile == models.AccessProfileDeveloper && !project.Visibility.Developer {
		return errors.New("developer project access revoked")
	}
	if c.identity.AccessProfile == models.AccessProfileClient && !project.Visibility.Client {
		return errors.New("client project access revoked")
	}
	return nil
}

func (c *socketConnection) actionBinding(msg models.SocketClientMessage, payloadHash string) models.SocketActionBinding {
	return models.SocketActionBinding{
		CompanyID: c.identity.CompanyID, UserID: c.identity.UserID, AccessProfile: c.identity.AccessProfile,
		ProjectID: c.identity.ProjectID, SessionID: msg.SessionID, ActionType: msg.Type,
		PayloadHash: payloadHash, IdempotencyKey: msg.IdempotencyKey,
	}
}

func (c *socketConnection) replayReceipt(msg models.SocketClientMessage, binding models.SocketActionBinding, receipt *models.SocketActionReceipt) {
	if receipt.SessionID != "" {
		if _, err := c.authorizeSession(receipt.SessionID); err != nil {
			c.sendError(msg, "session_forbidden", err.Error())
			return
		}
	}
	if binding.ActionType == models.SocketActionApprovalDecide || c.identity.AccessProfile == models.AccessProfileDeveloper {
		if err := c.revalidateCurrentAuthorization(); err != nil {
			c.closeAuthorizationError(err)
			return
		}
	}
	if receipt.SessionID != "" && binding.ActionType == models.SocketActionChatSend {
		c.sendEvent("chat.accepted", msg.RequestID, receipt.SessionID, map[string]any{"duplicate": true})
	}
	if receipt.FinalEventType != "" {
		c.sendEvent(receipt.FinalEventType, msg.RequestID, receipt.SessionID, receipt.FinalData)
		return
	}
	c.sendError(msg, "action_in_progress", "action is already pending")
}

func (c *socketConnection) closeAuthorizationError(err error) {
	if errors.Is(err, errAuthorizationUnavailable) {
		c.shutdown(socketCloseBusy, err.Error())
		return
	}
	c.shutdown(socketCloseForbidden, err.Error())
}

func (c *socketConnection) sendEvent(eventType, requestID, sessionID string, data map[string]any) {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()
	event, _, err := c.handler.store.RecordEvent(c.ctx, c.identity, models.SocketServerEvent{
		Type: eventType, RequestID: requestID, SessionID: sessionID, Timestamp: time.Now().UTC(), Data: data,
	})
	if err != nil {
		c.shutdown(socketCloseBusy, "event persistence failed")
		return
	}
	c.enqueue(*event)
}

func (c *socketConnection) sendEphemeral(eventType, requestID, sessionID string, data map[string]any) {
	c.enqueue(models.SocketServerEvent{Seq: c.localSeq.Add(1), Type: eventType, RequestID: requestID, SessionID: sessionID, Timestamp: time.Now().UTC(), Data: data})
}

func (c *socketConnection) sendError(msg models.SocketClientMessage, code, message string) {
	c.sendEphemeral("error", msg.RequestID, msg.SessionID, errorData(code, message))
}

func (c *socketConnection) persistTerminal(binding models.SocketActionBinding, state, eventType, requestID, sessionID string, data map[string]any) {
	c.persistTerminalWithLive(binding, state, eventType, requestID, sessionID, data, data)
}

func (c *socketConnection) persistTerminalWithLive(binding models.SocketActionBinding, state, eventType, requestID, sessionID string, receiptData, liveData map[string]any) {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), socketTerminalTimeout)
	defer cancel()
	_ = c.updateReceiptWithRetry(ctx, binding, state, sessionID, eventType, receiptData)
	event, _, err := c.handler.store.RecordEvent(ctx, c.identity, models.SocketServerEvent{
		Type: eventType, RequestID: requestID, SessionID: sessionID, Timestamp: time.Now().UTC(), Data: liveData,
	})
	if err == nil && c.ctx.Err() == nil {
		c.enqueue(*event)
	}
}

func (c *socketConnection) updateReceiptBackground(binding models.SocketActionBinding, state, sessionID, eventType string, data map[string]any) {
	ctx, cancel := context.WithTimeout(context.Background(), socketTerminalTimeout)
	defer cancel()
	_ = c.updateReceiptWithRetry(ctx, binding, state, sessionID, eventType, data)
}

func (c *socketConnection) updateReceiptWithRetry(ctx context.Context, binding models.SocketActionBinding, state, sessionID, eventType string, data map[string]any) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = c.handler.store.UpdateReceipt(ctx, binding, state, sessionID, eventType, data); err == nil {
			return nil
		}
		if errors.Is(err, repository.ErrSocketReceiptMismatch) {
			return err
		}
		select {
		case <-time.After(time.Duration(attempt+1) * 50 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (c *socketConnection) registerGeneration(requestID string, cancel context.CancelFunc) bool {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	if _, exists := c.generations[requestID]; exists {
		return false
	}
	c.generations[requestID] = cancel
	return true
}

func (c *socketConnection) unregisterGeneration(requestID string) {
	c.genMu.Lock()
	delete(c.generations, requestID)
	c.genMu.Unlock()
}

func (c *socketConnection) renewGenerationLease(ctx context.Context, lease *models.SocketLease, cancelGeneration context.CancelFunc) {
	interval := c.handler.options.GenerationPolicy.LeaseTTL / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			renewCtx, cancel := context.WithTimeout(context.Background(), socketWriteTimeout)
			err := c.handler.store.RenewLease(renewCtx, lease, c.handler.options.GenerationPolicy.LeaseTTL, now)
			cancel()
			if err != nil {
				cancelGeneration()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *socketConnection) releaseLease(lease *models.SocketLease) {
	ctx, cancel := context.WithTimeout(context.Background(), socketTerminalTimeout)
	defer cancel()
	c.handler.store.ReleaseLease(ctx, lease)
}

func (c *socketConnection) enqueue(event models.SocketServerEvent) bool {
	select {
	case c.send <- event:
		return true
	default:
		c.shutdown(socketCloseBusy, socketCloseSlow)
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

func extractSocketTicketProtocol(headers []string) (string, error) {
	if len(strings.Join(headers, ",")) > 512 {
		return "", errors.New("subprotocol header too large")
	}
	baseSeen := false
	token := ""
	for _, header := range headers {
		for _, raw := range strings.Split(header, ",") {
			protocol := strings.TrimSpace(raw)
			switch {
			case protocol == baseProtocol:
				if baseSeen {
					return "", errors.New("duplicate base protocol")
				}
				baseSeen = true
			case strings.HasPrefix(protocol, ticketProtocolPrefix):
				if token != "" {
					return "", errors.New("duplicate ticket protocol")
				}
				token = strings.TrimPrefix(protocol, ticketProtocolPrefix)
			default:
				return "", errors.New("unsupported protocol")
			}
		}
	}
	if !baseSeen || !socketTicketPattern.MatchString(token) {
		return "", errors.New("missing or malformed ticket protocol")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != 32 {
		return "", errors.New("malformed ticket")
	}
	return token, nil
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

// PeerIPMiddleware captures the transport peer before RealIP middleware can
// rewrite RemoteAddr from forwarding headers.
func PeerIPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		ctx := context.WithValue(r.Context(), ContextPeerIP, host)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *SocketHandler) remoteIP(r *http.Request) string {
	peer, _ := r.Context().Value(ContextPeerIP).(string)
	if peer == "" {
		peer, _, _ = net.SplitHostPort(r.RemoteAddr)
		if peer == "" {
			peer = r.RemoteAddr
		}
	}
	peerIP := net.ParseIP(peer)
	trusted := false
	for _, network := range h.trustedProxies {
		if peerIP != nil && network.Contains(peerIP) {
			trusted = true
			break
		}
	}
	if trusted {
		forwarded := strings.TrimSpace(r.Header.Get("X-Real-IP"))
		if forwarded == "" {
			forwarded = strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-For"), ",")[0])
		}
		if parsed := net.ParseIP(forwarded); parsed != nil {
			return parsed.String()
		}
	}
	if peerIP != nil {
		return peerIP.String()
	}
	return peer
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

func errorData(code, message string) map[string]any {
	return map[string]any{"code": code, "message": message}
}
func validActionIdentifiers(requestID, idempotencyKey string) bool {
	return requestID != "" && len(requestID) <= 128 && idempotencyKey != "" && len(idempotencyKey) <= 256
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

type tokenBucket struct {
	mu        sync.Mutex
	tokens    float64
	burst     float64
	perSecond float64
	last      time.Time
}

func newTokenBucket(burst, perMinute int, now time.Time) *tokenBucket {
	return &tokenBucket{tokens: float64(burst), burst: float64(burst), perSecond: float64(perMinute) / 60, last: now}
}

func (b *tokenBucket) Allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens += now.Sub(b.last).Seconds() * b.perSecond
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
