package handlers

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/repository"
	"github.com/gorilla/websocket"
)

type memorySocketStore struct {
	mu           sync.Mutex
	next         int
	tickets      map[string]*models.SocketTicket
	seq          map[string]int64
	events       []storedSocketEvent
	receipts     map[string]*models.SocketActionReceipt
	activeLeases int
	maxLeases    int
	consumeCalls int
}

type storedSocketEvent struct {
	identity models.SocketIdentity
	event    models.SocketServerEvent
}

func newMemorySocketStore() *memorySocketStore {
	return &memorySocketStore{
		tickets: make(map[string]*models.SocketTicket), seq: make(map[string]int64),
		receipts: make(map[string]*models.SocketActionReceipt),
	}
}

func (s *memorySocketStore) IssueTicket(_ context.Context, identity models.SocketIdentity, socketExpiresAt time.Time, _ repository.TicketPolicy, now time.Time) (string, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	raw := make([]byte, 32)
	raw[len(raw)-1] = byte(s.next)
	token := base64.RawURLEncoding.EncodeToString(raw)
	expires := now.Add(30 * time.Second)
	s.tickets[token] = &models.SocketTicket{SocketIdentity: identity, ExpiresAt: expires, SocketExpiresAt: socketExpiresAt}
	return token, expires, nil
}

func (s *memorySocketStore) ConsumeTicket(_ context.Context, token string, now time.Time) (*models.SocketTicket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumeCalls++
	ticket := s.tickets[token]
	if ticket == nil || ticket.ConsumedAt != nil || !ticket.ExpiresAt.After(now) || !ticket.SocketExpiresAt.After(now) {
		return nil, nil
	}
	consumed := now.UTC()
	ticket.ConsumedAt = &consumed
	copy := *ticket
	return &copy, nil
}

func socketScope(identity models.SocketIdentity, sessionID string) string {
	return identity.CompanyID + "\x00" + identity.UserID + "\x00" + sessionID
}

func (s *memorySocketStore) RecordEvent(_ context.Context, identity models.SocketIdentity, event models.SocketServerEvent) (*models.SocketServerEvent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	scope := socketScope(identity, event.SessionID)
	s.seq[scope]++
	event.Seq = s.seq[scope]
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	s.events = append(s.events, storedSocketEvent{identity: identity, event: event})
	return &event, true, nil
}

func (s *memorySocketStore) ReplayEvents(_ context.Context, identity models.SocketIdentity, sessionID string, afterSeq int64) (*models.SocketReplay, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []models.SocketServerEvent
	for _, stored := range s.events {
		if stored.identity.CompanyID == identity.CompanyID && stored.identity.UserID == identity.UserID &&
			stored.event.SessionID == sessionID && stored.event.Seq > afterSeq {
			result = append(result, stored.event)
		}
	}
	latest := s.seq[socketScope(identity, sessionID)]
	earliest := int64(0)
	if len(result) > 0 {
		earliest = result[0].Seq
	}
	return &models.SocketReplay{Events: result, EarliestAvailableSeq: earliest, LatestSeq: latest}, nil
}

func receiptKey(identity models.SocketIdentity, key string) string {
	return identity.CompanyID + "\x00" + identity.UserID + "\x00" + key
}

func (s *memorySocketStore) BeginReceipt(_ context.Context, binding models.SocketActionBinding, requestID string, now time.Time) (*models.SocketActionReceipt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity := models.SocketIdentity{CompanyID: binding.CompanyID, UserID: binding.UserID}
	id := receiptKey(identity, binding.IdempotencyKey)
	if receipt := s.receipts[id]; receipt != nil {
		copy := *receipt
		if receipt.AccessProfile != binding.AccessProfile || receipt.ProjectID != binding.ProjectID || receipt.BoundSessionID != binding.SessionID || receipt.ActionType != binding.ActionType || receipt.PayloadHash != binding.PayloadHash {
			return &copy, false, repository.ErrSocketReceiptMismatch
		}
		return &copy, false, nil
	}
	receipt := &models.SocketActionReceipt{
		ID: id, CompanyID: identity.CompanyID, UserID: identity.UserID,
		AccessProfile: binding.AccessProfile, ProjectID: binding.ProjectID, BoundSessionID: binding.SessionID,
		ActionType: binding.ActionType, PayloadHash: binding.PayloadHash, RequestID: requestID, State: "claimed",
		CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(24 * time.Hour),
	}
	s.receipts[id] = receipt
	copy := *receipt
	return &copy, true, nil
}

func (s *memorySocketStore) UpdateReceipt(_ context.Context, binding models.SocketActionBinding, state, sessionID, finalType string, data map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity := models.SocketIdentity{CompanyID: binding.CompanyID, UserID: binding.UserID}
	receipt := s.receipts[receiptKey(identity, binding.IdempotencyKey)]
	if receipt == nil {
		return errors.New("receipt not found")
	}
	receipt.State, receipt.SessionID, receipt.FinalEventType, receipt.FinalData = state, sessionID, finalType, data
	return nil
}

func (s *memorySocketStore) AcquireConnection(_ context.Context, _ models.SocketIdentity, _ string, policy repository.ConnectionPolicy, now time.Time) (*models.SocketLease, error) {
	return s.acquireLease(policy.GlobalLimit, policy.LeaseTTL, now)
}

func (s *memorySocketStore) AcquireGeneration(_ context.Context, _ models.SocketIdentity, policy repository.GenerationPolicy, now time.Time) (*models.SocketLease, error) {
	return s.acquireLease(policy.UserLimit, policy.LeaseTTL, now)
}

func (s *memorySocketStore) acquireLease(limit int, ttl time.Duration, now time.Time) (*models.SocketLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxLeases > 0 {
		limit = s.maxLeases
	}
	if s.activeLeases >= limit {
		return nil, repository.ErrSocketCapacity
	}
	s.activeLeases++
	return &models.SocketLease{ConnectionID: fmt.Sprintf("lease-%d", s.activeLeases), LeaseIDs: []string{"slot"}, ExpiresAt: now.Add(ttl)}, nil
}

func (s *memorySocketStore) RenewLease(_ context.Context, lease *models.SocketLease, ttl time.Duration, now time.Time) error {
	lease.ExpiresAt = now.Add(ttl)
	return nil
}

func (s *memorySocketStore) ReleaseLease(_ context.Context, lease *models.SocketLease) {
	if lease == nil {
		return
	}
	s.mu.Lock()
	if s.activeLeases > 0 {
		s.activeLeases--
	}
	s.mu.Unlock()
}

func TestSocketTicketExpiresAndIsConsumedOnceAtomically(t *testing.T) {
	store := newMemorySocketStore()
	identity := models.SocketIdentity{UserID: "user-1", CompanyID: "company-1"}
	now := time.Now().UTC()
	token, expires, err := store.IssueTicket(context.Background(), identity, now.Add(time.Hour), repository.TicketPolicy{}, now)
	if err != nil || expires.Sub(now) != 30*time.Second {
		t.Fatalf("issue ticket: expires=%v err=%v", expires, err)
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ticket, _ := store.ConsumeTicket(context.Background(), token, now.Add(time.Second)); ticket != nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("expected exactly one atomic consume, got %d", successes.Load())
	}
	expired, _, _ := store.IssueTicket(context.Background(), identity, now.Add(time.Hour), repository.TicketPolicy{}, now)
	if ticket, _ := store.ConsumeTicket(context.Background(), expired, now.Add(31*time.Second)); ticket != nil {
		t.Fatal("expired ticket was consumed")
	}
}

func TestSocketOriginRejectedWithoutConsumingTicket(t *testing.T) {
	store := newMemorySocketStore()
	identity := models.SocketIdentity{UserID: "user-1", CompanyID: "company-1", AccessProfile: "client", ProjectID: "project-1"}
	token, _, _ := store.IssueTicket(context.Background(), identity, time.Now().Add(time.Hour), repository.TicketPolicy{}, time.Now())
	handler := NewSocketHandler(store, nil, nil, SocketOptions{AllowedOrigins: []string{"https://dev2.solutions"}})
	server := httptest.NewServer(http.HandlerFunc(handler.Connect))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/chat/ws"
	dialer := websocket.Dialer{Subprotocols: []string{baseProtocol, ticketProtocolPrefix + token}}

	badHeader := http.Header{"Origin": []string{"https://evil.example"}}
	if conn, resp, err := dialer.Dial(wsURL, badHeader); err == nil {
		conn.Close()
		t.Fatal("unexpected successful connection from rejected origin")
	} else if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 origin rejection, response=%v err=%v", resp, err)
	}

	goodHeader := http.Header{"Origin": []string{"https://dev2.solutions"}}
	conn, _, err := dialer.Dial(wsURL, goodHeader)
	if err != nil {
		t.Fatalf("allowed origin could not use unconsumed ticket: %v", err)
	}
	var ready models.SocketServerEvent
	if err := conn.ReadJSON(&ready); err != nil || ready.Type != "connection.ready" {
		t.Fatalf("expected connection.ready, event=%#v err=%v", ready, err)
	}
	if conn.Subprotocol() != baseProtocol {
		t.Fatalf("server echoed unsafe subprotocol %q", conn.Subprotocol())
	}
	conn.Close()

	if conn, resp, err := dialer.Dial(wsURL, goodHeader); err == nil {
		conn.Close()
		t.Fatal("consumed ticket connected twice")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for reused ticket, response=%v err=%v", resp, err)
	}
}

func TestSocketRequiresTicketSubprotocolAndRejectsQueryBeforeConsume(t *testing.T) {
	store := newMemorySocketStore()
	identity := models.SocketIdentity{UserID: "u", CompanyID: "c", AccessProfile: "client", ProjectID: "p"}
	token, _, _ := store.IssueTicket(context.Background(), identity, time.Now().Add(time.Hour), repository.TicketPolicy{}, time.Now())
	handler := NewSocketHandler(store, nil, nil, SocketOptions{AllowedOrigins: []string{"https://dev2.solutions"}})

	queryRequest := httptest.NewRequest(http.MethodGet, "/chat/ws?ticket="+token, nil)
	queryRequest.Header.Set("Origin", "https://dev2.solutions")
	queryResponse := httptest.NewRecorder()
	handler.Connect(queryResponse, queryRequest)
	if queryResponse.Code != http.StatusBadRequest || store.consumeCalls != 0 {
		t.Fatalf("query ticket reached storage: status=%d consumes=%d", queryResponse.Code, store.consumeCalls)
	}

	missingRequest := httptest.NewRequest(http.MethodGet, "/chat/ws", nil)
	missingRequest.Header.Set("Origin", "https://dev2.solutions")
	missingResponse := httptest.NewRecorder()
	handler.Connect(missingResponse, missingRequest)
	if missingResponse.Code != http.StatusUnauthorized || store.consumeCalls != 0 {
		t.Fatalf("missing protocol reached storage: status=%d consumes=%d", missingResponse.Code, store.consumeCalls)
	}

	for _, header := range []string{
		baseProtocol + ", " + ticketProtocolPrefix + "short",
		baseProtocol + ", " + ticketProtocolPrefix + token + ", extra",
		strings.Repeat("x", 513),
	} {
		if _, err := extractSocketTicketProtocol([]string{header}); err == nil {
			t.Fatalf("malformed protocol accepted: %q", header)
		}
	}
}

func TestSocketClosesAtAuthorizationExpiry(t *testing.T) {
	store := newMemorySocketStore()
	now := time.Now()
	identity := models.SocketIdentity{UserID: "u", CompanyID: "c", AccessProfile: "client", ProjectID: "p", AuthExpiresAt: now.Add(time.Minute)}
	token, _, _ := store.IssueTicket(context.Background(), identity, now.Add(200*time.Millisecond), repository.TicketPolicy{}, now)
	handler := NewSocketHandler(store, nil, nil, SocketOptions{AllowedOrigins: []string{"https://dev2.solutions"}, PingInterval: time.Second})
	server := httptest.NewServer(http.HandlerFunc(handler.Connect))
	defer server.Close()
	dialer := websocket.Dialer{Subprotocols: []string{baseProtocol, ticketProtocolPrefix + token}}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/chat/ws", http.Header{"Origin": []string{"https://dev2.solutions"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var ready models.SocketServerEvent
	if err := conn.ReadJSON(&ready); err != nil || ready.Data["authExpiresAt"] == nil {
		t.Fatalf("missing auth expiry in ready event: %#v err=%v", ready, err)
	}
	_, _, err = conn.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != socketCloseAuth {
		t.Fatalf("expected auth close %d, got %v", socketCloseAuth, err)
	}
}

func TestSocketUsesRateAndForbiddenCloseCodes(t *testing.T) {
	t.Run("rate", func(t *testing.T) {
		store := newMemorySocketStore()
		now := time.Now()
		identity := models.SocketIdentity{UserID: "u", CompanyID: "c", AccessProfile: "client", ProjectID: "p", AuthExpiresAt: now.Add(time.Hour)}
		token, _, _ := store.IssueTicket(context.Background(), identity, now.Add(time.Hour), repository.TicketPolicy{}, now)
		handler := NewSocketHandler(store, nil, nil, SocketOptions{
			AllowedOrigins: []string{"https://dev2.solutions"}, MessageBurst: 1, MessagesPerMinute: 1,
		})
		server := httptest.NewServer(http.HandlerFunc(handler.Connect))
		defer server.Close()
		dialer := websocket.Dialer{Subprotocols: []string{baseProtocol, ticketProtocolPrefix + token}}
		conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/chat/ws", http.Header{"Origin": []string{"https://dev2.solutions"}})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		var ready models.SocketServerEvent
		if err := conn.ReadJSON(&ready); err != nil {
			t.Fatal(err)
		}
		message := map[string]any{"type": "generation.cancel", "data": map[string]any{"targetRequestId": "missing"}}
		if err := conn.WriteJSON(message); err != nil {
			t.Fatal(err)
		}
		var response models.SocketServerEvent
		if err := conn.ReadJSON(&response); err != nil || response.Type != "error" {
			t.Fatalf("first action failed: %#v %v", response, err)
		}
		if err := conn.WriteJSON(map[string]any{"type": "ping", "data": map[string]any{}}); err != nil {
			t.Fatal(err)
		}
		if err := conn.ReadJSON(&response); err != nil || response.Type != "pong" {
			t.Fatalf("ping consumed action capacity: %#v %v", response, err)
		}
		store.mu.Lock()
		persistedEvents := len(store.events)
		store.mu.Unlock()
		if persistedEvents != 0 {
			t.Fatalf("routine error/ping events were persisted: %d", persistedEvents)
		}
		if err := conn.WriteJSON(message); err != nil {
			t.Fatal(err)
		}
		_, _, err = conn.ReadMessage()
		var closeErr *websocket.CloseError
		if !errors.As(err, &closeErr) || closeErr.Code != socketCloseRate {
			t.Fatalf("expected 4429, got %v", err)
		}
	})

	t.Run("forbidden", func(t *testing.T) {
		store := newMemorySocketStore()
		now := time.Now()
		identity := models.SocketIdentity{UserID: "u", CompanyID: "c", IsAdmin: false, AccessProfile: "developer", ProjectID: "p", AuthExpiresAt: now.Add(time.Hour)}
		token, _, _ := store.IssueTicket(context.Background(), identity, now.Add(time.Hour), repository.TicketPolicy{}, now)
		handler := NewSocketHandler(store, &AgentHandler{}, nil, SocketOptions{AllowedOrigins: []string{"https://dev2.solutions"}})
		server := httptest.NewServer(http.HandlerFunc(handler.Connect))
		defer server.Close()
		dialer := websocket.Dialer{Subprotocols: []string{baseProtocol, ticketProtocolPrefix + token}}
		conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/chat/ws", http.Header{"Origin": []string{"https://dev2.solutions"}})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		var ready models.SocketServerEvent
		if err := conn.ReadJSON(&ready); err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteJSON(map[string]any{
			"type": "chat.send", "requestId": "r", "idempotencyKey": "k",
			"data": map[string]any{"message": "hello", "projectId": "p", "accessProfile": "developer"},
		}); err != nil {
			t.Fatal(err)
		}
		_, _, err = conn.ReadMessage()
		var closeErr *websocket.CloseError
		if !errors.As(err, &closeErr) || closeErr.Code != socketCloseForbidden {
			t.Fatalf("expected 4403, got %v", err)
		}
	})
}

func TestSocketHeartbeatSendsControlPing(t *testing.T) {
	store := newMemorySocketStore()
	identity := models.SocketIdentity{UserID: "user-1", CompanyID: "company-1", AccessProfile: "client", ProjectID: "project-1"}
	token, _, _ := store.IssueTicket(context.Background(), identity, time.Now().Add(time.Hour), repository.TicketPolicy{}, time.Now())
	handler := NewSocketHandler(store, nil, nil, SocketOptions{
		AllowedOrigins: []string{"https://dev2.solutions"},
		PingInterval:   10 * time.Millisecond,
		IdleTimeout:    time.Second,
	})
	server := httptest.NewServer(http.HandlerFunc(handler.Connect))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/chat/ws"
	dialer := websocket.Dialer{Subprotocols: []string{baseProtocol, ticketProtocolPrefix + token}}
	conn, _, err := dialer.Dial(wsURL, http.Header{"Origin": []string{"https://dev2.solutions"}})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	ping := make(chan struct{}, 1)
	conn.SetPingHandler(func(appData string) error {
		select {
		case ping <- struct{}{}:
		default:
		}
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
	})
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	select {
	case <-ping:
	case <-time.After(time.Second):
		t.Fatal("server did not send heartbeat ping")
	}
}

func TestSocketRejectsProfileAndProjectEscalation(t *testing.T) {
	store := newMemorySocketStore()
	ctx, cancel := context.WithCancel(context.Background())
	connection := &socketConnection{
		handler:  &SocketHandler{store: store},
		identity: models.SocketIdentity{UserID: "u", CompanyID: "c", AccessProfile: "client", ProjectID: "p1"},
		ctx:      ctx, cancel: cancel, send: make(chan models.SocketServerEvent, 2),
		generations: make(map[string]context.CancelFunc),
	}
	msg, err := parseSocketMessage([]byte(`{"type":"chat.send","requestId":"r1","idempotencyKey":"k1","data":{"message":"hello","projectId":"p2","accessProfile":"developer"}}`))
	if err != nil {
		t.Fatal(err)
	}
	connection.handleChatSend(msg)
	event := <-connection.send
	if event.Type != "error" || event.Data["code"] != "scope_mismatch" {
		t.Fatalf("expected scope mismatch, got %#v", event)
	}
}

func TestActionReceiptsAreIdempotentForSendAndApproval(t *testing.T) {
	for _, action := range []string{models.SocketActionChatSend, models.SocketActionApprovalDecide} {
		t.Run(action, func(t *testing.T) {
			store := newMemorySocketStore()
			identity := models.SocketIdentity{UserID: "u", CompanyID: "c"}
			var created atomic.Int32
			var wg sync.WaitGroup
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					binding := models.SocketActionBinding{CompanyID: identity.CompanyID, UserID: identity.UserID, AccessProfile: "client", ProjectID: "p", ActionType: action, PayloadHash: "hash", IdempotencyKey: "same-key"}
					_, isNew, err := store.BeginReceipt(context.Background(), binding, fmt.Sprintf("r%d", i), time.Now())
					if err != nil {
						t.Errorf("begin receipt: %v", err)
					}
					if isNew {
						created.Add(1)
					}
				}(i)
			}
			wg.Wait()
			if created.Load() != 1 {
				t.Fatalf("expected one action claim, got %d", created.Load())
			}
		})
	}
}

func TestReceiptBindingMismatchIsRejected(t *testing.T) {
	store := newMemorySocketStore()
	base := models.SocketActionBinding{
		CompanyID: "c", UserID: "u", AccessProfile: "developer", ProjectID: "p1",
		SessionID: "s1", ActionType: models.SocketActionChatSend, PayloadHash: "payload-1", IdempotencyKey: "key",
	}
	if _, created, err := store.BeginReceipt(context.Background(), base, "r1", time.Now()); err != nil || !created {
		t.Fatalf("create receipt: created=%v err=%v", created, err)
	}
	mutations := []models.SocketActionBinding{
		func() models.SocketActionBinding { value := base; value.AccessProfile = "client"; return value }(),
		func() models.SocketActionBinding { value := base; value.ProjectID = "p2"; return value }(),
		func() models.SocketActionBinding { value := base; value.SessionID = "s2"; return value }(),
		func() models.SocketActionBinding {
			value := base
			value.ActionType = models.SocketActionApprovalDecide
			return value
		}(),
		func() models.SocketActionBinding { value := base; value.PayloadHash = "payload-2"; return value }(),
	}
	for _, mutation := range mutations {
		if _, _, err := store.BeginReceipt(context.Background(), mutation, "r2", time.Now()); !errors.Is(err, repository.ErrSocketReceiptMismatch) {
			t.Fatalf("binding mismatch accepted: %#v err=%v", mutation, err)
		}
	}
}

func TestConnectionLeaseAndMessageBucketLimits(t *testing.T) {
	store := newMemorySocketStore()
	store.maxLeases = 1
	policy := repository.ConnectionPolicy{GlobalLimit: 10, LeaseTTL: time.Minute}
	first, err := store.AcquireConnection(context.Background(), models.SocketIdentity{}, "127.0.0.1", policy, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireConnection(context.Background(), models.SocketIdentity{}, "127.0.0.1", policy, time.Now()); !errors.Is(err, repository.ErrSocketCapacity) {
		t.Fatalf("connection capacity not enforced: %v", err)
	}
	store.ReleaseLease(context.Background(), first)
	if _, err := store.AcquireConnection(context.Background(), models.SocketIdentity{}, "127.0.0.1", policy, time.Now()); err != nil {
		t.Fatalf("released lease was not reusable: %v", err)
	}

	now := time.Now()
	bucket := newTokenBucket(2, 60, now)
	if !bucket.Allow(now) || !bucket.Allow(now) || bucket.Allow(now) {
		t.Fatal("token bucket burst was not enforced")
	}
	if !bucket.Allow(now.Add(time.Second)) {
		t.Fatal("token bucket did not refill")
	}
}

func TestSocketSequenceAndReplayAuthorization(t *testing.T) {
	store := newMemorySocketStore()
	owner := models.SocketIdentity{UserID: "u1", CompanyID: "c1"}
	otherUser := models.SocketIdentity{UserID: "u2", CompanyID: "c1"}
	for i := 0; i < 3; i++ {
		event, _, err := store.RecordEvent(context.Background(), owner, models.SocketServerEvent{Type: "trace", SessionID: "s1", Data: map[string]any{}})
		if err != nil || event.Seq != int64(i+1) {
			t.Fatalf("event %d sequence=%v err=%v", i, event.Seq, err)
		}
	}
	replayed, _ := store.ReplayEvents(context.Background(), owner, "s1", 1)
	if len(replayed.Events) != 2 || replayed.Events[0].Seq != 2 || replayed.Events[1].Seq != 3 {
		t.Fatalf("unexpected replay: %#v", replayed)
	}
	unauthorized, _ := store.ReplayEvents(context.Background(), otherUser, "s1", 0)
	if len(unauthorized.Events) != 0 {
		t.Fatalf("other user replayed owner events: %#v", unauthorized)
	}
}

func TestSocketEnvelopeParsing(t *testing.T) {
	valid := `{"type":"ping","requestId":"r1","data":{}}`
	if msg, err := parseSocketMessage([]byte(valid)); err != nil || msg.Type != "ping" {
		t.Fatalf("valid envelope rejected: %#v %v", msg, err)
	}
	for _, input := range []string{
		`{"type":"ping"}`,
		`{"type":"ping","data":[]}`,
		`{"type":"ping","data":{},"unexpected":true}`,
	} {
		if _, err := parseSocketMessage([]byte(input)); err == nil {
			t.Fatalf("invalid envelope accepted: %s", input)
		}
	}
}

func TestSocketBackpressureClosesAndCancellationTargetsGeneration(t *testing.T) {
	store := newMemorySocketStore()
	ctx, cancel := context.WithCancel(context.Background())
	generationCtx, generationCancel := context.WithCancel(context.Background())
	connection := &socketConnection{
		handler: &SocketHandler{store: store}, ctx: ctx, cancel: cancel,
		send: make(chan models.SocketServerEvent, 1), generations: map[string]context.CancelFunc{"target": generationCancel},
	}
	connection.handleCancel(models.SocketClientMessage{Data: []byte(`{"targetRequestId":"target"}`)})
	select {
	case <-generationCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("generation cancellation did not cancel target context")
	}
	connection.send <- models.SocketServerEvent{Type: "trace"}
	if connection.enqueue(models.SocketServerEvent{Type: "trace"}) {
		t.Fatal("full queue accepted another event")
	}
	if connection.closeCode != socketCloseBusy || connection.closeText != socketCloseSlow {
		t.Fatalf("unexpected backpressure close: %d %q", connection.closeCode, connection.closeText)
	}
}

func TestTerminalStatusPersistsAfterDisconnect(t *testing.T) {
	store := newMemorySocketStore()
	binding := models.SocketActionBinding{
		CompanyID: "c", UserID: "u", AccessProfile: "client", ProjectID: "p",
		SessionID: "s", ActionType: models.SocketActionChatSend, PayloadHash: "hash", IdempotencyKey: "key",
	}
	if _, _, err := store.BeginReceipt(context.Background(), binding, "request", time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	connection := &socketConnection{
		handler: &SocketHandler{store: store}, identity: models.SocketIdentity{CompanyID: "c", UserID: "u"},
		ctx: ctx, cancel: cancel, send: make(chan models.SocketServerEvent, 1), generations: make(map[string]context.CancelFunc),
	}
	connection.persistTerminal(binding, "cancelled", "generation.cancelled", "request", "s", map[string]any{"status": "cancelled"})
	store.mu.Lock()
	receipt := store.receipts[receiptKey(models.SocketIdentity{CompanyID: "c", UserID: "u"}, "key")]
	eventCount := len(store.events)
	store.mu.Unlock()
	if receipt.State != "cancelled" || receipt.FinalEventType != "generation.cancelled" || eventCount != 1 {
		t.Fatalf("terminal state not persisted after disconnect: receipt=%#v events=%d", receipt, eventCount)
	}
	if len(connection.send) != 0 {
		t.Fatal("disconnected connection was sent a terminal event")
	}
}

func TestOriginPolicyRequiresExactConfiguredOrigin(t *testing.T) {
	allowed := []string{"https://dev2.solutions", "http://localhost:3000"}
	for _, origin := range []string{"", "null", "https://evil.example", "http://dev2.solutions", "https://dev2.solutions.evil"} {
		if originAllowed(origin, allowed) {
			t.Fatalf("origin unexpectedly allowed: %q", origin)
		}
	}
	if !originAllowed("https://dev2.solutions", allowed) || !originAllowed("http://localhost:3000", allowed) {
		t.Fatal("configured origin rejected")
	}
}
