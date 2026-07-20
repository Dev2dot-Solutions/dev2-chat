package handlers

import (
	"context"
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
	"github.com/gorilla/websocket"
)

type memorySocketStore struct {
	mu       sync.Mutex
	next     int
	tickets  map[string]*models.SocketTicket
	seq      map[string]int64
	events   []storedSocketEvent
	receipts map[string]*models.SocketActionReceipt
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

func (s *memorySocketStore) IssueTicket(_ context.Context, identity models.SocketIdentity, now time.Time) (string, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	token := fmt.Sprintf("ticket-%d", s.next)
	expires := now.Add(30 * time.Second)
	s.tickets[token] = &models.SocketTicket{SocketIdentity: identity, ExpiresAt: expires}
	return token, expires, nil
}

func (s *memorySocketStore) ConsumeTicket(_ context.Context, token string, now time.Time) (*models.SocketTicket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ticket := s.tickets[token]
	if ticket == nil || ticket.ConsumedAt != nil || !ticket.ExpiresAt.After(now) {
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

func (s *memorySocketStore) AppendEvent(_ context.Context, identity models.SocketIdentity, event models.SocketServerEvent) (*models.SocketServerEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	scope := socketScope(identity, event.SessionID)
	s.seq[scope]++
	event.Seq = s.seq[scope]
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	s.events = append(s.events, storedSocketEvent{identity: identity, event: event})
	return &event, nil
}

func (s *memorySocketStore) ReplayEvents(_ context.Context, identity models.SocketIdentity, sessionID string, afterSeq int64) ([]models.SocketServerEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []models.SocketServerEvent
	for _, stored := range s.events {
		if stored.identity.CompanyID == identity.CompanyID && stored.identity.UserID == identity.UserID &&
			stored.event.SessionID == sessionID && stored.event.Seq > afterSeq {
			result = append(result, stored.event)
		}
	}
	return result, nil
}

func receiptKey(identity models.SocketIdentity, key string) string {
	return identity.CompanyID + "\x00" + identity.UserID + "\x00" + key
}

func (s *memorySocketStore) BeginReceipt(_ context.Context, identity models.SocketIdentity, key, actionType, requestID string, now time.Time) (*models.SocketActionReceipt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := receiptKey(identity, key)
	if receipt := s.receipts[id]; receipt != nil {
		copy := *receipt
		return &copy, false, nil
	}
	receipt := &models.SocketActionReceipt{
		ID: id, CompanyID: identity.CompanyID, UserID: identity.UserID,
		ActionType: actionType, RequestID: requestID, State: "processing",
		CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(24 * time.Hour),
	}
	s.receipts[id] = receipt
	copy := *receipt
	return &copy, true, nil
}

func (s *memorySocketStore) UpdateReceipt(_ context.Context, identity models.SocketIdentity, key, state, sessionID, finalType string, data map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	receipt := s.receipts[receiptKey(identity, key)]
	if receipt == nil {
		return errors.New("receipt not found")
	}
	receipt.State, receipt.SessionID, receipt.FinalEventType, receipt.FinalData = state, sessionID, finalType, data
	return nil
}

func TestSocketTicketExpiresAndIsConsumedOnceAtomically(t *testing.T) {
	store := newMemorySocketStore()
	identity := models.SocketIdentity{UserID: "user-1", CompanyID: "company-1"}
	now := time.Now().UTC()
	token, expires, err := store.IssueTicket(context.Background(), identity, now)
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
	expired, _, _ := store.IssueTicket(context.Background(), identity, now)
	if ticket, _ := store.ConsumeTicket(context.Background(), expired, now.Add(31*time.Second)); ticket != nil {
		t.Fatal("expired ticket was consumed")
	}
}

func TestSocketOriginRejectedWithoutConsumingTicket(t *testing.T) {
	store := newMemorySocketStore()
	identity := models.SocketIdentity{UserID: "user-1", CompanyID: "company-1", AccessProfile: "client", ProjectID: "project-1"}
	token, _, _ := store.IssueTicket(context.Background(), identity, time.Now())
	handler := NewSocketHandler(store, nil, nil, SocketOptions{AllowedOrigins: []string{"https://dev2.solutions"}})
	server := httptest.NewServer(SocketTicketRedactionMiddleware(http.HandlerFunc(handler.Connect)))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/chat/ws?ticket=" + token

	badHeader := http.Header{"Origin": []string{"https://evil.example"}}
	if conn, resp, err := websocket.DefaultDialer.Dial(wsURL, badHeader); err == nil {
		conn.Close()
		t.Fatal("unexpected successful connection from rejected origin")
	} else if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 origin rejection, response=%v err=%v", resp, err)
	}

	goodHeader := http.Header{"Origin": []string{"https://dev2.solutions"}}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, goodHeader)
	if err != nil {
		t.Fatalf("allowed origin could not use unconsumed ticket: %v", err)
	}
	var ready models.SocketServerEvent
	if err := conn.ReadJSON(&ready); err != nil || ready.Type != "connection.ready" {
		t.Fatalf("expected connection.ready, event=%#v err=%v", ready, err)
	}
	conn.Close()

	if conn, resp, err := websocket.DefaultDialer.Dial(wsURL, goodHeader); err == nil {
		conn.Close()
		t.Fatal("consumed ticket connected twice")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for reused ticket, response=%v err=%v", resp, err)
	}
}

func TestSocketHeartbeatSendsControlPing(t *testing.T) {
	store := newMemorySocketStore()
	identity := models.SocketIdentity{UserID: "user-1", CompanyID: "company-1", AccessProfile: "client", ProjectID: "project-1"}
	token, _, _ := store.IssueTicket(context.Background(), identity, time.Now())
	handler := NewSocketHandler(store, nil, nil, SocketOptions{
		AllowedOrigins: []string{"https://dev2.solutions"},
		PingInterval:   10 * time.Millisecond,
		IdleTimeout:    time.Second,
	})
	server := httptest.NewServer(SocketTicketRedactionMiddleware(http.HandlerFunc(handler.Connect)))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/chat/ws?ticket=" + token
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Origin": []string{"https://dev2.solutions"}})
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
					_, isNew, err := store.BeginReceipt(context.Background(), identity, "same-key", action, fmt.Sprintf("r%d", i), time.Now())
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

func TestSocketSequenceAndReplayAuthorization(t *testing.T) {
	store := newMemorySocketStore()
	owner := models.SocketIdentity{UserID: "u1", CompanyID: "c1"}
	otherUser := models.SocketIdentity{UserID: "u2", CompanyID: "c1"}
	for i := 0; i < 3; i++ {
		event, err := store.AppendEvent(context.Background(), owner, models.SocketServerEvent{Type: "trace", SessionID: "s1", Data: map[string]any{}})
		if err != nil || event.Seq != int64(i+1) {
			t.Fatalf("event %d sequence=%v err=%v", i, event.Seq, err)
		}
	}
	replayed, _ := store.ReplayEvents(context.Background(), owner, "s1", 1)
	if len(replayed) != 2 || replayed[0].Seq != 2 || replayed[1].Seq != 3 {
		t.Fatalf("unexpected replay: %#v", replayed)
	}
	unauthorized, _ := store.ReplayEvents(context.Background(), otherUser, "s1", 0)
	if len(unauthorized) != 0 {
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
	if connection.closeCode != websocket.ClosePolicyViolation || connection.closeText != socketCloseSlow {
		t.Fatalf("unexpected backpressure close: %d %q", connection.closeCode, connection.closeText)
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
