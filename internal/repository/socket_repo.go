package repository

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	socketTicketTTL      = 30 * time.Second
	socketHistoryTTL     = 24 * time.Hour
	socketHistoryCap     = int64(1000)
	socketReplayMaxBytes = 4096
	replayPageMaxEvents  = 100
	replayPageMaxBytes   = 256 << 10
	socketCleanupTimeout = 5 * time.Second
)

var (
	ErrSocketRateLimited     = errors.New("socket rate limited")
	ErrSocketCapacity        = errors.New("socket capacity reached")
	ErrSocketReceiptMismatch = errors.New("idempotency receipt binding mismatch")
)

type TicketPolicy struct {
	IssuePerMinute int
	MaxOutstanding int
}

type ConnectionPolicy struct {
	GlobalLimit  int
	CompanyLimit int
	UserLimit    int
	IPLimit      int
	LeaseTTL     time.Duration
}

type GenerationPolicy struct {
	CompanyLimit int
	UserLimit    int
	LeaseTTL     time.Duration
}

type MessageRatePolicy struct {
	UserPerMinute    int
	CompanyPerMinute int
	IPPerMinute      int
}

type SocketRepo struct {
	tickets     *mongo.Collection
	ticketSlots *mongo.Collection
	events      *mongo.Collection
	sequences   *mongo.Collection
	receipts    *mongo.Collection
	rateBuckets *mongo.Collection
	leases      *mongo.Collection
}

func NewSocketRepo(db *mongo.Database) *SocketRepo {
	return &SocketRepo{
		tickets: db.Collection("chat_socket_tickets"), ticketSlots: db.Collection("chat_socket_ticket_slots"),
		events: db.Collection("chat_socket_events"), sequences: db.Collection("chat_socket_sequences"),
		receipts: db.Collection("chat_socket_receipts"), rateBuckets: db.Collection("chat_socket_rate_limits"),
		leases: db.Collection("chat_socket_leases"),
	}
}

func (r *SocketRepo) EnsureIndexes(ctx context.Context) error {
	// Sequence counters are intentionally durable. Remove the TTL index created
	// by the initial WebSocket implementation if it exists.
	if _, err := r.sequences.Indexes().DropOne(ctx, "expiresAt_1"); err != nil && !isIndexNotFound(err) {
		return fmt.Errorf("drop sequence TTL index: %w", err)
	}
	indexes := []struct {
		coll *mongo.Collection
		mods []mongo.IndexModel
	}{
		{r.tickets, []mongo.IndexModel{
			{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
			{Keys: bson.D{{Key: "companyId", Value: 1}, {Key: "userId", Value: 1}, {Key: "consumedAt", Value: 1}, {Key: "expiresAt", Value: 1}}},
		}},
		{r.ticketSlots, []mongo.IndexModel{{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)}}},
		{r.events, []mongo.IndexModel{
			{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
			{Keys: bson.D{{Key: "companyId", Value: 1}, {Key: "userId", Value: 1}, {Key: "sessionId", Value: 1}, {Key: "seq", Value: 1}}, Options: options.Index().SetUnique(true)},
		}},
		{r.receipts, []mongo.IndexModel{
			{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
			{Keys: bson.D{{Key: "companyId", Value: 1}, {Key: "userId", Value: 1}, {Key: "createdAt", Value: -1}}},
		}},
		{r.rateBuckets, []mongo.IndexModel{{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)}}},
		{r.leases, []mongo.IndexModel{
			{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
			{Keys: bson.D{{Key: "connectionId", Value: 1}}},
		}},
	}
	for _, set := range indexes {
		if _, err := set.coll.Indexes().CreateMany(ctx, set.mods); err != nil {
			return fmt.Errorf("create socket indexes on %s: %w", set.coll.Name(), err)
		}
	}
	return nil
}

func isIndexNotFound(err error) bool {
	var commandErr mongo.CommandError
	return errors.As(err, &commandErr) && (commandErr.Code == 26 || commandErr.Code == 27)
}

func (r *SocketRepo) IssueTicket(ctx context.Context, identity models.SocketIdentity, socketExpiresAt time.Time, policy TicketPolicy, now time.Time) (string, time.Time, error) {
	if err := r.takeRate(ctx, "ticket", identity.CompanyID+"\x00"+identity.UserID, policy.IssuePerMinute, time.Minute, now); err != nil {
		return "", time.Time{}, err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", time.Time{}, fmt.Errorf("generate socket ticket: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	tokenHash := hashSocketSecret(token)
	expiresAt := now.UTC().Add(socketTicketTTL)
	slot, err := r.acquireSlot(ctx, "ticket:"+identity.CompanyID+":"+identity.UserID, tokenHash, policy.MaxOutstanding, expiresAt, now)
	if err != nil {
		return "", time.Time{}, err
	}
	doc := models.SocketTicket{
		TokenHash: tokenHash, SocketIdentity: identity, ExpiresAt: expiresAt,
		SocketExpiresAt: socketExpiresAt.UTC(), TicketSlot: slot, CreatedAt: now.UTC(),
	}
	if _, err := r.tickets.InsertOne(ctx, doc); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), socketCleanupTimeout)
		_, _ = r.ticketSlots.DeleteOne(cleanupCtx, bson.M{"_id": slot, "holderId": tokenHash})
		cancel()
		return "", time.Time{}, fmt.Errorf("store socket ticket: %w", err)
	}
	return token, expiresAt, nil
}

func (r *SocketRepo) ConsumeTicket(ctx context.Context, token string, now time.Time) (*models.SocketTicket, error) {
	if token == "" {
		return nil, nil
	}
	var ticket models.SocketTicket
	err := r.tickets.FindOneAndUpdate(ctx, bson.M{
		"_id": hashSocketSecret(token), "expiresAt": bson.M{"$gt": now.UTC()},
		"socketExpiresAt": bson.M{"$gt": now.UTC()}, "consumedAt": bson.M{"$exists": false},
	}, bson.M{"$set": bson.M{"consumedAt": now.UTC()}}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&ticket)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("consume socket ticket: %w", err)
	}
	if ticket.TicketSlot != "" {
		_, _ = r.ticketSlots.DeleteOne(ctx, bson.M{"_id": ticket.TicketSlot, "holderId": ticket.TokenHash})
	}
	return &ticket, nil
}

func (r *SocketRepo) AcquireConnection(ctx context.Context, identity models.SocketIdentity, ip string, policy ConnectionPolicy, now time.Time) (*models.SocketLease, error) {
	connectionID, err := randomID()
	if err != nil {
		return nil, err
	}
	dimensions := []struct {
		key   string
		limit int
	}{
		{"connection:global", policy.GlobalLimit},
		{"connection:company:" + identity.CompanyID, policy.CompanyLimit},
		{"connection:user:" + identity.CompanyID + ":" + identity.UserID, policy.UserLimit},
		{"connection:ip:" + ip, policy.IPLimit},
	}
	lease := &models.SocketLease{ConnectionID: connectionID, ExpiresAt: now.Add(policy.LeaseTTL)}
	for _, dimension := range dimensions {
		id, err := r.acquireLeaseSlot(ctx, dimension.key, connectionID, dimension.limit, lease.ExpiresAt, now)
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), socketCleanupTimeout)
			r.ReleaseLease(cleanupCtx, lease)
			cancel()
			return nil, err
		}
		lease.LeaseIDs = append(lease.LeaseIDs, id)
	}
	return lease, nil
}

func (r *SocketRepo) AcquireGeneration(ctx context.Context, identity models.SocketIdentity, policy GenerationPolicy, now time.Time) (*models.SocketLease, error) {
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	dimensions := []struct {
		key   string
		limit int
	}{
		{"generation:company:" + identity.CompanyID, policy.CompanyLimit},
		{"generation:user:" + identity.CompanyID + ":" + identity.UserID, policy.UserLimit},
	}
	lease := &models.SocketLease{ConnectionID: id, ExpiresAt: now.Add(policy.LeaseTTL)}
	for _, dimension := range dimensions {
		leaseID, err := r.acquireLeaseSlot(ctx, dimension.key, id, dimension.limit, lease.ExpiresAt, now)
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), socketCleanupTimeout)
			r.ReleaseLease(cleanupCtx, lease)
			cancel()
			return nil, err
		}
		lease.LeaseIDs = append(lease.LeaseIDs, leaseID)
	}
	return lease, nil
}

func (r *SocketRepo) RenewLease(ctx context.Context, lease *models.SocketLease, ttl time.Duration, now time.Time) error {
	if lease == nil {
		return nil
	}
	expires := now.Add(ttl)
	res, err := r.leases.UpdateMany(ctx, bson.M{"_id": bson.M{"$in": lease.LeaseIDs}, "connectionId": lease.ConnectionID}, bson.M{"$set": bson.M{"expiresAt": expires}})
	if err != nil {
		return fmt.Errorf("renew socket lease: %w", err)
	}
	if res.MatchedCount != int64(len(lease.LeaseIDs)) {
		return ErrSocketCapacity
	}
	lease.ExpiresAt = expires
	return nil
}

func (r *SocketRepo) ReleaseLease(ctx context.Context, lease *models.SocketLease) {
	if lease == nil || len(lease.LeaseIDs) == 0 {
		return
	}
	_, _ = r.leases.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": lease.LeaseIDs}, "connectionId": lease.ConnectionID})
}

func (r *SocketRepo) TakeMessageRate(ctx context.Context, identity models.SocketIdentity, ip string, policy MessageRatePolicy, now time.Time) error {
	dimensions := []struct {
		kind  string
		scope string
		limit int
	}{
		{"message-user", identity.CompanyID + "\x00" + identity.UserID, policy.UserPerMinute},
		{"message-company", identity.CompanyID, policy.CompanyPerMinute},
		{"message-ip", ip, policy.IPPerMinute},
	}
	for _, dimension := range dimensions {
		if err := r.takeRate(ctx, dimension.kind, dimension.scope, dimension.limit, time.Minute, now); err != nil {
			return err
		}
	}
	return nil
}

func (r *SocketRepo) RecordEvent(ctx context.Context, identity models.SocketIdentity, event models.SocketServerEvent) (*models.SocketServerEvent, bool, error) {
	scope := event.SessionID
	if scope == "" {
		return &event, false, nil
	}
	sequenceID := sequenceKey(identity, scope)
	var counter struct {
		Seq int64 `bson:"seq"`
	}
	update := durableSequenceUpdate(identity, scope, time.Now().UTC())
	err := r.sequences.FindOneAndUpdate(ctx, bson.M{"_id": sequenceID}, update,
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&counter)
	if mongo.IsDuplicateKeyError(err) {
		err = r.sequences.FindOneAndUpdate(ctx, bson.M{"_id": sequenceID}, update,
			options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&counter)
	}
	if err != nil {
		return nil, false, fmt.Errorf("allocate socket sequence: %w", err)
	}
	event.Seq = counter.Seq
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	safe, persist := safeReplayEvent(event)
	if !persist {
		return &event, false, nil
	}
	payload, err := json.Marshal(safe)
	if err != nil || len(payload) > socketReplayMaxBytes {
		return &event, false, nil
	}
	doc := bson.M{
		"companyId": identity.CompanyID, "userId": identity.UserID, "sessionId": event.SessionID,
		"seq": event.Seq, "type": safe.Type, "requestId": event.RequestID,
		"timestamp": event.Timestamp, "data": safe.Data, "expiresAt": event.Timestamp.Add(socketHistoryTTL),
	}
	if _, err := r.events.InsertOne(ctx, doc); err != nil {
		return nil, false, fmt.Errorf("store socket event: %w", err)
	}
	if event.Seq > socketHistoryCap {
		_, _ = r.events.DeleteMany(ctx, bson.M{
			"companyId": identity.CompanyID, "userId": identity.UserID, "sessionId": event.SessionID,
			"seq": bson.M{"$lte": event.Seq - socketHistoryCap},
		})
	}
	return &event, true, nil
}

func (r *SocketRepo) ReplayEvents(ctx context.Context, identity models.SocketIdentity, sessionID string, afterSeq int64) (*models.SocketReplay, error) {
	filter := bson.M{"companyId": identity.CompanyID, "userId": identity.UserID, "sessionId": sessionID}
	var counter struct {
		Seq int64 `bson:"seq"`
	}
	err := r.sequences.FindOne(ctx, bson.M{"_id": sequenceKey(identity, sessionID)}).Decode(&counter)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("load socket sequence: %w", err)
	}
	window := &models.SocketReplay{LatestSeq: counter.Seq}
	var earliest struct {
		Seq int64 `bson:"seq"`
	}
	if err := r.events.FindOne(ctx, filter, options.FindOne().SetSort(bson.D{{Key: "seq", Value: 1}})).Decode(&earliest); err == nil {
		window.EarliestAvailableSeq = earliest.Seq
	} else if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("load earliest socket event: %w", err)
	}
	cursor, err := r.events.Find(ctx, bson.M{
		"companyId": identity.CompanyID, "userId": identity.UserID, "sessionId": sessionID, "seq": bson.M{"$gt": afterSeq},
	}, options.Find().SetSort(bson.D{{Key: "seq", Value: 1}}).SetLimit(replayPageMaxEvents+1))
	if err != nil {
		return nil, fmt.Errorf("find socket replay: %w", err)
	}
	defer cursor.Close(ctx)
	window.NextSeq = afterSeq
	aggregateBytes := 0
	for cursor.Next(ctx) {
		var event models.SocketServerEvent
		if err := cursor.Decode(&event); err != nil {
			return nil, fmt.Errorf("decode socket replay: %w", err)
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("size socket replay: %w", err)
		}
		if len(window.Events) >= replayPageMaxEvents || aggregateBytes+len(payload) > replayPageMaxBytes {
			window.Truncated = true
			break
		}
		window.Events = append(window.Events, event)
		aggregateBytes += len(payload)
		window.NextSeq = event.Seq
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate socket replay: %w", err)
	}
	window.GapDetected = detectReplayGap(window.Events, afterSeq, window.EarliestAvailableSeq, window.LatestSeq)
	if window.Truncated {
		window.GapDetected = true
	}
	return window, nil
}

func (r *SocketRepo) BeginReceipt(ctx context.Context, binding models.SocketActionBinding, requestID string, now time.Time) (*models.SocketActionReceipt, bool, error) {
	idempotencyHash := hashSocketSecret(binding.IdempotencyKey)
	id := hashSocketSecret(binding.CompanyID + "\x00" + binding.UserID + "\x00" + binding.IdempotencyKey)
	receipt := &models.SocketActionReceipt{
		ID: id, CompanyID: binding.CompanyID, UserID: binding.UserID, AccessProfile: binding.AccessProfile,
		ProjectID: binding.ProjectID, BoundSessionID: binding.SessionID, ActionType: binding.ActionType,
		PayloadHash: binding.PayloadHash, IdempotencyHash: idempotencyHash,
		RequestID: requestID, State: "claimed", CreatedAt: now.UTC(), UpdatedAt: now.UTC(), ExpiresAt: now.UTC().Add(socketHistoryTTL),
	}
	if _, err := r.receipts.InsertOne(ctx, receipt); err == nil {
		return receipt, true, nil
	} else if !mongo.IsDuplicateKeyError(err) {
		return nil, false, fmt.Errorf("create socket receipt: %w", err)
	}
	var existing models.SocketActionReceipt
	if err := r.receipts.FindOne(ctx, bson.M{"_id": id}).Decode(&existing); err != nil {
		return nil, false, fmt.Errorf("load socket receipt: %w", err)
	}
	if existing.CompanyID != binding.CompanyID || existing.UserID != binding.UserID ||
		existing.AccessProfile != binding.AccessProfile || existing.ProjectID != binding.ProjectID ||
		existing.BoundSessionID != binding.SessionID || existing.ActionType != binding.ActionType ||
		existing.PayloadHash != binding.PayloadHash || existing.IdempotencyHash != idempotencyHash {
		return &existing, false, ErrSocketReceiptMismatch
	}
	return &existing, false, nil
}

func (r *SocketRepo) UpdateReceipt(ctx context.Context, binding models.SocketActionBinding, state, sessionID, finalType string, data map[string]any) error {
	id := hashSocketSecret(binding.CompanyID + "\x00" + binding.UserID + "\x00" + binding.IdempotencyKey)
	set := bson.M{"state": state, "updatedAt": time.Now().UTC()}
	if sessionID != "" {
		set["sessionId"] = sessionID
	}
	if finalType != "" {
		set["finalEventType"], set["finalData"] = finalType, data
	}
	res, err := r.receipts.UpdateOne(ctx, bson.M{
		"_id": id, "companyId": binding.CompanyID, "userId": binding.UserID,
		"accessProfile": binding.AccessProfile, "projectId": binding.ProjectID,
		"boundSessionId": binding.SessionID, "actionType": binding.ActionType,
		"payloadHash": binding.PayloadHash, "idempotencyHash": hashSocketSecret(binding.IdempotencyKey),
	}, bson.M{"$set": set})
	if err != nil {
		return fmt.Errorf("update socket receipt: %w", err)
	}
	if res.MatchedCount == 0 {
		return ErrSocketReceiptMismatch
	}
	return nil
}

func (r *SocketRepo) takeRate(ctx context.Context, kind, scope string, limit int, window time.Duration, now time.Time) error {
	if limit <= 0 {
		return ErrSocketRateLimited
	}
	bucketStart := now.UTC().Truncate(window)
	id := hashSocketSecret(kind + "\x00" + scope + "\x00" + bucketStart.Format(time.RFC3339Nano))
	filter := bson.M{"_id": id, "$or": bson.A{bson.M{"count": bson.M{"$lt": limit}}, bson.M{"count": bson.M{"$exists": false}}}}
	update := bson.M{"$inc": bson.M{"count": 1}, "$setOnInsert": bson.M{"expiresAt": bucketStart.Add(2 * window), "createdAt": now.UTC()}}
	err := r.rateBuckets.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Err()
	if err == nil {
		return nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) || mongo.IsDuplicateKeyError(err) {
		return ErrSocketRateLimited
	}
	return fmt.Errorf("apply socket rate limit: %w", err)
}

func (r *SocketRepo) acquireSlot(ctx context.Context, dimension, holder string, limit int, expiresAt, now time.Time) (string, error) {
	for i := 0; i < limit; i++ {
		id := hashSocketSecret(dimension + fmt.Sprintf(":%d", i))
		filter := bson.M{"_id": id, "$or": bson.A{bson.M{"expiresAt": bson.M{"$lte": now.UTC()}}, bson.M{"expiresAt": bson.M{"$exists": false}}}}
		update := bson.M{"$set": bson.M{"holderId": holder, "expiresAt": expiresAt.UTC(), "dimension": dimension}}
		err := r.ticketSlots.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Err()
		if err == nil {
			return id, nil
		}
		if !mongo.IsDuplicateKeyError(err) && !errors.Is(err, mongo.ErrNoDocuments) {
			return "", fmt.Errorf("acquire ticket slot: %w", err)
		}
	}
	return "", ErrSocketCapacity
}

func (r *SocketRepo) acquireLeaseSlot(ctx context.Context, dimension, connectionID string, limit int, expiresAt, now time.Time) (string, error) {
	if limit <= 0 {
		return "", ErrSocketCapacity
	}
	start := int(hashByte(connectionID) % byte(min(limit, 255)))
	for n := 0; n < limit; n++ {
		i := (start + n) % limit
		id := hashSocketSecret(dimension + fmt.Sprintf(":%d", i))
		filter := bson.M{"_id": id, "$or": bson.A{
			bson.M{"expiresAt": bson.M{"$lte": now.UTC()}}, bson.M{"expiresAt": bson.M{"$exists": false}}, bson.M{"connectionId": connectionID},
		}}
		update := bson.M{"$set": bson.M{"connectionId": connectionID, "dimension": dimension, "expiresAt": expiresAt.UTC()}}
		err := r.leases.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Err()
		if err == nil {
			return id, nil
		}
		if !mongo.IsDuplicateKeyError(err) && !errors.Is(err, mongo.ErrNoDocuments) {
			return "", fmt.Errorf("acquire socket lease: %w", err)
		}
	}
	return "", ErrSocketCapacity
}

func safeReplayEvent(event models.SocketServerEvent) (models.SocketServerEvent, bool) {
	safe := event
	safe.Data = map[string]any{}
	if event.Ephemeral || event.Seq == 0 {
		return safe, false
	}
	switch event.Type {
	case "chat.accepted":
		safe.Data["accepted"] = true
		copyBoolField(safe.Data, event.Data, "duplicate")
	case "trace":
		for _, key := range []string{"eventId", "type", "spanId", "parentSpanId", "toolCallId", "parentToolCallId", "toolName", "personaName", "personaScope", "summary", "status", "timestamp"} {
			copyStringField(safe.Data, event.Data, key, 512)
		}
		copyNumberField(safe.Data, event.Data, "delegationDepth")
		copyNumberField(safe.Data, event.Data, "durationMs")
		copyBoolField(safe.Data, event.Data, "success")
	case "generation.completed":
		safe.Data["status"] = "completed"
	case "generation.cancelled":
		safe.Data["status"] = "cancelled"
		copyStringField(safe.Data, event.Data, "targetRequestId", 128)
	case "approval.resolved":
		for _, key := range []string{"approvalId", "decision", "status"} {
			copyStringField(safe.Data, event.Data, key, 128)
		}
	default:
		return safe, false
	}
	return safe, true
}

func copyStringField(dst, src map[string]any, key string, max int) {
	if value, ok := src[key].(string); ok {
		if len(value) > max {
			value = value[:max]
		}
		dst[key] = value
	}
}

func copyBoolField(dst, src map[string]any, key string) {
	if value, ok := src[key].(bool); ok {
		dst[key] = value
	}
}
func copyNumberField(dst, src map[string]any, key string) {
	switch value := src[key].(type) {
	case int:
		dst[key] = value
	case int64:
		dst[key] = value
	case float64:
		dst[key] = value
	}
}

func sequenceKey(identity models.SocketIdentity, sessionID string) string {
	return hashSocketSecret(identity.CompanyID + "\x00" + identity.UserID + "\x00" + sessionID)
}

func durableSequenceUpdate(identity models.SocketIdentity, sessionID string, now time.Time) bson.M {
	return bson.M{"$inc": bson.M{"seq": 1}, "$set": bson.M{
		"companyId": identity.CompanyID, "userId": identity.UserID, "sessionId": sessionID, "updatedAt": now,
	}}
}

func detectReplayGap(events []models.SocketServerEvent, afterSeq, earliest, latest int64) bool {
	expected := afterSeq + 1
	for _, event := range events {
		if event.Seq != expected {
			return true
		}
		expected = event.Seq + 1
	}
	return (afterSeq < latest && expected <= latest) || (earliest > 0 && afterSeq+1 < earliest)
}

func randomID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate lease id: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func hashSocketSecret(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func hashByte(value string) byte { sum := sha256.Sum256([]byte(value)); return sum[0] }
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// CanonicalPayloadHash hashes a fixed-schema JSON value used for receipt binding.
func CanonicalPayloadHash(value any) string {
	payload, _ := json.Marshal(value)
	return hashSocketSecret(strings.TrimSpace(string(payload)))
}
