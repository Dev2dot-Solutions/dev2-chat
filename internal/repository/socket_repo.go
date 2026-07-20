package repository

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	socketTicketTTL   = 30 * time.Second
	socketHistoryTTL  = 24 * time.Hour
	socketSequenceTTL = 25 * time.Hour
	socketHistoryCap  = int64(1000)
)

type SocketRepo struct {
	tickets   *mongo.Collection
	events    *mongo.Collection
	sequences *mongo.Collection
	receipts  *mongo.Collection
}

func NewSocketRepo(db *mongo.Database) *SocketRepo {
	return &SocketRepo{
		tickets:   db.Collection("chat_socket_tickets"),
		events:    db.Collection("chat_socket_events"),
		sequences: db.Collection("chat_socket_sequences"),
		receipts:  db.Collection("chat_socket_receipts"),
	}
}

func (r *SocketRepo) EnsureIndexes(ctx context.Context) error {
	indexes := []struct {
		coll *mongo.Collection
		mods []mongo.IndexModel
	}{
		{r.tickets, []mongo.IndexModel{{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)}}},
		{r.events, []mongo.IndexModel{
			{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
			{Keys: bson.D{{Key: "companyId", Value: 1}, {Key: "userId", Value: 1}, {Key: "sessionId", Value: 1}, {Key: "seq", Value: 1}}, Options: options.Index().SetUnique(true)},
		}},
		{r.sequences, []mongo.IndexModel{{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)}}},
		{r.receipts, []mongo.IndexModel{
			{Keys: bson.D{{Key: "expiresAt", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
			{Keys: bson.D{{Key: "companyId", Value: 1}, {Key: "userId", Value: 1}, {Key: "createdAt", Value: -1}}},
		}},
	}
	for _, set := range indexes {
		if _, err := set.coll.Indexes().CreateMany(ctx, set.mods); err != nil {
			return fmt.Errorf("create socket indexes on %s: %w", set.coll.Name(), err)
		}
	}
	return nil
}

// IssueTicket returns the only copy of a cryptographically random ticket. The
// database stores its SHA-256 digest as the document ID, never the raw value.
func (r *SocketRepo) IssueTicket(ctx context.Context, identity models.SocketIdentity, now time.Time) (string, time.Time, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", time.Time{}, fmt.Errorf("generate socket ticket: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	expiresAt := now.UTC().Add(socketTicketTTL)
	doc := models.SocketTicket{
		TokenHash:      hashSocketSecret(token),
		SocketIdentity: identity,
		ExpiresAt:      expiresAt,
		CreatedAt:      now.UTC(),
	}
	if _, err := r.tickets.InsertOne(ctx, doc); err != nil {
		return "", time.Time{}, fmt.Errorf("store socket ticket: %w", err)
	}
	return token, expiresAt, nil
}

// ConsumeTicket atomically marks a valid ticket consumed. The conditional
// update makes one-time use work across all service instances.
func (r *SocketRepo) ConsumeTicket(ctx context.Context, token string, now time.Time) (*models.SocketTicket, error) {
	if token == "" {
		return nil, nil
	}
	var ticket models.SocketTicket
	err := r.tickets.FindOneAndUpdate(ctx, bson.M{
		"_id":        hashSocketSecret(token),
		"expiresAt":  bson.M{"$gt": now.UTC()},
		"consumedAt": bson.M{"$exists": false},
	}, bson.M{"$set": bson.M{"consumedAt": now.UTC()}}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&ticket)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("consume socket ticket: %w", err)
	}
	return &ticket, nil
}

func (r *SocketRepo) AppendEvent(ctx context.Context, identity models.SocketIdentity, event models.SocketServerEvent) (*models.SocketServerEvent, error) {
	scope := event.SessionID
	if scope == "" {
		scope = "__connection__"
	}
	sequenceID := hashSocketSecret(identity.CompanyID + "\x00" + identity.UserID + "\x00" + scope)
	var counter struct {
		Seq int64 `bson:"seq"`
	}
	update := bson.M{
		"$inc": bson.M{"seq": 1},
		"$set": bson.M{"companyId": identity.CompanyID, "userId": identity.UserID, "sessionId": scope, "updatedAt": time.Now().UTC(), "expiresAt": time.Now().UTC().Add(socketSequenceTTL)},
	}
	err := r.sequences.FindOneAndUpdate(ctx, bson.M{"_id": sequenceID}, update,
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&counter)
	// Concurrent first events can race on an upsert across service instances.
	// The losing writer retries as a normal update against the counter created
	// by the winner.
	if mongo.IsDuplicateKeyError(err) {
		err = r.sequences.FindOneAndUpdate(ctx, bson.M{"_id": sequenceID}, update,
			options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&counter)
	}
	if err != nil {
		return nil, fmt.Errorf("allocate socket sequence: %w", err)
	}
	event.Seq = counter.Seq
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.Data == nil {
		event.Data = map[string]any{}
	}
	doc := bson.M{
		"companyId": identity.CompanyID,
		"userId":    identity.UserID,
		"sessionId": event.SessionID,
		"seq":       event.Seq,
		"type":      event.Type,
		"requestId": event.RequestID,
		"timestamp": event.Timestamp,
		"data":      event.Data,
		"expiresAt": event.Timestamp.Add(socketHistoryTTL),
	}
	if _, err := r.events.InsertOne(ctx, doc); err != nil {
		return nil, fmt.Errorf("store socket event: %w", err)
	}
	if event.SessionID != "" && event.Seq > socketHistoryCap {
		_, _ = r.events.DeleteMany(ctx, bson.M{
			"companyId": identity.CompanyID,
			"userId":    identity.UserID,
			"sessionId": event.SessionID,
			"seq":       bson.M{"$lte": event.Seq - socketHistoryCap},
		})
	}
	return &event, nil
}

func (r *SocketRepo) ReplayEvents(ctx context.Context, identity models.SocketIdentity, sessionID string, afterSeq int64) ([]models.SocketServerEvent, error) {
	cursor, err := r.events.Find(ctx, bson.M{
		"companyId": identity.CompanyID,
		"userId":    identity.UserID,
		"sessionId": sessionID,
		"seq":       bson.M{"$gt": afterSeq},
	}, options.Find().SetSort(bson.D{{Key: "seq", Value: 1}}).SetLimit(socketHistoryCap))
	if err != nil {
		return nil, fmt.Errorf("find socket replay: %w", err)
	}
	defer cursor.Close(ctx)
	var events []models.SocketServerEvent
	if err := cursor.All(ctx, &events); err != nil {
		return nil, fmt.Errorf("decode socket replay: %w", err)
	}
	return events, nil
}

// BeginReceipt atomically claims an idempotency key. created=false returns the
// prior action state, including accepted/final data when available.
func (r *SocketRepo) BeginReceipt(ctx context.Context, identity models.SocketIdentity, key, actionType, requestID string, now time.Time) (*models.SocketActionReceipt, bool, error) {
	id := hashSocketSecret(identity.CompanyID + "\x00" + identity.UserID + "\x00" + key)
	receipt := &models.SocketActionReceipt{
		ID: id, CompanyID: identity.CompanyID, UserID: identity.UserID,
		ActionType: actionType, RequestID: requestID, State: "processing",
		CreatedAt: now.UTC(), UpdatedAt: now.UTC(), ExpiresAt: now.UTC().Add(socketHistoryTTL),
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
	return &existing, false, nil
}

func (r *SocketRepo) UpdateReceipt(ctx context.Context, identity models.SocketIdentity, key, state, sessionID, finalType string, data map[string]any) error {
	id := hashSocketSecret(identity.CompanyID + "\x00" + identity.UserID + "\x00" + key)
	set := bson.M{"state": state, "updatedAt": time.Now().UTC()}
	if sessionID != "" {
		set["sessionId"] = sessionID
	}
	if finalType != "" {
		set["finalEventType"] = finalType
		set["finalData"] = data
	}
	res, err := r.receipts.UpdateOne(ctx, bson.M{"_id": id, "companyId": identity.CompanyID, "userId": identity.UserID}, bson.M{"$set": set})
	if err != nil {
		return fmt.Errorf("update socket receipt: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("socket receipt not found")
	}
	return nil
}

func hashSocketSecret(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
