package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestSafeReplayAllowlistRedactsSensitivePayloadsAndBoundsData(t *testing.T) {
	for _, eventType := range []string{"content.delta", "chat.meta", "approval.requested", "error", "pong"} {
		if _, persist := safeReplayEvent(models.SocketServerEvent{Type: eventType, Data: map[string]any{"content": "secret"}}); persist {
			t.Fatalf("sensitive event %s was replayable", eventType)
		}
	}
	resolved, persist := safeReplayEvent(models.SocketServerEvent{Type: "approval.resolved", Data: map[string]any{
		"approvalId": "a1", "decision": "approve", "status": "executed", "result": "secret tool output", "preview": "secret preview",
	}})
	if !persist || resolved.Data["result"] != nil || resolved.Data["preview"] != nil {
		t.Fatalf("approval replay leaked sensitive data: %#v", resolved.Data)
	}
	trace, persist := safeReplayEvent(models.SocketServerEvent{Type: "trace", Data: map[string]any{
		"summary": strings.Repeat("x", socketReplayMaxBytes*2), "rawOutput": "secret",
	}})
	if !persist || trace.Data["rawOutput"] != nil {
		t.Fatalf("trace replay leaked raw output: %#v", trace.Data)
	}
	payload, err := json.Marshal(trace.Data)
	if err != nil || len(payload) > socketReplayMaxBytes {
		t.Fatalf("safe replay exceeds cap: bytes=%d err=%v", len(payload), err)
	}
}

func TestMongoBackedRateAndLeaseCapacity(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("ticket rate", func(mt *mtest.T) {
		repo := &SocketRepo{rateBuckets: mt.Coll}
		mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "value", Value: bson.D{{Key: "count", Value: 1}}}))
		if err := repo.takeRate(context.Background(), "ticket", "scope", 1, time.Minute, time.Now()); err != nil {
			mt.Fatalf("first rate token rejected: %v", err)
		}
		mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 11000, Message: "duplicate"}))
		if err := repo.takeRate(context.Background(), "ticket", "scope", 1, time.Minute, time.Now()); !errors.Is(err, ErrSocketRateLimited) {
			mt.Fatalf("rate capacity not enforced: %v", err)
		}
	})
	mt.Run("connection leases", func(mt *mtest.T) {
		repo := &SocketRepo{leases: mt.Coll}
		for i := 0; i < 4; i++ {
			mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "value", Value: bson.D{{Key: "connectionId", Value: "lease"}}}))
		}
		lease, err := repo.AcquireConnection(context.Background(), models.SocketIdentity{CompanyID: "c", UserID: "u"}, "127.0.0.1", ConnectionPolicy{
			GlobalLimit: 1, CompanyLimit: 1, UserLimit: 1, IPLimit: 1, LeaseTTL: time.Minute,
		}, time.Now())
		if err != nil || len(lease.LeaseIDs) != 4 {
			mt.Fatalf("connection lease failed: %#v err=%v", lease, err)
		}
	})
	mt.Run("connection capacity", func(mt *mtest.T) {
		repo := &SocketRepo{leases: mt.Coll}
		mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 11000, Message: "duplicate"}))
		_, err := repo.AcquireConnection(context.Background(), models.SocketIdentity{CompanyID: "c", UserID: "u"}, "127.0.0.1", ConnectionPolicy{
			GlobalLimit: 1, CompanyLimit: 1, UserLimit: 1, IPLimit: 1, LeaseTTL: time.Minute,
		}, time.Now())
		if !errors.Is(err, ErrSocketCapacity) {
			mt.Fatalf("connection capacity not enforced: %v", err)
		}
	})
	mt.Run("outstanding ticket slots", func(mt *mtest.T) {
		repo := &SocketRepo{tickets: mt.Coll, ticketSlots: mt.Coll, rateBuckets: mt.Coll}
		now := time.Now()
		identity := models.SocketIdentity{CompanyID: "c", UserID: "u"}
		mt.AddMockResponses(
			mtest.CreateSuccessResponse(bson.E{Key: "value", Value: bson.D{{Key: "count", Value: 1}}}),
			mtest.CreateSuccessResponse(bson.E{Key: "value", Value: bson.D{{Key: "holderId", Value: "ticket"}}}),
			mtest.CreateSuccessResponse(),
		)
		if _, _, err := repo.IssueTicket(context.Background(), identity, now.Add(time.Hour), TicketPolicy{IssuePerMinute: 10, MaxOutstanding: 1}, now); err != nil {
			mt.Fatalf("first outstanding ticket rejected: %v", err)
		}
		mt.AddMockResponses(
			mtest.CreateSuccessResponse(bson.E{Key: "value", Value: bson.D{{Key: "count", Value: 2}}}),
			mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 11000, Message: "duplicate"}),
		)
		if _, _, err := repo.IssueTicket(context.Background(), identity, now.Add(time.Hour), TicketPolicy{IssuePerMinute: 10, MaxOutstanding: 1}, now); !errors.Is(err, ErrSocketCapacity) {
			mt.Fatalf("outstanding ticket capacity not enforced: %v", err)
		}
	})
}

func TestSequenceUpdateIsDurableAndReplayGapIsDetected(t *testing.T) {
	update := durableSequenceUpdate(models.SocketIdentity{CompanyID: "c", UserID: "u"}, "s", time.Now())
	set := update["$set"].(bson.M)
	if _, hasTTL := set["expiresAt"]; hasTTL {
		t.Fatal("durable sequence update includes an expiry")
	}
	if !detectReplayGap([]models.SocketServerEvent{{Seq: 2}, {Seq: 4}}, 1, 2, 4) {
		t.Fatal("omitted sensitive event gap was not detected")
	}
	if detectReplayGap([]models.SocketServerEvent{{Seq: 2}, {Seq: 3}}, 1, 2, 3) {
		t.Fatal("contiguous replay reported a gap")
	}
	if !detectReplayGap([]models.SocketServerEvent{{Seq: 8}}, 2, 8, 8) {
		t.Fatal("cursor before retained history did not report a gap")
	}
}

func TestReverseMessagesReturnsLatestQueryInChronologicalOrder(t *testing.T) {
	messages := []models.ChatMessage{{ID: "latest"}, {ID: "middle"}, {ID: "oldest"}}
	reverseMessages(messages)
	if messages[0].ID != "oldest" || messages[2].ID != "latest" {
		t.Fatalf("unexpected message order: %#v", messages)
	}
}
