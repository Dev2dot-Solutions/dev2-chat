package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ApprovalRepo persists approvalId → session mappings (DEV2-108) so the
// decision endpoint can resolve ownership and block repeat decisions.
type ApprovalRepo struct {
	coll *mongo.Collection
}

func NewApprovalRepo(db *mongo.Database) *ApprovalRepo {
	return &ApprovalRepo{coll: db.Collection("chat_approvals")}
}

// RecordPending registers (or refreshes) a pending approval mapping. An
// already-decided record keeps its terminal status and decision fields —
// only the descriptive fields are refreshed.
func (r *ApprovalRepo) RecordPending(ctx context.Context, rec *models.ApprovalRecord) error {
	now := time.Now().UTC()
	update := bson.M{
		"$set": bson.M{
			"sessionId": rec.SessionID,
			"companyId": rec.CompanyID,
			"userId":    rec.UserID,
			"tool":      rec.Tool,
			"summary":   rec.Summary,
			"preview":   rec.Preview,
			"expiresAt": rec.ExpiresAt,
		},
		"$setOnInsert": bson.M{
			"status":    models.ApprovalStatusPending,
			"createdAt": now,
		},
	}
	_, err := r.coll.UpdateOne(ctx, bson.M{"_id": rec.ID}, update, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("record approval: %w", err)
	}
	return nil
}

func (r *ApprovalRepo) GetByID(ctx context.Context, id string) (*models.ApprovalRecord, error) {
	var rec models.ApprovalRecord
	err := r.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&rec)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get approval: %w", err)
	}
	return &rec, nil
}

// MarkDecided transitions a pending approval to a terminal status. It
// returns false when the record was already decided (conditional update
// matched nothing), so concurrent or repeat decisions are rejected.
func (r *ApprovalRepo) MarkDecided(ctx context.Context, id, decision, status string) (bool, error) {
	now := time.Now().UTC()
	res, err := r.coll.UpdateOne(ctx,
		bson.M{"_id": id, "status": models.ApprovalStatusPending},
		bson.M{"$set": bson.M{"status": status, "decision": decision, "decidedAt": now}})
	if err != nil {
		return false, fmt.Errorf("mark approval decided: %w", err)
	}
	return res.ModifiedCount > 0, nil
}
