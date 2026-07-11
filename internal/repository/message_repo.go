package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MessageRepo struct {
	coll *mongo.Collection
}

func NewMessageRepo(db *mongo.Database) *MessageRepo {
	return &MessageRepo{coll: db.Collection("chat_messages")}
}

func (r *MessageRepo) Create(ctx context.Context, msg *models.ChatMessage) error {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	msg.CreatedAt = time.Now().UTC()
	_, err := r.coll.InsertOne(ctx, msg)
	if err != nil {
		return fmt.Errorf("insert message: %w", err)
	}
	return nil
}

func (r *MessageRepo) ListBySession(ctx context.Context, sessionID string, limit int) ([]models.ChatMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	cursor, err := r.coll.Find(ctx, bson.M{"session_id": sessionID},
		options.Find().SetSort(bson.M{"created_at": 1}).SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("find messages: %w", err)
	}
	defer cursor.Close(ctx)

	var messages []models.ChatMessage
	if err := cursor.All(ctx, &messages); err != nil {
		return nil, fmt.Errorf("decode messages: %w", err)
	}
	if messages == nil {
		messages = []models.ChatMessage{}
	}
	return messages, nil
}

func (r *MessageRepo) CountBySession(ctx context.Context, sessionID string) (int, error) {
	count, err := r.coll.CountDocuments(ctx, bson.M{"session_id": sessionID})
	if err != nil {
		return 0, fmt.Errorf("count messages: %w", err)
	}
	return int(count), nil
}

func (r *MessageRepo) GetLastBySession(ctx context.Context, sessionID string) (*models.ChatMessage, error) {
	var msg models.ChatMessage
	err := r.coll.FindOne(ctx, bson.M{"session_id": sessionID},
		options.FindOne().SetSort(bson.M{"created_at": -1})).Decode(&msg)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get last message: %w", err)
	}
	return &msg, nil
}
