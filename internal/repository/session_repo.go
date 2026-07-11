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

type SessionRepo struct {
	coll *mongo.Collection
}

func NewSessionRepo(db *mongo.Database) *SessionRepo {
	return &SessionRepo{coll: db.Collection("chat_sessions")}
}

func (r *SessionRepo) Create(ctx context.Context, input models.ChatSessionInput) (*models.ChatSession, error) {
	now := time.Now().UTC()
	model := input.Model
	if model == "" {
		model = "gpt-4o"
	}
	provider := input.Provider
	if provider == "" {
		provider = "openai"
	}
	session := &models.ChatSession{
		ID:         uuid.New().String(),
		CompanyID:  input.CompanyID,
		UserID:     input.UserID,
		Title:      input.Title,
		Model:      model,
		Provider:   provider,
		Status:     "active",
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	_, err := r.coll.InsertOne(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("insert session: %w", err)
	}
	return session, nil
}

func (r *SessionRepo) GetByID(ctx context.Context, id string) (*models.ChatSession, error) {
	var session models.ChatSession
	err := r.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&session)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get session: %w", err)
	}
	return &session, nil
}

func (r *SessionRepo) ListByCompany(ctx context.Context, companyID, userID string, limit, offset int) (*models.SessionListResponse, error) {
	filter := bson.M{"company_id": companyID}
	if userID != "" {
		filter["user_id"] = userID
	}

	total, err := r.coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("count sessions: %w", err)
	}

	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	cursor, err := r.coll.Find(ctx, filter,
		options.Find().SetSort(bson.M{"updated_at": -1}).SetSkip(int64(offset)).SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("find sessions: %w", err)
	}
	defer cursor.Close(ctx)

	var sessions []models.ChatSession
	if err := cursor.All(ctx, &sessions); err != nil {
		return nil, fmt.Errorf("decode sessions: %w", err)
	}

	items := make([]models.SessionListItem, len(sessions))
	for i, s := range sessions {
		items[i] = models.SessionListItem{
			ID:        s.ID,
			Title:     s.Title,
			Model:     s.Model,
			CreatedAt: s.CreatedAt,
			UpdatedAt: s.UpdatedAt,
		}
	}

	return &models.SessionListResponse{Sessions: items, Total: int(total)}, nil
}

func (r *SessionRepo) UpdateTitle(ctx context.Context, id, title string) error {
	_, err := r.coll.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"title": title, "updated_at": time.Now().UTC()}})
	return err
}

func (r *SessionRepo) UpdateTokenCount(ctx context.Context, id string, count int) error {
	_, err := r.coll.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"token_count": count, "updated_at": time.Now().UTC()}})
	return err
}
