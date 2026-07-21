package repository

import (
	"context"
	"errors"
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
		ID:            uuid.New().String(),
		CompanyID:     input.CompanyID,
		UserID:        input.UserID,
		Title:         input.Title,
		Model:         model,
		Provider:      provider,
		Status:        "active",
		AccessProfile: input.AccessProfile,
		ProjectID:     input.ProjectID,
		CreatedAt:     now,
		UpdatedAt:     now,
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

// BindLegacyProject atomically assigns the first authorized project to a
// legacy session whose projectId is empty/missing. Concurrent continuations
// cannot bind the same session to different projects.
func (r *SessionRepo) BindLegacyProject(ctx context.Context, id, companyID, userID, profile, projectID string) (*models.ChatSession, error) {
	profileFilter := bson.A{bson.M{"accessProfile": profile}}
	if profile == models.AccessProfileClient {
		profileFilter = append(profileFilter, bson.M{"accessProfile": ""}, bson.M{"accessProfile": bson.M{"$exists": false}})
	}
	filter := bson.M{
		"_id": id, "companyId": companyID, "userId": userID,
		"$and": bson.A{
			bson.M{"$or": bson.A{bson.M{"projectId": ""}, bson.M{"projectId": bson.M{"$exists": false}}}},
			bson.M{"$or": profileFilter},
		},
	}
	var session models.ChatSession
	err := r.coll.FindOneAndUpdate(ctx, filter, bson.M{"$set": bson.M{"projectId": projectID, "updatedAt": time.Now().UTC()}},
		options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&session)
	if errors.Is(err, mongo.ErrNoDocuments) {
		// A concurrent caller may have completed the same binding.
		existing, getErr := r.GetByID(ctx, id)
		if getErr != nil {
			return nil, getErr
		}
		if existing == nil || existing.ProjectID != projectID {
			return nil, fmt.Errorf("legacy session project already bound")
		}
		return existing, nil
	}
	if err != nil {
		return nil, fmt.Errorf("bind legacy session project: %w", err)
	}
	return &session, nil
}

// ListByCompany lists sessions within pre-authorized project sets. Client
// filtering keeps unbound legacy sessions visible for first-send binding;
// project-bound sessions must match the current profile visibility snapshot.
func (r *SessionRepo) ListByCompany(ctx context.Context, companyID, userID, accessProfile string, clientProjectIDs, developerProjectIDs []string, limit, offset int) (*models.SessionListResponse, error) {
	filter := buildSessionListFilter(companyID, userID, accessProfile, clientProjectIDs, developerProjectIDs)

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
		options.Find().SetSort(bson.M{"updatedAt": -1}).SetSkip(int64(offset)).SetLimit(int64(limit)))
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
			ID:            s.ID,
			Title:         s.Title,
			Model:         s.Model,
			AccessProfile: s.AccessProfile,
			ProjectID:     s.ProjectID,
			CreatedAt:     s.CreatedAt,
			UpdatedAt:     s.UpdatedAt,
		}
	}

	return &models.SessionListResponse{Sessions: items, Total: int(total)}, nil
}

func buildSessionListFilter(companyID, userID, accessProfile string, clientProjectIDs, developerProjectIDs []string) bson.M {
	filter := bson.M{"companyId": companyID}
	if userID != "" {
		filter["userId"] = userID
	}
	clientProfile := bson.M{"$or": bson.A{
		bson.M{"accessProfile": models.AccessProfileClient},
		bson.M{"accessProfile": ""},
		bson.M{"accessProfile": bson.M{"$exists": false}},
	}}
	clientProject := bson.M{"$or": bson.A{
		bson.M{"projectId": bson.M{"$in": clientProjectIDs}},
		bson.M{"projectId": ""},
		bson.M{"projectId": bson.M{"$exists": false}},
	}}
	clientCondition := bson.M{"$and": bson.A{clientProfile, clientProject}}
	developerCondition := bson.M{"accessProfile": models.AccessProfileDeveloper, "projectId": bson.M{"$in": developerProjectIDs}}
	switch {
	case accessProfile == models.AccessProfileClient:
		filter["$and"] = clientCondition["$and"]
	case accessProfile == models.AccessProfileDeveloper:
		for key, value := range developerCondition {
			filter[key] = value
		}
	default:
		filter["$or"] = bson.A{clientCondition, developerCondition}
	}
	return filter
}

func (r *SessionRepo) UpdateTitle(ctx context.Context, id, title string) error {
	_, err := r.coll.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"title": title, "updatedAt": time.Now().UTC()}})
	return err
}

func (r *SessionRepo) UpdateTokenCount(ctx context.Context, id string, count int) error {
	_, err := r.coll.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"tokenCount": count, "updatedAt": time.Now().UTC()}})
	return err
}
