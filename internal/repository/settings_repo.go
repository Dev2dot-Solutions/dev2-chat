package repository

import (
	"context"
	"fmt"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SettingsRepo struct {
	coll *mongo.Collection
}

func NewSettingsRepo(db *mongo.Database) *SettingsRepo {
	return &SettingsRepo{coll: db.Collection("companies")}
}

func (r *SettingsRepo) GetLLMConfig(ctx context.Context, companyID string) (*models.LlmConfig, error) {
	var company struct {
		Settings struct {
			LLM *models.LlmConfig `bson:"llm"`
		} `bson:"settings"`
	}
	err := r.coll.FindOne(ctx, bson.M{"_id": companyID},
		options.FindOne().SetProjection(bson.M{"settings.llm": 1})).Decode(&company)
	if err == mongo.ErrNoDocuments { return nil, nil }
	if err != nil { return nil, fmt.Errorf("get LLM config: %w", err) }
	return company.Settings.LLM, nil
}

func (r *SettingsRepo) GetPTConfig(ctx context.Context, companyID string) (*models.PtConfig, error) {
	var company struct {
		Settings struct {
			PT *models.PtConfig `bson:"pt"`
		} `bson:"settings"`
	}
	err := r.coll.FindOne(ctx, bson.M{"_id": companyID},
		options.FindOne().SetProjection(bson.M{"settings.pt": 1})).Decode(&company)
	if err == mongo.ErrNoDocuments { return nil, nil }
	if err != nil { return nil, fmt.Errorf("get PT config: %w", err) }
	return company.Settings.PT, nil
}
