package repository

import (
	"context"
	"fmt"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// KnowledgeRepo provides direct MongoDB access to knowledge graph entities.
// Used as a fallback until dev2-knowledge service is available.
// Eventually all knowledge access goes through knowledge.* NATS subjects.
type KnowledgeRepo struct {
	db *mongo.Database
}

func NewKnowledgeRepo(db *mongo.Database) *KnowledgeRepo {
	return &KnowledgeRepo{db: db}
}

// SearchEntityByText performs $text search on a single collection.
func (r *KnowledgeRepo) SearchEntityByText(ctx context.Context, collection, query, companyID string, limit int) ([]map[string]any, error) {
	coll := r.db.Collection(collection)
	filter := bson.M{"$text": bson.M{"$search": query}}
	if companyID != "" {
		// Only add companyId filter if the collection has it (conventions, business_rules, etc.)
		// We include it even for non-tenant-scoped collections — $text will match regardless
		filter["companyId"] = companyID
	}

	cur, err := coll.Find(ctx, filter,
		options.Find().SetSort(bson.M{"score": bson.M{"$meta": "textScore"}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("search %s: %w", collection, err)
	}
	defer cur.Close(ctx)

	var results []map[string]any
	if err := cur.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("decode %s: %w", collection, err)
	}
	return results, nil
}

// GetEntityByID fetches a single entity by ID from a collection.
func (r *KnowledgeRepo) GetEntityByID(ctx context.Context, collection, id string) (map[string]any, error) {
	coll := r.db.Collection(collection)
	var result map[string]any
	err := coll.FindOne(ctx, bson.M{"_id": id}).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get %s/%s: %w", collection, id, err)
	}
	return result, nil
}

// Tier1Types are the primary knowledge entity types for context search.
var Tier1Types = []string{
	"conventions", "business_rules", "architecture_decisions", "domain_terms", "processes",
}

// SearchKnowledgeGraph searches all Tier 1 knowledge types and returns combined results.
func (r *KnowledgeRepo) SearchKnowledgeGraph(ctx context.Context, query, companyID string, limit int) (*models.KnowledgeSearchResponse, error) {
	response := &models.KnowledgeSearchResponse{
		Query:   query,
		Results: make(map[string][]models.KnowledgeSearchResult),
	}

	types := Tier1Types
	if limit <= 0 {
		limit = 5
	}

	for _, entityType := range types {
		docs, err := r.SearchEntityByText(ctx, entityType, query, companyID, limit)
		if err != nil {
			continue
		}
		for _, doc := range docs {
			id, _ := doc["_id"].(string)
			if id == "" {
				continue
			}
			name, _ := doc["name"].(string)
			if name == "" {
				if v, ok := doc["rule"].(string); ok {
					name = v
				} else if v, ok := doc["term"].(string); ok {
					name = v
				} else if v, ok := doc["topic"].(string); ok {
					name = v
				}
			}
			snippet := ""
			if v, ok := doc["description"].(string); ok {
				snippet = truncate(v, 200)
			} else if v, ok := doc["rule"].(string); ok {
				snippet = truncate(v, 200)
			} else if v, ok := doc["definition"].(string); ok {
				snippet = truncate(v, 200)
			}
			response.Results[entityType] = append(response.Results[entityType], models.KnowledgeSearchResult{
				Type:    entityType,
				ID:      id,
				Name:    name,
				Snippet: snippet,
			})
			response.TotalMatches++
		}
	}
	return response, nil
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}
