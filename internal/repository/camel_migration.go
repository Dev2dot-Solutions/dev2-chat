package repository

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// MigrateSnakeToCamel renames snake_case document keys to lowerCamelCase in the
// given collections. It runs at startup before traffic is served.
//
// Mongo's $rename operator cannot touch array element fields, so this uses a
// read-transform-write strategy: each document is decoded into a map, keys are
// renamed recursively (nested documents and arrays of documents included), and
// the document is written back with ReplaceOne by _id only when something
// changed. Already-migrated documents produce no change, so the migration is
// idempotent and safe to run on every startup.
func MigrateSnakeToCamel(ctx context.Context, db *mongo.Database, collections []string, renames map[string]string) {
	for _, name := range collections {
		coll := db.Collection(name)
		cursor, err := coll.Find(ctx, bson.M{})
		if err != nil {
			log.Printf("[migration] %s: find failed: %v", name, err)
			continue
		}
		migrated := 0
		for cursor.Next(ctx) {
			var doc bson.M
			if err := cursor.Decode(&doc); err != nil {
				log.Printf("[migration] %s: decode failed: %v", name, err)
				continue
			}
			newDoc, changed := renameMapKeys(doc, renames)
			if !changed {
				continue
			}
			if _, err := coll.ReplaceOne(ctx, bson.M{"_id": doc["_id"]}, newDoc); err != nil {
				log.Printf("[migration] %s: replace %v failed: %v", name, doc["_id"], err)
				continue
			}
			migrated++
		}
		if err := cursor.Err(); err != nil {
			log.Printf("[migration] %s: cursor error: %v", name, err)
		}
		cursor.Close(ctx)
		log.Printf("[migration] %s: migrated %d documents to camelCase keys", name, migrated)
	}
}

// renameMapKeys returns a copy of m with snake_case keys replaced by their
// camelCase equivalents (recursively). If both the snake_case key and its
// camelCase target exist, the camelCase value wins and the rename is skipped.
func renameMapKeys(m map[string]any, renames map[string]string) (map[string]any, bool) {
	changed := false
	out := make(map[string]any, len(m))
	for k, v := range m {
		nk := k
		if camel, ok := renames[k]; ok {
			if _, exists := m[camel]; !exists {
				nk = camel
				changed = true
			}
		}
		nv, childChanged := renameValueKeys(v, renames)
		if childChanged {
			changed = true
		}
		out[nk] = nv
	}
	return out, changed
}

// renameValueKeys recurses into nested documents and arrays of documents.
func renameValueKeys(v any, renames map[string]string) (any, bool) {
	switch t := v.(type) {
	case map[string]any:
		return renameMapKeys(t, renames)
	case []any:
		changed := false
		for i, e := range t {
			ne, childChanged := renameValueKeys(e, renames)
			if childChanged {
				t[i] = ne
				changed = true
			}
		}
		return t, changed
	default:
		return v, false
	}
}
