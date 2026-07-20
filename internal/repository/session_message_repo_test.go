package repository

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestBindLegacyProjectUsesAtomicConditionalUpdate(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("bind", func(mt *mtest.T) {
		repo := &SessionRepo{coll: mt.Coll}
		doc := bson.D{
			{Key: "_id", Value: "session-1"}, {Key: "companyId", Value: "company-1"},
			{Key: "userId", Value: "user-1"}, {Key: "accessProfile", Value: "client"},
			{Key: "projectId", Value: "project-1"}, {Key: "createdAt", Value: time.Now()}, {Key: "updatedAt", Value: time.Now()},
		}
		mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "value", Value: doc}))
		session, err := repo.BindLegacyProject(context.Background(), "session-1", "company-1", "user-1", "client", "project-1")
		if err != nil || session.ProjectID != "project-1" {
			mt.Fatalf("legacy binding failed: session=%#v err=%v", session, err)
		}
		started := mt.GetStartedEvent()
		if started == nil || started.CommandName != "findAndModify" {
			mt.Fatalf("expected atomic findAndModify, got %#v", started)
		}
		update := started.Command.Lookup("update").Document().Lookup("$set").Document()
		if update.Lookup("projectId").StringValue() != "project-1" {
			mt.Fatalf("project binding missing from update: %s", update)
		}
	})
}

func TestListBySessionQueriesLatestThenReturnsChronological(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("latest", func(mt *mtest.T) {
		repo := &MessageRepo{coll: mt.Coll}
		namespace := mt.Coll.Database().Name() + "." + mt.Coll.Name()
		mt.AddMockResponses(mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch,
			bson.D{{Key: "_id", Value: "latest"}, {Key: "sessionId", Value: "s"}, {Key: "createdAt", Value: time.Now()}},
			bson.D{{Key: "_id", Value: "older"}, {Key: "sessionId", Value: "s"}, {Key: "createdAt", Value: time.Now().Add(-time.Minute)}},
		))
		messages, err := repo.ListBySession(context.Background(), "s", 2)
		if err != nil || len(messages) != 2 || messages[0].ID != "older" || messages[1].ID != "latest" {
			mt.Fatalf("unexpected latest history: %#v err=%v", messages, err)
		}
		started := mt.GetStartedEvent()
		sort := started.Command.Lookup("sort").Document()
		if sort.Lookup("createdAt").Int32() != -1 || started.Command.Lookup("limit").Int64() != 2 {
			mt.Fatalf("query did not select latest messages: %s", started.Command)
		}
	})
}

func TestClientSessionListIncludesLegacyProfilesWithoutDeveloper(t *testing.T) {
	filter := buildSessionListFilter("company", "user", "client", false)
	alternatives, ok := filter["$or"].(bson.A)
	if !ok || len(alternatives) != 3 {
		t.Fatalf("client legacy profile alternatives missing: %#v", filter)
	}
	encoded, _ := bson.MarshalExtJSON(filter, false, false)
	if strings.Contains(string(encoded), `"developer"`) {
		t.Fatalf("client filter exposes developer sessions: %s", encoded)
	}
}
