package models

import (
	"encoding/json"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestChatMessageRequestIDSupportsRESTReconciliation(t *testing.T) {
	message := ChatMessage{SessionID: "s", RequestID: "request-1", Role: "assistant", Content: "done"}
	jsonPayload, err := json.Marshal(message)
	if err != nil || !json.Valid(jsonPayload) {
		t.Fatalf("marshal JSON: %s err=%v", jsonPayload, err)
	}
	var jsonDoc map[string]any
	_ = json.Unmarshal(jsonPayload, &jsonDoc)
	if jsonDoc["requestId"] != "request-1" {
		t.Fatalf("requestId missing from JSON: %s", jsonPayload)
	}
	bsonPayload, err := bson.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	var bsonDoc bson.M
	_ = bson.Unmarshal(bsonPayload, &bsonDoc)
	if bsonDoc["requestId"] != "request-1" {
		t.Fatalf("requestId missing from BSON: %#v", bsonDoc)
	}
}
