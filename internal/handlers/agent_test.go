package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
	"github.com/Dev2dot-Solutions/dev2-chat/internal/repository"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestBuildSocketMetaSerializesFinalAnswer(t *testing.T) {
	const answer = "The exact final answer"
	meta := buildSocketMeta("session-1", agentResult{answer: answer}, []models.ToolTraceEvent{{SpanID: "span-1", Type: "tool_completed"}}, nil)

	payload, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	var decoded models.ChatResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	if decoded.Answer != answer {
		t.Fatalf("expected final answer %q in meta, got %q", answer, decoded.Answer)
	}
	if len(decoded.ToolTrace) != 1 || decoded.ToolTrace[0].SpanID != "span-1" {
		t.Fatalf("expected normalized trace in meta, got %#v", decoded.ToolTrace)
	}
}

func TestGeneratedUserAndAssistantMessagesPersistRequestID(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("request id", func(mt *mtest.T) {
		handler := &AgentHandler{messageRepo: repository.NewMessageRepo(mt.DB)}
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		mt.AddMockResponses(mtest.CreateSuccessResponse())
		handler.saveMessage(request, "session", "request-1", "user", "hello", "", "")
		assertInsertedRequestID(mt, "request-1")

		mt.AddMockResponses(mtest.CreateSuccessResponse())
		handler.finishAsk(request, &models.ChatSession{ID: "session"}, models.ChatRequest{RequestID: "request-1"}, "user", agentResult{answer: "done"}, nil)
		assertInsertedRequestID(mt, "request-1")
	})
}

func assertInsertedRequestID(mt *mtest.T, expected string) {
	mt.Helper()
	event := mt.GetStartedEvent()
	if event == nil || event.CommandName != "insert" {
		mt.Fatalf("expected message insert, got %#v", event)
	}
	values, err := event.Command.Lookup("documents").Array().Values()
	if err != nil || len(values) != 1 {
		mt.Fatalf("invalid insert command: %s", event.Command)
	}
	if value := values[0].Document().Lookup("requestId").StringValue(); value != expected {
		mt.Fatalf("requestId=%q want %q", value, expected)
	}
}
