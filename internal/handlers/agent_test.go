package handlers

import (
	"encoding/json"
	"testing"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
)

func TestBuildStreamMetaSerializesFinalAnswer(t *testing.T) {
	const answer = "The exact final answer"
	meta := buildStreamMeta("session-1", agentResult{answer: answer}, []models.ToolTraceEvent{{SpanID: "span-1", Type: "tool_completed"}}, nil)

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
