package models

import "testing"

func TestNormalizeToolTraceMergesBySpanAndDropsRequestNoise(t *testing.T) {
	duration := int64(42)
	success := true
	events := []ToolTraceEvent{
		{EventID: "request-start", Type: "request_started", SessionID: "session-1", SpanID: "request-span", Status: "started"},
		{
			EventID: "tool-a-start", Type: "tool_started", SessionID: "session-1",
			SpanID: "span-a", ParentSpanID: "root-span", ToolCallID: "provider-call-1",
			ParentToolCallID: "parent-1", ToolName: "search", DelegationDepth: 1,
			Summary: "Searching first source", Status: "started", Timestamp: "2026-07-20T10:00:00Z",
		},
		{
			EventID: "tool-a-done", Type: "tool_completed", SessionID: "session-1",
			SpanID: "span-a", ParentSpanID: "root-span", ToolCallID: "provider-call-1",
			ParentToolCallID: "parent-1", DelegationDepth: 1, Summary: "First search complete",
			Status: "completed", Timestamp: "2026-07-20T10:00:01Z",
			DurationMS: &duration, Success: &success,
		},
		// Providers may reuse a toolCallId; a distinct span must remain distinct.
		{EventID: "tool-b-start", Type: "tool_started", SpanID: "span-b", ToolCallID: "provider-call-1", ToolName: "search", DelegationDepth: 1, Status: "started"},
		{EventID: "tool-b-done", Type: "tool_completed", SpanID: "span-b", ToolCallID: "provider-call-1", DelegationDepth: 1, Status: "completed", Success: &success},
		{
			EventID: "delegation-start", Type: "delegation_started", SessionID: "session-1",
			SpanID: "span-delegation", ParentSpanID: "root-span", ParentToolCallID: "parent-1",
			PersonaName: "reviewer", PersonaScope: "project", DelegationDepth: 2, Status: "started",
		},
		{
			EventID: "delegation-done", Type: "delegation_completed", SessionID: "session-1",
			SpanID: "span-delegation", ParentSpanID: "root-span", ParentToolCallID: "parent-1",
			DelegationDepth: 2, Status: "completed", Success: &success,
		},
		{EventID: "request-done", Type: "request_completed", SessionID: "session-1", SpanID: "request-span", Status: "completed"},
	}

	trace := NormalizeToolTrace(events)
	if len(trace) != 3 {
		t.Fatalf("expected 3 persisted span entries, got %d: %#v", len(trace), trace)
	}

	firstTool := trace[0]
	if firstTool.SpanID != "span-a" || firstTool.EventID != "tool-a-done" || firstTool.Type != "tool_completed" || firstTool.Status != "completed" {
		t.Fatalf("expected completed first tool span, got %#v", firstTool)
	}
	if firstTool.ToolName != "search" || firstTool.ParentSpanID != "root-span" || firstTool.ParentToolCallID != "parent-1" {
		t.Fatalf("expected start metadata to survive merge, got %#v", firstTool)
	}
	if trace[1].SpanID != "span-b" || trace[1].ToolCallID != firstTool.ToolCallID {
		t.Fatalf("expected repeated toolCallId in a distinct span, got %#v", trace[1])
	}

	delegation := trace[2]
	if delegation.SpanID != "span-delegation" || delegation.EventID != "delegation-done" || delegation.Type != "delegation_completed" {
		t.Fatalf("expected delegation lifecycle pair to merge, got %#v", delegation)
	}
	if delegation.PersonaName != "reviewer" || delegation.PersonaScope != "project" || delegation.ParentSpanID != "root-span" || delegation.DelegationDepth != 2 {
		t.Fatalf("expected delegation attribution to survive merge, got %#v", delegation)
	}
	for _, event := range trace {
		if isRequestTraceEvent(event.Type) {
			t.Fatalf("request lifecycle noise persisted: %#v", event)
		}
	}
}

func TestNormalizeToolTraceFallsBackForLegacyEmitter(t *testing.T) {
	events := []ToolTraceEvent{
		{EventID: "legacy-start", Type: "tool_started", ToolCallID: "tool-1", ParentToolCallID: "parent-1", ToolName: "read_file", DelegationDepth: 2, Status: "started"},
		{EventID: "legacy-done", Type: "tool_completed", ToolCallID: "tool-1", ParentToolCallID: "parent-1", DelegationDepth: 2, Status: "completed"},
		{EventID: "model-1", Type: "model_started", Status: "started"},
	}

	trace := NormalizeToolTrace(events)
	if len(trace) != 1 || trace[0].EventID != "legacy-done" || trace[0].ToolName != "read_file" {
		t.Fatalf("unexpected legacy normalized trace: %#v", trace)
	}
}
