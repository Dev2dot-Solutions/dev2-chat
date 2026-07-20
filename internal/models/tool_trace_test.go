package models

import "testing"

func TestNormalizeToolTraceMergesLifecycleAndDropsRequestNoise(t *testing.T) {
	duration := int64(42)
	success := true
	events := []ToolTraceEvent{
		{EventID: "request-1", Type: "request_started", SessionID: "session-1", Status: "started"},
		{
			EventID: "tool-1-start", Type: "tool_started", SessionID: "session-1",
			ToolCallID: "tool-1", ParentToolCallID: "parent-1", ToolName: "search",
			PersonaName: "researcher", PersonaScope: "project", DelegationDepth: 2,
			Summary: "Searching", Status: "started", Timestamp: "2026-07-20T10:00:00Z",
		},
		// Redelivery of the same progress event must not create another entry.
		{EventID: "tool-1-start", Type: "tool_started", ToolCallID: "tool-1", Status: "started"},
		{
			EventID: "tool-1-done", Type: "tool_completed", SessionID: "session-1",
			ToolCallID: "tool-1", DelegationDepth: 2, Summary: "Search complete",
			Status: "completed", Timestamp: "2026-07-20T10:00:01Z",
			DurationMS: &duration, Success: &success,
		},
		{EventID: "delegation-1", Type: "delegation_started", SessionID: "session-1", PersonaName: "reviewer", DelegationDepth: 1, Status: "started"},
		{EventID: "request-2", Type: "request_completed", SessionID: "session-1", Status: "completed"},
	}

	trace := NormalizeToolTrace(events)
	if len(trace) != 2 {
		t.Fatalf("expected 2 persisted trace entries, got %d: %#v", len(trace), trace)
	}

	tool := trace[0]
	if tool.EventID != "tool-1-done" || tool.Type != "tool_completed" || tool.Status != "completed" {
		t.Fatalf("expected completed tool state, got %#v", tool)
	}
	if tool.ToolName != "search" || tool.ParentToolCallID != "parent-1" || tool.PersonaName != "researcher" || tool.PersonaScope != "project" || tool.DelegationDepth != 2 {
		t.Fatalf("expected attribution and delegation metadata to survive merge, got %#v", tool)
	}
	if tool.DurationMS == nil || *tool.DurationMS != duration || tool.Success == nil || !*tool.Success {
		t.Fatalf("expected completion metadata, got %#v", tool)
	}
	if trace[1].EventID != "delegation-1" {
		t.Fatalf("expected unkeyed delegation event to retain order, got %#v", trace[1])
	}
}

func TestNormalizeToolTraceKeepsOnlyToolAndDelegationMetadata(t *testing.T) {
	events := []ToolTraceEvent{
		{EventID: "model-1", Type: "model_started", Status: "started"},
		{EventID: "tool-1", Type: "progress", ToolName: "read_file", Status: "started"},
		{EventID: "persona-1", Type: "progress", PersonaName: "tester", Status: "started"},
	}

	trace := NormalizeToolTrace(events)
	if len(trace) != 2 || trace[0].EventID != "tool-1" || trace[1].EventID != "persona-1" {
		t.Fatalf("unexpected normalized trace: %#v", trace)
	}
}
