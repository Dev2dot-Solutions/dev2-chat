package models

import (
	"strconv"
	"strings"
)

// NormalizeToolTrace removes request-level noise and folds lifecycle updates
// for the same tool/delegation into one final persisted entry. Input order is
// retained according to the first event for each lifecycle span.
func NormalizeToolTrace(events []ToolTraceEvent) []ToolTraceEvent {
	var normalized []ToolTraceEvent
	byLifecycle := make(map[string]int)
	seenEventID := make(map[string]struct{})

	for _, event := range events {
		if !IsToolTraceEvent(event) {
			continue
		}
		if event.EventID != "" {
			if _, seen := seenEventID[event.EventID]; seen {
				continue
			}
			seenEventID[event.EventID] = struct{}{}
		}

		key := traceLifecycleKey(event)
		if index, ok := byLifecycle[key]; ok {
			normalized[index] = mergeToolTraceEvent(normalized[index], event)
			continue
		}
		byLifecycle[key] = len(normalized)
		normalized = append(normalized, event)
	}

	return normalized
}

func isRequestTraceEvent(eventType string) bool {
	normalized := strings.ReplaceAll(strings.ToLower(eventType), "-", "_")
	return strings.HasPrefix(normalized, "request_")
}

// IsToolTraceEvent reports whether an event represents tool or delegation
// activity suitable for display and persistence. Request/model lifecycle noise
// is intentionally excluded.
func IsToolTraceEvent(event ToolTraceEvent) bool {
	if isRequestTraceEvent(event.Type) {
		return false
	}
	eventType := strings.ToLower(event.Type)
	return strings.Contains(eventType, "tool") || strings.Contains(eventType, "delegat")
}

func traceLifecycleKey(event ToolTraceEvent) string {
	if event.SpanID != "" {
		return "span:" + event.SpanID
	}

	// Older emitters lack spans. The lifecycle family removes started/completed
	// state while the remaining stable attribution fields avoid merging
	// unrelated tools or nested delegations where possible.
	return strings.Join([]string{
		"legacy", traceEventFamily(event.Type), event.ToolCallID,
		event.ParentSpanID, event.ParentToolCallID, strconv.Itoa(event.DelegationDepth),
	}, "|")
}

func traceEventFamily(eventType string) string {
	normalized := strings.ReplaceAll(strings.ToLower(eventType), "-", "_")
	switch {
	case strings.Contains(normalized, "delegat"):
		return "delegation"
	case strings.Contains(normalized, "tool"):
		return "tool"
	}
	for _, suffix := range []string{"_started", "_completed", "_failed", "_cancelled", "_canceled"} {
		normalized = strings.TrimSuffix(normalized, suffix)
	}
	return normalized
}

func mergeToolTraceEvent(current, update ToolTraceEvent) ToolTraceEvent {
	if update.EventID != "" {
		current.EventID = update.EventID
	}
	if update.Type != "" {
		current.Type = update.Type
	}
	if update.SessionID != "" {
		current.SessionID = update.SessionID
	}
	if update.SpanID != "" {
		current.SpanID = update.SpanID
	}
	if update.ParentSpanID != "" {
		current.ParentSpanID = update.ParentSpanID
	}
	if update.ToolCallID != "" {
		current.ToolCallID = update.ToolCallID
	}
	if update.ParentToolCallID != "" {
		current.ParentToolCallID = update.ParentToolCallID
	}
	if update.ToolName != "" {
		current.ToolName = update.ToolName
	}
	if update.PersonaName != "" {
		current.PersonaName = update.PersonaName
	}
	if update.PersonaScope != "" {
		current.PersonaScope = update.PersonaScope
	}
	current.DelegationDepth = update.DelegationDepth
	if update.Summary != "" {
		current.Summary = update.Summary
	}
	if update.Status != "" {
		current.Status = update.Status
	}
	if update.Timestamp != "" {
		current.Timestamp = update.Timestamp
	}
	if update.DurationMS != nil {
		current.DurationMS = update.DurationMS
	}
	if update.Success != nil {
		current.Success = update.Success
	}
	return current
}
