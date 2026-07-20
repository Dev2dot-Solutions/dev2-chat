package models

import "strings"

// NormalizeToolTrace removes request-level noise and folds lifecycle updates
// for the same tool/delegation into one final persisted entry. Input order is
// retained according to the first event for each tool call.
func NormalizeToolTrace(events []ToolTraceEvent) []ToolTraceEvent {
	var normalized []ToolTraceEvent
	byToolCallID := make(map[string]int)
	seenEventID := make(map[string]struct{})

	for _, event := range events {
		if !isPersistableTraceEvent(event) {
			continue
		}
		if event.EventID != "" {
			if _, seen := seenEventID[event.EventID]; seen {
				continue
			}
			seenEventID[event.EventID] = struct{}{}
		}

		if event.ToolCallID == "" {
			normalized = append(normalized, event)
			continue
		}
		if index, ok := byToolCallID[event.ToolCallID]; ok {
			normalized[index] = mergeToolTraceEvent(normalized[index], event)
			continue
		}
		byToolCallID[event.ToolCallID] = len(normalized)
		normalized = append(normalized, event)
	}

	return normalized
}

func isRequestTraceEvent(eventType string) bool {
	normalized := strings.ReplaceAll(strings.ToLower(eventType), "-", "_")
	return strings.HasPrefix(normalized, "request_")
}

func isPersistableTraceEvent(event ToolTraceEvent) bool {
	if isRequestTraceEvent(event.Type) {
		return false
	}
	eventType := strings.ToLower(event.Type)
	return event.ToolCallID != "" || event.ParentToolCallID != "" ||
		event.ToolName != "" || event.PersonaName != "" ||
		strings.Contains(eventType, "tool") || strings.Contains(eventType, "delegat")
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
