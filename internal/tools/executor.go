package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
)

// Executor routes tool calls from the LLM to the appropriate handler.
type Executor struct {
	knowledgeRepo KnowledgeProvider
	ticketsClient TicketsClient
	ptClient      PtClientProvider
	ptConfigFn    func(ctx context.Context, companyID string) (*models.PtConfig, error)
}

// KnowledgeProvider interface for knowledge graph lookups.
type KnowledgeProvider interface {
	SearchEntityByText(ctx context.Context, collection, query, companyID string, limit int) ([]map[string]any, error)
	GetEntityByID(ctx context.Context, collection, id string) (map[string]any, error)
	SearchKnowledgeGraph(ctx context.Context, query, companyID string, limit int) (*models.KnowledgeSearchResponse, error)
}

// TicketsClient interface for ticket operations (calls dev2-tickets via HTTP).
type TicketsClient interface {
	CreateTicket(ctx context.Context, companyID, title, description, ticketType, createdBy string, priority int) (map[string]any, error)
	GetTicket(ctx context.Context, ticketID string) (map[string]any, error)
	ListTickets(ctx context.Context, companyID string, status, ticketType, assignedTo, search string, limit int) ([]map[string]any, error)
	AddComment(ctx context.Context, ticketID, authorID, body string) (map[string]any, error)
}

// PtClientProvider interface for Project Tracker operations.
type PtClientProvider interface {
	CreateItem(token, projectKey string, item *models.PtItem) (*models.PtItem, error)
	GetItem(token, itemKey string) (*models.PtItem, error)
	SearchItems(token, query string) ([]models.PtItem, error)
	UpdateItem(token, itemKey string, changes map[string]any) (*models.PtItem, error)
}

// NewExecutor creates a new tool executor.
func NewExecutor(kr KnowledgeProvider, tc TicketsClient, ptc PtClientProvider, ptFn func(ctx context.Context, companyID string) (*models.PtConfig, error)) *Executor {
	return &Executor{
		knowledgeRepo: kr,
		ticketsClient: tc,
		ptClient:      ptc,
		ptConfigFn:    ptFn,
	}
}

// ToolDefinitions returns all tools the LLM can call.
func (e *Executor) ToolDefinitions() []models.ToolDefinition {
	return []models.ToolDefinition{
		{
			Type: "function",
			Function: models.ToolFunction{
				Name:        "search_knowledge",
				Description: "Full-text search across conventions, business rules, domain terms, decisions, and processes",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"query":      map[string]any{"type": "string", "description": "Search query"},
						"companyId": map[string]any{"type": "string", "format": "uuid"},
					},
					"required": []string{"query", "companyId"},
				},
			},
		},
		{
			Type: "function",
			Function: models.ToolFunction{
				Name:        "get_entity",
				Description: "Get a single knowledge graph entity by type and ID",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"type":       map[string]any{"type": "string", "enum": []string{"conventions", "business_rules", "domain_terms", "architecture_decisions", "processes", "functions", "classes", "files", "tickets"}},
						"id":         map[string]any{"type": "string", "format": "uuid"},
						"companyId": map[string]any{"type": "string", "format": "uuid"},
					},
					"required": []string{"type", "id", "companyId"},
				},
			},
		},
		{
			Type: "function",
			Function: models.ToolFunction{
				Name:        "create_ticket",
				Description: "Create a new helpdesk/request ticket",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"companyId":  map[string]any{"type": "string", "format": "uuid"},
						"title":       map[string]any{"type": "string"},
						"description": map[string]any{"type": "string"},
						"type":        map[string]any{"type": "string", "enum": []string{"bug", "feature", "task", "improvement"}},
						"priority":    map[string]any{"type": "integer", "minimum": 1, "maximum": 5},
						"createdBy":  map[string]any{"type": "string", "format": "uuid"},
					},
					"required": []string{"companyId", "title", "description", "createdBy"},
				},
			},
		},
		{
			Type: "function",
			Function: models.ToolFunction{
				Name:        "get_ticket",
				Description: "Get ticket details with conversations",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"ticketId": map[string]any{"type": "string", "format": "uuid"},
					},
					"required": []string{"ticketId"},
				},
			},
		},
		{
			Type: "function",
			Function: models.ToolFunction{
				Name:        "list_tickets",
				Description: "List tickets with filters",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"companyId":  map[string]any{"type": "string", "format": "uuid"},
						"status":      map[string]any{"type": "string", "enum": []string{"open", "in_progress", "resolved", "closed"}},
						"type":        map[string]any{"type": "string", "enum": []string{"bug", "feature", "task", "improvement"}},
						"assignedTo": map[string]any{"type": "string", "format": "uuid"},
						"search":      map[string]any{"type": "string"},
						"limit":       map[string]any{"type": "integer", "maximum": 50},
					},
					"required": []string{"companyId"},
				},
			},
		},
		{
			Type: "function",
			Function: models.ToolFunction{
				Name:        "add_comment",
				Description: "Add a comment to a ticket",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"ticketId": map[string]any{"type": "string", "format": "uuid"},
						"authorId": map[string]any{"type": "string", "format": "uuid"},
						"body":      map[string]any{"type": "string"},
					},
					"required": []string{"ticketId", "authorId", "body"},
				},
			},
		},
		{
			Type: "function",
			Function: models.ToolFunction{
				Name:        "create_pt_item",
				Description: "Create a story or task in the Project Tracker",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"companyId":  map[string]any{"type": "string", "format": "uuid"},
						"type":        map[string]any{"type": "string", "enum": []string{"story", "task"}},
						"title":       map[string]any{"type": "string"},
						"description": map[string]any{"type": "string"},
						"priority":    map[string]any{"type": "string", "enum": []string{"low", "medium", "high", "critical"}},
						"labels":      map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required": []string{"companyId", "title", "description"},
				},
			},
		},
		{
			Type: "function",
			Function: models.ToolFunction{
				Name:        "read_pt_item",
				Description: "Get the full details of a Project Tracker item",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"companyId": map[string]any{"type": "string", "format": "uuid"},
						"itemKey":   map[string]any{"type": "string", "description": "e.g. DEV2-15"},
					},
					"required": []string{"companyId", "itemKey"},
				},
			},
		},
		{
			Type: "function",
			Function: models.ToolFunction{
				Name:        "search_pt",
				Description: "Search Project Tracker items by keyword",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"companyId": map[string]any{"type": "string", "format": "uuid"},
						"query":      map[string]any{"type": "string"},
					},
					"required": []string{"companyId", "query"},
				},
			},
		},
		{
			Type: "function",
			Function: models.ToolFunction{
				Name:        "update_pt_item",
				Description: "Update the status or priority of a Project Tracker item",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"companyId":    map[string]any{"type": "string", "format": "uuid"},
						"itemKey":      map[string]any{"type": "string"},
						"status":        map[string]any{"type": "string", "enum": []string{"backlog", "todo", "in_progress", "review", "done", "blocked"}},
						"priority":      map[string]any{"type": "string", "enum": []string{"low", "medium", "high", "critical"}},
						"blockedReason": map[string]any{"type": "string"},
					},
					"required": []string{"companyId", "itemKey"},
				},
			},
		},
	}
}

// Execute runs a single tool call and returns the result as a JSON string.
func (e *Executor) Execute(ctx context.Context, toolCall models.LLMToolCall, companyID, userID string) string {
	var args map[string]any
	if err := json.Unmarshal([]byte(toolCall.Function.Arguments), &args); err != nil {
		return jsonError("failed to parse arguments: " + err.Error())
	}

	// Inject context
	if _, ok := args["companyId"]; !ok && companyID != "" {
		args["companyId"] = companyID
	}
	if _, ok := args["createdBy"]; !ok && userID != "" && toolCall.Function.Name == "create_ticket" {
		args["createdBy"] = userID
	}
	if _, ok := args["authorId"]; !ok && userID != "" && toolCall.Function.Name == "add_comment" {
		args["authorId"] = userID
	}

	switch toolCall.Function.Name {
	case "search_knowledge":
		return e.execSearchKnowledge(ctx, args)
	case "get_entity":
		return e.execGetEntity(ctx, args)
	case "create_ticket":
		return e.execCreateTicket(ctx, args)
	case "get_ticket":
		return e.execGetTicket(ctx, args)
	case "list_tickets":
		return e.execListTickets(ctx, args)
	case "add_comment":
		return e.execAddComment(ctx, args)
	case "create_pt_item":
		return e.execCreatePtItem(ctx, args)
	case "read_pt_item":
		return e.execReadPtItem(ctx, args)
	case "search_pt":
		return e.execSearchPt(ctx, args)
	case "update_pt_item":
		return e.execUpdatePtItem(ctx, args)
	default:
		return jsonError(fmt.Sprintf("unknown tool: %s", toolCall.Function.Name))
	}
}

// ── Knowledge Tools ─────────────────────────────────────────────────────────

func (e *Executor) execSearchKnowledge(ctx context.Context, args map[string]any) string {
	query, _ := args["query"].(string)
	companyID, _ := args["companyId"].(string)
	if query == "" || companyID == "" {
		return jsonError("query and companyId are required")
	}

	result, err := e.knowledgeRepo.SearchKnowledgeGraph(ctx, query, companyID, 5)
	if err != nil {
		return jsonError("search failed: " + err.Error())
	}
	data, _ := json.Marshal(result)
	return string(data)
}

func (e *Executor) execGetEntity(ctx context.Context, args map[string]any) string {
	entityType, _ := args["type"].(string)
	id, _ := args["id"].(string)
	if entityType == "" || id == "" {
		return jsonError("type and id are required")
	}

	result, err := e.knowledgeRepo.GetEntityByID(ctx, entityType, id)
	if err != nil {
		return jsonError("get entity failed: " + err.Error())
	}
	if result == nil {
		return jsonError("entity not found")
	}
	data, _ := json.Marshal(result)
	return string(data)
}

// ── Ticket Tools ────────────────────────────────────────────────────────────

func (e *Executor) execCreateTicket(ctx context.Context, args map[string]any) string {
	companyID, _ := args["companyId"].(string)
	title, _ := args["title"].(string)
	description, _ := args["description"].(string)
	ticketType, _ := args["type"].(string)
	createdBy, _ := args["createdBy"].(string)
	priority := 3
	if p, ok := args["priority"].(float64); ok {
		priority = int(p)
	}

	if companyID == "" || title == "" || createdBy == "" {
		return jsonError("companyId, title, and createdBy are required")
	}

	result, err := e.ticketsClient.CreateTicket(ctx, companyID, title, description, ticketType, createdBy, priority)
	if err != nil {
		return jsonError("create ticket failed: " + err.Error())
	}
	data, _ := json.Marshal(result)
	return string(data)
}

func (e *Executor) execGetTicket(ctx context.Context, args map[string]any) string {
	ticketID, _ := args["ticketId"].(string)
	if ticketID == "" {
		return jsonError("ticketId is required")
	}

	result, err := e.ticketsClient.GetTicket(ctx, ticketID)
	if err != nil {
		return jsonError("get ticket failed: " + err.Error())
	}
	if result == nil {
		return jsonError("ticket not found")
	}
	data, _ := json.Marshal(result)
	return string(data)
}

func (e *Executor) execListTickets(ctx context.Context, args map[string]any) string {
	companyID, _ := args["companyId"].(string)
	if companyID == "" {
		return jsonError("companyId is required")
	}

	status, _ := args["status"].(string)
	ticketType, _ := args["type"].(string)
	assignedTo, _ := args["assignedTo"].(string)
	search, _ := args["search"].(string)
	limit := 20
	if l, ok := args["limit"].(float64); ok {
		limit = int(l)
	}

	tickets, err := e.ticketsClient.ListTickets(ctx, companyID, status, ticketType, assignedTo, search, limit)
	if err != nil {
		return jsonError("list tickets failed: " + err.Error())
	}
	data, _ := json.Marshal(map[string]any{"tickets": tickets, "total": len(tickets)})
	return string(data)
}

func (e *Executor) execAddComment(ctx context.Context, args map[string]any) string {
	ticketID, _ := args["ticketId"].(string)
	authorID, _ := args["authorId"].(string)
	body, _ := args["body"].(string)
	if ticketID == "" || authorID == "" || body == "" {
		return jsonError("ticketId, authorId, and body are required")
	}

	result, err := e.ticketsClient.AddComment(ctx, ticketID, authorID, body)
	if err != nil {
		return jsonError("add comment failed: " + err.Error())
	}
	data, _ := json.Marshal(result)
	return string(data)
}

// ── Project Tracker Tools ───────────────────────────────────────────────────

func (e *Executor) execCreatePtItem(ctx context.Context, args map[string]any) string {
	companyID, _ := args["companyId"].(string)
	title, _ := args["title"].(string)
	description, _ := args["description"].(string)
	itemType, _ := args["type"].(string)
	priority, _ := args["priority"].(string)

	if companyID == "" || title == "" {
		return jsonError("companyId and title are required")
	}

	ptConfig, err := e.ptConfigFn(ctx, companyID)
	if err != nil || ptConfig == nil {
		return jsonError("PT not configured — set up your PT token in settings")
	}

	if itemType == "" {
		itemType = "story"
	}

	item := &models.PtItem{
		Type:        itemType,
		Title:       title,
		Description: description,
		Priority:    priority,
	}

	// Auto-enrich with knowledge graph context
	if e.knowledgeRepo != nil && description != "" {
		searchResult, err := e.knowledgeRepo.SearchKnowledgeGraph(ctx, title+" "+description, companyID, 3)
		if err == nil && searchResult.TotalMatches > 0 {
			contextLines := ""
			for entityType, results := range searchResult.Results {
				for _, r := range results {
					contextLines += fmt.Sprintf("- %s: %s (id: %s)\n", entityType, r.Name, r.ID)
				}
			}
			if contextLines != "" {
				item.Description += "\n\n---\nAuto-attached context:\n" + contextLines
			}
		}
	}

	var labels []string
	if l, ok := args["labels"].([]any); ok {
		for _, v := range l {
			if s, ok := v.(string); ok {
				labels = append(labels, s)
			}
		}
	}
	item.Labels = labels

	result, err := e.ptClient.CreateItem(ptConfig.Token, ptConfig.ProjectKey, item)
	if err != nil {
		return jsonError("create PT item failed: " + err.Error())
	}
	data, _ := json.Marshal(result)
	return string(data)
}

func (e *Executor) execReadPtItem(ctx context.Context, args map[string]any) string {
	companyID, _ := args["companyId"].(string)
	itemKey, _ := args["itemKey"].(string)
	if companyID == "" || itemKey == "" {
		return jsonError("companyId and itemKey are required")
	}

	ptConfig, err := e.ptConfigFn(ctx, companyID)
	if err != nil || ptConfig == nil {
		return jsonError("PT not configured")
	}

	result, err := e.ptClient.GetItem(ptConfig.Token, itemKey)
	if err != nil {
		return jsonError("get PT item failed: " + err.Error())
	}
	data, _ := json.Marshal(result)
	return string(data)
}

func (e *Executor) execSearchPt(ctx context.Context, args map[string]any) string {
	companyID, _ := args["companyId"].(string)
	query, _ := args["query"].(string)
	if companyID == "" || query == "" {
		return jsonError("companyId and query are required")
	}

	ptConfig, err := e.ptConfigFn(ctx, companyID)
	if err != nil || ptConfig == nil {
		return jsonError("PT not configured")
	}

	items, err := e.ptClient.SearchItems(ptConfig.Token, query)
	if err != nil {
		return jsonError("search PT failed: " + err.Error())
	}
	data, _ := json.Marshal(map[string]any{"results": items, "total": len(items)})
	return string(data)
}

func (e *Executor) execUpdatePtItem(ctx context.Context, args map[string]any) string {
	companyID, _ := args["companyId"].(string)
	itemKey, _ := args["itemKey"].(string)
	if companyID == "" || itemKey == "" {
		return jsonError("companyId and itemKey are required")
	}

	ptConfig, err := e.ptConfigFn(ctx, companyID)
	if err != nil || ptConfig == nil {
		return jsonError("PT not configured")
	}

	changes := make(map[string]any)
	if s, ok := args["status"].(string); ok {
		changes["status"] = s
	}
	if p, ok := args["priority"].(string); ok {
		changes["priority"] = p
	}
	if r, ok := args["blockedReason"].(string); ok {
		changes["blockedReason"] = r
	}

	result, err := e.ptClient.UpdateItem(ptConfig.Token, itemKey, changes)
	if err != nil {
		return jsonError("update PT item failed: " + err.Error())
	}
	data, _ := json.Marshal(result)
	return string(data)
}

func jsonError(msg string) string {
	data, _ := json.Marshal(map[string]string{"error": msg})
	return string(data)
}
