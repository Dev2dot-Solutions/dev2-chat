package models

// LlmConfig holds the LLM provider configuration for a company.
type LlmConfig struct {
	Provider string `bson:"provider" json:"provider"`
	Model    string `bson:"model" json:"model"`
	APIKey   string `bson:"apiKey,omitempty" json:"-"`
	BaseURL  string `bson:"baseUrl,omitempty" json:"baseUrl,omitempty"`
}

// PtConfig holds the Project Tracker configuration for a company.
type PtConfig struct {
	Token      string `bson:"token" json:"-"`
	ProjectKey string `bson:"projectKey" json:"projectKey"`
}

// CompanySettings holds all per-company settings.
type CompanySettings struct {
	CompanyID string    `bson:"_id" json:"companyId"`
	LLM       LlmConfig `bson:"llm" json:"llm"`
	PT        PtConfig  `bson:"pt" json:"pt"`
}

// PtItem represents a Project Tracker item (story or task).
type PtItem struct {
	Key         string   `json:"key"`
	Title       string   `json:"title"`
	Description string   `json:"description,omitempty"`
	Type        string   `json:"type"`
	Status      string   `json:"status"`
	Priority    string   `json:"priority,omitempty"`
	Labels      []string `json:"labels,omitempty"`
	Value       int      `json:"value,omitempty"`
	Effort      int      `json:"effort,omitempty"`
	Assignee    string   `json:"assignee,omitempty"`
}

// LLMRequest is sent to dev2-llm-service (or direct LLM API).
type LLMRequest struct {
	Model       string           `json:"model"`
	Messages    []LLMMessage     `json:"messages"`
	Tools       []ToolDefinition `json:"tools,omitempty"`
	MaxTokens   int              `json:"max_tokens,omitempty"`
	Temperature float64          `json:"temperature,omitempty"`
	// SessionID and UserID identify the authenticated chat actor and
	// conversation on llm.request. They are not forwarded to direct LLM APIs.
	SessionID string `json:"sessionId,omitempty"`
	UserID    string `json:"userId,omitempty"`
	// AccessProfile is the session's access profile ("client"|"developer");
	// dev2-llm-service filters its own tool advertisement/enforcement with it.
	AccessProfile string `json:"accessProfile,omitempty"`
	// Workspace scoping for project-bound sessions; dev2-llm-service uses
	// these to activate its code-agent tool flow and persona resolution.
	WorkspaceCompanyID    string `json:"workspaceCompanyId,omitempty"`
	WorkspaceProjectID    string `json:"workspaceProjectId,omitempty"`
	WorkspacePTProjectKey string `json:"workspacePtProjectKey,omitempty"`
}

// LLMMessage is a message in the LLM conversation.
type LLMMessage struct {
	Role       string        `json:"role"`
	Content    string        `json:"content"`
	ToolCalls  []LLMToolCall `json:"tool_calls,omitempty"`
	ToolCallID string        `json:"tool_call_id,omitempty"`
	Name       string        `json:"name,omitempty"`
}

// LLMToolCall is a tool call from the LLM.
type LLMToolCall struct {
	ID       string      `json:"id"`
	Type     string      `json:"type"`
	Function LLMFunction `json:"function"`
}

// LLMFunction is the function details in a tool call.
type LLMFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolDefinition describes a tool the LLM can call.
type ToolDefinition struct {
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
}

// ToolFunction describes a function tool.
type ToolFunction struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"`
}

// LLMResponse is the response from the LLM.
type LLMResponse struct {
	Content   string        `json:"content"`
	ToolCalls []LLMToolCall `json:"tool_calls,omitempty"`
	// ToolResults are the tool executions dev2-llm-service performed during
	// its internal tool loop (NATS llm.request flow). Write/execute tools
	// surface approval requests here as Output payloads with status
	// "pending_approval" (DEV2-108).
	ToolResults []LLMToolResult `json:"toolResults,omitempty"`
	Usage       *Usage          `json:"usage,omitempty"`
}

// LLMToolResult is one tool execution reported by dev2-llm-service. Output
// is a stringified JSON payload; for approval-gated tools it decodes to
// {"status":"pending_approval","approvalId":...,"preview":...}.
type LLMToolResult struct {
	ToolCallID string `json:"tool_call_id"`
	ToolName   string `json:"toolName"`
	Success    bool   `json:"success"`
	Output     string `json:"output"`
}

// Usage tracks token usage.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// KnowledgeSearchRequest is sent to dev2-knowledge for context search.
type KnowledgeSearchRequest struct {
	Query     string   `json:"query"`
	CompanyID string   `json:"companyId"`
	Types     []string `json:"types,omitempty"`
	Limit     int      `json:"limit,omitempty"`
}

// KnowledgeSearchResult is a single search result from knowledge graph.
type KnowledgeSearchResult struct {
	Type    string  `json:"type"`
	ID      string  `json:"id"`
	Name    string  `json:"name"`
	Snippet string  `json:"snippet"`
	Score   float64 `json:"score,omitempty"`
}

// KnowledgeSearchResponse wraps search results.
type KnowledgeSearchResponse struct {
	Query        string                             `json:"query"`
	Results      map[string][]KnowledgeSearchResult `json:"results"`
	TotalMatches int                                `json:"totalMatches"`
}

// KnowledgeEntityRequest requests a single entity by type + ID.
type KnowledgeEntityRequest struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	CompanyID string `json:"companyId,omitempty"`
}
