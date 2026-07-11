package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
)

// Client handles direct HTTP calls to OpenAI-compatible LLM APIs.
// Used as fallback when NATS-based dev2-llm-service is unavailable.
type Client struct {
	apiKey  string
	baseURL string
	http    *http.Client
}

// NewClient creates a new LLM client.
func NewClient(apiKey, baseURL string) *Client {
	return &Client{
		apiKey:  apiKey,
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

// ChatCompletion sends a chat completion request to an OpenAI-compatible API.
func (c *Client) ChatCompletion(req *models.LLMRequest) (*models.LLMResponse, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("LLM API key not configured")
	}

	// Build OpenAI-compatible request body
	body := map[string]any{
		"model":    req.Model,
		"messages": req.Messages,
	}
	if len(req.Tools) > 0 {
		body["tools"] = req.Tools
	}
	if req.MaxTokens > 0 {
		body["max_tokens"] = req.MaxTokens
	}
	if req.Temperature > 0 {
		body["temperature"] = req.Temperature
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := c.baseURL + "/chat/completions"
	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("LLM API error %d: %s", resp.StatusCode, string(respBody))
	}

	// Parse OpenAI response
	var openAIResp struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Type     string `json:"type"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(respBody, &openAIResp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	if len(openAIResp.Choices) == 0 {
		return nil, fmt.Errorf("LLM returned no choices")
	}

	msg := openAIResp.Choices[0].Message
	result := &models.LLMResponse{
		Content: msg.Content,
	}

	for _, tc := range msg.ToolCalls {
		result.ToolCalls = append(result.ToolCalls, models.LLMToolCall{
			ID:   tc.ID,
			Type: tc.Type,
			Function: models.LLMFunction{
				Name:      tc.Function.Name,
				Arguments: tc.Function.Arguments,
			},
		})
	}

	if openAIResp.Usage != nil {
		result.Usage = &models.Usage{
			PromptTokens:     openAIResp.Usage.PromptTokens,
			CompletionTokens: openAIResp.Usage.CompletionTokens,
			TotalTokens:      openAIResp.Usage.TotalTokens,
		}
	}

	return result, nil
}

// HealthCheck verifies the LLM provider is reachable.
func (c *Client) HealthCheck() bool {
	if c.apiKey == "" {
		return false
	}
	// Just check we can reach the base URL
	req, err := http.NewRequest("GET", c.baseURL+"/models", nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	resp, err := c.http.Do(req)
	if err != nil {
		log.Printf("[llm] Health check failed: %v", err)
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}
