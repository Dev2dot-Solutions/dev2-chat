package pt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
)

// Client handles HTTP calls to the Project Tracker API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new Project Tracker client.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

func (c *Client) doRequest(method, path, token string, body any) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	url := c.baseURL + path
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == 401 {
		return nil, fmt.Errorf("invalid PT token")
	}
	if resp.StatusCode == 403 {
		return nil, fmt.Errorf("permission denied")
	}
	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("item not found")
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("PT API error %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// CreateItem creates a new story or task in the Project Tracker.
func (c *Client) CreateItem(token, projectKey string, item *models.PtItem) (*models.PtItem, error) {
	body := map[string]any{
		"type":        item.Type,
		"title":       item.Title,
		"description": item.Description,
		"project":     projectKey,
	}
	if item.Priority != "" {
		body["priority"] = item.Priority
	}
	if len(item.Labels) > 0 {
		body["labels"] = item.Labels
	}
	if item.Value > 0 {
		body["value"] = item.Value
	}
	if item.Effort > 0 {
		body["effort"] = item.Effort
	}

	respBody, err := c.doRequest("POST", "/items", token, body)
	if err != nil {
		return nil, err
	}

	var result models.PtItem
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return &result, nil
}

// GetItem retrieves a single Project Tracker item by key.
func (c *Client) GetItem(token, itemKey string) (*models.PtItem, error) {
	respBody, err := c.doRequest("GET", "/items/"+itemKey, token, nil)
	if err != nil {
		return nil, err
	}

	var result models.PtItem
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return &result, nil
}

// SearchItems searches Project Tracker items by query.
func (c *Client) SearchItems(token, query string) ([]models.PtItem, error) {
	respBody, err := c.doRequest("GET", fmt.Sprintf("/items?search=%s", query), token, nil)
	if err != nil {
		return nil, err
	}

	var response struct {
		Items []models.PtItem `json:"items"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return response.Items, nil
}

// UpdateItem updates a Project Tracker item.
func (c *Client) UpdateItem(token, itemKey string, changes map[string]any) (*models.PtItem, error) {
	respBody, err := c.doRequest("PATCH", "/items/"+itemKey, token, changes)
	if err != nil {
		return nil, err
	}

	var result models.PtItem
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return &result, nil
}
