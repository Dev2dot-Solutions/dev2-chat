package tickets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Client handles HTTP calls to the dev2-tickets service.
type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{}}
}

func (c *Client) doRequest(method, path string, body any) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}
	url := c.baseURL + path
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("tickets API error %d: %s", resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

func (c *Client) CreateTicket(ctx context.Context, companyID, title, description, ticketType, createdBy string, priority int) (map[string]any, error) {
	body := map[string]any{
		"companyId": companyID, "title": title, "createdBy": createdBy,
	}
	if description != "" {
		body["description"] = description
	}
	if ticketType != "" {
		body["type"] = ticketType
	}
	if priority > 0 {
		body["priority"] = priority
	}
	respBody, err := c.doRequest("POST", "/tickets", body)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return result, nil
}

func (c *Client) GetTicket(ctx context.Context, ticketID string) (map[string]any, error) {
	respBody, err := c.doRequest("GET", "/tickets/"+ticketID, nil)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return result, nil
}

func (c *Client) ListTickets(ctx context.Context, companyID, status, ticketType, assignedTo, search string, limit int) ([]map[string]any, error) {
	path := fmt.Sprintf("/tickets?companyId=%s", companyID)
	if status != "" {
		path += "&status=" + status
	}
	if ticketType != "" {
		path += "&type=" + ticketType
	}
	if assignedTo != "" {
		path += "&assignedTo=" + assignedTo
	}
	if search != "" {
		path += "&search=" + search
	}
	if limit > 0 {
		path += fmt.Sprintf("&limit=%d", limit)
	}
	respBody, err := c.doRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}
	var response struct {
		Tickets []map[string]any `json:"tickets"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return response.Tickets, nil
}

func (c *Client) AddComment(ctx context.Context, ticketID, authorID, body string) (map[string]any, error) {
	reqBody := map[string]any{"authorId": authorID, "body": body}
	respBody, err := c.doRequest("POST", "/tickets/"+ticketID+"/comments", reqBody)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return result, nil
}
