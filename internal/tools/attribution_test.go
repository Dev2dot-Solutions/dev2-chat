package tools

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
)

type fakePTClient struct {
	createdProject string
	createdItem    *models.PtItem
	updatedKey     string
	updatedChanges map[string]any
}

func (f *fakePTClient) CreateItem(_ string, projectKey string, item *models.PtItem) (*models.PtItem, error) {
	copy := *item
	copy.Labels = append([]string(nil), item.Labels...)
	copy.Key = "DEV2-123"
	f.createdProject = projectKey
	f.createdItem = &copy
	return &copy, nil
}

func (f *fakePTClient) GetItem(string, string) (*models.PtItem, error)      { return nil, nil }
func (f *fakePTClient) SearchItems(string, string) ([]models.PtItem, error) { return nil, nil }
func (f *fakePTClient) UpdateItem(_ string, itemKey string, changes map[string]any) (*models.PtItem, error) {
	f.updatedKey = itemKey
	f.updatedChanges = changes
	return f.createdItem, nil
}

func TestCreatePTItemAddsOriginAndChangeLog(t *testing.T) {
	ptClient := &fakePTClient{}
	executor := NewExecutor(nil, nil, ptClient, func(context.Context, string) (*models.PtConfig, error) {
		return &models.PtConfig{Token: "token", ProjectKey: "DEFAULT"}, nil
	}, nil)

	result := executor.Execute(context.Background(), models.LLMToolCall{Function: models.LLMFunction{
		Name:      "create_pt_item",
		Arguments: `{"companyId":"spoofed","title":"Audit me","description":"Details","labels":["backend"]}`,
	}}, ExecContext{
		CompanyID: "company-1", UserID: "user-1", SessionID: "session-1",
		ProjectID: "project-1", PTProjectKey: "DEV2", AccessProfile: models.AccessProfileDeveloper,
	})

	if strings.Contains(result, `"error"`) {
		t.Fatalf("create PT item failed: %s", result)
	}
	if ptClient.createdProject != "DEV2" {
		t.Fatalf("created in %q, want DEV2", ptClient.createdProject)
	}
	if !reflect.DeepEqual(ptClient.createdItem.Labels, []string{"backend", "source:developer"}) {
		t.Fatalf("labels = %#v", ptClient.createdItem.Labels)
	}
	wantChangeLog := "Created via Dev2 developer chat by user user-1, conversation session-1, Dev2Project project-1."
	if ptClient.updatedKey != "DEV2-123" || ptClient.updatedChanges["changeLog"] != wantChangeLog {
		t.Fatalf("attribution patch = key %q changes %#v", ptClient.updatedKey, ptClient.updatedChanges)
	}
}

type fakeTicketsClient struct {
	companyID   string
	createdBy   string
	attribution models.TicketAttribution
}

func (f *fakeTicketsClient) CreateTicket(_ context.Context, companyID, _, _, _, createdBy string, _ int, attribution models.TicketAttribution) (map[string]any, error) {
	f.companyID = companyID
	f.createdBy = createdBy
	f.attribution = attribution
	return map[string]any{"id": "ticket-1", "password": "result-secret"}, nil
}
func (f *fakeTicketsClient) GetTicket(context.Context, string) (map[string]any, error) {
	return nil, nil
}
func (f *fakeTicketsClient) ListTickets(context.Context, string, string, string, string, string, int) ([]map[string]any, error) {
	return nil, nil
}
func (f *fakeTicketsClient) AddComment(context.Context, string, string, string) (map[string]any, error) {
	return nil, nil
}

type channelAuditPublisher struct{ events chan models.ToolAuditEvent }

func (p channelAuditPublisher) PublishToolInvocation(event models.ToolAuditEvent) { p.events <- event }

func TestCreateTicketUsesTrustedAttributionAndPublishesRedactedAudit(t *testing.T) {
	ticketsClient := &fakeTicketsClient{}
	auditEvents := make(chan models.ToolAuditEvent, 1)
	executor := NewExecutor(nil, ticketsClient, nil, nil, channelAuditPublisher{events: auditEvents})
	exec := ExecContext{
		CompanyID: "company-1", UserID: "user-1", SessionID: "session-1",
		ProjectID: "project-1", AccessProfile: models.AccessProfileClient,
	}
	executor.Execute(context.Background(), models.LLMToolCall{Function: models.LLMFunction{
		Name:      "create_ticket",
		Arguments: `{"companyId":"spoofed-company","createdBy":"spoofed-user","title":"Help","apiToken":"argument-secret"}`,
	}}, exec)

	if ticketsClient.companyID != exec.CompanyID || ticketsClient.createdBy != exec.UserID {
		t.Fatalf("ticket identity = company %q user %q", ticketsClient.companyID, ticketsClient.createdBy)
	}
	wantAttribution := models.TicketAttribution{
		Origin: "client", SourceUserID: "user-1", SourceSessionID: "session-1", SourceProjectID: "project-1",
	}
	if !reflect.DeepEqual(ticketsClient.attribution, wantAttribution) {
		t.Fatalf("ticket attribution = %#v", ticketsClient.attribution)
	}

	select {
	case event := <-auditEvents:
		if event.ToolName != "create_ticket" || !event.Success || event.EventID == "" {
			t.Fatalf("unexpected audit event: %#v", event)
		}
		if event.CompanyID != exec.CompanyID || event.UserID != exec.UserID || event.SessionID != exec.SessionID || event.ProjectID != exec.ProjectID {
			t.Fatalf("audit identity mismatch: %#v", event)
		}
		if strings.Contains(event.Arguments, "argument-secret") || strings.Contains(event.Result, "result-secret") {
			t.Fatalf("audit event leaked a secret: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for audit event")
	}
}
