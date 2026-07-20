package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Dev2dot-Solutions/dev2-chat/internal/models"
)

func TestLegacyActiveTransportDisabled(t *testing.T) {
	agent := &AgentHandler{}
	request := httptest.NewRequest(http.MethodPost, "/agent/ask", strings.NewReader(`{"question":"hello"}`))
	response := httptest.NewRecorder()
	agent.Ask(response, request)
	if response.Code != http.StatusGone {
		t.Fatalf("disabled agent transport returned %d", response.Code)
	}

	chat := &ChatHandler{}
	response = httptest.NewRecorder()
	chat.DecideApproval(response, httptest.NewRequest(http.MethodPost, "/chat/approvals/a", strings.NewReader(`{"decision":"approve"}`)))
	if response.Code != http.StatusGone {
		t.Fatalf("disabled REST approval transport returned %d", response.Code)
	}
}

func TestApprovalAuthorizationIsDeveloperAdminOnly(t *testing.T) {
	for _, test := range []struct {
		profile string
		admin   bool
		allowed bool
	}{
		{models.AccessProfileClient, false, false},
		{models.AccessProfileClient, true, false},
		{models.AccessProfileDeveloper, false, false},
		{models.AccessProfileDeveloper, true, true},
	} {
		allowed := approvalAuthorizationError(&models.ChatSession{AccessProfile: test.profile}, test.admin) == ""
		if allowed != test.allowed {
			t.Fatalf("profile=%s admin=%v allowed=%v", test.profile, test.admin, allowed)
		}
	}
}

func TestClientHistoryCanStripActionableApprovals(t *testing.T) {
	messages := []models.ChatMessage{{PendingApprovals: []models.PendingApproval{{ApprovalID: "a"}}}}
	stripActionableApprovals(messages)
	if len(messages[0].PendingApprovals) != 0 {
		t.Fatal("client history retained actionable approval")
	}
}

func TestNonAdminHistoryRejectsFormerCompanyReassignmentAndRevocation(t *testing.T) {
	session := &models.ChatSession{
		ID: "s", CompanyID: "550e8400-e29b-41d4-a716-446655440000", UserID: "current-user",
		AccessProfile: models.AccessProfileClient, ProjectID: "project-1",
	}
	if errMsg := nonAdminHistoryAuthorizationError(session, "550e8400-e29b-41d4-a716-446655440001", "current-user"); errMsg == "" {
		t.Fatal("former-company session was authorized")
	}
	if errMsg := nonAdminHistoryAuthorizationError(session, session.CompanyID, "reassigned-user"); errMsg == "" {
		t.Fatal("session was authorized after user reassignment")
	}
	if historySessionVisible(session, []models.CompanyProject{{ID: "project-1", CompanyID: session.CompanyID, Visibility: models.ProjectVisibility{Client: false}}}) {
		t.Fatal("revoked project history remained visible")
	}
	if !historySessionVisible(session, []models.CompanyProject{{ID: "project-1", CompanyID: session.CompanyID, Visibility: models.ProjectVisibility{Client: true}}}) {
		t.Fatal("visible project history was rejected")
	}
}

func TestNonAdminListForcesAuthenticatedCompanyAndFailsClosed(t *testing.T) {
	handler := &ChatHandler{}
	request := httptest.NewRequest(http.MethodGet, "/chat/sessions?companyId=550e8400-e29b-41d4-a716-446655440001&userId=other", nil)
	ctx := context.WithValue(request.Context(), ContextCompanyID, "550e8400-e29b-41d4-a716-446655440000")
	ctx = context.WithValue(ctx, ContextUserID, "authenticated-user")
	ctx = context.WithValue(ctx, ContextIsAdmin, false)
	response := httptest.NewRecorder()
	handler.ListSessions(response, request.WithContext(ctx))
	if response.Code != http.StatusForbidden {
		t.Fatalf("former company query was not rejected: %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/chat/sessions", nil).WithContext(ctx)
	response = httptest.NewRecorder()
	handler.ListSessions(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing fresh project service did not fail closed: %d", response.Code)
	}
}
