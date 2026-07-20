package handlers

import (
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
