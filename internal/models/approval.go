package models

import "time"

// Approval lifecycle statuses (DEV2-108). "pending" is the only actionable
// state; the terminal statuses mirror the dev2-llm-service tool.approve
// reply so a decided card cannot be actioned twice.
const (
	ApprovalStatusPending  = "pending"
	ApprovalStatusExecuted = "executed"
	ApprovalStatusRejected = "rejected"
	ApprovalStatusExpired  = "expired"
)

// Approval decisions accepted by POST /chat/approvals/{approvalId}.
const (
	ApprovalDecisionApprove = "approve"
	ApprovalDecisionReject  = "reject"
)

// ApprovalRecord maps a dev2-llm-service approval ID to the chat session
// (and owning user) that surfaced it, persisted in the chat_approvals
// collection when a pending-approval tool result is returned. It is the
// ownership anchor for the decision endpoint: sessionId/userId forwarded on
// tool.approve come from this record and its session, never from the
// request body.
type ApprovalRecord struct {
	ID        string     `bson:"_id" json:"approvalId"` // llm-service approvalId
	SessionID string     `bson:"sessionId" json:"sessionId"`
	CompanyID string     `bson:"companyId" json:"companyId"`
	UserID    string     `bson:"userId" json:"userId"`
	Tool      string     `bson:"tool" json:"tool"`
	Summary   string     `bson:"summary,omitempty" json:"summary,omitempty"`
	Preview   string     `bson:"preview,omitempty" json:"preview,omitempty"`
	ExpiresAt time.Time  `bson:"expiresAt,omitempty" json:"expiresAt,omitempty"`
	Status    string     `bson:"status" json:"status"`                         // pending|executed|rejected|expired
	Decision  string     `bson:"decision,omitempty" json:"decision,omitempty"` // approve|reject
	CreatedAt time.Time  `bson:"createdAt" json:"createdAt"`
	DecidedAt *time.Time `bson:"decidedAt,omitempty" json:"decidedAt,omitempty"`
}

// ToolApprovalRequest is the NATS tool.approve request payload sent to
// dev2-llm-service (10s request-reply).
type ToolApprovalRequest struct {
	ApprovalID string `json:"approvalId"`
	Decision   string `json:"decision"` // "approve"|"reject"
	SessionID  string `json:"sessionId"`
	UserID     string `json:"userId"`
}

// ToolApprovalResponse is the tool.approve reply. Status is one of
// "executed", "rejected", "expired", "unauthorized", "not_found".
type ToolApprovalResponse struct {
	Status string `json:"status"`
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}
