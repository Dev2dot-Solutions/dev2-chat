package models

import (
	"encoding/json"
	"time"
)

const (
	SocketActionChatSend       = "chat.send"
	SocketActionApprovalDecide = "approval.decide"
)

// SocketIdentity is the immutable identity and chat scope stamped into a
// short-lived socket ticket after JWT authorization.
type SocketIdentity struct {
	UserID        string `bson:"userId" json:"userId"`
	CompanyID     string `bson:"companyId" json:"companyId"`
	IsAdmin       bool   `bson:"isAdmin" json:"isAdmin"`
	AccessProfile string `bson:"accessProfile" json:"accessProfile"`
	ProjectID     string `bson:"projectId" json:"projectId"`
}

type SocketTicket struct {
	TokenHash      string `bson:"_id" json:"-"`
	SocketIdentity `bson:",inline"`
	ExpiresAt      time.Time  `bson:"expiresAt" json:"expiresAt"`
	ConsumedAt     *time.Time `bson:"consumedAt,omitempty" json:"-"`
	CreatedAt      time.Time  `bson:"createdAt" json:"-"`
}

// SocketServerEvent is the durable server-to-browser WebSocket envelope.
type SocketServerEvent struct {
	Seq       int64          `bson:"seq" json:"seq"`
	Type      string         `bson:"type" json:"type"`
	RequestID string         `bson:"requestId,omitempty" json:"requestId,omitempty"`
	SessionID string         `bson:"sessionId,omitempty" json:"sessionId,omitempty"`
	Timestamp time.Time      `bson:"timestamp" json:"timestamp"`
	Data      map[string]any `bson:"data" json:"data"`
}

// SocketClientMessage is the browser-to-server WebSocket envelope. Data is
// decoded only after Type has selected its exact schema.
type SocketClientMessage struct {
	Type           string          `json:"type"`
	RequestID      string          `json:"requestId,omitempty"`
	SessionID      string          `json:"sessionId,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	Data           json.RawMessage `json:"data"`
}

type SocketActionReceipt struct {
	ID             string         `bson:"_id" json:"-"`
	CompanyID      string         `bson:"companyId" json:"companyId"`
	UserID         string         `bson:"userId" json:"userId"`
	ActionType     string         `bson:"actionType" json:"actionType"`
	RequestID      string         `bson:"requestId" json:"requestId"`
	SessionID      string         `bson:"sessionId,omitempty" json:"sessionId,omitempty"`
	State          string         `bson:"state" json:"state"`
	FinalEventType string         `bson:"finalEventType,omitempty" json:"finalEventType,omitempty"`
	FinalData      map[string]any `bson:"finalData,omitempty" json:"finalData,omitempty"`
	CreatedAt      time.Time      `bson:"createdAt" json:"createdAt"`
	UpdatedAt      time.Time      `bson:"updatedAt" json:"updatedAt"`
	ExpiresAt      time.Time      `bson:"expiresAt" json:"expiresAt"`
}
