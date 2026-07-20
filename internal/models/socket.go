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
	UserID        string    `bson:"userId" json:"userId"`
	CompanyID     string    `bson:"companyId" json:"companyId"`
	IsAdmin       bool      `bson:"isAdmin" json:"isAdmin"`
	AccessProfile string    `bson:"accessProfile" json:"accessProfile"`
	ProjectID     string    `bson:"projectId" json:"projectId"`
	AuthSource    string    `bson:"authSource" json:"-"`
	AuthIssuedAt  time.Time `bson:"authIssuedAt" json:"-"`
	AuthExpiresAt time.Time `bson:"authExpiresAt" json:"authExpiresAt"`
}

type SocketTicket struct {
	TokenHash       string `bson:"_id" json:"-"`
	SocketIdentity  `bson:",inline"`
	ExpiresAt       time.Time  `bson:"expiresAt" json:"expiresAt"`
	SocketExpiresAt time.Time  `bson:"socketExpiresAt" json:"socketExpiresAt"`
	TicketSlot      string     `bson:"ticketSlot" json:"-"`
	ConsumedAt      *time.Time `bson:"consumedAt,omitempty" json:"-"`
	CreatedAt       time.Time  `bson:"createdAt" json:"-"`
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
	ID              string         `bson:"_id" json:"-"`
	CompanyID       string         `bson:"companyId" json:"companyId"`
	UserID          string         `bson:"userId" json:"userId"`
	ActionType      string         `bson:"actionType" json:"actionType"`
	AccessProfile   string         `bson:"accessProfile" json:"accessProfile"`
	ProjectID       string         `bson:"projectId" json:"projectId"`
	BoundSessionID  string         `bson:"boundSessionId" json:"boundSessionId"`
	PayloadHash     string         `bson:"payloadHash" json:"payloadHash"`
	IdempotencyHash string         `bson:"idempotencyHash" json:"-"`
	RequestID       string         `bson:"requestId" json:"requestId"`
	SessionID       string         `bson:"sessionId,omitempty" json:"sessionId,omitempty"`
	State           string         `bson:"state" json:"state"`
	FinalEventType  string         `bson:"finalEventType,omitempty" json:"finalEventType,omitempty"`
	FinalData       map[string]any `bson:"finalData,omitempty" json:"finalData,omitempty"`
	CreatedAt       time.Time      `bson:"createdAt" json:"createdAt"`
	UpdatedAt       time.Time      `bson:"updatedAt" json:"updatedAt"`
	ExpiresAt       time.Time      `bson:"expiresAt" json:"expiresAt"`
}

type SocketActionBinding struct {
	CompanyID      string
	UserID         string
	AccessProfile  string
	ProjectID      string
	SessionID      string
	ActionType     string
	PayloadHash    string
	IdempotencyKey string
}

type SocketReplay struct {
	Events               []SocketServerEvent
	EarliestAvailableSeq int64
	LatestSeq            int64
	GapDetected          bool
}

type SocketLease struct {
	ConnectionID string
	LeaseIDs     []string
	ExpiresAt    time.Time
}
