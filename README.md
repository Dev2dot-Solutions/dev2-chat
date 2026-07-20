# dev2-chat

Knowledge-augmented conversational Q&A service for the dev2.solutions platform.

A Go microservice that owns chat sessions, knowledge-augmented Q&A, and LLM
tool orchestration. Calls `dev2-llm-service` via NATS for LLM completions and
`dev2-knowledge` via NATS for knowledge graph search. Falls back to direct
HTTP calls when NATS is unavailable.

## Architecture

```
User ──HTTP/WebSocket──▶ dev2-chat ──NATS──▶ dev2-llm-service
                        │
                        ├─NATS──▶ dev2-knowledge (knowledge search)
                        │
                        ├─HTTP──▶ dev2-tickets (ticket operations)
                        │
                        └─HTTP──▶ Project Tracker API (PT operations)
```

## API

| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Health check |
| POST | /agent/ask | Send question, get answer (context-aware Q&A) |
| POST | /chat | Legacy endpoint (creates sessions) |
| POST | /chat/socket-ticket | Issue a 30-second, one-use WebSocket ticket |
| GET | /chat/ws?ticket=... | Primary active-chat WebSocket transport |
| GET | /chat/sessions | List chat sessions |
| GET | /chat/sessions/{id} | Get session with messages |
| GET | /settings/llm?company_id= | Get LLM config |
| GET | /settings/pt?company_id= | Get PT config |

`POST /agent/ask?stream=true` (SSE) remains available as a **deprecated
compatibility fallback**. Durable session/history APIs remain REST endpoints.

## WebSocket chat

1. With the normal JWT, call `POST /chat/socket-ticket`:

   ```json
   {"accessProfile":"client","projectId":"project-uuid"}
   ```

   The authenticated user/company, admin flag, profile and visible project are
   bound into the ticket. The response is
   `{"ticket":"opaque-value","expiresAt":"RFC3339"}`. Tickets expire after
   30 seconds and can be consumed once. Raw values are not stored or logged.
2. Connect to `wss://chat.dev2.solutions/chat/ws?ticket=<opaque>`. Browser
   `Origin` must exactly match `CHAT_SOCKET_ALLOWED_ORIGINS`.
3. Send client envelopes and retain the highest `seq` per session. On
   reconnect, obtain a new ticket and send `session.resume` with `lastSeq`.

Client envelope:

```json
{
  "type": "chat.send",
  "requestId": "request-uuid",
  "sessionId": "optional-session-uuid",
  "idempotencyKey": "stable-action-uuid",
  "data": {"message":"Hello","projectId":"project-uuid","accessProfile":"client"}
}
```

Client types are `chat.send`, `approval.decide`, `generation.cancel`,
`session.resume`, and `ping`. `chat.send` and `approval.decide` require both
`requestId` and `idempotencyKey`. The profile/project must equal the ticket
scope. `generation.cancel` targets an active `requestId`; cancellation flows
through the existing request context and NATS cancellation protocol.

Every server event has the following shape:

```json
{
  "seq": 42,
  "type": "content.delta",
  "requestId": "request-uuid",
  "sessionId": "session-uuid",
  "timestamp": "2026-07-20T12:00:00Z",
  "data": {"content":"answer chunk"}
}
```

Server types are `connection.ready`, `chat.accepted`, `trace`,
`content.delta`, `chat.meta`, `approval.requested`, `approval.resolved`,
`generation.completed`, `generation.cancelled`, `replay.completed`, `error`,
and `pong`. Sequence numbers are monotonic within a user/company/session
scope. Replay history and action receipts live for 24 hours; each session is
bounded to the newest 1,000 socket events. Duplicate action keys do not repeat
generation or approval execution and return their prior accepted/final state
when available.

The server uses one writer goroutine per connection, a bounded event queue,
64 KiB default read limit, ping control frames every 25 seconds, and a 60
second idle deadline renewed by pong/read activity. A client that cannot drain
the queue is closed with WebSocket policy-violation code `1008`.

MongoDB storage used by this transport:

- `chat_socket_tickets`: SHA-256 token digest (`_id`), bound identity/scope,
  `expiresAt`, and atomic `consumedAt`; TTL index on `expiresAt`.
- `chat_socket_events`: user/company/session event envelopes; unique compound
  index on `(companyId,userId,sessionId,seq)` and TTL index on `expiresAt`.
- `chat_socket_sequences`: cross-instance sequence counters with a staggered
  25-hour TTL (after the 24-hour event retention window).
- `chat_socket_receipts`: hashed user/company/idempotency key receipts and
  prior final state; TTL index on `expiresAt`.

### Reverse proxy

The proxy in front of `/chat/ws` must use HTTP/1.1 and forward Upgrade headers:

```nginx
proxy_http_version 1.1;
proxy_set_header Upgrade $http_upgrade;
proxy_set_header Connection "upgrade";
proxy_read_timeout 75s;
```

Avoid access-log query strings for `/chat/ws`, because the one-use ticket is a
credential. The application strips it before its own request logger runs.

## POST /agent/ask

### Request
```json
{
  "company_id": "uuid",
  "user_id": "uuid",
  "question": "How do I add CSV export?",
  "conversation_id": "uuid"
}
```

### Response
```json
{
  "answer": "To add CSV export...",
  "conversation_id": "uuid",
  "tool_calls": [
    {"name": "search_knowledge", "result": "..."}
  ],
  "sources": [
    {"type": "knowledge_graph", "label": "Context from knowledge graph"}
  ]
}
```

## Available Tools

| Tool | Description | Backend |
|------|-------------|---------|
| search_knowledge | Full-text search across knowledge graph | dev2-knowledge / direct MongoDB |
| get_entity | Get single knowledge entity by type + ID | dev2-knowledge / direct MongoDB |
| create_ticket | Create a helpdesk ticket | dev2-tickets |
| get_ticket | Get ticket details | dev2-tickets |
| list_tickets | List/filter tickets | dev2-tickets |
| add_comment | Add comment to a ticket | dev2-tickets |
| create_pt_item | Create PT story/task | Project Tracker API |
| read_pt_item | Get PT item details | Project Tracker API |
| search_pt | Search PT items | Project Tracker API |
| update_pt_item | Update PT item status | Project Tracker API |

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| PORT | 8080 | HTTP port |
| MONGO_URI | mongodb://root:dev2@mongodb:27017/dev2knowledge | MongoDB |
| NATS_URL | nats://nats:4223 | NATS server |
| LLM_API_KEY | — | OpenAI-compatible API key |
| LLM_BASE_URL | https://api.openai.com/v1 | LLM API base URL |
| LLM_MODEL | gpt-4o | Default model |
| TICKETS_SVC_URL | http://dev2-tickets:8080 | dev2-tickets HTTP URL |
| PT_SVC_URL | https://app.project-tracker.ai/api | Project Tracker API |
| CHAT_SOCKET_ALLOWED_ORIGINS | https://dev2.solutions,http://localhost:3000 | Exact browser origins (no wildcards) |
| CHAT_SOCKET_SEND_QUEUE | 128 | Per-connection outbound event capacity |
| CHAT_SOCKET_READ_LIMIT_BYTES | 65536 | Maximum client message size |
| CHAT_SOCKET_PING_INTERVAL | 25s | WebSocket control-ping interval |
| CHAT_SOCKET_IDLE_TIMEOUT | 60s | Read/pong idle deadline |

## Dependencies

- MongoDB (chat sessions, messages, knowledge graph)
- NATS (optional — for service-to-service calls)
- dev2-llm-service (optional — LLM completions via NATS)
- dev2-tickets (optional — ticket operations via HTTP)
- Project Tracker API (optional — PT operations)

All external dependencies are optional — the service functions without NATS,
relying on direct HTTP calls and in-process MongoDB queries.
