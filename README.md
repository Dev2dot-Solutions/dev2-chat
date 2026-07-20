# dev2-chat

Knowledge-augmented conversational Q&A service for the dev2.solutions platform.

A Go microservice that owns chat sessions, knowledge-augmented Q&A, and LLM
tool orchestration. Calls `dev2-llm-service` via NATS for LLM completions and
`dev2-knowledge` via NATS for knowledge graph search. Falls back to direct
HTTP calls when NATS is unavailable.

## Architecture

```
User ──WebSocket──▶ dev2-chat ──NATS──▶ dev2-llm-service
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
| POST | /chat/socket-ticket | Issue a 30-second, one-use WebSocket ticket |
| GET | /chat/ws | Active chat WebSocket transport (subprotocol ticket) |
| GET | /chat/sessions | List chat sessions |
| GET | /chat/sessions/{id} | Get session with messages |
| GET | /settings/llm?company_id= | Get LLM config |
| GET | /settings/pt?company_id= | Get PT config |

WebSocket is the only active chat transport. Durable session/history and
settings APIs remain REST endpoints.

Session/history authorization is also live: non-admin list scope always comes
from authenticated company/user claims, and both list/detail endpoints filter
against a fresh project-visibility snapshot. Admins deliberately retain
cross-company history access by supplying `companyId`, but revoked projects
remain hidden for admins too.

## WebSocket chat

1. With the normal JWT, call `POST /chat/socket-ticket`:

   ```json
   {"accessProfile":"client","projectId":"project-uuid"}
   ```

   The authenticated user/company, admin flag, profile and visible project are
   bound into the ticket. The response is
   `{"ticket":"opaque-value","expiresAt":"RFC3339"}`. Tickets expire after
   30 seconds and can be consumed once. Raw values are not stored or logged.
2. Connect to `wss://chat.dev2.solutions/chat/ws` and offer exactly the
   subprotocols `dev2-chat` and `dev2-ticket.<opaque>`:

   ```js
   new WebSocket('wss://chat.dev2.solutions/chat/ws', [
     'dev2-chat',
     `dev2-ticket.${ticket}`
   ])
   ```

   The server selects and echoes only `dev2-chat`; the credential protocol is
   never echoed. Query-string tickets are unsupported. Browser `Origin` must
   exactly match `CHAT_SOCKET_ALLOWED_ORIGINS`.
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
through the existing request context and NATS cancellation protocol. Approval
messages and actionable approval cards are developer-admin only.

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

Non-durable control responses and routine errors carry `"seq":0` and
`"ephemeral":true`; clients must never compare those events with a durable
session cursor. Persisted user and assistant messages include the originating
`requestId`, allowing REST hydration to reconcile optimistic UI rows.

Server types are `connection.ready`, `chat.accepted`, `trace`,
`content.delta`, `chat.meta`, `approval.requested`, `approval.resolved`,
`generation.completed`, `generation.cancelled`, `replay.completed`, `error`,
and `pong`. Sequence numbers are monotonic within a user/company/session
scope. Replay history and action receipts live for 24 hours; each session is
bounded to the newest 1,000 socket events. Duplicate action keys do not repeat
generation or approval execution and return their prior accepted/final state
when available.

`connection.ready.data.authExpiresAt` is the effective connection expiry. A
socket closes with code `4401` at the earliest of JWT expiry and its configured
maximum lifetime. Project/company visibility is fetched without cache before
every send, resume, and receipt replay for both profiles. Developer sockets
have a shorter default lifetime and stricter admin checks. Developer ticketing
also requires `iat` and limits sockets
to five minutes from that issue time, forcing a fresh access token. There is
currently no live Authentik group-membership API in this service, so developer
admin membership remains the signed JWT snapshot within that bounded window.
Trusted service API-key callers must assert a canonical company UUID with
`X-Company-ID` when requesting a ticket and receive the independently
configured short service lifetime.

The server uses one writer goroutine per connection, a bounded event queue,
64 KiB default read limit, ping control frames every 25 seconds, and a 60
second idle deadline renewed by pong/read activity. Retryable capacity and
backpressure close with `1013`; auth, authorization, and rate-limit closures
use `4401`, `4403`, and `4429` respectively. Ticket issuance, connections and
active generations have cross-instance MongoDB limits/leases. Every inbound
data frame—including app pings and unsupported messages—consumes both a
per-socket token and distributed user/company/IP rate capacity. Pings still
bypass expensive action slots, sequence storage, and replay storage. Malformed
frames close the connection without creating an error-response loop.

Replay is intentionally metadata-only. Answer deltas, full `chat.meta`, raw
tool output, approval previews/results, routine errors and ping/pong events are
not stored. Safe trace/control DTOs are capped at 4 KiB. Each replay page is at
most 100 events and 256 KiB and is further bounded by available connection
queue capacity. `replay.completed` includes `earliestAvailableSeq`,
`latestSeq`, `gapDetected`, `truncated`, and `nextSeq`; clients must hydrate
durable message history over REST when a gap/truncation is reported. Sequence
counters do not expire, so sequence numbers never reset after retention.

MongoDB storage used by this transport:

- `chat_socket_tickets`: SHA-256 token digest (`_id`), bound identity/scope,
  JWT/effective auth expiry, `expiresAt`, and atomic `consumedAt`; TTL index.
- `chat_socket_ticket_slots`: per-user outstanding-ticket slots with TTL.
- `chat_socket_events`: user/company/session event envelopes; unique compound
  index on `(companyId,userId,sessionId,seq)` and TTL index on `expiresAt`.
- `chat_socket_sequences`: durable cross-instance sequence counters (no TTL).
- `chat_socket_receipts`: hashed user/company/idempotency key receipts and
  profile/project/session/action/payload binding plus terminal state; TTL index.
- `chat_socket_rate_limits`: ticket and distributed frame-rate minute buckets with TTL.
- `chat_socket_leases`: global/company/user/IP connection and active-generation
  slots, renewed while active and TTL-reaped after crashes.

### Reverse proxy

The proxy in front of `/chat/ws` must use HTTP/1.1 and forward Upgrade headers:

```nginx
# http context
limit_req_zone $binary_remote_addr zone=chat_ws_handshake:10m rate=30r/m;
limit_conn_zone $binary_remote_addr zone=chat_ws_connections:10m;

# /chat/ws location
limit_req zone=chat_ws_handshake burst=10 nodelay;
limit_conn chat_ws_connections 20;
proxy_http_version 1.1;
proxy_set_header Upgrade $http_upgrade;
proxy_set_header Connection "upgrade";
proxy_set_header Sec-WebSocket-Protocol $http_sec_websocket_protocol;
proxy_read_timeout 75s;
```

Do not log `Sec-WebSocket-Protocol` for `/chat/ws`: it temporarily carries the
one-use credential. Apply proxy handshake/IP connection limits at or below the
backend limits for early rejection, but do not strip either offered protocol.
Set `CHAT_SOCKET_TRUSTED_PROXY_CIDRS` to only the proxy's actual source
host/network; forwarded client IP headers are ignored from every other peer.
Production startup fails when `CHAT_SOCKET_REQUIRE_TRUSTED_PROXY=true` and this
list is empty. The WebSocket handler also rejects any connection whose actual
transport peer is outside those CIDRs. Do not substitute a broad
private-network CIDR. The tracked Compose port is loopback-only
(`127.0.0.1:8085:8080`).

## Durable session history

`GET /chat/sessions` and `GET /chat/sessions/{id}` provide durable session and
message history used after reconnect, while live generation stays on the
WebSocket transport.

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
| ENVIRONMENT | production | `development` adds localhost to defaults |
| AUTHENTIK_ISSUER | — | Required JWT issuer |
| AUTHENTIK_AUDIENCE | — | Required JWT audience |
| CHAT_ALLOWED_ORIGINS | https://dev2.solutions | REST CORS origins |
| CHAT_SOCKET_ALLOWED_ORIGINS | https://dev2.solutions | Exact WebSocket origins (no wildcards) |
| CHAT_SOCKET_TRUSTED_PROXY_CIDRS | empty | Proxy CIDRs trusted for forwarded client IP |
| CHAT_SOCKET_REQUIRE_TRUSTED_PROXY | true in production | Fail startup without a trusted proxy CIDR |
| CHAT_SOCKET_SEND_QUEUE | 128 | Per-connection outbound event capacity |
| CHAT_SOCKET_READ_LIMIT_BYTES | 65536 | Maximum client message size |
| CHAT_SOCKET_PING_INTERVAL | 25s | WebSocket control-ping interval |
| CHAT_SOCKET_IDLE_TIMEOUT | 60s | Read/pong idle deadline |
| CHAT_SOCKET_MAX_LIFETIME | 30m | Maximum JWT socket lifetime |
| CHAT_SOCKET_DEVELOPER_MAX_LIFETIME | 5m | Developer/admin snapshot lifetime |
| CHAT_SOCKET_SERVICE_MAX_LIFETIME | 5m | Service credential socket lifetime |
| CHAT_SOCKET_TICKET_RATE_PER_MINUTE | 10 | Ticket issues per user/company |
| CHAT_SOCKET_MAX_OUTSTANDING_TICKETS | 3 | Unused tickets per user |
| CHAT_SOCKET_CONNECTIONS_GLOBAL | 500 | Global active connection leases |
| CHAT_SOCKET_CONNECTIONS_PER_COMPANY | 50 | Company active connection leases |
| CHAT_SOCKET_CONNECTIONS_PER_USER | 3 | User active connection leases |
| CHAT_SOCKET_CONNECTIONS_PER_IP | 20 | IP active connection leases |
| CHAT_SOCKET_CONNECTION_LEASE_TTL | 75s | Crash-recovery connection lease TTL |
| CHAT_SOCKET_GENERATIONS_PER_COMPANY | 20 | Company active generations |
| CHAT_SOCKET_GENERATIONS_GLOBAL | 100 | Service-wide active generation leases |
| CHAT_SOCKET_GENERATIONS_PER_USER | 2 | User active generations |
| CHAT_SOCKET_GENERATION_LEASE_TTL | 3m | Crash-recovery generation lease TTL |
| CHAT_SOCKET_MESSAGES_PER_MINUTE | 60 | Per-socket frame refill rate |
| CHAT_SOCKET_MESSAGE_BURST | 20 | Per-socket frame burst |
| CHAT_SOCKET_MESSAGES_PER_MINUTE_PER_USER | 120 | Distributed user frame limit |
| CHAT_SOCKET_MESSAGES_PER_MINUTE_PER_COMPANY | 1200 | Distributed company frame limit |
| CHAT_SOCKET_MESSAGES_PER_MINUTE_PER_IP | 600 | Distributed client-IP frame limit |
| CHAT_SOCKET_HANDSHAKE_RATE_PER_MINUTE | 30 | Pre-Mongo per-process/IP handshake refill |
| CHAT_SOCKET_HANDSHAKE_BURST | 10 | Pre-Mongo per-process/IP handshake burst |

## Dependencies

- MongoDB (chat sessions, messages, knowledge graph)
- NATS (optional — for service-to-service calls)
- dev2-llm-service (optional — LLM completions via NATS)
- dev2-tickets (optional — ticket operations via HTTP)
- Project Tracker API (optional — PT operations)

All external dependencies are optional — the service functions without NATS,
relying on direct HTTP calls and in-process MongoDB queries.
