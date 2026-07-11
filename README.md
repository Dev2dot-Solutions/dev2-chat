# dev2-chat

Knowledge-augmented conversational Q&A service for the dev2.solutions platform.

A Go microservice that owns chat sessions, knowledge-augmented Q&A, and LLM
tool orchestration. Calls `dev2-llm-service` via NATS for LLM completions and
`dev2-knowledge` via NATS for knowledge graph search. Falls back to direct
HTTP calls when NATS is unavailable.

## Architecture

```
User ──HTTP──▶ dev2-chat ──NATS──▶ dev2-llm-service
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
| GET | /chat/sessions | List chat sessions |
| GET | /chat/sessions/{id} | Get session with messages |
| GET | /settings/llm?company_id= | Get LLM config |
| GET | /settings/pt?company_id= | Get PT config |

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

## Dependencies

- MongoDB (chat sessions, messages, knowledge graph)
- NATS (optional — for service-to-service calls)
- dev2-llm-service (optional — LLM completions via NATS)
- dev2-tickets (optional — ticket operations via HTTP)
- Project Tracker API (optional — PT operations)

All external dependencies are optional — the service functions without NATS,
relying on direct HTTP calls and in-process MongoDB queries.
