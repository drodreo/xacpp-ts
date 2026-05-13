# AGENTS.md

## Project Overview

xacpp (Agent Control Plane Protocol) — TypeScript implementation.

This is the TS counterpart of [xacpp-rs](https://github.com/drodreo/xacpp-rs). The two projects share identical wire protocol (JSONL envelope) and must stay in sync.

## Architecture

Three-layer design, mirroring xacpp-rs:

```
┌──────────────────────────────────────────────────────────────┐
│  Peer (protocol layer)                                       │
│  Typed operations: requestCommand / requestEvent             │
│  Session routing: sessionId → XacppSessionHandler            │
├──────────────────────────────────────────────────────────────┤
│  Session (session layer)                                     │
│  Holds XacppSessionHandler, sends directly via Transport     │
│  Does not go through Peer                                    │
├──────────────────────────────────────────────────────────────┤
│  Transport (transport layer)                                 │
│  Single semantic: send (request-response), caller may skip   │
│  Internally: envelope assembly/disassembly, id correlation,  │
│  pending matching                                            │
│  Does not know about specific business event types           │
├──────────────────────────────────────────────────────────────┤
│  Underlying pipe (Stdio / TCP / WebSocket)                   │
│  Raw byte stream, frame splitting (JSONL)                    │
└──────────────────────────────────────────────────────────────┘
```

### Layer boundaries

- **Transport → upper**: exposes `send` / `onRequest`, hides envelope id, encoding/decoding
- **Peer → upper**: exposes typed `establish` / `requestCommand` / `requestEvent` + session routing
- **Session → upper**: holds `XacppSessionHandler`, sends directly via Transport, bypasses Peer

### File structure

| File | Responsibility |
|------|---------------|
| `src/transport.ts` | `XacppTransport` interface + `RequestHandler` type |
| `src/handler.ts` | `XacppSessionHandler` + `EstablishHandler` interfaces |
| `src/session.ts` | `XacppSession` class |
| `src/peer.ts` | `XacppPeer` class + `PeerState` enum |
| `src/message.ts` | `XacppError`, `XacppRequest`, `XacppResponse`, `XacppEnvelope` |
| `src/commands/index.ts` | `XacppCommand` union type |
| `src/events/` | Event type definitions |
| `src/stdio-transport.ts` | Stdio transport (stdin/stdout JSONL) |
| `src/socket-transport.ts` | TCP transport (`net.Socket`, spawn-per-request concurrency) |

## Wire Protocol

All wire messages are `XacppEnvelope` with `type` field routing:

```json
Request:  {"type":"request","id":"r1","session_id":null,"payload":{"kind":"command","payload":{"establish":{"credentials":null}}}}
Request:  {"type":"request","id":"r2","session_id":"s1","payload":{"kind":"event","payload":{"type":"think","content":"hi"}}}
Response: {"type":"response","id":"r1","payload":{"kind":"established","sessionId":"s1"}}
Response: {"type":"response","id":"r2","session_id":"s1","payload":{"kind":"action","requestId":"req-1","type":"approve"}}
```

### Naming convention

- Envelope layer: `session_id` (snake_case)
- Response payload: `sessionId`, `requestId` (camelCase) — mirrors Rust `rename_all_fields = "camelCase"`

### XacppCommand wire format

Uses externally tagged serde (mirrors Rust):

- `{ "establish": { "credentials": null } }` — Establish command
- `"new_activity"` — string literal commands

## Build

```bash
pnpm build     # rslib → dist/esm/ + dist/cjs/
pnpm test      # vitest run
```

Build tool is **rslib** (not tsc). Do not add `tsc` build steps.

## Testing

- `tests/peer.test.ts` — Transport + Peer + Session e2e (16 tests)
- `tests/serde.test.ts` — Serialization round-trip (15 tests)
- `tests/socket-concurrent.test.ts` — SocketTransport concurrency (3 tests)

Must stay in sync with xacpp-rs test suite.

## Conventions

- All comments in English
- Keep in sync with xacpp-rs: when adding/removing a command, response variant, or envelope field, update both projects
- `XacppCommand` is a union type (not a string enum) — Establish carries `credentials`
- Transport implementations must handle envelope `session_id` field
