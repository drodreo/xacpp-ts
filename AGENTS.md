# AGENTS.md

## Project Overview

xacpp (Agent Control Plane Protocol) — TypeScript implementation.

This is the TS counterpart of [xacpp-rs](https://github.com/drodreo/xacpp-rs). The two projects share identical wire protocol (binary frames per `docs/SPEC.md`; envelope JSON travels in DATA_JSON frames) and must stay in sync.

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
│  Raw byte stream, binary frame splitting (SPEC §3)           │
└──────────────────────────────────────────────────────────────┘
```

### Layer boundaries

- **Transport → upper**: exposes `send` / `onRequest`, hides envelope id, encoding/decoding
- **Peer → upper**: exposes typed `establish` / `requestCommand` / `requestEvent` + session routing
- **Session → upper**: holds `XacppSessionHandler`, sends directly via Transport, bypasses Peer

### File structure

| File | Responsibility |
|------|---------------|
| `src/transport.ts` | `XacppTransport` interface (`send` / `sendFaf` / `onRequest` / `onClosed`) + `RequestHandler` type |
| `src/fetch.ts` | Fetch channel types (FetchHandler / FetchOutcome / FetchServerFacet) + fetch error code registry |
| `src/handler.ts` | `XacppSessionHandler` + `EstablishHandler` interfaces |
| `src/session.ts` | `XacppSession` class |
| `src/peer.ts` | `XacppPeer` class + `PeerState` enum |
| `src/message.ts` | `XacppError`, `XacppRequest`, `XacppResponse`, `XacppEnvelope` |
| `src/commands/index.ts` | `XacppCommand` union type |
| `src/events/` | Event type definitions |
| `src/egress.ts` | Ordered egress (single FIFO drain loop), `ClosedSignal` close broadcast, pending slot types |
| `src/stdio-transport.ts` | Stdio transport (stdin/stdout binary frames) |
| `src/socket-transport.ts` | TCP transport (`net.Socket`, spawn-per-request concurrency) |

## Wire Protocol

Wire framing follows `docs/SPEC.md` (binary frame format, protocol v1.0): every envelope is serialized to JSON and sent as a single-frame `DATA_JSON` transaction (`seq=0`, `total_len` = payload length, fresh `transaction_id`). The JSON below is the `DATA_JSON` payload.

All wire messages are `XacppEnvelope` with `type` field routing:

```json
Request:  {"type":"request","id":"r1","session_id":null,"payload":{"kind":"command","payload":{"establish":{"credentials":null}}}}
Request:  {"type":"request","id":"r2","session_id":"s1","payload":{"kind":"event","payload":{"type":"think","content":"hi"}}}
Response: {"type":"response","id":"r1","payload":{"kind":"established","sessionId":"s1"}}
Response: {"type":"response","id":"r2","session_id":"s1","payload":{"kind":"action","type":"approve"}}
```

### Naming convention

- Envelope layer: `session_id` (snake_case)
- Response payload: `sessionId` (camelCase) — mirrors Rust `rename_all_fields = "camelCase"`

### XacppCommand wire format

Uses externally tagged serde (mirrors Rust):

- `{ "establish": { "credentials": null } }` — Establish command
- `"new_activity"` — string literal commands

## Build

```bash
pnpm build     # rslib → dist/esm/ + dist/cjs/
pnpm test      # vitest run
pnpm bench:egress   # egress throughput benchmark (examples/egress-bench.mts, imports dist bundle; XACPP_BENCH_N overrides scale)
```

Build tool is **rslib** (not tsc). Do not add `tsc` build steps.

## Testing

- `tests/activity-ref.test.ts` — ActivityRef parsing
- `tests/capability.test.ts` — Capability negotiation
- `tests/peer.test.ts` — Transport + Peer + Session e2e
- `tests/serde.test.ts` — Serialization round-trip
- `tests/socket-concurrent.test.ts` — SocketTransport concurrency + ordered egress + closed notification
- `tests/egress-wire.test.ts` — Wire-level egress order/throughput/stability, raw TCP peer
- `tests/fetch.test.ts` — Fetch channel matrix (SPEC §8, wire format byte-exactness)

Must stay in sync with xacpp-rs test suite.

## Transfer protocol invariants (SPEC §5)

Four invariants govern the transfer layer; every change must preserve them:

1. **Send-side pointer advances on ACK only** — unacknowledged bytes cap the
   in-flight window (backpressure); copies release on ACK, terminal release on
   final ACK or abort.
2. **Receiver slots close on byte-count or control frame** — a transaction
   completes when its bytes add up (`TRANSACTION_COMPLETED` announcement /
   `TRANSACTION_ABORT` end it otherwise); no tombstones — a consumed slot
   leaves no trace.
3. **Everything outside the live lists is answered, never silently absorbed** —
   a data frame for a slotless txid is not reassembled (late retransmission of
   a settled transaction, or an unannounced stream); the holder's closure
   signal is the RESUME verdict, never a data-frame reply.
4. **Closure is adjudicated from local knowledge on both sides** — on an
   `unknown`/`rejected` verdict the holder closes from its own state: all
   frames sent (tail frame plus the announcement) ⇒ confirmed; frames still
   owed ⇒ failed. The receiver never needs to explain why a transaction is
   gone — no terminal trace is required for the loop to close.

### Known misreadings (each caused a real defect)

- **"A slotless data frame opens a fresh slot"** — wrong. The reassembler once
  absorbed late retransmissions by opening resurrection slots; that path
  produced both the RESUME deadlock (a resurrected slot never completes, the
  final ACK never re-emerges) and a duplicate-delivery window. A txid reaches
  the receiver only through the peer's explicit announcement (§8 causality
  chain: metadata observed before any byte); a slotless frame is dropped.
- **"`TRANSACTION_COMPLETED` is the receiver's final ACK"** — wrong direction.
  It is holder → receiver: the sender declaring its data plane finished. The
  final ACK is the receiver's cumulative ACK reaching the last frame.
- **"An `unknown`/`rejected` verdict means the transfer failed"** — wrong as an
  unconditional rule. The verdict only says "no open slot here"; the holder's
  own completeness decides confirmed vs failed (invariant 4).
- **"An expired transaction errors out"** — retired semantics. Copies carry no
  retention deadline and no expiry sweep; error code `0x0002` stays registered
  for pre-1.1 tolerance only.
- **Design instinct check**: if a fix seems to need a new frame type, a new
  verdict state, or memory of past transactions, re-read the invariants — the
  recovery design closes its loops with pieces already in the protocol
  (verdict + local knowledge). Complexity here has always meant a misread, not
  a gap.

## Conventions

- All comments and commit messages in English
- Keep in sync with xacpp-rs: when adding/removing a command, response variant, or envelope field, update both projects
- `XacppCommand` is a union type (not a string enum) — Establish carries `credentials`
- Transport implementations must handle envelope `session_id` field
