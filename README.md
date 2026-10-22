# xacpp

[中文](./README.zh-CN.md)

Agent Control Plane Protocol — TypeScript implementation.

xacpp defines the communication protocol between an agent and its peers. It provides a layered architecture for request-response messaging with session management over multiple transport backends.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Peer (lifecycle orchestration, initiator side)              │
│  negotiate → connect → establish → disconnect                │
├──────────────────────────────────────────────────────────────┤
│  Session (communication facade + transfer state)             │
│  requestCommand / sendEvent / sendBinary;                    │
│  owns TransferSession (ACK/NACK/RESUME/heartbeat),           │
│  wires itself onto the current transport                     │
├──────────────────────────────────────────────────────────────┤
│  Transport (connection entity)                               │
│  Physical IO, binary framing, single-FIFO egress             │
├──────────────────────────────────────────────────────────────┤
│  Stdio / TCP                                                  │
└──────────────────────────────────────────────────────────────┘
```

A session outlives individual connections: on disconnect the session detaches
its transfer mount from the dead transport immediately — frames still flushing
through the dead connection are dropped, never absorbed into session state —
and the transfer state stays in the session. On reconnect the session wires
itself onto the new transport and interrupted transfers resume through the
RESUME handshake (verdict-gated retransmission, SPEC §5).

## Install

```bash
npm install xacpp
```

## Quick Start

### Establish a session (initiator side)

```typescript
import {
  XacppPeer, XacppSession, SocketTransport,
  NegotiateHandler, EstablishHandler, XacppSessionHandler,
} from "xacpp";

// A session owns its transfer state; it is created independently of any
// connection and wires itself onto transports as they come and go.
const session = new XacppSession(); // TransferConfig::default()

const transport = SocketTransport.connectTo(8080, "127.0.0.1");

// Peer orchestrates the lifecycle and auto-wires the session on connect.
const peer = new XacppPeer(
  { commands: [], produceEvents: [], acceptEvents: [] }, // capabilities
  transport,
  negotiateHandler,
  establishHandler,
  session,
);
await peer.connect();

// Establish a logical session (fills identity/emit-events into the session).
await peer.establish(undefined, mySessionHandler, () => {});

// Send commands/events/binary through the session facade.
const response = await session.requestCommand("new_activity");
await session.sendEvent({ type: "think", content: "Hello!" });
const txid = session.sendBinary(payload, (result) => {
  /* BinarySendResult: exactly-once terminal state */
});
```

### Handle incoming connections (responder side)

```typescript
// Responder side has no peer: accept a connection, create a session,
// restore identity if reconnecting, and wire the session onto the transport.
const session = new XacppSession();
session.bindIdentity("session-1", credentials);
session.wire(transport); // auto-unwires on close
session.onBinary((txid, payload) => { /* consume */ });

transport.onRequest((_sessionId, payload) => {
  // Return XacppResponse for inbound commands/events
});
```

### TCP Transport (for network communication)

```typescript
import { SocketTransport } from "xacpp";

// Client
const client = SocketTransport.connectTo(8080, "127.0.0.1");

// Server (with accepted socket)
const server = new SocketTransport(acceptedSocket);
```

## API

### Types

| Type | Description |
|------|-------------|
| `XacppPeer` | Lifecycle orchestration (negotiate/connect/establish/disconnect); wires the session onto the transport automatically (initiator side) |
| `XacppSession` | Communication facade (`requestCommand`/`sendEvent`/`sendBinary`); owns transfer state, survives reconnections, `wire`/`unwire` onto transports |
| `TransferSession` | Transfer-transaction state machine (ACK/NACK/RESUME/heartbeat/backpressure); owned by `XacppSession` |
| `TransferConfig` | Transfer tuning (heartbeat interval, silent timeout, ACK batching, tick) |
| `XacppTransport` | Connection entity: physical IO, binary framing, single-FIFO egress |
| `XacppSessionHandler` | Handles inbound Command/Event for a session |
| `NegotiateHandler` / `EstablishHandler` | Handshake handlers |
| `XacppCommand` / `XacppEvent` | Protocol commands and events |
| `ActivityRef` | Activity identifier reference shared by command and event envelopes (`{id}`) |
| `XacppResponse` | Response payload (`established`, `acknowledge`, `action`, etc.) |
| `XacppError` | Error class with machine-readable codes |
| `PeerState` | Peer state enum (`Disconnected`, `Connected`) |

### Transports

| Class | Description |
|-------|-------------|
| `StdioTransport` | stdin/stdout binary-frame pipe |
| `SocketTransport` | TCP socket (`net.Socket`), spawn-per-request concurrency |

## Wire Protocol

Framing is the XACPP binary frame protocol (docs/SPEC.md §3/§4): every envelope travels as the payload of a `DATA_JSON` frame. Envelope structure:

```json
{"type":"request","id":"r1","payload":{"kind":"command","payload":{"establish":{"credentials":null}}}}
{"type":"response","id":"r1","payload":{"kind":"established","sessionId":"s1"}}
```

Generic commands carry an optional `activity` reference (`activity: {"id": string}`). When absent the field does not appear on the wire; the protocol layer does not enforce it — validation belongs to the command implementation.

The activity event envelope carries a structured `activity: {"id": string}` reference:

```json
{"type":"request","id":"r2","payload":{"kind":"event","payload":{"activity":{"id":"act-1"},"event":{"name":"think","data":null}}}}
```

## Command Declarations

Command declaration schemas may carry a `dispatcher` field with enum value `"bridge" | "tool"`, defaulting to `"bridge"`: `bridge` means the command goes through the event bridge system path; `tool` means injection into the receiving side's model tool surface (same-name replace, peer declarations take precedence). Declaration schemas are pass-through JSON — the protocol library does not enforce types.

## Performance

Baselines measured by `pnpm bench:protocol` and `pnpm bench:egress` (TCP loopback; requires Node >= 22.2). These numbers are the reference for future optimization work.

| Metric | Baseline 2026-10-14 | Re-verified 2026-10-23 |
|---|---|---|
| Codec overhead vs JSONL | −38% to −83% (CRC + 40 B header) | group PASS (chunked path) |
| Request-response RTT | p50 0.071 ms @ ~11.9k req/s | p50 0.070 ms |
| 64 MiB `sendBinary` (1 MiB frames) | 448 MiB/s single stream | 461 MiB/s |
| 4 × 16 MiB concurrent | 399 MiB/s | 390–439 MiB/s |
| Heartbeat p50 under bulk | ≈98 ms | 178–196 ms |
| Ordered egress (raw peer) | ~196k events/s enqueue, ~38k drain | 184k enqueue, 35k drain |

## License

MIT
