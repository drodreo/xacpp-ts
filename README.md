# xacpp

Agent Control Plane Protocol — TypeScript implementation.

xacpp defines the communication protocol between an agent and its peers. It provides a layered architecture for request-response messaging with session management over multiple transport backends.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Peer (protocol layer)                                       │
│  Typed operations + session routing                          │
├──────────────────────────────────────────────────────────────┤
│  Session (session layer)                                     │
│  Independent session context, sends directly via Transport   │
├──────────────────────────────────────────────────────────────┤
│  Transport (transport layer)                                 │
│  Envelope assembly, id correlation, pending matching         │
├──────────────────────────────────────────────────────────────┤
│  Stdio / TCP / WebSocket                                     │
└──────────────────────────────────────────────────────────────┘
```

## Install

```bash
npm install xacpp
```

## Quick Start

### Establish a session (initiator side)

```typescript
import { XacppPeer, XacppSession, StdioTransport, XacppSessionHandler, XacppResponse } from "xacpp";

// Create transport + peer
const transport = new StdioTransport(process.stdout, process.stdin);

const peer = new XacppPeer(transport, {
  async onEstablish(transport, credentials) {
    return { sessionId: "server-session", handler: mySessionHandler };
  },
});

await peer.connect();

// Establish a logical session
const session = await peer.establish(null, mySessionHandler);

// Send commands/events through the session
const response = await session.requestCommand("new_activity");
await session.requestEvent({ type: "think", content: "Hello!" });
```

### Handle incoming requests (responder side)

```typescript
const sessionHandler: XacppSessionHandler = {
  async onCommand(command) {
    // Handle command
    return { kind: "acknowledge" };
  },
  async onEvent(event) {
    // Handle event
    return { kind: "acknowledge" };
  },
};
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
| `XacppTransport` | Transport interface (`connect`, `disconnect`, `send`, `onRequest`) |
| `XacppPeer` | Protocol endpoint with session routing |
| `XacppSession` | Logical session, sends via Transport directly |
| `XacppSessionHandler` | Handles inbound Command/Event for a session |
| `EstablishHandler` | Handles Establish handshake requests |
| `XacppCommand` | Protocol commands (`establish`, `new_activity`, etc.) |
| `XacppEvent` | Protocol events (think, action_request, question, etc.) |
| `XacppRequest` | Request payload (`command` or `event`) |
| `XacppResponse` | Response payload (`established`, `acknowledge`, `action`, etc.) |
| `XacppError` | Error class with machine-readable codes |
| `PeerState` | Peer state enum (`Disconnected`, `Connected`) |

### Transports

| Class | Description |
|-------|-------------|
| `StdioTransport` | stdin/stdout JSONL pipe |
| `SocketTransport` | TCP socket (`net.Socket`), spawn-per-request concurrency |

## Wire Protocol

JSONL (one JSON object per line) with envelope structure:

```json
{"type":"request","id":"r1","payload":{"kind":"command","payload":{"establish":{"credentials":null}}}}
{"type":"response","id":"r1","payload":{"kind":"established","sessionId":"s1"}}
```

## License

MIT
