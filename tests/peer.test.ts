/**
 * Transport + Peer + Session end-to-end tests.
 *
 * Covers core scenarios (aligned with xacpp-rs tests/peer_e2e_tests.rs):
 * 1. send: send request, handler callback processes and auto-replies (Acknowledge / business data / Error)
 * 2. Routing: session_id correctly routes to the corresponding Session handler
 * 3. Establish: initiator and responder handshake flow
 * 4. Disconnect detection
 */

import { describe, it, expect } from "vitest";
import type { XacppRequest, XacppResponse } from "../src/message";
import { XacppError } from "../src/message";
import type { XacppTransport, RequestHandler } from "../src/transport";
import type { XacppCommand } from "../src/commands";
import type { XacppActivityEvent } from "../src/events";
import type { XacppSessionHandler, EstablishHandler } from "../src/handler";
import { XacppPeer, PeerState } from "../src/peer";

// ---- Test Handler implementations ----

/** Generic Session handler: returns Acknowledge for both Command and Event. */
class TestSessionHandler implements XacppSessionHandler {
  async onCommand(): Promise<XacppResponse> {
    return { kind: "acknowledge" };
  }
  async onEvent(_event: XacppActivityEvent): Promise<XacppResponse> {
    return { kind: "acknowledge" };
  }
}

/** Auto-approves Establish and returns TestSessionHandler. */
class AutoApproveEstablishHandler implements EstablishHandler {
  async onEstablish(): Promise<{ sessionId: string; handler: XacppSessionHandler }> {
    return { sessionId: "auto-sid", handler: new TestSessionHandler() };
  }
}

/** Identified Session handler: identifies itself via sessionId in responses. */
class IdentifiedHandler implements XacppSessionHandler {
  constructor(private id: string) {}
  async onCommand(): Promise<XacppResponse> {
    return { kind: "established", sessionId: this.id };
  }
  async onEvent(_event: XacppActivityEvent): Promise<XacppResponse> {
    return { kind: "established", sessionId: this.id };
  }
}

/** EstablishHandler that assigns IdentifiedHandlers in sequence. */
class SequencedEstablishHandler implements EstablishHandler {
  private counter = 0;
  async onEstablish(): Promise<{ sessionId: string; handler: XacppSessionHandler }> {
    const n = ++this.counter;
    const sid = `handler-${n}`;
    return { sessionId: sid, handler: new IdentifiedHandler(sid) };
  }
}

// ---- DirectTransport ----

/** In-memory direct Transport, simulates bidirectional communication. */
class DirectTransport implements XacppTransport {
  private _connected = false;
  private _exhausted = false; // cannot reconnect after disconnect
  private peer: DirectTransport | null = null;
  private requestHandler: RequestHandler | null = null;
  private pending: Map<
    string,
    { resolve: (response: XacppResponse) => void; reject: (err: XacppError) => void }
  > = new Map();
  private nextId = 1;

  /** Bind peer transport. */
  setPeer(peer: DirectTransport): void {
    this.peer = peer;
  }

  async connect(): Promise<void> {
    if (this._exhausted) throw XacppError.alreadyConnected();
    if (this._connected) throw XacppError.alreadyConnected();
    this._connected = true;
  }

  async disconnect(): Promise<void> {
    this._connected = false;
    this._exhausted = true;
    this.rejectAllPending();

    // Notify peer to also clean up pending (simulates pipe disconnect)
    if (this.peer) {
      this.peer.onPeerDisconnect();
    }
  }

  /** Peer disconnect notification. */
  private onPeerDisconnect(): void {
    this.rejectAllPending();
  }

  /** Reject the pending request with the given id (called by peer's deliver). */
  private rejectPending(id: string): void {
    const p = this.pending.get(id);
    if (p) {
      this.pending.delete(id);
      p.reject(XacppError.closed());
    }
  }

  /** Reject all pending requests (used by disconnect / onPeerDisconnect). */
  private rejectAllPending(): void {
    const err = XacppError.closed();
    for (const [, p] of this.pending) {
      p.reject(err);
    }
    this.pending.clear();
  }

  async send(sessionId: string | null, payload: XacppRequest): Promise<XacppResponse> {
    if (!this._connected) throw XacppError.notConnected();

    const id = `r${this.nextId++}`;

    return new Promise<XacppResponse>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });

      const envelope = {
        type: "request" as const,
        id,
        ...(sessionId != null ? { session_id: sessionId } : {}),
        payload,
      };
      const json = JSON.stringify(envelope);

      // Asynchronously deliver to peer for processing (simulates IO delay, allowing disconnect to intervene)
      const peer = this.peer!;
      queueMicrotask(() => peer.deliver(json));
    });
  }

  onRequest(handler: RequestHandler): void {
    if (this._connected) throw XacppError.alreadyConnected();
    this.requestHandler = handler;
  }

  /** Receive envelope JSON from peer. */
  private async deliver(json: string): Promise<void> {
    if (!this._connected) {
      // Already disconnected, notify sender to reject the corresponding pending
      const envelope = JSON.parse(json);
      this.peer!.rejectPending(envelope.id);
      return;
    }

    const envelope = JSON.parse(json);

    if (envelope.type === "request") {
      const sessionId: string | null = envelope.session_id ?? null;
      let responsePayload: XacppResponse;

      try {
        if (!this.requestHandler) throw XacppError.noHandler();
        responsePayload = await this.requestHandler(sessionId, envelope.payload);
      } catch (e) {
        const err =
          e instanceof XacppError
            ? e
            : XacppError.internal(e instanceof Error ? e.message : String(e));
        responsePayload = { kind: "error", code: err.code, message: err.message };
      }

      const responseEnvelope = {
        type: "response" as const,
        id: envelope.id,
        ...(sessionId != null ? { session_id: sessionId } : {}),
        payload: responsePayload,
      };
      this.peer!.resolveResponse(JSON.stringify(responseEnvelope));
    }
  }

  /** Receive response from peer. */
  private resolveResponse(json: string): void {
    const envelope = JSON.parse(json);
    const pending = this.pending.get(envelope.id);
    if (pending) {
      this.pending.delete(envelope.id);
      pending.resolve(envelope.payload);
    }
  }
}

// ---- Helper functions ----

/** Create a pair of directly connected DirectTransports (not yet connected). */
function duplexPair(): [DirectTransport, DirectTransport] {
  const a = new DirectTransport();
  const b = new DirectTransport();
  a.setPeer(b);
  b.setPeer(a);
  return [a, b];
}

/** Create a pair of connected Peers (side B auto-approves Establish). */
async function connectedPeers(): Promise<[XacppPeer, XacppPeer]> {
  const [a, b] = duplexPair();
  const peerA = new XacppPeer(a, new AutoApproveEstablishHandler());
  const peerB = new XacppPeer(b, new AutoApproveEstablishHandler());
  await peerA.connect();
  await peerB.connect();
  return [peerA, peerB];
}

/** Timeout wrapper (5s). */
function timeout<T>(promise: Promise<T>, ms = 5000): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timeout")), ms);
    promise.then(
      (v) => {
        clearTimeout(timer);
        resolve(v);
      },
      (e) => {
        clearTimeout(timer);
        reject(e);
      },
    );
  });
}

// ---- Transport layer tests ----

describe("Transport send", () => {
  it("send establish returns established response", async () => {
    const [transportA, transportB] = duplexPair();

    transportB.onRequest((_sessionId, payload) => {
      if (
        payload.kind === "command" &&
        typeof payload.payload === "object" &&
        "establish" in payload.payload
      ) {
        return Promise.resolve({ kind: "established" as const, sessionId: "sid-1" });
      }
      return Promise.resolve({ kind: "acknowledge" });
    });

    await transportA.connect();
    await transportB.connect();

    const response = await timeout(
      transportA.send(null, {
        kind: "command",
        payload: { establish: { credentials: null } },
      }),
    );

    expect(response.kind).toBe("established");
    if (response.kind === "established") {
      expect(response.sessionId).toBe("sid-1");
    }
  });

  it("send event returns acknowledge", async () => {
    const [transportA, transportB] = duplexPair();
    const received: XacppActivityEvent[] = [];

    transportB.onRequest((_sessionId, payload) => {
      if (payload.kind === "event") {
        received.push(payload.payload);
      }
      return Promise.resolve({ kind: "acknowledge" });
    });

    await transportA.connect();
    await transportB.connect();

    const response = await timeout(
      transportA.send("s1", {
        kind: "event",
        payload: { activity: "test-act", event: { type: "think", content: "hello" } },
      }),
    );

    expect(response.kind).toBe("acknowledge");

    expect(received[0].activity).toBe("test-act");
    expect(received[0].event.type).toBe("think");
    if (received[0].event.type === "think") {
      expect(received[0].event.content).toBe("hello");
    }
  });

  it("send interactive event returns action response", async () => {
    const [transportA, transportB] = duplexPair();

    transportB.onRequest((_sessionId, payload) => {
      if (payload.kind === "event" && payload.payload.event.type === "action_request") {
        return Promise.resolve({
          kind: "action" as const,
          requestId: payload.payload.event.requestId,
          type: "approve" as const,
        });
      }
      return Promise.resolve({ kind: "acknowledge" });
    });

    await transportA.connect();
    await transportB.connect();

    const response = await timeout(
      transportA.send("s1", {
        kind: "event",
        payload: {
          activity: "test-act",
          event: {
            type: "action_request",
            requestId: "req-1",
            toolName: "bash",
            arguments: "{}",
            actionId: "act-1",
            description: "test",
            alert: "info",
          },
        },
      }),
    );

    expect(response.kind).toBe("action");
    if (response.kind === "action") {
      expect(response.requestId).toBe("req-1");
      expect(response.type).toBe("approve");
    }
  });

  it("handler error returns error response", async () => {
    const [transportA, transportB] = duplexPair();

    transportB.onRequest(() => {
      throw XacppError.internal("something went wrong");
    });

    await transportA.connect();
    await transportB.connect();

    const response = await timeout(
      transportA.send(null, {
        kind: "command",
        payload: { establish: { credentials: null } },
      }),
    );

    expect(response.kind).toBe("error");
    if (response.kind === "error") {
      expect(response.code).toBe("internal_error");
      expect(response.message).toBe("internal error: something went wrong");
    }
  });

  it("no handler returns error response", async () => {
    // B does not register handler, but connects
    const [transportA, transportB] = duplexPair();
    await transportA.connect();
    await transportB.connect();

    const response = await timeout(
      transportA.send(null, {
        kind: "command",
        payload: { establish: { credentials: null } },
      }),
    );

    expect(response.kind).toBe("error");
    if (response.kind === "error") {
      expect(response.code).toBe("no_handler");
    }
  });

  it("bidirectional send", async () => {
    const [transportA, transportB] = duplexPair();

    transportA.onRequest(() =>
      Promise.resolve({ kind: "established" as const, sessionId: "from-a" }),
    );
    transportB.onRequest(() =>
      Promise.resolve({ kind: "established" as const, sessionId: "from-b" }),
    );

    await transportA.connect();
    await transportB.connect();

    // A → B
    const respAB = await timeout(
      transportA.send(null, {
        kind: "command",
        payload: { establish: { credentials: null } },
      }),
    );
    expect(respAB.kind).toBe("established");
    if (respAB.kind === "established") expect(respAB.sessionId).toBe("from-b");

    // B → A
    const respBA = await timeout(
      transportB.send(null, {
        kind: "command",
        payload: { establish: { credentials: null } },
      }),
    );
    expect(respBA.kind).toBe("established");
    if (respBA.kind === "established") expect(respBA.sessionId).toBe("from-a");
  });
});

// ---- Peer layer tests ----

describe("Peer", () => {
  it("connect state is Connected", async () => {
    const [peerA] = await connectedPeers();
    expect(peerA.state).toBe(PeerState.Connected);
  });

  it("disconnect state is Disconnected", async () => {
    const [peerA] = await connectedPeers();
    await peerA.disconnect();
    expect(peerA.state).toBe(PeerState.Disconnected);
  });

  it("establish creates session", async () => {
    const [peerA] = await connectedPeers();

    const handler = new TestSessionHandler();
    const session = await timeout(peerA.establish(null, handler));

    expect(session.sessionId).toBeTruthy();
    expect(session.credentials).toBeNull();
  });

  it("session request command", async () => {
    const [peerA] = await connectedPeers();

    const handler = new TestSessionHandler();
    const session = await timeout(peerA.establish(null, handler));

    const response = await timeout(session.requestCommand({ new_activity: { title: null } }));
    expect(response.kind).toBe("acknowledge");
  });

  it("session request event", async () => {
    const [peerA] = await connectedPeers();

    const handler = new TestSessionHandler();
    const session = await timeout(peerA.establish(null, handler));

    const response = await timeout(
      session.requestEvent({ activity: "test-act", event: { type: "think", content: "hi" } }),
    );
    expect(response.kind).toBe("acknowledge");
  });
});

// ---- Disconnect scenario tests ----

describe("Disconnect", () => {
  it("send after disconnect returns error", async () => {
    const [transportA, transportB] = duplexPair();

    transportB.onRequest(() => Promise.resolve({ kind: "acknowledge" }));

    await transportA.connect();
    await transportB.connect();

    // Normal communication
    const response = await timeout(
      transportA.send(null, {
        kind: "command",
        payload: { establish: { credentials: null } },
      }),
    );
    expect(response.kind).toBe("acknowledge");

    // Disconnect B
    await transportB.disconnect();

    // A sending again should receive an error
    await expect(
      timeout(
        transportA.send(null, {
          kind: "command",
          payload: { establish: { credentials: null } },
        }),
      ),
    ).rejects.toThrow();
  });

  it("onRequest after connect throws", async () => {
    const [transportA] = duplexPair();
    await transportA.connect();

    // Registering handler after connect should throw
    expect(() =>
      transportA.onRequest(async () => ({ kind: "acknowledge" })),
    ).toThrow();
  });

  it("connect disconnect connect fails", async () => {
    const [transportA, transportB] = duplexPair();
    await transportA.connect();
    await transportB.connect();

    await transportA.disconnect();
    await transportB.disconnect();

    // Connecting again should fail
    await expect(transportA.connect()).rejects.toThrow();
  });
});

// ---- Concurrent request tests ----

describe("Concurrent", () => {
  it("concurrent requests id matching", async () => {
    const [transportA, transportB] = duplexPair();

    transportB.onRequest((_sessionId, payload) => {
      // Return different sessionId based on command type, for verifying correct matching
      let sid: string;
      if (payload.kind === "command") {
        const cmd = payload.payload;
        if (typeof cmd === "object" && "establish" in cmd) sid = "establish";
        else if (typeof cmd === "object" && cmd !== null && "new_activity" in cmd) sid = "new";
        else if (typeof cmd === "object" && cmd !== null && "invoke_activity" in cmd) sid = "invoke";
        else if (typeof cmd === "object" && cmd !== null && "compact_activity" in cmd) sid = "compact";
        else if (typeof cmd === "object" && cmd !== null && "cancel_activity" in cmd) sid = "cancel";
      } else {
        sid = "event";
      }
      return Promise.resolve({ kind: "established" as const, sessionId: sid });
    });

    await transportA.connect();
    await transportB.connect();

    const commands: XacppCommand[] = [
      { establish: { credentials: null } },
      { new_activity: { title: null } },
      { invoke_activity: { activity: "act-1", messages: [] } },
      { compact_activity: { activity: "act-1" } },
      { cancel_activity: { activity: "act-1" } },
    ];

    // Send 5 concurrent requests
    const promises = commands.map((cmd) =>
      timeout(transportA.send(null, { kind: "command", payload: cmd })),
    );
    const responses = await Promise.all(promises);

    const sids = responses.map((r) => {
      expect(r.kind).toBe("established");
      if (r.kind === "established") return r.sessionId;
      return "";
    });

    // All 5 distinct sids received, no duplicates or losses
    const sorted = [...sids].sort();
    expect(sorted).toEqual(["cancel", "compact", "establish", "invoke", "new"]);
  });
});

// ---- Multi-session routing isolation tests ----

describe("Multi session routing", () => {
  it("multi session routing isolation", async () => {
    // peerB uses SequencedEstablishHandler, each session gets an identified handler
    const [transportA, transportB] = duplexPair();
    const peerA = new XacppPeer(transportA, new SequencedEstablishHandler());
    const peerB = new XacppPeer(transportB, new SequencedEstablishHandler());
    await peerA.connect();
    await peerB.connect();

    // A as initiator: establish two sessions
    const handlerA = new TestSessionHandler();
    const session1 = await timeout(peerA.establish(null, handlerA));
    const session2 = await timeout(peerA.establish(null, handlerA));

    const sid1 = session1.sessionId;
    const sid2 = session2.sessionId;
    expect(sid1).not.toBe(sid2);

    // session_1 sends command → B-side routes to handler-1 → response sessionId = "handler-1"
    const resp1 = await timeout(session1.requestCommand({ new_activity: { title: null } }));
    expect(resp1.kind).toBe("established");
    if (resp1.kind === "established") {
      expect(resp1.sessionId).toBe("handler-1");
    }

    // session_2 sends command → B-side routes to handler-2 → response sessionId = "handler-2"
    const resp2 = await timeout(session2.requestCommand({ new_activity: { title: null } }));
    expect(resp2.kind).toBe("established");
    if (resp2.kind === "established") {
      expect(resp2.sessionId).toBe("handler-2");
    }

    // Cross-validation: session_1 sends again, still routes to handler-1
    const resp1Again = await timeout(session1.requestCommand({ cancel_activity: { activity: "act-1" } }));
    expect(resp1Again.kind).toBe("established");
    if (resp1Again.kind === "established") {
      expect(resp1Again.sessionId).toBe("handler-1");
    }
  });
});
