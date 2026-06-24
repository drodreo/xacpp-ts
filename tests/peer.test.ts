/**
 * Transport + Peer + Session end-to-end tests.
 *
 * Covers core scenarios (aligned with xacpp-rs tests/peer_e2e_tests.rs):
 * 1. send: send request, handler callback processes and auto-replies (Acknowledge / business data / Error)
 * 2. Routing: session_id correctly routes to the corresponding Session handler
 * 3. Establish: initiator and responder handshake flow
 * 4. Disconnect detection
 * 5. Interaction command lifecycle (action_request, question, sensitive_info_operation)
 */

import { describe, it, expect } from "vitest";
import type { XacppRequest, XacppResponse } from "../src/message";
import { XacppError, acknowledge, genericResponse } from "../src/message";
import type { XacppTransport, RequestHandler } from "../src/transport";
import type { XacppCommand } from "../src/commands";
import { genericCommand } from "../src/commands";
import type { XacppActivityEvent } from "../src/events";
import { newEvent, newActivityEvent } from "../src/events";
import type { XacppSessionHandler, EstablishHandler, EstablishDecision, NegotiateHandler } from "../src/handler";
import type { Capabilities, EffectiveCapabilities } from "../src/capability";
import { XacppPeer, PeerState } from "../src/peer";
import type {
  ActionRequestPayload,
  ActionResponse,
  QuestionPayload,
  QuestionResponse,
  SensitiveInfoOperationPayload,
  SensitiveInfoResult,
} from "../src/events/interaction";

// ---- Test Handler implementations ----

/** Generic Session handler: returns Acknowledge for both Command and Event. */
class TestSessionHandler implements XacppSessionHandler {
  async onCommand(): Promise<XacppResponse> {
    return acknowledge();
  }
  async onEvent(_event: XacppActivityEvent): Promise<XacppResponse> {
    return acknowledge();
  }
}

/** Auto-approves Establish and returns TestSessionHandler. */
class AutoApproveEstablishHandler implements EstablishHandler {
  async onEstablish(_transport: XacppTransport, _credentials: string | undefined): Promise<EstablishDecision> {
    return { type: "established", sessionId: "auto-sid", credentials: "auto-creds", handler: new TestSessionHandler() };
  }
  async onEstablishConfirm(_transport: XacppTransport): Promise<{ sessionId: string; handler: XacppSessionHandler; credentials: string }> {
    return { sessionId: "auto-sid", handler: new TestSessionHandler(), credentials: "auto-creds" };
  }
}

/** Identified Session handler: identifies itself via generic response. */
class IdentifiedHandler implements XacppSessionHandler {
  constructor(private id: string, private creds: string) {}
  async onCommand(): Promise<XacppResponse> {
    return genericResponse("identified", { sessionId: this.id, credentials: this.creds });
  }
  async onEvent(_event: XacppActivityEvent): Promise<XacppResponse> {
    return genericResponse("identified", { sessionId: this.id, credentials: this.creds });
  }
}

/** EstablishHandler that assigns IdentifiedHandlers in sequence. */
class SequencedEstablishHandler implements EstablishHandler {
  private counter = 0;
  async onEstablish(_transport: XacppTransport, _credentials: string | undefined): Promise<EstablishDecision> {
    const n = ++this.counter;
    const sid = `handler-${n}`;
    return { type: "established", sessionId: sid, credentials: `creds-${n}`, handler: new IdentifiedHandler(sid, `creds-${n}`) };
  }
  async onEstablishConfirm(_transport: XacppTransport): Promise<{ sessionId: string; handler: XacppSessionHandler; credentials: string }> {
    const n = ++this.counter;
    const sid = `handler-${n}`;
    return { sessionId: sid, handler: new IdentifiedHandler(sid, `creds-${n}`), credentials: `creds-${n}` };
  }
}

/** Challenge-aware handler: onEstablish returns challenge_required, onEstablishConfirm returns (sid, handler, credentials). */
class ChallengeEstablishHandler implements EstablishHandler {
  async onEstablish(
    _transport: XacppTransport,
    _credentials: string | undefined,
  ): Promise<EstablishDecision> {
    return { type: "challenge_required", challenge: "test-challenge" };
  }

  async onEstablishConfirm(
    _transport: XacppTransport,
  ): Promise<{ sessionId: string; handler: XacppSessionHandler; credentials: string }> {
    return { sessionId: "challenge-sid", handler: new TestSessionHandler(), credentials: "issued-creds" };
  }
}

/** Accepts all capabilities during negotiation. */
class AcceptAllNegotiateHandler implements NegotiateHandler {
  async onNegotiate(_effective: EffectiveCapabilities): Promise<void> {
    // Accept all
  }
}

/** Handler that processes interaction commands (action_request, question, sensitive_info_operation). */
class InteractionSessionHandler implements XacppSessionHandler {
  async onCommand(command: XacppCommand): Promise<XacppResponse> {
    if (typeof command === "object" && "generic" in command) {
      const { name, arguments: args } = command.generic;

      if (name === "action_request") {
        const payload = args as ActionRequestPayload;
        return genericResponse("action", { requestId: payload.requestId, type: "approve" } satisfies ActionResponse);
      }

      if (name === "question") {
        return genericResponse("question", { type: "answer", content: "yes" } satisfies QuestionResponse);
      }

      if (name === "sensitive_info_operation") {
        const payload = args as SensitiveInfoOperationPayload;
        if (payload.operation.type === "collect") {
          const results: SensitiveInfoResult[] = payload.operation.items.map((item) => ({
            type: "provided",
            key: item.key,
            value: `value-for-${item.key}`,
          }));
          return genericResponse("sensitive_info_operation", { results });
        }
        if (payload.operation.type === "delete") {
          const results: SensitiveInfoResult[] = payload.operation.items.map((item) => ({
            type: "deleted",
            id: item.key,
          }));
          return genericResponse("sensitive_info_operation", { results });
        }
      }
    }
    return acknowledge();
  }

  async onEvent(_event: XacppActivityEvent): Promise<XacppResponse> {
    return acknowledge();
  }
}

/** EstablishHandler that creates InteractionSessionHandler. */
class InteractionEstablishHandler implements EstablishHandler {
  async onEstablish(_transport: XacppTransport, _credentials: string | undefined): Promise<EstablishDecision> {
    return { type: "established", sessionId: "int-sid", credentials: "int-creds", handler: new InteractionSessionHandler() };
  }
  async onEstablishConfirm(_transport: XacppTransport): Promise<{ sessionId: string; handler: XacppSessionHandler; credentials: string }> {
    return { sessionId: "int-sid", handler: new InteractionSessionHandler(), credentials: "int-creds" };
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
  const capsAgent: Capabilities = {
    commands: [{ name: "new_activity" }, { name: "list_activity" }],
    produceEvents: [{ name: "think" }, { name: "info" }],
  };
  const capsBot: Capabilities = { commands: [], produceEvents: [], acceptEvents: [] };
  const peerA = new XacppPeer(capsAgent, a, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
  const peerB = new XacppPeer(capsBot, b, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
  await peerA.connect();
  await peerB.connect();
  return [peerA, peerB];
}

/** Creates a pair of connected + negotiated Peers ready for Establish. */
async function negotiatedPeers(): Promise<[XacppPeer, XacppPeer]> {
  const [peerA, peerB] = await connectedPeers();
  await peerA.negotiate();
  return [peerA, peerB];
}

/** Creates a pair of negotiated Peers where side B uses InteractionSessionHandler. */
async function interactionPeers(): Promise<[XacppPeer, XacppPeer]> {
  const [a, b] = duplexPair();
  const caps: Capabilities = { commands: [], produceEvents: [], acceptEvents: [] };
  const peerA = new XacppPeer(caps, a, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
  const peerB = new XacppPeer(caps, b, new AcceptAllNegotiateHandler(), new InteractionEstablishHandler());
  await peerA.connect();
  await peerB.connect();
  await peerA.negotiate();
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
        return Promise.resolve({ kind: "established" as const, sessionId: "sid-1", credentials: "test-creds" });
      }
      return Promise.resolve(acknowledge());
    });

    await transportA.connect();
    await transportB.connect();

    const response = await timeout(
      transportA.send(null, {
        kind: "command",
        payload: { establish: { credentials: undefined } },
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
      return Promise.resolve(acknowledge());
    });

    await transportA.connect();
    await transportB.connect();

    const response = await timeout(
      transportA.send("s1", {
        kind: "event",
        payload: newActivityEvent("test-act", newEvent("think", { content: "hello" })),
      }),
    );

    expect(response.kind).toBe("generic");
    if (response.kind === "generic") {
      expect(response.name).toBe("acknowledge");
    }

    expect(received[0].activity).toBe("test-act");
    expect(received[0].event.name).toBe("think");
    expect((received[0].event.data as { content: string }).content).toBe("hello");
  });

  it("send interaction command returns action response", async () => {
    const [transportA, transportB] = duplexPair();

    transportB.onRequest((_sessionId, payload) => {
      if (payload.kind === "command") {
        const cmd = payload.payload;
        if (typeof cmd === "object" && "generic" in cmd && cmd.generic.name === "action_request") {
          const args = cmd.generic.arguments as ActionRequestPayload;
          return Promise.resolve(
            genericResponse("action", { requestId: args.requestId, type: "approve" }),
          );
        }
      }
      return Promise.resolve(acknowledge());
    });

    await transportA.connect();
    await transportB.connect();

    const response = await timeout(
      transportA.send("s1", {
        kind: "command",
        payload: genericCommand("action_request", {
          activity: "act-1",
          requestId: "req-1",
          toolName: "bash",
          arguments: "{}",
          actionId: "act-1",
          description: "test",
          alert: "info",
          intent: "test",
        } satisfies ActionRequestPayload),
      }),
    );

    expect(response.kind).toBe("generic");
    if (response.kind === "generic") {
      expect(response.name).toBe("action");
      const data = response.data as ActionResponse;
      expect(data.type).toBe("approve");
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
        payload: { establish: { credentials: undefined } },
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
        payload: { establish: { credentials: undefined } },
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
      Promise.resolve(genericResponse("from-a", null)),
    );
    transportB.onRequest(() =>
      Promise.resolve(genericResponse("from-b", null)),
    );

    await transportA.connect();
    await transportB.connect();

    // A → B
    const respAB = await timeout(
      transportA.send(null, {
        kind: "command",
        payload: { establish: { credentials: undefined } },
      }),
    );
    expect(respAB.kind).toBe("generic");
    if (respAB.kind === "generic") expect(respAB.name).toBe("from-b");

    // B → A
    const respBA = await timeout(
      transportB.send(null, {
        kind: "command",
        payload: { establish: { credentials: undefined } },
      }),
    );
    expect(respBA.kind).toBe("generic");
    if (respBA.kind === "generic") expect(respBA.name).toBe("from-a");
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
    const [peerA] = await negotiatedPeers();

    const handler = new TestSessionHandler();
    const session = await timeout(peerA.establish(undefined, handler, () => {}));

    expect(session.sessionId).toBeTruthy();
    expect(session.credentials).toBe("auto-creds");
  });

  it("session request command", async () => {
    const [peerA] = await negotiatedPeers();

    const handler = new TestSessionHandler();
    const session = await timeout(peerA.establish(undefined, handler, () => {}));

    const response = await timeout(session.requestCommand(genericCommand("new_activity", { title: null })));
    expect(response.kind).toBe("generic");
    if (response.kind === "generic") {
      expect(response.name).toBe("acknowledge");
    }
  });

  it("session request event", async () => {
    const [peerA] = await negotiatedPeers();

    const handler = new TestSessionHandler();
    const session = await timeout(peerA.establish(undefined, handler, () => {}));

    const response = await timeout(
      session.requestEvent(newActivityEvent("test-act", newEvent("think", { content: "hi" }))),
    );
    expect(response.kind).toBe("generic");
    if (response.kind === "generic") {
      expect(response.name).toBe("acknowledge");
    }
  });
});

// ---- Negotiate tests ----

describe("Negotiate", () => {
  const capsAgent: Capabilities = {
    commands: [
      { name: "new_activity" },
      { name: "switch_activity" },
    ],
    produceEvents: [
      { name: "content_delta" },
      { name: "activity_done" },
    ],
  };
  const capsBot: Capabilities = { commands: [], produceEvents: [], acceptEvents: [] };

  it("full flow: negotiate exchanges capabilities and transitions to Negotiated", async () => {
    const [transportA, transportB] = duplexPair();
    const peerA = new XacppPeer(capsAgent, transportA, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    const peerB = new XacppPeer(capsBot, transportB, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    await peerA.connect();
    await peerB.connect();

    expect(peerA.state).toBe(PeerState.Connected);
    expect(peerB.state).toBe(PeerState.Connected);

    await peerA.negotiate();

    expect(peerA.state).toBe(PeerState.Negotiated);
    // A received B's capabilities (empty bot)
    expect(peerA.remoteCapabilities.commands).toEqual([]);
    expect(peerA.remoteCapabilities.produceEvents).toEqual([]);
  });

  it("responder rejects negotiation", async () => {
    const [transportA, transportB] = duplexPair();

    /** Rejects negotiation by throwing. */
    const rejectNegotiateHandler: NegotiateHandler = {
      async onNegotiate(_effective: EffectiveCapabilities): Promise<void> {
        throw XacppError.application("unsupported", "capability not supported");
      },
    };

    const peerA = new XacppPeer(capsAgent, transportA, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    const peerB = new XacppPeer(capsBot, transportB, rejectNegotiateHandler, new AutoApproveEstablishHandler());
    await peerA.connect();
    await peerB.connect();

    // A initiates negotiate; B's handler rejects → B responds with error → A receives error
    await expect(timeout(peerA.negotiate())).rejects.toThrow("capability not supported");
    expect(peerA.state).toBe(PeerState.Connected);
  });

  it("initiator rejects negotiation (local handler throws after receiving remote caps)", async () => {
    const [transportA, transportB] = duplexPair();

    /** Rejects negotiation by throwing. */
    const rejectNegotiateHandler: NegotiateHandler = {
      async onNegotiate(_effective: EffectiveCapabilities): Promise<void> {
        throw XacppError.application("incompatible", "incompatible capabilities");
      },
    };

    const peerA = new XacppPeer(capsAgent, transportA, rejectNegotiateHandler, new AutoApproveEstablishHandler());
    const peerB = new XacppPeer(capsBot, transportB, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    await peerA.connect();
    await peerB.connect();

    // A initiates negotiate; remote B accepts and returns caps; but A's local handler rejects
    await expect(timeout(peerA.negotiate())).rejects.toThrow("incompatible capabilities");
    expect(peerA.state).toBe(PeerState.Connected);
  });

  it("establish without negotiate fails", async () => {
    const [transportA, transportB] = duplexPair();
    const peerA = new XacppPeer(capsAgent, transportA, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    const peerB = new XacppPeer(capsBot, transportB, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    await peerA.connect();
    await peerB.connect();

    // State is Connected, not Negotiated → establish should fail
    await expect(
      timeout(peerA.establish(undefined, new TestSessionHandler(), () => {})),
    ).rejects.toThrow("establish requires Negotiated state");
  });

  it("capabilities preserved after full flow", async () => {
    const [transportA, transportB] = duplexPair();
    const capsAWithMore: Capabilities = {
      commands: [
        { name: "new_activity", version: "1.0" },
        { name: "cancel_activity" },
      ],
      produceEvents: [
        { name: "content_delta" },
      ],
    };
    const capsBWithMore: Capabilities = {
      commands: [],
      produceEvents: [
        { name: "action_request", version: "2.0" },
        { name: "question" },
      ],
    };
    const peerA = new XacppPeer(capsAWithMore, transportA, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    const peerB = new XacppPeer(capsBWithMore, transportB, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    await peerA.connect();
    await peerB.connect();
    await peerA.negotiate();

    // A's remote caps should be B's caps
    expect(peerA.remoteCapabilities.commands).toEqual([]);
    expect(peerA.remoteCapabilities.produceEvents!.length).toBe(2);
    expect(peerA.remoteCapabilities.produceEvents![0]).toEqual({ name: "action_request", version: "2.0" });
    expect(peerA.remoteCapabilities.produceEvents![1]).toEqual({ name: "question" });

    // After disconnect, remote caps cleared
    await peerA.disconnect();
    expect(peerA.remoteCapabilities.commands).toEqual([]);
    expect(peerA.remoteCapabilities.produceEvents).toEqual([]);
  });
});

// ---- EffectiveCapabilities compute tests ----

describe("EffectiveCapabilities", () => {
  it("emit events intersection is computed correctly", async () => {
    // capsA: produceEvents = [{name: "content_delta"}, {name: "think"}], acceptEvents = []
    // capsB: produceEvents = [], acceptEvents = [{name: "content_delta"}, {name: "info"}]
    // Expected: emitEvents = ["content_delta"] (intersection of A.produceEvents and B.acceptEvents)
    const capsA: Capabilities = {
      produceEvents: [{ name: "content_delta" }, { name: "think" }],
      acceptEvents: [],
    };
    const capsB: Capabilities = {
      produceEvents: [],
      acceptEvents: [{ name: "content_delta" }, { name: "info" }],
    };

    // Create handler that captures effective
    let capturedEffective: EffectiveCapabilities | null = null;
    const captureHandler: NegotiateHandler = {
      async onNegotiate(effective: EffectiveCapabilities): Promise<void> {
        capturedEffective = effective;
      },
    };

    const [transportA, transportB] = duplexPair();
    const peerA = new XacppPeer(capsA, transportA, captureHandler, new AutoApproveEstablishHandler());
    const peerB = new XacppPeer(capsB, transportB, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    await peerA.connect();
    await peerB.connect();
    await peerA.negotiate();

    // Assert emitEvents = intersection of A.produceEvents and B.acceptEvents = ["content_delta"]
    expect(capturedEffective).not.toBeNull();
    expect(capturedEffective!.emitEvents).toEqual(["content_delta"]);
  });

  it("request event not in emitEvents returns error", async () => {
    // capsA: produceEvents = [{name: "content_delta"}], acceptEvents = []
    // capsB: produceEvents = [], acceptEvents = [{name: "content_delta"}, {name: "info"}]
    // Expected: emitEvents = ["content_delta"], sending "think" should error
    const capsA: Capabilities = {
      produceEvents: [{ name: "content_delta" }],
      acceptEvents: [],
    };
    const capsB: Capabilities = {
      produceEvents: [],
      acceptEvents: [{ name: "content_delta" }, { name: "info" }],
    };

    const [transportA, transportB] = duplexPair();
    const peerA = new XacppPeer(capsA, transportA, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    const peerB = new XacppPeer(capsB, transportB, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    await peerA.connect();
    await peerB.connect();
    await peerA.negotiate();

    // Establish session
    await timeout(peerA.establish(undefined, new TestSessionHandler(), () => {}));

    // Try to emit "think" event which is NOT in emitEvents (only "content_delta")
    // peer.requestEvent validates against emitEvents; session.requestEvent bypasses peer
    await expect(
      timeout(peerA.requestEvent(null, newActivityEvent("test-act", newEvent("think", { content: "hi" })))),
    ).rejects.toThrow();
  });

  it("remoteCommands full schema is preserved", async () => {
    // capsA: commands = []
    // capsB: commands = [{ name: "get_weather", description: "...", parameters: {...} }]
    const capsA: Capabilities = {
      commands: [],
      produceEvents: [],
      acceptEvents: [],
    };
    const capsB: Capabilities = {
      commands: [
        {
          name: "get_weather",
          description: "Get weather for a location",
          parameters: {
            type: "object",
            properties: {
              location: { type: "string", description: "City name" },
            },
            required: ["location"],
          },
        },
      ],
      produceEvents: [],
      acceptEvents: [],
    };

    let capturedEffective: EffectiveCapabilities | null = null;
    const captureHandler: NegotiateHandler = {
      async onNegotiate(effective: EffectiveCapabilities): Promise<void> {
        capturedEffective = effective;
      },
    };

    const [transportA, transportB] = duplexPair();
    const peerA = new XacppPeer(capsA, transportA, captureHandler, new AutoApproveEstablishHandler());
    const peerB = new XacppPeer(capsB, transportB, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    await peerA.connect();
    await peerB.connect();
    await peerA.negotiate();

    // Assert remoteCommands contains the full schema with description and parameters
    expect(capturedEffective).not.toBeNull();
    expect(capturedEffective!.remoteCommands.length).toBe(1);
    expect(capturedEffective!.remoteCommands[0]).toEqual({
      name: "get_weather",
      description: "Get weather for a location",
      parameters: {
        type: "object",
        properties: {
          location: { type: "string", description: "City name" },
        },
        required: ["location"],
      },
    });
  });
});

// ---- Disconnect scenario tests ----

describe("Disconnect", () => {
  it("send after disconnect returns error", async () => {
    const [transportA, transportB] = duplexPair();

    transportB.onRequest(() => Promise.resolve(acknowledge()));

    await transportA.connect();
    await transportB.connect();

    // Normal communication
    const response = await timeout(
      transportA.send(null, {
        kind: "command",
        payload: { establish: { credentials: undefined } },
      }),
    );
    expect(response.kind).toBe("generic");

    // Disconnect B
    await transportB.disconnect();

    // A sending again should receive an error
    await expect(
      timeout(
        transportA.send(null, {
          kind: "command",
          payload: { establish: { credentials: undefined } },
        }),
      ),
    ).rejects.toThrow();
  });

  it("onRequest after connect throws", async () => {
    const [transportA] = duplexPair();
    await transportA.connect();

    // Registering handler after connect should throw
    expect(() =>
      transportA.onRequest(async () => acknowledge()),
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
      // Return different name based on command name, for verifying correct matching
      let name = "other";
      if (payload.kind === "command") {
        const cmd = payload.payload;
        if (typeof cmd === "object" && "generic" in cmd) name = cmd.generic.name;
        else if (typeof cmd === "object" && "establish" in cmd) name = "establish";
        else if (typeof cmd === "object" && "negotiate" in cmd) name = "negotiate";
        else if (cmd === "establish_confirm") name = "establish_confirm";
      } else {
        name = "event";
      }
      return Promise.resolve(genericResponse(name, null));
    });

    await transportA.connect();
    await transportB.connect();

    const commands: XacppCommand[] = [
      { establish: { credentials: undefined } },
      genericCommand("new_activity", { title: null }),
      genericCommand("invoke_activity", { activity: "act-1", messages: [] }),
      genericCommand("compact_activity", { activity: "act-1" }),
      genericCommand("cancel_activity", { activity: "act-1" }),
      genericCommand("last_activity", null),
      genericCommand("list_activity", { pageNum: 1, pageSize: 10 }),
      genericCommand("switch_activity", { activity: "act-1" }),
    ];

    // Send 8 concurrent requests
    const promises = commands.map((cmd) =>
      timeout(transportA.send(null, { kind: "command", payload: cmd })),
    );
    const responses = await Promise.all(promises);

    const names = responses.map((r) => {
      expect(r.kind).toBe("generic");
      if (r.kind === "generic") return r.name;
      return "";
    });

    // All 8 distinct names received, no duplicates or losses
    const sorted = [...names].sort();
    expect(sorted).toEqual([
      "cancel_activity",
      "compact_activity",
      "establish",
      "invoke_activity",
      "last_activity",
      "list_activity",
      "new_activity",
      "switch_activity",
    ]);
  });
});

// ---- Multi-session routing isolation tests ----

describe("Multi session routing", () => {
  it("multi session routing isolation", async () => {
    // peerB uses SequencedEstablishHandler, each session gets an identified handler
    const [transportA, transportB] = duplexPair();
    const emptyCaps: Capabilities = { commands: [], produceEvents: [], acceptEvents: [] };
    const peerA = new XacppPeer(emptyCaps, transportA, new AcceptAllNegotiateHandler(), new SequencedEstablishHandler());
    const peerB = new XacppPeer(emptyCaps, transportB, new AcceptAllNegotiateHandler(), new SequencedEstablishHandler());
    await peerA.connect();
    await peerB.connect();
    await peerA.negotiate();
    await peerB.negotiate();

    // A as initiator: establish two sessions
    const handlerA = new TestSessionHandler();
    const session1 = await timeout(peerA.establish(undefined, handlerA, () => {}));
    const session2 = await timeout(peerA.establish(undefined, handlerA, () => {}));

    const sid1 = session1.sessionId;
    const sid2 = session2.sessionId;
    expect(sid1).not.toBe(sid2);

    // session_1 sends command → B-side routes to handler-1 → response name = "identified", data.sessionId = "handler-1"
    const resp1 = await timeout(session1.requestCommand(genericCommand("new_activity", { title: null })));
    expect(resp1.kind).toBe("generic");
    if (resp1.kind === "generic") {
      expect(resp1.name).toBe("identified");
      expect((resp1.data as { sessionId: string }).sessionId).toBe("handler-1");
    }

    // session_2 sends command → B-side routes to handler-2 → response name = "identified", data.sessionId = "handler-2"
    const resp2 = await timeout(session2.requestCommand(genericCommand("new_activity", { title: null })));
    expect(resp2.kind).toBe("generic");
    if (resp2.kind === "generic") {
      expect(resp2.name).toBe("identified");
      expect((resp2.data as { sessionId: string }).sessionId).toBe("handler-2");
    }

    // Cross-validation: session_1 sends again, still routes to handler-1
    const resp1Again = await timeout(session1.requestCommand(genericCommand("cancel_activity", { activity: "act-1" })));
    expect(resp1Again.kind).toBe("generic");
    if (resp1Again.kind === "generic") {
      expect(resp1Again.name).toBe("identified");
      expect((resp1Again.data as { sessionId: string }).sessionId).toBe("handler-1");
    }
  });
});

// ---- Challenge handshake tests ----

describe("Challenge handshake", () => {
  const emptyCaps: Capabilities = { commands: [], produceEvents: [], acceptEvents: [] };

  it("establish challenge flow", async () => {
    const [transportA, transportB] = duplexPair();
    const peerA = new XacppPeer(emptyCaps, transportA, new AcceptAllNegotiateHandler(), new ChallengeEstablishHandler());
    const peerB = new XacppPeer(emptyCaps, transportB, new AcceptAllNegotiateHandler(), new ChallengeEstablishHandler());
    await peerA.connect();
    await peerB.connect();
    await peerA.negotiate();

    let challengeReceived = false;
    const session = await timeout(
      peerA.establish(undefined, new TestSessionHandler(), (challenge) => {
        expect(challenge).toBe("test-challenge");
        challengeReceived = true;
      }),
    );

    expect(challengeReceived).toBe(true);
    expect(session.sessionId).toBe("challenge-sid");
  });

  it("establish challenge issues credentials", async () => {
    const [transportA, transportB] = duplexPair();
    const peerA = new XacppPeer(emptyCaps, transportA, new AcceptAllNegotiateHandler(), new ChallengeEstablishHandler());
    const peerB = new XacppPeer(emptyCaps, transportB, new AcceptAllNegotiateHandler(), new ChallengeEstablishHandler());
    await peerA.connect();
    await peerB.connect();
    await peerA.negotiate();

    const session = await timeout(
      peerA.establish(undefined, new TestSessionHandler(), (challenge) => {
        expect(challenge).toBe("test-challenge");
      }),
    );

    expect(session.sessionId).toBe("challenge-sid");
    expect(session.credentials).toBe("issued-creds");
  });

  it("session send message", async () => {
    const [peerA] = await negotiatedPeers();
    const session = await timeout(
      peerA.establish(undefined, new TestSessionHandler(), () => {}),
    );
    const response = await timeout(
      session.requestCommand(genericCommand("message", { content: [{ type: "text", text: "hello" }] })),
    );
    expect(response.kind).toBe("generic");
    if (response.kind === "generic") {
      expect(response.name).toBe("acknowledge");
    }
  });
});

// ---- Interaction command lifecycle tests ----

describe("Interaction commands", () => {
  it("action request → approve response", async () => {
    const [peerA] = await interactionPeers();
    const session = await timeout(
      peerA.establish(undefined, new TestSessionHandler(), () => {}),
    );

    const response = await timeout(
      session.requestCommand(
        genericCommand("action_request", {
          activity: "act-1",
          requestId: "req-1",
          toolName: "bash",
          arguments: '{"command":"ls"}',
          actionId: "act-1",
          description: "list files",
          alert: "warn",
          intent: "list files",
        } satisfies ActionRequestPayload),
      ),
    );

    expect(response.kind).toBe("generic");
    if (response.kind === "generic") {
      expect(response.name).toBe("action");
      const data = response.data as ActionResponse;
      expect(data.type).toBe("approve");
    }
  });

  it("action request → reject response", async () => {
    // Custom handler that rejects
    const [a, b] = duplexPair();
    const caps: Capabilities = { commands: [], produceEvents: [], acceptEvents: [] };

    class RejectHandler implements XacppSessionHandler {
      async onCommand(command: XacppCommand): Promise<XacppResponse> {
        if (typeof command === "object" && "generic" in command && command.generic.name === "action_request") {
          const args = command.generic.arguments as ActionRequestPayload;
          return genericResponse("action", { requestId: args.requestId, type: "reject", reason: "forbidden" });
        }
        return acknowledge();
      }
      async onEvent(): Promise<XacppResponse> {
        return acknowledge();
      }
    }

    class RejectEstablishHandler implements EstablishHandler {
      async onEstablish(_t: XacppTransport, _c: string | undefined): Promise<EstablishDecision> {
        return { type: "established", sessionId: "rej-sid", credentials: "rej-creds", handler: new RejectHandler() };
      }
      async onEstablishConfirm(_t: XacppTransport): Promise<{ sessionId: string; handler: XacppSessionHandler; credentials: string }> {
        return { sessionId: "rej-sid", handler: new RejectHandler(), credentials: "rej-creds" };
      }
    }

    const peerA = new XacppPeer(caps, a, new AcceptAllNegotiateHandler(), new AutoApproveEstablishHandler());
    const peerB = new XacppPeer(caps, b, new AcceptAllNegotiateHandler(), new RejectEstablishHandler());
    await peerA.connect();
    await peerB.connect();
    await peerA.negotiate();

    const session = await timeout(peerA.establish(undefined, new TestSessionHandler(), () => {}));

    const response = await timeout(
      session.requestCommand(
        genericCommand("action_request", {
          activity: "act-1",
          requestId: "req-2",
          toolName: "rm",
          arguments: '{"command":"rm -rf /"}',
          actionId: "act-2",
          description: "dangerous",
          alert: "critical",
          intent: "delete everything",
        } satisfies ActionRequestPayload),
      ),
    );

    expect(response.kind).toBe("generic");
    if (response.kind === "generic") {
      expect(response.name).toBe("action");
      const data = response.data as ActionResponse;
      expect(data.type).toBe("reject");
    }
  });

  it("question → answer response", async () => {
    const [peerA] = await interactionPeers();
    const session = await timeout(
      peerA.establish(undefined, new TestSessionHandler(), () => {}),
    );

    const response = await timeout(
      session.requestCommand(
        genericCommand("question", {
          activity: "act-1",
          requestId: "req-q1",
          question: "continue?",
          options: ["yes", "no"],
        } satisfies QuestionPayload),
      ),
    );

    expect(response.kind).toBe("generic");
    if (response.kind === "generic") {
      expect(response.name).toBe("question");
      const data = response.data as QuestionResponse;
      expect(data.type).toBe("answer");
      if (data.type === "answer") {
        expect(data.content).toBe("yes");
      }
    }
  });

  it("sensitive info collect → provided results", async () => {
    const [peerA] = await interactionPeers();
    const session = await timeout(
      peerA.establish(undefined, new TestSessionHandler(), () => {}),
    );

    const response = await timeout(
      session.requestCommand(
        genericCommand("sensitive_info_operation", {
          activity: "act-1",
          requestId: "req-si1",
          operation: {
            type: "collect",
            items: [
              { key: "API_KEY", displayText: "API Key", hint: "enter key", siType: "secret" },
              { key: "DB_PASSWORD", displayText: "DB Password", hint: "enter password", siType: "secret" },
            ],
          },
        } satisfies SensitiveInfoOperationPayload),
      ),
    );

    expect(response.kind).toBe("generic");
    if (response.kind === "generic") {
      expect(response.name).toBe("sensitive_info_operation");
      const data = response.data as { results: SensitiveInfoResult[] };
      expect(data.results).toHaveLength(2);
      expect(data.results[0].type).toBe("provided");
      expect(data.results[0].type === "provided" ? data.results[0].key : "").toBe("API_KEY");
    }
  });
});
