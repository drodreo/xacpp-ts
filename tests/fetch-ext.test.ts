/**
 * fetchExt negotiation tests (SPEC §8.7) — architecture counterexamples.
 *
 * Harness mirrors tests/fetch.test.ts (full Peer + Session stacks over real
 * localhost TCP). The counterexamples pin the contract's architecture:
 * declaration is application-owned protocol support (never derived from
 * responder assembly); enablement is the negotiated fact (both sides
 * declared the supported version); the responder handler alone gates
 * nothing.
 *
 * Counterexample matrix:
 *  1. absence on either side ⇒ outbound fetch loud-fails (never sent);
 *     other commands and establish continue
 *  2. declared version 2 (either both or mixed with 1) ⇒ negotiate still
 *     succeeds, fetch stays disabled
 *  3. requester-only initiator (no local responder) with both sides
 *     declaring 1 ⇒ enabled and a REAL fetch round-trip succeeds
 *  4. declaration and outbound enablement do not follow local responder
 *     assembly
 *  5. responder handler present but negotiation not enabled ⇒ the wire
 *     routing answers fetch_unavailable (raw transport probe bypassing the
 *     requester-side gate)
 *  6. invoke/steer/push_task share ONE payload shape; mixed text/image/file
 *     parts arrive in order, one command per action
 *  7. disconnect clears the negotiated fact
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import * as net from "node:net";
import { createHash } from "node:crypto";

import { SocketTransport } from "../src/carrier/socket";
import { XacppPeer } from "../src/session/peer";
import { XacppSession } from "../src/session/session";
import { acknowledge, XacppError } from "../src/wire/message";
import type { XacppResponse } from "../src/wire/message";
import { genericCommand } from "../src/wire/commands";
import type { XacppCommand, InvokeActivityPayload } from "../src/wire/commands";
import { computeEffectiveCapabilities, FETCH_EXT_VERSION } from "../src/wire/capability";
import type { Capabilities } from "../src/wire/capability";
import { FETCH_UNAVAILABLE, fetchNotFound } from "../src/wire/fetch";
import { newTransactionId } from "../src/wire/frame";
import type { EstablishHandler, NegotiateHandler, XacppSessionHandler } from "../src/session/handler";
import type { XacppTransport } from "../src/carrier/transport";
import type { TransferConfig, TransferWiring } from "../src/transfer";
import type { ContentPart } from "../src/wire/events/content";

// ---- Shared helpers ----

function filler(length: number, seed = 0x2545): Uint8Array {
  const out = new Uint8Array(length);
  let s = seed >>> 0;
  for (let i = 0; i < length; i++) {
    s = (s * 1664525 + 1013904223) >>> 0;
    out[i] = s & 0xff;
  }
  return out;
}

function sha256Hex(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/** Yield to the real event loop a few turns (socket IO callbacks are real macrotasks). */
async function settle(turns = 12): Promise<void> {
  for (let i = 0; i < turns; i++) await new Promise((r) => setImmediate(r));
}

/** Poll a condition with real time until it holds or the timeout elapses. */
async function waitFor(cond: () => boolean, what: string, timeoutMs = 10_000): Promise<void> {
  const start = performance.now();
  while (!cond()) {
    if (performance.now() - start > timeoutMs) throw new Error(`timeout waiting for: ${what}`);
    await settle(4);
  }
}

const acceptNegotiate: NegotiateHandler = { onNegotiate: async () => {} };

const unusedEstablish: EstablishHandler = {
  onEstablish: async () => {
    throw new Error("client side never accepts establish");
  },
  onEstablishConfirm: async () => {
    throw new Error("unused");
  },
};

// ---- Socket harness: full Peer + Session stacks over real localhost TCP ----

interface ServerOptions {
  /** The declaring side's capabilities (the server's own declaration). */
  caps: Capabilities;
  /** Load a hand-rolled fetch responder as the session handler (default: plain ack handler). */
  responder?: boolean;
  /** uri → resource served by the responder handler. */
  resources?: Map<string, { bytes: Uint8Array; mime: string }>;
  transferConfig?: TransferConfig;
}

interface Server {
  port(): number;
  consumerCommands(): XacppCommand[];
  negotiatedRemoteFetchExt(): number | undefined;
  stop(): Promise<void>;
}

/**
 * Responder side: one listener, one XacppSession reused across connections.
 * The session handler either declares a fetch responder (hand-rolled
 * serving, SPEC §8) or is a plain ack handler.
 */
function startServer(opts: ServerOptions): Server {
  const listener = net.createServer();
  let port = 0;
  let session: XacppSession | null = null;
  const record = { commands: [] as XacppCommand[] };
  let remoteFetchExt: number | undefined = undefined;
  const transports: SocketTransport[] = [];

  const establish: EstablishHandler = {
    onEstablish: async (transport) => {
      if (session === null) {
        session = new XacppSession(opts.transferConfig);
        session.bindIdentity("srv-sid", "srv-creds");
      }
      if (!session.wired) {
        session.wire(transport as XacppTransport & TransferWiring);
      }
      const handler: XacppSessionHandler = opts.responder
        ? {
            // Hand-rolled responder: declares itself so the peer's
            // fetch gate lets the fetch family through when (and only
            // when) the negotiation enabled fetch (SPEC §8.2/§8.7).
            isFetchResponder: true,
            onCommand: async (command) => {
              record.commands.push(command);
              if (typeof command === "object" && "fetch" in command) {
                return serveFrom(session!, command, opts.resources);
              }
              return acknowledge();
            },
            onEvent: async () => acknowledge(),
          }
        : {
            onCommand: async (command) => {
              record.commands.push(command);
              return acknowledge();
            },
            onEvent: async () => acknowledge(),
          };
      return { type: "established", sessionId: "srv-sid", credentials: "srv-creds", handler };
    },
    onEstablishConfirm: async () => {
      throw new Error("challenge path not used in these tests");
    },
  };

  const negotiate: NegotiateHandler = {
    onNegotiate: async (effective) => {
      remoteFetchExt = effective.remoteFetchExt;
    },
  };

  listener.on("connection", (socket) => {
    const transport = new SocketTransport(socket);
    transports.push(transport);
    const peer = new XacppPeer(opts.caps, transport, negotiate, establish);
    void peer.connect().catch(() => {});
  });
  listener.listen(0, "127.0.0.1", () => {
    port = (listener.address() as net.AddressInfo).port;
  });
  return {
    port: () => port,
    consumerCommands: () => record.commands.slice(),
    negotiatedRemoteFetchExt: () => remoteFetchExt,
    stop: async () => {
      await Promise.all(transports.map((t) => t.disconnect().catch(() => {})));
      await new Promise<void>((resolve) => listener.close(() => resolve()));
    },
  };
}

/** Consumer-side serving: answer fetch_ready, then stream the bytes with the SAME txid (SPEC §8). */
async function serveFrom(
  session: XacppSession,
  command: Extract<XacppCommand, { fetch: { uri: string } }>,
  resources?: Map<string, { bytes: Uint8Array; mime: string }>,
): Promise<XacppResponse> {
  const resource = resources?.get(command.fetch.uri);
  if (!resource) {
    throw fetchNotFound(`no such resource: ${command.fetch.uri}`);
  }
  const txid = newTransactionId();
  const sink = session.openBinarySink({ totalLen: resource.bytes.length }, txid);
  const ready = {
    kind: "fetch_ready" as const,
    txid,
    sha256: sha256Hex(resource.bytes),
    sizeBytes: resource.bytes.length,
    mime: resource.mime,
  };
  // SPEC §8.4: the responder MUST complete the fetch_ready response before
  // starting the binary transaction — defer the frames one macrotask.
  setImmediate(() => {
    void sink
      .write(resource.bytes)
      .then(() => sink.finish())
      .catch(() => {});
  });
  return ready;
}

interface Client {
  transport: SocketTransport;
  session: XacppSession;
  peer: XacppPeer;
}

/** Full client stack: connect → negotiate → establish (ack session handler). */
async function connectClient(
  port: number,
  caps: Capabilities,
  opts?: { transferConfig?: TransferConfig },
): Promise<Client> {
  const transport = SocketTransport.connectTo(port, "127.0.0.1");
  const session = new XacppSession(opts?.transferConfig);
  const peer = new XacppPeer(caps, transport, acceptNegotiate, unusedEstablish, session);
  await peer.connect();
  await peer.negotiate();
  await peer.establish(
    undefined,
    {
      onCommand: async () => acknowledge(),
      onEvent: async () => acknowledge(),
    },
    () => {
      throw new Error("challenge path not used in these tests");
    },
  );
  return { transport, session, peer };
}

/** Expect a rejection carrying the given registered wire error code. */
async function expectRejectWith(promise: Promise<unknown>, code: string): Promise<XacppError> {
  try {
    await promise;
  } catch (e) {
    expect(e).toBeInstanceOf(XacppError);
    expect((e as XacppError).code).toBe(code);
    return e as XacppError;
  }
  throw new Error(`expected rejection with code ${code}, but the promise resolved`);
}

// ---- Unit level: computeEffectiveCapabilities truth table ----

describe("computeEffectiveCapabilities — fetchExt (SPEC §8.7)", () => {
  it("both sides declare version 1 ⇒ enabled", () => {
    const eff = computeEffectiveCapabilities({ fetchExt: 1 }, { fetchExt: 1 });
    expect(eff.fetchEnabled).toBe(true);
    expect(eff.remoteFetchExt).toBe(1);
  });

  it("either side absent ⇒ disabled; remoteFetchExt records the wire fact", () => {
    const localAbsent = computeEffectiveCapabilities({}, { fetchExt: 1 });
    expect(localAbsent.fetchEnabled).toBe(false);
    expect(localAbsent.remoteFetchExt).toBe(1);

    const remoteAbsent = computeEffectiveCapabilities({ fetchExt: 1 }, {});
    expect(remoteAbsent.fetchEnabled).toBe(false);
    expect(remoteAbsent.remoteFetchExt).toBeUndefined();
  });

  it("declared version outside the supported range ⇒ disabled, not an error", () => {
    const both2 = computeEffectiveCapabilities({ fetchExt: 2 }, { fetchExt: 2 });
    expect(both2.fetchEnabled).toBe(false);
    expect(both2.remoteFetchExt).toBe(2);

    const mixed12 = computeEffectiveCapabilities({ fetchExt: 1 }, { fetchExt: 2 });
    expect(mixed12.fetchEnabled).toBe(false);
    const mixed21 = computeEffectiveCapabilities({ fetchExt: 2 }, { fetchExt: 1 });
    expect(mixed21.fetchEnabled).toBe(false);
  });

  it("non-fetch capability fields are unaffected by fetchExt", () => {
    const local: Capabilities = { fetchExt: 1, produceEvents: [{ name: "info" }] };
    const remote: Capabilities = { fetchExt: 1, acceptEvents: [{ name: "info" }], commands: [{ name: "ping" }] };
    const eff = computeEffectiveCapabilities(local, remote);
    expect(eff.emitEvents).toEqual(["info"]);
    expect(eff.remoteCommands).toEqual([{ name: "ping" }]);
  });

  it("FETCH_EXT_VERSION is the currently-defined version", () => {
    expect(FETCH_EXT_VERSION).toBe(1);
  });
});

// ---- End to end: negotiated gate over real TCP ----

describe("negotiated fetch gate — end to end (SPEC §8.7)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("1a. remote absent ⇒ outbound fetch loud-fails before leaving; other commands and establish continue", async () => {
    const server = startServer({ caps: {} });
    try {
      await waitFor(() => server.port() !== 0, "server listen");
      const client = await connectClient(server.port(), { fetchExt: 1 });

      // establish succeeded (connectClient) — the negotiation enabled
      // nothing, but the session exists and non-fetch commands flow.
      const ack = await client.session.requestCommand(genericCommand("ping", null));
      expect(ack).toEqual(acknowledge());

      // fetch loud-fails with the registered code; the request never
      // reaches the wire (the consumer handler sees only the ping).
      await expectRejectWith(client.session.fetch("res:anything"), FETCH_UNAVAILABLE);
      expect(server.consumerCommands()).toEqual([genericCommand("ping", null)]);

      await client.transport.disconnect();
    } finally {
      await server.stop();
    }
  }, 20_000);

  it("1b. both sides absent ⇒ same loud failure", async () => {
    const server = startServer({ caps: {} });
    try {
      await waitFor(() => server.port() !== 0, "server listen");
      const client = await connectClient(server.port(), {});
      await expectRejectWith(client.session.fetch("res:anything"), FETCH_UNAVAILABLE);
      expect(server.consumerCommands()).toEqual([]);
      await client.transport.disconnect();
    } finally {
      await server.stop();
    }
  }, 20_000);

  it("1c. a loaded responder handler does not sneak enablement past the negotiation", async () => {
    // Server declares nothing but HAS a responder; client declares 1.
    // The outbound gate must still refuse: assembly is not declaration.
    const server = startServer({ caps: {}, responder: true });
    try {
      await waitFor(() => server.port() !== 0, "server listen");
      const client = await connectClient(server.port(), { fetchExt: 1 });
      await expectRejectWith(client.session.fetch("fixture.bin"), FETCH_UNAVAILABLE);
      expect(server.consumerCommands()).toEqual([]);
      await client.transport.disconnect();
    } finally {
      await server.stop();
    }
  }, 20_000);

  it("2a. both sides declare version 2 ⇒ negotiate and establish still succeed, fetch stays disabled", async () => {
    const server = startServer({ caps: { fetchExt: 2 }, responder: true });
    try {
      await waitFor(() => server.port() !== 0, "server listen");
      // connectClient completing IS the counterexample: an unsupported
      // version does not fail the negotiation (it only disables fetch).
      const client = await connectClient(server.port(), { fetchExt: 2 });
      expect(server.negotiatedRemoteFetchExt()).toBe(2);
      const ack = await client.session.requestCommand(genericCommand("ping", null));
      expect(ack).toEqual(acknowledge());
      await expectRejectWith(client.session.fetch("fixture.bin"), FETCH_UNAVAILABLE);
      await client.transport.disconnect();
    } finally {
      await server.stop();
    }
  }, 20_000);

  it("2b. mixed versions (local 1, remote 2) ⇒ negotiate succeeds, fetch disabled", async () => {
    const server = startServer({ caps: { fetchExt: 2 }, responder: true });
    try {
      await waitFor(() => server.port() !== 0, "server listen");
      const client = await connectClient(server.port(), { fetchExt: 1 });
      const ack = await client.session.requestCommand(genericCommand("ping", null));
      expect(ack).toEqual(acknowledge());
      await expectRejectWith(client.session.fetch("fixture.bin"), FETCH_UNAVAILABLE);
      await client.transport.disconnect();
    } finally {
      await server.stop();
    }
  }, 20_000);

  it("3. requester-only initiator (no local responder), both declaring 1 ⇒ REAL fetch round-trip succeeds", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(4096);
    const resources = new Map([["fixture.bin", { bytes: payload, mime: "application/x-test" }]]);
    const server = startServer({ caps: { fetchExt: 1 }, responder: true, resources });
    try {
      await waitFor(() => server.port() !== 0, "server listen");
      // The client assembles NOTHING fetch-related: no FetchService, no
      // responder handler (connectClient installs a plain ack handler).
      // Enablement comes from the declarations alone.
      const client = await connectClient(server.port(), { fetchExt: 1 });

      const claims = new Map<string, { chunks: Uint8Array[]; done: boolean }>();
      client.session.onBinaryStream((stream) => {
        const claim = { chunks: [] as Uint8Array[], done: false };
        claims.set(stream.transactionId, claim);
        stream.onChunk((chunk) => claim.chunks.push(chunk));
        stream.onEnd(() => {
          claim.done = true;
        });
      });

      const ready = await client.session.fetch("fixture.bin");
      expect(ready.sha256).toBe(sha256Hex(payload));
      expect(ready.sizeBytes).toBe(payload.length);

      await waitFor(() => claims.get(ready.txid)?.done === true, "stream completes");
      const claim = claims.get(ready.txid)!;
      const got = Buffer.concat(claim.chunks.map((c) => Buffer.from(c)));
      expect(got).toEqual(Buffer.from(payload));

      await client.transport.disconnect();
    } finally {
      await server.stop();
    }
  }, 20_000);

  it("4. outbound enablement does not follow local responder assembly", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const resources = () => new Map([["fixture.bin", { bytes: filler(1024, 0x77), mime: "application/x-test" }]]);
    // Two identical servers; the two clients differ ONLY in local
    // responder assembly (none for either — A via the default ack
    // handler). The outbound gate must treat them identically.
    const serverA = startServer({ caps: { fetchExt: 1 }, responder: true, resources: resources() });
    const serverB = startServer({ caps: { fetchExt: 1 }, responder: true, resources: resources() });
    try {
      await waitFor(() => serverA.port() !== 0 && serverB.port() !== 0, "servers listen");
      const clientA = await connectClient(serverA.port(), { fetchExt: 1 });
      const readyA = await clientA.session.fetch("fixture.bin");
      expect(readyA.sizeBytes).toBe(1024);
      await clientA.transport.disconnect();

      const clientB = await connectClient(serverB.port(), { fetchExt: 1 });
      const readyB = await clientB.session.fetch("fixture.bin");
      expect(readyB.sizeBytes).toBe(1024);
      await clientB.transport.disconnect();
    } finally {
      await serverA.stop();
      await serverB.stop();
    }
  }, 20_000);

  it("5. responder handler present but negotiation not enabled ⇒ the wire routing answers fetch_unavailable", async () => {
    // Server HAS a responder handler but declares no fetchExt ⇒ the
    // responder-side negotiated fact is false. The probe sends a raw
    // fetch command over the transport (bypassing the requester-side
    // session gate) to reach the peer's INBOUND routing — which must
    // answer fetch_unavailable on the missing negotiation condition
    // alone, even though the handler declares a responder.
    const payload = filler(512, 0x11);
    const resources = new Map([["fixture.bin", { bytes: payload, mime: "application/x-test" }]]);
    const server = startServer({ caps: {}, responder: true, resources });
    try {
      await waitFor(() => server.port() !== 0, "server listen");
      const client = await connectClient(server.port(), { fetchExt: 1 });

      // Raw wire probe: same envelope the session layer would send, sent
      // from the transport directly.
      const rawFetch = await client.transport.send(client.session.sessionId, {
        kind: "command",
        payload: { fetch: { uri: "fixture.bin" } },
      });
      expect(rawFetch.kind).toBe("error");
      if (rawFetch.kind === "error") expect(rawFetch.code).toBe(FETCH_UNAVAILABLE);
      // The handler never saw the command (the peer routing cut it).
      expect(server.consumerCommands()).toEqual([]);

      // Non-fetch commands on the same raw path reach the handler.
      const rawPing = await client.transport.send(client.session.sessionId, {
        kind: "command",
        payload: genericCommand("ping", null),
      });
      expect(rawPing).toEqual(acknowledge());
      expect(server.consumerCommands()).toEqual([genericCommand("ping", null)]);

      await client.transport.disconnect();
    } finally {
      await server.stop();
    }
  }, 20_000);

  it("5b. negotiation enabled + responder present ⇒ the same raw path reaches the handler", async () => {
    const server = startServer({ caps: { fetchExt: 1 }, responder: true });
    try {
      await waitFor(() => server.port() !== 0, "server listen");
      const client = await connectClient(server.port(), { fetchExt: 1 });
      const rawFetch = await client.transport.send(client.session.sessionId, {
        kind: "command",
        payload: { fetch: { uri: "missing.bin" } },
      });
      // The command REACHED the responder (its not-found verdict is the
      // answer — proving the routing let it through).
      expect(rawFetch.kind).toBe("error");
      if (rawFetch.kind === "error") expect(rawFetch.code).toBe("fetch_not_found");
      expect(server.consumerCommands()).toEqual([{ fetch: { uri: "missing.bin" } }]);
      await client.transport.disconnect();
    } finally {
      await server.stop();
    }
  }, 20_000);

  it("6. invoke/steer/push_task share ONE payload shape; mixed text/image/file parts arrive in order", async () => {
    const server = startServer({ caps: { fetchExt: 1 } });
    try {
      await waitFor(() => server.port() !== 0, "server listen");
      const client = await connectClient(server.port(), { fetchExt: 1 });

      const fileRef = {
        remoteUrl: "https://example.test/a.png",
        localUri: "file:///tmp/a.png",
        mimeType: "image/png",
        sizeBytes: 3,
      };
      // One action = one command = one complete ordered part list. All
      // three commands carry the SAME type — the type checker enforces
      // the shared shape (no alias, no parallel payload type).
      const parts: ContentPart[] = [
        { type: "text", text: "看这张图和附件", partType: "default" },
        { type: "image", source: fileRef, partType: "default" },
        { type: "file", source: { ...fileRef, remoteUrl: "https://example.test/a.pdf", mimeType: "application/pdf" }, partType: "default" },
      ];
      const invokePayload: InvokeActivityPayload = { activity: "a-1", messages: parts };
      const steerPayload: InvokeActivityPayload = { activity: "a-1", messages: parts };
      const pushPayload: InvokeActivityPayload = { activity: "a-1", messages: parts };

      await client.session.requestCommand(genericCommand("invoke_activity", invokePayload));
      await client.session.requestCommand(genericCommand("steer_activity", steerPayload));
      await client.session.requestCommand(genericCommand("push_task", pushPayload));

      const seen = server.consumerCommands();
      expect(seen.map((c) => (typeof c === "object" && "generic" in c ? c.generic.name : "?"))).toEqual([
        "invoke_activity",
        "steer_activity",
        "push_task",
      ]);
      // Same shape, complete ordered parts, nothing split or reordered.
      for (const name of ["invoke_activity", "steer_activity", "push_task"]) {
        const cmd = seen.find((c) => typeof c === "object" && "generic" in c && c.generic.name === name) as
          | Extract<XacppCommand, { generic: { name: string; arguments: unknown } }>
          | undefined;
        expect(cmd).toBeDefined();
        const args = cmd!.generic.arguments as InvokeActivityPayload;
        expect(args.activity).toBe("a-1");
        expect(args.messages).toHaveLength(3);
        expect(args.messages[0]).toEqual({ type: "text", text: "看这张图和附件", partType: "default" });
        expect(args.messages[1]).toMatchObject({ type: "image", source: { remoteUrl: "https://example.test/a.png" } });
        expect(args.messages[2]).toMatchObject({ type: "file", source: { remoteUrl: "https://example.test/a.pdf" } });
      }

      await client.transport.disconnect();
    } finally {
      await server.stop();
    }
  }, 20_000);

  it("7. disconnect clears the negotiated fact — fetch loud-fails again", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(256, 0x33);
    const resources = new Map([["fixture.bin", { bytes: payload, mime: "application/x-test" }]]);
    const server = startServer({ caps: { fetchExt: 1 }, responder: true, resources });
    try {
      await waitFor(() => server.port() !== 0, "server listen");
      const client = await connectClient(server.port(), { fetchExt: 1 });
      // Enabled: the fetch goes out (fails on the missing resource at the
      // responder — proof it left the session).
      await expectRejectWith(client.session.fetch("missing.bin"), "fetch_not_found");

      await client.peer.disconnect();
      // Cleared: the outbound gate refuses again with the negotiated-gate
      // code — distinct from a not-connected error, proving the fact was
      // cleared rather than the transport merely gone.
      await expectRejectWith(client.session.fetch("fixture.bin"), FETCH_UNAVAILABLE);
    } finally {
      await server.stop();
    }
  }, 20_000);
});
