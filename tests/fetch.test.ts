/**
 * Fetch kernel tests — stateless fetch (SPEC §8).
 *
 * The kernel is stateless: `fetch(uri)` is a thin command round-trip that
 * returns metadata only; the bytes travel as an ordinary binary
 * transaction claimed through the binary receive path. Serving is the
 * consumer handler's business (the Peer no longer intercepts fetch
 * commands), so the e2e serving side answers fetch_ready itself and
 * streams the bytes with the SAME txid via `openBinarySink`.
 *
 * Matrix items (SPEC §8): wire samples (byte-exact,
 * mirrored with xacpp-rs), consumer-handler routing, three error codes,
 * thin-shell semantics, onBinary without interception. Interop (rs ↔ ts)
 * lives in interop.test.ts.
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import * as net from "node:net";
import { createHash } from "node:crypto";

import { SocketTransport } from "../src/carrier/socket";
import { XacppPeer } from "../src/session/peer";
import { XacppSession } from "../src/session/session";
import { newTransactionId } from "../src/wire/frame";
import { XacppError, acknowledge } from "../src/wire/message";
import type { XacppRequest, XacppResponse } from "../src/wire/message";
import type { XacppCommand } from "../src/wire/commands";
import type { Capabilities } from "../src/wire/capability";
import type { EstablishHandler, NegotiateHandler, XacppSessionHandler } from "../src/session/handler";
import type { XacppTransport } from "../src/carrier/transport";
import type { TransferActivityHandlers, TransferConfig, TransferWiring } from "../src/transfer";
import type { StreamEndReason } from "../src/transfer";
import {
  FETCH_NOT_FOUND,
  FETCH_REFUSED,
  fetchNotFound,
  fetchRefused,
} from "../src/wire/fetch";

// ---- Shared helpers ----

function filler(length: number, seed = 0x9e37): Uint8Array {
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

function expectBytesEqual(a: Uint8Array, b: Uint8Array): void {
  expect(a.length).toBe(b.length);
  expect(Buffer.from(a).equals(Buffer.from(b))).toBe(true);
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

async function expectXacppError(promise: Promise<unknown>, code: string): Promise<XacppError> {
  try {
    await promise;
  } catch (e) {
    expect(e).toBeInstanceOf(XacppError);
    expect((e as XacppError).code).toBe(code);
    return e as XacppError;
  }
  throw new Error(`expected rejection with code ${code}, but the promise resolved`);
}

const EMPTY_CAPS: Capabilities = {};

const acceptNegotiate: NegotiateHandler = { onNegotiate: async () => {} };

const unusedEstablish: EstablishHandler = {
  onEstablish: async () => {
    throw new Error("client side never accepts establish");
  },
  onEstablishConfirm: async () => {
    throw new Error("unused");
  },
};

/** Consumer session handler that records every command/event it sees. */
function recordingHandler(record: { commands: XacppCommand[] }): XacppSessionHandler {
  return {
    onCommand: async (command) => {
      record.commands.push(command);
      return acknowledge();
    },
    onEvent: async () => acknowledge(),
  };
}

// ---- Socket harness: full Peer + Session stacks on both ends ----

interface ServerOptions {
  transferConfig?: TransferConfig;
  /** Configure the responder session when it is created (e.g. stream claims). */
  configure?: (session: XacppSession) => void;
  /** uri → resource served by the consumer handler (SPEC §8: serving is consumer business). */
  resources?: Map<string, { bytes: Uint8Array; mime: string }>;
  /** Handler override for custom decline semantics. */
  onCommandOverride?: (command: XacppCommand) => Promise<XacppResponse>;
}

interface Server {
  port(): number;
  session(): XacppSession | null;
  connectionCount(): number;
  consumerCommands(): XacppCommand[];
  stop(): Promise<void>;
}

/**
 * Responder side: one listener, one XacppSession reused across connections.
 * The consumer session handler answers fetch commands itself: fetch_ready +
 * streaming the bytes with the SAME txid (SPEC §8.1/§8.2). The frames are
 * deferred one macrotask so the response frame precedes them on the wire
 * (SPEC §8.4 ordering).
 */
function startServer(opts: ServerOptions): Server {
  const listener = net.createServer();
  let port = 0;
  let session: XacppSession | null = null;
  let conns = 0;
  const record = { commands: [] as XacppCommand[] };
  const transports: SocketTransport[] = [];

  const establish: EstablishHandler = {
    onEstablish: async (transport) => {
      if (session === null) {
        session = new XacppSession(opts.transferConfig);
        session.bindIdentity("srv-sid", "srv-creds");
        opts.configure?.(session);
      }
      if (!session.wired) {
        session.wire(transport as XacppTransport & TransferWiring);
      }
      const handler: XacppSessionHandler = {
        // Hand-rolled responder: declares itself so the peer's
        // fetch_unavailable gate lets the fetch family through (SPEC §8.2).
        isFetchResponder: true,
        onCommand: async (command) => {
          record.commands.push(command);
          if (opts.onCommandOverride) return opts.onCommandOverride(command);
          if (typeof command === "object" && "fetch" in command) {
            return serveFrom(session!, command, opts.resources);
          }
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

  listener.on("connection", (socket) => {
    const transport = new SocketTransport(socket);
    transports.push(transport);
    const peer = new XacppPeer(EMPTY_CAPS, transport, acceptNegotiate, establish);
    void peer
      .connect()
      .then(() => {
        conns++;
      })
      .catch(() => {});
  });
  listener.listen(0, "127.0.0.1", () => {
    port = (listener.address() as net.AddressInfo).port;
  });
  return {
    port: () => port,
    session: () => session,
    connectionCount: () => conns,
    consumerCommands: () => record.commands.slice(),
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
  // Total length is declared upfront: the sink's frames carry it (wire §3),
  // and fetch_ready commits to the same size (SPEC §8.2: self-computed).
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
  consumerCommands(): XacppCommand[];
}

async function connectClient(
  port: number,
  opts?: {
    transferConfig?: TransferConfig;
    configure?: (session: XacppSession) => void;
    onCommandOverride?: (command: XacppCommand) => Promise<XacppResponse>;
  },
): Promise<Client> {
  const record = { commands: [] as XacppCommand[] };
  const transport = SocketTransport.connectTo(port, "127.0.0.1");
  const session = new XacppSession(opts?.transferConfig);
  opts?.configure?.(session);
  const peer = new XacppPeer(EMPTY_CAPS, transport, acceptNegotiate, unusedEstablish, session);
  await peer.connect();
  await peer.negotiate();
  const handler: XacppSessionHandler = opts?.onCommandOverride
    ? {
        // The override serves fetch (bidirectional tests): declare it.
        isFetchResponder: true,
        onCommand: async (command) => {
          record.commands.push(command);
          return opts.onCommandOverride!(command);
        },
        onEvent: async () => acknowledge(),
      }
    : recordingHandler(record);
  await peer.establish(undefined, handler, () => {
    throw new Error("challenge path not used in these tests");
  });
  return { transport, session, peer, consumerCommands: () => record.commands.slice() };
}

/** Single-slot stream claimant: routes every inbound stream by txid. */
interface Claim {
  chunks: Uint8Array[];
  ends: StreamEndReason[];
}
function claimStreams(session: XacppSession): Map<string, Claim> {
  const claims = new Map<string, Claim>();
  session.onBinaryStream((stream) => {
    let claim = claims.get(stream.transactionId);
    if (!claim) {
      claim = { chunks: [], ends: [] };
      claims.set(stream.transactionId, claim);
    }
    stream.onChunk((chunk) => claim!.chunks.push(chunk));
    stream.onEnd((reason) => claim!.ends.push(reason));
  });
  return claims;
}

async function awaitClaim(
  claims: Map<string, Claim>,
  txid: string,
  expectedBytes: number,
  what: string,
): Promise<Claim> {
  await waitFor(() => claims.get(txid)?.ends.includes("completed") === true, what);
  const claim = claims.get(txid)!;
  expect(claim.ends).toEqual(["completed"]);
  expect(claim.chunks.reduce((n, c) => n + c.length, 0)).toBe(expectedBytes);
  return claim;
}

// ---- Wire format (byte-exact, mirrored with xacpp-rs) ----

describe("fetch wire format (byte-exact, mirrored with xacpp-rs)", () => {
  it("serializes the fetch command with snake_case tag and the bare uri field", () => {
    const command: XacppCommand = { fetch: { uri: "file://a" } };
    expect(JSON.stringify(command)).toBe('{"fetch":{"uri":"file://a"}}');
  });

  it("serializes companion commands with camelCase fields (SPEC §8.3)", () => {
    const txid = "0123456789abcdef0123456789abcdef";
    expect(JSON.stringify({ fetch_begin: { txid } })).toBe(`{"fetch_begin":{"txid":"${txid}"}}`);
    expect(JSON.stringify({ fetch_abort: { txid } })).toBe(`{"fetch_abort":{"txid":"${txid}"}}`);
    expect(JSON.stringify({ fetch_pause: { txid } })).toBe(`{"fetch_pause":{"txid":"${txid}"}}`);
    expect(JSON.stringify({ fetch_resume: { txid } })).toBe(
      `{"fetch_resume":{"txid":"${txid}"}}`,
    );
  });

  it("serializes fetch_ready with kind tag, txid rename, and camelCase fields (SPEC §8.2)", () => {
    const response: XacppResponse = {
      kind: "fetch_ready",
      txid: "0123456789abcdef0123456789abcdef",
      sha256: "deadbeef",
      sizeBytes: 42,
      mime: "application/octet-stream",
    };
    expect(JSON.stringify(response)).toBe(
      '{"kind":"fetch_ready","txid":"0123456789abcdef0123456789abcdef","sha256":"deadbeef","sizeBytes":42,"mime":"application/octet-stream"}',
    );
  });
});

// ---- Basic e2e over real TCP ----

describe("fetch kernel — basic e2e", () => {
  it("1. thin fetch resolves metadata first; the consumer handler routes the command; bytes stream to the claimant", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(512);
    const resources = new Map([["res:small", { bytes: payload, mime: "application/x-test" }]]);
    const server = startServer({ resources });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const claims = claimStreams(client.session);

    const ready = await client.session.fetch("res:small");
    expect(ready.txid).toMatch(/^[0-9a-f]{32}$/);
    expect(ready.sha256).toBe(sha256Hex(payload));
    expect(ready.sizeBytes).toBe(payload.length);
    expect(ready.mime).toBe("application/x-test");

    // The consumer handler saw the fetch command (no library interception).
    const fetchCommands = server.consumerCommands().filter(
      (c) => typeof c === "object" && "fetch" in c,
    );
    expect(fetchCommands).toHaveLength(1);

    // Bytes claimed through the binary receive path, correlated by txid.
    const claim = await awaitClaim(claims, ready.txid, payload.length, "stream completes");
    expectBytesEqual(Buffer.concat(claim.chunks.map((c) => Buffer.from(c))), payload);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("2. large payload across multiple frames streams bit-exact", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(2 * 1024 * 1024 + 512 * 1024); // 2.5 MiB > 1 MiB working frame
    const resources = new Map([["res:big", { bytes: payload, mime: "application/x-big" }]]);
    const server = startServer({ resources });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const claims = claimStreams(client.session);

    const ready = await client.session.fetch("res:big");
    expect(ready.sizeBytes).toBe(payload.length);
    expect(ready.sha256).toBe(sha256Hex(payload));

    const claim = await awaitClaim(claims, ready.txid, payload.length, "big stream completes");
    expectBytesEqual(Buffer.concat(claim.chunks.map((c) => Buffer.from(c))), payload);

    await client.transport.disconnect();
    await server.stop();
  }, 30_000);

  it("4. a consumer handler that declines fetch answers fetch_refused", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const server = startServer({
      onCommandOverride: async (command) => {
        if (typeof command === "object" && "fetch" in command) {
          throw fetchRefused("fetch refused: not served here");
        }
        return acknowledge();
      },
    });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    await expectXacppError(client.session.fetch("res:anything"), FETCH_REFUSED);
    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("5. a consumer handler with no such resource answers fetch_not_found", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const server = startServer({ resources: new Map() });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const err = await expectXacppError(client.session.fetch("res:missing"), FETCH_NOT_FOUND);
    expect(err.message).toContain("res:missing");
    await client.transport.disconnect();
    await server.stop();
  }, 20_000);
});

// ---- Bidirectional + concurrent ----

describe("fetch kernel — bidirectional and concurrent", () => {
  it("3. both directions on one connection resolve the right metas and stream the right bytes", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const serverPayload = filler(777, 1);
    const clientPayload = filler(999, 2);
    const serverResources = new Map([["res:server", { bytes: serverPayload, mime: "from-server" }]]);
    const clientResources = new Map([["res:client", { bytes: clientPayload, mime: "from-client" }]]);

    const server = startServer({ resources: serverResources });
    await waitFor(() => server.port() !== 0, "server listen");
    let clientServingSession: XacppSession | null = null;
    const client = await connectClient(server.port(), {
      configure: (s) => {
        clientServingSession = s;
      },
      onCommandOverride: async (command) => {
        if (typeof command === "object" && "fetch" in command && clientServingSession !== null) {
          return serveFrom(clientServingSession, command, clientResources);
        }
        return acknowledge();
      },
    });
    const clientClaims = claimStreams(client.session);
    const serverClaims = claimStreams(server.session()!);

    // Client pulls from server: the server's consumer handler serves.
    const fromServer = await client.session.fetch("res:server");
    expect(fromServer.mime).toBe("from-server");
    const serverClaim = await awaitClaim(clientClaims, fromServer.txid, serverPayload.length, "server→client stream");
    expectBytesEqual(Buffer.concat(serverClaim.chunks.map((c) => Buffer.from(c))), serverPayload);

    // Server pulls from client: the client's consumer handler serves.
    const fromClient = await server.session()!.fetch("res:client");
    expect(fromClient.mime).toBe("from-client");
    const clientClaim = await awaitClaim(serverClaims, fromClient.txid, clientPayload.length, "client→server stream");
    expectBytesEqual(Buffer.concat(clientClaim.chunks.map((c) => Buffer.from(c))), clientPayload);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("7. N concurrent fetches never cross wires — each stream lands on its own txid", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payloads = new Map<string, Uint8Array>();
    for (let i = 0; i < 5; i++) payloads.set(`res:s${i}`, filler(10_000 + i * 4096, 100 + i));
    const resources = new Map(
      [...payloads.entries()].map(([uri, bytes]) => [uri, { bytes, mime: "application/x-conc" }]),
    );

    const server = startServer({ resources });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const claims = claimStreams(client.session);

    const readies = await Promise.all([...payloads.keys()].map((uri) => client.session.fetch(uri)));
    expect(new Set(readies.map((r) => r.txid)).size).toBe(readies.length); // unique txids

    await Promise.all(readies.map((r) => awaitClaim(claims, r.txid, r.sizeBytes, `stream ${r.txid}`)));
    for (const ready of readies) {
      const entry = [...payloads.entries()].find(([, bytes]) => ready.sha256 === sha256Hex(bytes));
      expect(entry).toBeDefined();
      expectBytesEqual(
        Buffer.concat(claims.get(ready.txid)!.chunks.map((c) => Buffer.from(c))),
        entry![1],
      );
    }

    await client.transport.disconnect();
    await server.stop();
  }, 30_000);
});

// ---- Thin-shell semantics ----

describe("fetch kernel — thin shell", () => {
  it("fetch() resolves with metadata alone and never waits for the bytes", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    // The handler answers fetch_ready and streams NOTHING: the thin shell
    // must still resolve (byte delivery is the application's business).
    const server = startServer({
      onCommandOverride: async (command) => {
        if (typeof command === "object" && "fetch" in command) {
          return {
            kind: "fetch_ready",
            txid: newTransactionId(),
            sha256: "ab".repeat(32),
            sizeBytes: 4,
            mime: "application/x-empty",
          };
        }
        return acknowledge();
      },
    });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const claims = claimStreams(client.session);

    const ready = await client.session.fetch("res:never-streamed");
    expect(ready.sizeBytes).toBe(4);
    expect(ready.mime).toBe("application/x-empty");
    await settle();
    expect(claims.size).toBe(0); // no bytes ever arrived, no stream ever opened

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);
});

afterEach(() => {
  vi.useRealTimers();
});
