/**
 * Fetch extension tests — ext/fetch.ts assembly (contract matrix).
 *
 * Harness mirrors tests/fetch.test.ts (full Peer + Session stacks over real
 * localhost TCP), with the responder's consumer handler assembled by
 * `FetchService` + canned providers — the extension under test IS the
 * serving side. Matrix items:
 *
 *  1 happy_bytes          6 begin_unknown / begin_twice
 *  2 happy_file           7 abort_mid_stream
 *  3 sink_access          8 pause_resume
 *  4 duplicate_fetch      9 disconnect_resume
 *  5 begin_gate          10 completed_replay_not_redelivered
 * 11 fetch_unavailable_gate
 *
 * Boundary coverage (review §二 — the eight edges with zero coverage on
 * both sides) lives in its own describe block at the bottom:
 * 12 pull_cancelled_by_signal / 13 peer_death_keeps_pull_pending /
 * 14 integrity_mismatch_end_to_end / 15 resolve_failure /
 * 16 serve_failure_forces_abort / 17 abort_not_begun /
 * 18 size_zero_rejected / 19 size_oversized_not_announced /
 * 20 abort_finish_race / 21 resolving_placeholder_rendezvous.
 *
 * Time: `setInterval` / `clearInterval` / `Date` are faked (retransmission
 * clock determinism); real socket IO stays real (`setTimeout` /
 * `setImmediate` / `performance` untouched).
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import * as net from "node:net";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { createHash } from "node:crypto";

import { SocketTransport } from "../src/carrier/socket";
import { FrameDecoder } from "../src/wire/frame";
import { XacppPeer } from "../src/session/peer";
import { XacppSession } from "../src/session/session";
import { acknowledge, XacppError } from "../src/wire/message";
import { genericCommand } from "../src/wire/commands";
import type { XacppResponse } from "../src/wire/message";
import type { XacppCommand } from "../src/wire/commands";
import type { Capabilities } from "../src/wire/capability";
import type { EstablishHandler, NegotiateHandler, XacppSessionHandler } from "../src/session/handler";
import type { XacppTransport } from "../src/carrier/transport";
import type { TransferConfig, TransferWiring, StreamEndReason } from "../src/transfer";
import { FETCH_NOT_FOUND, FETCH_REFUSED, FETCH_UNAVAILABLE } from "../src/wire/fetch";
import {
  FetchPuller,
  FetchService,
  FetchError,
  bytesProvider,
  fileProvider,
  fetchVerify,
  fetchPause,
  fetchResume,
  fetchAbort,
} from "../src/ext/fetch";
import type { FetchProvider } from "../src/ext/fetch";

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

/** Real-time sleep (setTimeout is NOT faked by the harness). */
function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// Fetch tests declare fetchExt support on BOTH sides (SPEC §8.7: the
// declaration is the application's own decision — here, the test harness
// exercises the fetch family, so both peers declare it; the
// fetch_unavailable gate (case 11) keeps a bare handler so the wire-level
// rejection stays the tested path).
const FETCH_CAPS: Capabilities = { fetchExt: 1 };

const acceptNegotiate: NegotiateHandler = { onNegotiate: async () => {} };

const unusedEstablish: EstablishHandler = {
  onEstablish: async () => {
    throw new Error("client side never accepts establish");
  },
  onEstablishConfirm: async () => {
    throw new Error("unused");
  },
};

const ackHandler: XacppSessionHandler = {
  onCommand: async () => acknowledge(),
  onEvent: async () => acknowledge(),
};

/** Wrap a provider with resolve/serve call counters. */
function countingProvider(inner: FetchProvider): FetchProvider & { resolveCalls(): number; serveCalls(): number } {
  let resolves = 0;
  let serves = 0;
  return {
    async resolve(uri) {
      resolves++;
      return inner.resolve(uri);
    },
    async serve(uri, sink) {
      serves++;
      return inner.serve(uri, sink);
    },
    resolveCalls: () => resolves,
    serveCalls: () => serves,
  };
}

// ---- Socket harness: full Peer + Session stacks, FetchService on the responder ----

interface ServerOptions {
  transferConfig?: TransferConfig;
  /** Serving-side provider (assembled into FetchService). Unused in bare mode. */
  provider?: FetchProvider;
  /** No fetch extension loaded: the session handler is a plain ack handler. */
  bare?: boolean;
  /** Business handler wrapped behind FetchService (fetch family + generic routing). */
  inner?: XacppSessionHandler;
  /**
   * Intercept BEFORE FetchService; return the response to answer with, or
   * `null` to pass the command through to FetchService.
   */
  onCommandOverride?: (command: XacppCommand, session: XacppSession) => Promise<XacppResponse | null>;
}

interface Server {
  port(): number;
  session(): XacppSession | null;
  connectionCount(): number;
  sentBytes(): number;
  stop(): Promise<void>;
}

/**
 * Responder side: one listener, one XacppSession + one FetchService reused
 * across connections (session-scoped assembly).
 */
function startServer(opts: ServerOptions): Server {
  const listener = net.createServer();
  let port = 0;
  let session: XacppSession | null = null;
  let conns = 0;
  let sentTotal = 0;
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
      let handler: XacppSessionHandler;
      if (opts.bare) {
        // No fetch extension loaded — the fetch_unavailable gate test's
        // responder shape (SPEC §8.2).
        handler = ackHandler;
      } else {
        const service = new FetchService(session, opts.provider!, opts.inner ?? null);
        handler = {
          // Interception wrapper around FetchService: it IS the loaded
          // responder, so the declaration must be forwarded — otherwise the
          // peer's fetch_unavailable gate would cut the fetch family off
          // before it ever reaches the wrapper (SPEC §8.2).
          isFetchResponder: true,
          onCommand: async (command) => {
            if (opts.onCommandOverride) {
              const intercepted = await opts.onCommandOverride(command, session!);
              if (intercepted !== null) return intercepted;
            }
            return service.onCommand(command);
          },
          onEvent: async (event) => service.onEvent(event),
        };
      }
      return { type: "established", sessionId: "srv-sid", credentials: "srv-creds", handler };
    },
    onEstablishConfirm: async () => {
      throw new Error("challenge path not used in these tests");
    },
  };

  listener.on("connection", (socket) => {
    const origWrite = socket.write.bind(socket);
    (socket as unknown as { write: (...a: unknown[]) => unknown }).write = (...a: unknown[]) => {
      const first = a[0] as string | { length?: number; byteLength?: number };
      sentTotal += typeof first === "string" ? first.length : (first?.length ?? first?.byteLength ?? 0);
      return origWrite(...(a as Parameters<typeof origWrite>));
    };
    const transport = new SocketTransport(socket);
    transports.push(transport);
    const peer = new XacppPeer(FETCH_CAPS, transport, acceptNegotiate, establish);
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
    sentBytes: () => sentTotal,
    stop: async () => {
      await Promise.all(transports.map((t) => t.disconnect().catch(() => {})));
      await new Promise<void>((resolve) => listener.close(() => resolve()));
    },
  };
}

interface Client {
  transport: SocketTransport;
  session: XacppSession;
  peer: XacppPeer;
}

async function connectClient(
  port: number,
  opts?: {
    transferConfig?: TransferConfig;
    configure?: (session: XacppSession) => void;
  },
): Promise<Client> {
  const transport = SocketTransport.connectTo(port, "127.0.0.1");
  const session = new XacppSession(opts?.transferConfig);
  opts?.configure?.(session);
  const peer = new XacppPeer(FETCH_CAPS, transport, acceptNegotiate, unusedEstablish, session);
  await peer.connect();
  await peer.negotiate();
  await peer.establish(undefined, ackHandler, () => {
    throw new Error("challenge path not used in these tests");
  });
  return { transport, session, peer };
}

/**
 * Real reconnect (matrix 9): the SAME session wired into a fresh transport
 * + peer — the wire-time recovery handshake (holder declares, receiver
 * adjudicates) resumes interrupted transfers.
 */
async function reconnectClient(port: number, session: XacppSession): Promise<{ transport: SocketTransport; gotBytes: () => number }> {
  const sock = net.connect(port, "127.0.0.1");
  await new Promise<void>((resolve, reject) => {
    sock.once("connect", resolve);
    sock.once("error", reject);
  });
  let got = 0;
  const traceDecoder = new FrameDecoder();
  sock.on("data", (d: Buffer) => {
    got += d.length;
    try {
      for (const frame of traceDecoder.push(new Uint8Array(d))) {
        if (frame.frameType === 0x1002) {
          console.log(`[cli-frame] txid=${frame.transactionId.slice(0, 6)} seq=${frame.seq} totalLen=${frame.totalLen} payload=${frame.payload.length}`);
        }
      }
    } catch {
      // tracing only
    }
  });
  const transport = new SocketTransport(sock);
  const peer = new XacppPeer(FETCH_CAPS, transport, acceptNegotiate, unusedEstablish, session);
  await peer.connect();
  await peer.negotiate();
  await peer.establish(undefined, ackHandler, () => {
    throw new Error("challenge path not used in these tests");
  });
  return { transport, gotBytes: () => got };
}

/** Callback-style native consumption of a bare BinaryStream (matrix 3 form). */
interface NativeClaim {
  chunks: Uint8Array[];
  endReason: StreamEndReason | null;
}

function claimNatively(stream: {
  onChunk(handler: ((chunk: Uint8Array, s: unknown) => void) | null): void;
  onEnd(handler: ((reason: StreamEndReason, s: unknown) => void) | null): void;
}): NativeClaim {
  const claim: NativeClaim = { chunks: [], endReason: null };
  stream.onChunk((chunk) => claim.chunks.push(chunk));
  stream.onEnd((reason) => {
    claim.endReason = reason;
  });
  return claim;
}

function totalBytes(chunks: Uint8Array[]): number {
  return chunks.reduce((n, c) => n + c.length, 0);
}

function joinChunks(chunks: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(totalBytes(chunks));
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.length;
  }
  return out;
}

/**
 * Claim that COUNTS terminal events. The race edges (abort vs finish)
 * must take the terminal state exactly once — a counting claim is the
 * only way to see a double terminal (a plain claim only keeps the last
 * reason).
 */
interface CountingClaim extends NativeClaim {
  endCount: number;
}

function claimCounting(stream: {
  onChunk(handler: ((chunk: Uint8Array, s: unknown) => void) | null): void;
  onEnd(handler: ((reason: StreamEndReason, s: unknown) => void) | null): void;
}): CountingClaim {
  const claim: CountingClaim = { chunks: [], endReason: null, endCount: 0 };
  stream.onChunk((chunk) => claim.chunks.push(chunk));
  stream.onEnd((reason) => {
    claim.endCount += 1;
    claim.endReason = reason;
  });
  return claim;
}

/** Small-chunk responder config: multi-frame streams for gate/abort tests. */
function smallChunkConfig(): TransferConfig {
  return {
    sender: { workingFrameBytes: 256, initialTimeoutMs: 1_000_000 },
    silenceTimeoutMs: 1_000_000_000,
    heartbeatIntervalMs: 1_000_000_000,
  };
}

/**
 * paced provider: serves the first quarter, then holds 400 ms before the
 * rest — a command window so pause/abort lands while the serving task is
 * still alive (loopback TCP would otherwise drain the whole resource
 * before the companion command arrives).
 */
function pacedBytesProvider(data: Uint8Array, mime: string, secondGate?: Promise<void>): FetchProvider {
  const inner = bytesProvider(data, mime);
  return {
    resolve: (uri) => inner.resolve(uri),
    serve: async (uri, sink) => {
      const quarter = data.length >> 2;
      await sink.write(data.subarray(0, quarter));
      await sleep(400);
      if (secondGate) await secondGate;
      for (let offset = quarter; offset < data.length; offset += 64 * 1024) {
        await sink.write(data.subarray(offset, Math.min(offset + 64 * 1024, data.length)));
      }
    },
  };
}

afterEach(() => {
  vi.useRealTimers();
});

// ---- Contract matrix ----

describe("fetch extension — contract matrix", () => {
  it("1. happy_bytes: canned bytes provider → pull_to_vec, bit-exact with matching meta", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(256 * 1024 + 13);
    const provider = countingProvider(bytesProvider(payload, "application/x-test"));
    const server = startServer({ provider });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    const { bytes, meta } = await puller.pullToVec("res:small");
    expectBytesEqual(bytes, payload);
    expect(meta.txid).toMatch(/^[0-9a-f]{32}$/);
    expect(meta.sha256).toBe(sha256Hex(payload));
    expect(meta.sizeBytes).toBe(payload.length);
    expect(meta.mime).toBe("application/x-test");
    expect(provider.resolveCalls()).toBe(1);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("2. happy_file: canned file provider (128 KiB + 7, spans chunks) → pull_to_vec bit-exact", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "xacpp-ext-fetch-"));
    try {
      const filePath = path.join(dir, "blob.bin");
      const payload = filler(128 * 1024 + 7);
      await fs.writeFile(filePath, payload);
      const server = startServer({ provider: fileProvider(filePath, "application/x-big") });
      await waitFor(() => server.port() !== 0, "server listen");
      const client = await connectClient(server.port());
      const puller = new FetchPuller(client.session);

      const { bytes, meta } = await puller.pullToVec("res:file");
      expectBytesEqual(bytes, payload);
      expect(meta.sha256).toBe(sha256Hex(payload));
      expect(meta.sizeBytes).toBe(payload.length);

      await client.transport.disconnect();
      await server.stop();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  }, 20_000);

  it("3. sink_access: custom provider writes the real sink; the puller hands back the bare kernel stream", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(1024);
    const server = startServer({
      provider: {
        resolve: async () => ({
          sizeBytes: payload.length,
          sha256: sha256Hex(payload),
          mime: "application/x-custom",
        }),
        serve: async (_uri, sink) => {
          // The provider writes the REAL kernel sink — no wrapper in sight.
          await sink.write(payload.subarray(0, 100));
          await sink.write(payload.subarray(100));
        },
      },
    });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    const { stream, meta } = await puller.pull("res:custom");
    // The delivered object IS the kernel BinaryStream (identity surface).
    expect(stream.transactionId).toBe(meta.txid);
    expect(stream.totalLen).toBe(payload.length);

    const claim = claimNatively(stream);
    await waitFor(() => claim.endReason === "completed", "stream completes");
    expect(claim.endReason).toBe("completed");
    expect(totalBytes(claim.chunks)).toBe(payload.length);
    fetchVerify(meta, sha256Hex(joinChunks(claim.chunks)), totalBytes(claim.chunks));
    expectBytesEqual(joinChunks(claim.chunks), payload);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("4. duplicate_fetch_idempotent: bare fetch during pull AND two concurrent fetches share one resolve", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(8 * 1024);
    const slow = bytesProvider(payload, "application/x-dup");
    const provider = countingProvider({
      resolve: async (uri) => {
        await sleep(30); // widen the resolving-placeholder window
        return slow.resolve(uri);
      },
      serve: slow.serve,
    });
    const server = startServer({ provider });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    // Part A: bare session.fetch while a pull is in flight → same txid.
    const pullPromise = puller.pull("res:dup");
    await waitFor(() => provider.resolveCalls() >= 1, "resolve entered");
    const bare = await client.session.fetch("res:dup");
    const pulled = await pullPromise;
    expect(bare.txid).toBe(pulled.meta.txid);

    // Part B: two concurrent fetches on one uri (placeholder race path).
    const [r1, r2] = await Promise.all([
      client.session.fetch("res:dup2"),
      client.session.fetch("res:dup2"),
    ]);
    expect(r1.txid).toBe(r2.txid);
    expect(r1.txid).toMatch(/^[0-9a-f]{32}$/);

    // Exactly one resolve per distinct uri — never re-consulted.
    expect(provider.resolveCalls()).toBe(2);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("5. begin_gate: bare fetch emits nothing for 200 ms; fetch_begin opens the stream", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(4 * 1024);
    const server = startServer({ provider: bytesProvider(payload, "application/x-gate") });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const arrived: string[] = [];
    client.session.onBinaryStream((s) => arrived.push(s.transactionId));

    const ready = await client.session.fetch("res:gate");
    await sleep(200);
    expect(arrived).toHaveLength(0); // gate shut: not a single frame

    const ack = await client.session.requestCommand({ fetch_begin: { txid: ready.txid } });
    expect(ack.kind).toBe("generic");
    await waitFor(() => arrived.length >= 1, "stream arrives after begin");
    expect(arrived).toEqual([ready.txid]);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("6. begin_unknown / begin_twice: unknown refused; repeated begin succeeds and serve runs once", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(2 * 1024);
    const provider = countingProvider(bytesProvider(payload, "application/x-begin"));
    const server = startServer({ provider });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const arrived: string[] = [];
    client.session.onBinaryStream((s) => arrived.push(s.transactionId));

    const unknown = await client.session.requestCommand({
      fetch_begin: { txid: "0123456789abcdef0123456789abcdef" },
    });
    expect(unknown.kind).toBe("error");
    if (unknown.kind !== "error") throw new Error("unreachable");
    expect(unknown.code).toBe(FETCH_REFUSED);

    const ready = await client.session.fetch("res:x");
    const b1 = await client.session.requestCommand({ fetch_begin: { txid: ready.txid } });
    const b2 = await client.session.requestCommand({ fetch_begin: { txid: ready.txid } });
    expect(b1.kind).toBe("generic");
    expect(b2.kind).toBe("generic");

    await waitFor(() => provider.serveCalls() === 1, "serve started");
    await waitFor(() => arrived.length >= 1, "stream arrives");
    await settle();
    expect(arrived).toEqual([ready.txid]); // exactly one stream
    expect(provider.serveCalls()).toBe(1); // repeated begin never re-spawns

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("7. abort_mid_stream: fetch::abort cancels the stream, frees the serving table, idempotent on repeat", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(64 * 1024);
    const provider = countingProvider(pacedBytesProvider(payload, "application/x-abort"));
    const server = startServer({ provider, transferConfig: smallChunkConfig() });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    const { stream, meta } = await puller.pull("res:abort");
    const claim = claimNatively(stream);
    await waitFor(() => claim.chunks.length >= 1, "first chunk");

    await fetchAbort(client.session, meta.txid);
    await waitFor(() => claim.endReason !== null, "stream reaches terminal state");
    expect(claim.endReason).toBe("cancelled");

    // b-side table entry is gone: a fresh fetch re-resolves with a NEW txid.
    const ready2 = await client.session.fetch("res:abort");
    expect(ready2.txid).not.toBe(meta.txid);
    expect(provider.resolveCalls()).toBe(2);

    // Repeated abort: idempotent success (unknown/completed answers success).
    await expect(fetchAbort(client.session, meta.txid)).resolves.toBeUndefined();
    await sleep(100);
    expect(claim.endReason).toBe("cancelled"); // no second terminal event

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("8. pause_resume: fetch::pause stalls the flow (no new chunks), fetch::resume completes it", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(16 * 1024);
    const server = startServer({ provider: pacedBytesProvider(payload, "application/x-pause"), transferConfig: smallChunkConfig() });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    const { stream, meta } = await puller.pull("res:pause");
    const claim = claimNatively(stream);
    await waitFor(() => claim.chunks.length >= 1, "first chunk");

    await fetchPause(client.session, meta.txid);
    await waitFor(() => stream.paused === true, "paused state visible");
    await sleep(300);
    const frozenAt = totalBytes(claim.chunks);
    await sleep(300);
    expect(totalBytes(claim.chunks)).toBe(frozenAt); // 300 ms, zero new chunks
    expect(stream.paused).toBe(true);

    await fetchResume(client.session, meta.txid);
    await waitFor(() => claim.endReason === "completed", "stream completes after resume");
    expect(totalBytes(claim.chunks)).toBe(payload.length);
    fetchVerify(meta, sha256Hex(joinChunks(claim.chunks)), totalBytes(claim.chunks));

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("9. disconnect_resume: the SAME bare stream object continues across a reconnect and verifies", async () => {
    // REAL TIMERS + EXPECTED RED (kernel gap, deliberately not skipped):
    // the full-stack downstream of DATA_BINARY frames on a SECOND
    // connection (XacppPeer + XacppSession rewire) does not reach the
    // client's reassembler — reproduced with a bare openBinarySink stream
    // and zero fetch/extension involvement, so this is a kernel/session
    // layer gap, not an extension bug. Kept red as a roadblock per
    // project discipline (no skip-masked reds); turns green the moment
    // the kernel delivers downstream frames across replacement transports.
    // Everything the extension owns on this path (pull pending survival,
    // stream object identity, stall flags, zero-reconnection user code)
    // is asserted before the point where the kernel gap bites.
    const payload = filler(64 * 1024);
    // Gate the second segment: released only AFTER the reconnect, so the
    // serving task resumes writing on the live egress — provider/service
    // code carries zero reconnection logic; the kernel absorbs the drop,
    // the write simply continues.
    let releaseSecond!: () => void;
    const secondGate = new Promise<void>((resolve) => {
      releaseSecond = resolve;
    });
    const resumeConfig: TransferConfig = {
      sender: { workingFrameBytes: 256, initialTimeoutMs: 200, maxTimeoutMs: 400 },
      silenceTimeoutMs: 1_000_000_000,
      heartbeatIntervalMs: 1_000_000_000,
    };
    const server = startServer({
      provider: pacedBytesProvider(payload, "application/x-resume", secondGate),
      transferConfig: resumeConfig,
    });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    const { stream, meta } = await puller.pull("res:resume");
    const claim = claimNatively(stream);
    await waitFor(() => claim.chunks.length >= 1, "first chunk");

    // Active close on the initiator: SocketTransport.disconnect() does not
    // broadcast onClosed (passive-death notification only), so the caller
    // tears the session down explicitly — equivalent to the automatic
    // unwire a real network drop performs. The responder side observes the
    // drop through its own socket close event.
    await client.transport.disconnect();
    client.session.unwire();
    await waitFor(() => stream.stalled === true, "stream stalls on disconnect");
    expect(claim.endReason).toBeNull(); // alive — no terminal event
    await waitFor(() => server.session()?.wired === false, "server observed the drop");

    // Real reconnect: same session, fresh transport + peer. The stream
    // object identity survives.
    const reconnected = await reconnectClient(server.port(), client.session);
    const transport2 = reconnected.transport;
    await waitFor(() => stream.stalled === false, "stall cleared after rewire");

    // Release the second segment: the serving task keeps writing on the
    // live egress — no resume API touched anywhere in user code.
    releaseSecond();
    await waitFor(() => claim.endReason === "completed", "stream completes after resume", 20_000);
    expect(totalBytes(claim.chunks)).toBe(payload.length);
    fetchVerify(meta, sha256Hex(joinChunks(claim.chunks)), totalBytes(claim.chunks));

    await transport2.disconnect();
    await server.stop();
  }, 40_000);

  it("10. completed_replay_not_redelivered: a replayed terminal txid yields no second delivery; the session stays healthy", async () => {
    // Aligned with xacpp-rs ext10_completed_replay_not_redelivered: after a
    // fetch completes end-to-end, re-opening a sink under the SAME txid and
    // replaying the bytes must NOT produce a second consumer delivery —
    // unclaimed streams are logged and dropped by the puller's dispatch —
    // and the session stays healthy for subsequent fetches.
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(64 * 1024);
    const provider = countingProvider(bytesProvider(payload, "application/x-replay"));
    let replayTxid: string | null = null;
    const server = startServer({
      provider,
      onCommandOverride: async (command, session) => {
        if (typeof command === "object" && "generic" in command && command.generic.name === "replay") {
          if (replayTxid === null) throw new Error("replay before the first fetch completed");
          const sink = session.openBinarySink({ totalLen: payload.length }, replayTxid);
          setImmediate(() => {
            void sink
              .write(payload)
              .then(() => sink.finish())
              .catch(() => {});
          });
          return acknowledge();
        }
        return null; // everything else goes to FetchService
      },
    });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    // Full fetch chain completes (this is the txid that gets replayed).
    const { bytes, meta } = await puller.pullToVec("res:replay");
    expectBytesEqual(bytes, payload);
    expect(meta.txid).toMatch(/^[0-9a-f]{32}$/);
    replayTxid = meta.txid;

    // Replay the SAME txid over a freshly opened sink. The puller's
    // dispatch must log+drop the unclaimed stream (observable via its
    // warn log — there is no passthrough escape hatch) and never deliver
    // it a second time.
    const warnSpy = vi.spyOn(console, "warn");
    await client.session.requestCommand(genericCommand("replay", null));
    await settle();
    await sleep(300);
    expect(
      warnSpy.mock.calls.some((args) =>
        args.some(
          (a) => typeof a === "string" && a.includes(`unclaimed binary stream ${meta.txid}`),
        ),
      ),
    ).toBe(true); // logged + dropped — the replay never reached a consumer
    warnSpy.mockRestore();
    expect(provider.resolveCalls()).toBe(1); // replay never re-consults the provider

    // Session health: a subsequent fetch on the same connection works.
    const second = await puller.pullToVec("res:after");
    expectBytesEqual(second.bytes, payload);
    expect(second.meta.txid).not.toBe(meta.txid);
    expect(provider.resolveCalls()).toBe(2);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);
});

// ---- Companion free functions ----

describe("fetch extension — companions", () => {
  it("verify compares the declared digest and reports the full mismatch payload", () => {
    const meta = {
      txid: "0123456789abcdef0123456789abcdef",
      sha256: "aa".repeat(32),
      sizeBytes: 3,
      mime: "application/x-test",
    };
    expect(() => fetchVerify(meta, meta.sha256, 3)).not.toThrow();
    try {
      fetchVerify(meta, "bb".repeat(32), 3);
      throw new Error("expected fetchVerify to throw");
    } catch (e) {
      expect(e).toBeInstanceOf(FetchError);
      const err = e as FetchError;
      expect(err.kind).toBe("integrity_mismatch");
      expect(err.expected).toBe(meta.sha256);
      expect(err.actual).toBe("bb".repeat(32));
      expect(err.received).toBe(3);
    }
  });
});

// ---- fetch_unavailable gate (SPEC §8.2) ----

describe("fetch extension — fetch_unavailable gate", () => {
  it("11. a peer without a loaded fetch responder answers the whole fetch family with fetch_unavailable", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const server = startServer({ bare: true });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());

    // `fetch` on a peer without a loaded responder: the library answers
    // `fetch_unavailable` (surfaced as a rejected XacppError).
    const err = await client.session.fetch("res:anything").then(
      () => {
        throw new Error("fetch must reject without a loaded responder");
      },
      (e) => e,
    );
    expect(err).toBeInstanceOf(XacppError);
    expect(err.code).toBe(FETCH_UNAVAILABLE);

    // The gate covers the whole fetch family — begin/pause/resume/abort
    // get the same loud answer.
    const family: XacppCommand[] = [
      { fetch_begin: { txid: "0".repeat(32) } },
      { fetch_pause: { txid: "0".repeat(32) } },
      { fetch_resume: { txid: "0".repeat(32) } },
      { fetch_abort: { txid: "0".repeat(32) } },
    ];
    for (const cmd of family) {
      const resp = await client.session.requestCommand(cmd);
      expect(resp.kind).toBe("error");
      if (resp.kind === "error") expect(resp.code).toBe(FETCH_UNAVAILABLE);
    }

    // Non-fetch commands are not the gate's business: they reach the
    // consumer handler untouched.
    const ack = await client.session.requestCommand(genericCommand("ping", null));
    expect(ack).toEqual(acknowledge());

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);
});

// ---- Boundary coverage (review §二 — the eight edges with zero coverage) ----
//
// One case per boundary class, each asserting an OBSERVABLE behaviour
// (wire error response / terminal state / refusal / eviction) — never an
// internal implementation detail. Numbering continues after the contract
// matrix (1..11).

describe("fetch extension — boundary coverage", () => {
  it("12. pull_cancelled_by_signal: the consumer's abort tears the pending registration down; the late stream is dropped, never delivered", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(8 * 1024);
    let releaseServe!: () => void;
    const serveGate = new Promise<void>((resolve) => {
      releaseServe = resolve;
    });
    let beginSeen = 0;
    const provider = countingProvider({
      resolve: async () => ({
        sizeBytes: payload.length,
        sha256: sha256Hex(payload),
        mime: "application/x-cancel",
      }),
      serve: async (_uri, sink) => {
        await serveGate; // the bytes only start flowing after the cancel
        await sink.write(payload);
      },
    });
    const server = startServer({
      provider,
      onCommandOverride: async (command) => {
        if (typeof command === "object" && "fetch_begin" in command) beginSeen++;
        return null; // pass through to FetchService
      },
    });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);
    const controller = new AbortController();

    const pullPromise = puller.pull("res:cancel", controller.signal);
    await waitFor(() => beginSeen >= 1, "begin reached the responder");
    await sleep(100); // the begin response travels back; the pull is suspended

    // The consumer cancels: the registration is torn down and the pull
    // rejects with the abort reason.
    controller.abort();
    const err = await pullPromise.then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(FetchError);
    const cancelled = err as FetchError;
    expect(cancelled.kind).toBe("transport");
    expect((cancelled.error as XacppError).code).toBe("aborted");

    // The late stream of the cancelled pull is logged + dropped — it is
    // never pushed into the dead registration.
    const warnSpy = vi.spyOn(console, "warn");
    releaseServe();
    await waitFor(
      () =>
        warnSpy.mock.calls.some((args) =>
          args.some((a) => typeof a === "string" && a.includes("dropping unclaimed binary stream")),
        ),
      "late stream dropped",
    );
    warnSpy.mockRestore();

    // The session stays healthy: the next pull still completes.
    const { bytes } = await puller.pullToVec("res:after-cancel");
    expectBytesEqual(bytes, payload);
    expect(provider.resolveCalls()).toBe(2);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("13. peer_death_keeps_pull_pending: a dead peer never resolves the pull — the consumer's signal is the self-rescue", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(4 * 1024);
    let beginSeen = 0;
    const provider = countingProvider({
      resolve: async () => ({
        sizeBytes: payload.length,
        sha256: sha256Hex(payload),
        mime: "application/x-dead",
      }),
      serve: async () => {
        // The peer dies before it can serve: nothing ever arrives.
      },
    });
    const server = startServer({
      provider,
      onCommandOverride: async (command) => {
        if (typeof command === "object" && "fetch_begin" in command) beginSeen++;
        return null;
      },
    });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);
    const controller = new AbortController();

    const pullPromise = puller.pull("res:dead", controller.signal);
    await waitFor(() => beginSeen >= 1, "begin reached the responder");
    await sleep(100);

    // The peer dies for good. The pull has no timeout by design (the
    // pending registration survives disconnects) — it stays suspended.
    await client.transport.disconnect();
    let settled = false;
    void pullPromise.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await sleep(300);
    expect(settled).toBe(false);

    // Self-rescue: the consumer aborts its own signal.
    controller.abort();
    const err = await pullPromise.then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(FetchError);
    expect((err as FetchError).kind).toBe("transport");
    expect(((err as FetchError).error as XacppError).code).toBe("aborted");

    await server.stop();
  }, 20_000);

  it("14. integrity_mismatch_end_to_end: a provider that declares a digest it never serves is rejected by the consumer", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(4 * 1024);
    // The declared digest belongs to different bytes than the served ones.
    const declared = sha256Hex(filler(4 * 1024, 0x5eed));
    const provider = countingProvider({
      resolve: async () => ({
        sizeBytes: payload.length,
        sha256: declared,
        mime: "application/x-liar",
      }),
      serve: async (_uri, sink) => {
        await sink.write(payload);
      },
    });
    const server = startServer({ provider });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    // The transfer itself is intact — the refusal is the integrity check.
    const err = await puller.pullToVec("res:liar").then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(FetchError);
    const mismatch = err as FetchError;
    expect(mismatch.kind).toBe("integrity_mismatch");
    expect(mismatch.expected).toBe(declared);
    expect(mismatch.actual).toBe(sha256Hex(payload));
    expect(mismatch.received).toBe(payload.length);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("15. resolve_failure: a provider that cannot resolve answers a wire error; the pull surfaces a refusal and leaves no residue", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(2 * 1024);
    const provider = countingProvider({
      resolve: async (uri) => {
        if (uri === "res:missing") throw new XacppError(FETCH_NOT_FOUND, "no such resource");
        return { sizeBytes: payload.length, sha256: sha256Hex(payload), mime: "application/x-resolve" };
      },
      serve: async (_uri, sink) => {
        await sink.write(payload);
      },
    });
    const server = startServer({ provider });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    // Wire level: the fetch command answers an error response, never a ready.
    const resp = await client.session.requestCommand({ fetch: { uri: "res:missing" } });
    expect(resp.kind).toBe("error");
    if (resp.kind === "error") expect(resp.code).toBe(FETCH_NOT_FOUND);

    // Puller level: the peer's code is surfaced as a refusal.
    const err = await puller.pull("res:missing").then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(FetchError);
    const refused = err as FetchError;
    expect(refused.kind).toBe("refused");
    expect(refused.code).toBe(FETCH_NOT_FOUND);

    // No residue: the failed attempt evicted its slot — the retry re-resolves.
    expect(provider.resolveCalls()).toBe(2);
    const { bytes } = await puller.pullToVec("res:ok");
    expectBytesEqual(bytes, payload);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("16. serve_failure_forces_abort: a provider that fails mid-serve ends the transaction in the abort terminal state", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(8 * 1024);
    const provider = countingProvider({
      resolve: async () => ({
        sizeBytes: payload.length,
        sha256: sha256Hex(payload),
        mime: "application/x-servefail",
      }),
      serve: async (_uri, sink) => {
        await sink.write(payload.subarray(0, 1024)); // partial write
        throw new Error("provider exploded mid-serve");
      },
    });
    const server = startServer({ provider, transferConfig: smallChunkConfig() });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    const { stream, meta } = await puller.pull("res:servefail");
    const claim = claimCounting(stream);
    await waitFor(() => claim.endReason !== null, "terminal state");
    // The failed serve is forced into the abort terminal state — never
    // "completed": the declared bytes never arrived.
    expect(claim.endReason).toBe("cancelled");
    expect(claim.endCount).toBe(1);
    expect(totalBytes(claim.chunks)).toBeLessThan(payload.length);

    // The failed serving is evicted: a retry re-resolves under a new txid.
    const ready2 = await client.session.fetch("res:servefail");
    expect(ready2.txid).not.toBe(meta.txid);
    expect(provider.resolveCalls()).toBe(2);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("17. abort_not_begun: aborting an announced-but-never-begun transaction succeeds, serves nothing and evicts the slot", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(2 * 1024);
    const provider = countingProvider(bytesProvider(payload, "application/x-nobegin"));
    const server = startServer({ provider });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const arrived: string[] = [];
    client.session.onBinaryStream((s) => arrived.push(s.transactionId));

    const ready = await client.session.fetch("res:nobegin");
    // The abort answers success (SPEC §8.3 idempotency).
    const ack = await client.session.requestCommand({ fetch_abort: { txid: ready.txid } });
    expect(ack.kind).toBe("generic");

    // No serving task ever existed → the provider never ran, no frame flew.
    expect(provider.serveCalls()).toBe(0);
    expect(arrived).toHaveLength(0);

    // The slot is gone: a later begin for that txid is refused as unknown.
    const late = await client.session.requestCommand({ fetch_begin: { txid: ready.txid } });
    expect(late.kind).toBe("error");
    if (late.kind === "error") expect(late.code).toBe(FETCH_REFUSED);

    // A retry re-resolves under a new txid.
    const ready2 = await client.session.fetch("res:nobegin");
    expect(ready2.txid).not.toBe(ready.txid);
    expect(provider.resolveCalls()).toBe(2);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("18. size_zero_rejected: a zero-length resource is refused loudly and never served", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const provider = countingProvider({
      resolve: async () => ({
        sizeBytes: 0,
        sha256: sha256Hex(new Uint8Array(0)),
        mime: "application/x-zero",
      }),
      serve: async () => {
        throw new Error("serve must never run for a zero-length resource");
      },
    });
    const server = startServer({ provider });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    // Wire level: a loud, registered refusal — never a fetch_ready.
    const resp = await client.session.requestCommand({ fetch: { uri: "res:zero" } });
    expect(resp.kind).toBe("error");
    if (resp.kind === "error") expect([FETCH_NOT_FOUND, FETCH_REFUSED]).toContain(resp.code);
    expect(provider.serveCalls()).toBe(0);

    // Puller level: the refusal surfaces as a FetchError.
    const err = await puller.pull("res:zero").then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(FetchError);
    expect((err as FetchError).kind).toBe("refused");

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("19. size_oversized_not_announced: a size beyond the wire's u32 totalLen must be refused, not advertised", async () => {
    // The size gate is in place (src/ext/fetch.ts: the responder refuses
    // size 0 and anything beyond u32::MAX with fetch_refused — SPEC §3 caps
    // a transaction at 4 GiB, "larger content is split into multiple
    // transactions by the message layer"). This test pins the gate: without
    // it the responder would answer fetch_ready with a size the u32 frame
    // header cannot carry, and the wire's total_len would truncate to 0
    // (setUint32 of 2^32). The rs twin carries the same double gate
    // (src/ext/fetch.rs).
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const oversize = 0x1_0000_0000; // 2^32 — one past the u32 totalLen
    const provider = countingProvider({
      resolve: async () => ({
        sizeBytes: oversize,
        sha256: "aa".repeat(32),
        mime: "application/x-huge",
      }),
      serve: async () => {
        throw new Error("serve must never run for an unannounceable size");
      },
    });
    const server = startServer({ provider });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());

    const resp = await client.session.requestCommand({ fetch: { uri: "res:huge" } });
    expect(resp.kind).toBe("error");
    if (resp.kind === "error") expect([FETCH_NOT_FOUND, FETCH_REFUSED]).toContain(resp.code);
    expect(provider.serveCalls()).toBe(0);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("20. abort_finish_race: the abort command and the provider's completion race — exactly one terminal state, healthy session", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(8 * 1024);
    let releaseServe!: () => void;
    const serveGate = new Promise<void>((resolve) => {
      releaseServe = resolve;
    });
    const provider = countingProvider({
      resolve: async () => ({
        sizeBytes: payload.length,
        sha256: sha256Hex(payload),
        mime: "application/x-race",
      }),
      serve: async (_uri, sink) => {
        await sink.write(payload); // the whole resource — a well-behaved provider
        await serveGate; // the completion is held right before the finish
      },
    });
    const server = startServer({ provider, transferConfig: smallChunkConfig() });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    // Part A: the abort lands while the serving task is still alive (the
    // provider has not returned yet) — the serving must not take a second
    // terminal op once the abort took the terminal state.
    const { stream, meta } = await puller.pull("res:race");
    const claim = claimCounting(stream);
    await waitFor(() => claim.endReason === "completed", "stream completes on the byte total");
    const ack = await client.session.requestCommand({ fetch_abort: { txid: meta.txid } });
    expect(ack.kind).toBe("generic");
    releaseServe();
    await sleep(200);
    expect(claim.endCount).toBe(1); // the late abort adds no second terminal
    expect(claim.endReason).toBe("completed");
    expect(totalBytes(claim.chunks)).toBe(payload.length);

    // Part B: the serving completes first — the abort then finds the slot
    // already evicted and answers idempotently (SPEC §8.3).
    const second = await puller.pull("res:race2");
    const secondClaim = claimCounting(second.stream);
    await waitFor(() => secondClaim.endReason === "completed", "second stream completes");
    await sleep(200); // the serving task finishes and evicts its slot
    const ack2 = await client.session.requestCommand({ fetch_abort: { txid: second.meta.txid } });
    expect(ack2.kind).toBe("generic");
    await sleep(100);
    expect(secondClaim.endCount).toBe(1);

    // The session and the serving table survive the race.
    const ready2 = await client.session.fetch("res:race");
    expect(ready2.txid).not.toBe(meta.txid);
    const { bytes } = await puller.pullToVec("res:after-race");
    expectBytesEqual(bytes, payload);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("21. resolving_placeholder_rendezvous: a concurrent fetch waits out the in-flight resolve and gets the same meta; a failed resolve reaches the waiter too", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(2 * 1024);
    let releaseResolve!: () => void;
    const resolveGate = new Promise<void>((resolve) => {
      releaseResolve = resolve;
    });
    let failResolve!: (err: Error) => void;
    const failGate = new Promise<never>((_resolve, reject) => {
      failResolve = reject;
    });
    const provider = countingProvider({
      resolve: async (uri) => {
        if (uri === "res:slow") {
          await resolveGate; // widen the resolving-placeholder window
          return { sizeBytes: payload.length, sha256: sha256Hex(payload), mime: "application/x-slow" };
        }
        if (uri === "res:boom") {
          await failGate;
          return { sizeBytes: 0, sha256: "aa".repeat(32), mime: "application/x-boom" };
        }
        return { sizeBytes: payload.length, sha256: sha256Hex(payload), mime: "application/x-resolve" };
      },
      serve: async (_uri, sink) => {
        await sink.write(payload);
      },
    });
    let fetchCommands = 0;
    const server = startServer({
      provider,
      onCommandOverride: async (command) => {
        if (typeof command === "object" && "fetch" in command) fetchCommands++;
        return null;
      },
    });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());

    // Part A: the waiter rendezvous on a successful resolve.
    const first = client.session.fetch("res:slow");
    await waitFor(() => provider.resolveCalls() === 1, "resolve in flight");
    let secondSettled = false;
    const second = client.session.fetch("res:slow").then((m) => {
      secondSettled = true;
      return m;
    });
    await waitFor(() => fetchCommands >= 2, "second fetch reached the responder");
    await sleep(100);
    // The second fetch is parked on the live placeholder — it did not
    // answer from the fast path.
    expect(secondSettled).toBe(false);
    releaseResolve();
    const [m1, m2] = await Promise.all([first, second]);
    expect(m1.txid).toBe(m2.txid);
    expect(m1.txid).toMatch(/^[0-9a-f]{32}$/);
    expect(provider.resolveCalls()).toBe(1); // resolve runs exactly once

    // Part B: the placeholder's failure reaches the waiter with the same code.
    const firstFail = client.session.fetch("res:boom");
    await waitFor(() => provider.resolveCalls() === 2, "failing resolve in flight");
    const secondFail = client.session.fetch("res:boom");
    failResolve(new XacppError(FETCH_NOT_FOUND, "resource vanished"));
    const e1 = await firstFail.then(
      () => null,
      (e: unknown) => e,
    );
    const e2 = await secondFail.then(
      () => null,
      (e: unknown) => e,
    );
    expect(e1).toBeInstanceOf(XacppError);
    expect(e2).toBeInstanceOf(XacppError);
    expect((e1 as XacppError).code).toBe(FETCH_NOT_FOUND);
    expect((e2 as XacppError).code).toBe(FETCH_NOT_FOUND);

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);

  it("22. short_write_fails_the_pull: a provider that delivers fewer bytes than it declared ends the pull explicitly — no hang, no integrity verdict", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
    const payload = filler(8 * 1024);
    const provider = countingProvider({
      resolve: async () => ({
        sizeBytes: payload.length,
        sha256: sha256Hex(payload),
        mime: "application/x-shortwrite",
      }),
      serve: async (_uri, sink) => {
        await sink.write(payload.subarray(0, 1024)); // a quarter of the declared bytes
      },
    });
    const server = startServer({ provider, transferConfig: smallChunkConfig() });
    await waitFor(() => server.port() !== 0, "server listen");
    const client = await connectClient(server.port());
    const puller = new FetchPuller(client.session);

    // Part A: the bare stream terminates explicitly — the consumer is never
    // left waiting for bytes that will not arrive. The holder announced
    // the abort (a short write is never announced as a completion).
    const { stream } = await puller.pull("res:shortwrite");
    const claim = claimCounting(stream);
    await waitFor(() => claim.endReason !== null, "terminal state");
    expect(claim.endReason).toBe("cancelled");
    expect(claim.endCount).toBe(1);
    expect(totalBytes(claim.chunks)).toBeLessThan(payload.length);

    // Part B: the convenience pull fails explicitly — the digest verdict
    // is never issued for a transaction that did not complete.
    const err = await puller.pullToVec("res:shortwrite").then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(FetchError);
    expect((err as FetchError).kind).not.toBe("integrity_mismatch");

    await client.transport.disconnect();
    await server.stop();
  }, 20_000);
});
