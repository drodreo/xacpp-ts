/**
 * Cross-library interop tests: xacpp-ts client ↔ xacpp-rs peer over real
 * TCP, driven by the `interop_peer` example binary from the xacpp-rs
 * working tree (peer semantics: negotiate/establish echo, generic command
 * echo, event counting on stderr, binary echo, reliability session per
 * connection with SPEC §5 default timings — heartbeats 30 s, silence 90 s).
 *
 * The binary location comes from `XACPP_INTEROP_BIN` (default: a
 * side-by-side `xacpp-rs` checkout's debug example build). When the
 * binary is missing or not executable the whole suite skips — a prepared
 * environment has it built.
 *
 * Scenarios (mission matrix):
 *   a. lifecycle — negotiate/establish/establish_confirm + generic command
 *      echo round-trips, field-by-field (unicode + boundary values);
 *   b. events — faf events counted on the peer's stderr (`EVENT name=x n=y`);
 *   c. binary echo — 2 MiB + irregular tail, bit-exact both ways with
 *      sender confirmation;
 *   d. retransmit across reconnect — a 4 MiB transaction interrupted
 *      mid-flight, then resumed on a fresh transport bound to the same
 *      TransferSession (sender retained copies + peer reassembly),
 *      completing bit-exact with confirmation;
 *   e. heartbeat — the client's first heartbeat hop observed on the wire
 *      through a tee proxy (FrameDecoder-level observation);
 *   f. dirty stream — garbage bytes + a valid unknown-type frame injected
 *      into the peer's stream before real traffic; the peer tolerates both
 *      and keeps working.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import * as fs from "node:fs";
import * as net from "node:net";
import * as readline from "node:readline";

import { SocketTransport } from "../src/carrier/socket";
import { TransferSession } from "../src/transfer";
import { XacppPeer } from "../src/session/peer";
import { XacppSession } from "../src/session/session";
import { createHash } from "node:crypto";
import { FrameDecoder, FrameType, encodeFrame, makeFrame, newTransactionId } from "../src/wire/frame";
import type { Frame } from "../src/wire/frame";
import { genericCommand } from "../src/wire/commands";
import type { XacppCommand } from "../src/wire/commands";
import { acknowledge } from "../src/wire/message";
import type { XacppResponse } from "../src/wire/message";
import type { Capabilities } from "../src/wire/capability";
import { newActivityEvent, newEvent } from "../src/wire/events";
import type { RequestHandler } from "../src/carrier/transport";

// ---- Peer binary plumbing ----

// The rs interop peer binary: override with XACPP_INTEROP_BIN, or build it
// from a side-by-side xacpp-rs checkout (`cargo build --example
// interop_peer`). Tests skip when the binary is absent.
const DEFAULT_INTEROP_BIN = new URL(
  "../../xacpp-rs/target/debug/examples/interop_peer",
  import.meta.url,
).pathname;
const INTEROP_BIN = process.env.XACPP_INTEROP_BIN ?? DEFAULT_INTEROP_BIN;

const BIN_AVAILABLE = (() => {
  try {
    fs.accessSync(INTEROP_BIN, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
})();

interface PeerProcess {
  port: number;
  stderrLines(): string[];
  kill(): Promise<void>;
}

/** Spawn `interop_peer --port 0` and wait for its `READY port=N` line. */
async function spawnPeer(bin: string): Promise<PeerProcess> {
  const child = spawn(bin, ["--port", "0"], { stdio: ["ignore", "ignore", "pipe"] });
  child.stderr!.setEncoding("utf8");

  const lines: string[] = [];
  const port = await new Promise<number>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`interop_peer READY timeout (stderr so far: ${lines.join(" | ")})`));
    }, 10_000);
    // The line listener stays attached for the whole peer lifetime — the
    // tests assert on later lines (EVENT counters, binary echo log).
    const onLine = (line: string) => {
      lines.push(line);
      const m = /READY port=(\d+)/.exec(line);
      if (m) {
        clearTimeout(timer);
        child.off("exit", onExit);
        resolve(Number(m[1]));
      }
    };
    const onExit = (code: number | null) => {
      cleanup();
      reject(new Error(`interop_peer exited early with code ${code}`));
    };
    function cleanup(): void {
      clearTimeout(timer);
      lineReader.off("line", onLine);
      child.off("exit", onExit);
    }
    const lineReader = readline.createInterface({ input: child.stderr! });
    lineReader.on("line", onLine);
    child.on("exit", onExit);
  });

  let killed = false;
  return {
    port,
    stderrLines: () => lines.slice(),
    kill: async () => {
      if (killed || child.exitCode !== null) return;
      killed = true;
      // Attach the exit waiter BEFORE signaling (the process may die between
      // SIGTERM and a later once("exit") registration — a waiter attached
      // after that event fires would never resolve).
      const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
      child.kill("SIGTERM");
      const gone = await Promise.race([exited.then(() => true), new Promise<false>((r) => setTimeout(() => r(false), 500))]);
      if (!gone && child.exitCode === null) child.kill("SIGKILL");
      await Promise.race([exited, new Promise<void>((r) => setTimeout(r, 2_000))]);
    },
  };
}

// ---- Shared helpers ----

function pattern(len: number): Uint8Array {
  const out = new Uint8Array(len);
  for (let i = 0; i < len; i++) out[i] = (i * 31 + (i >> 7)) & 0xff;
  return out;
}

async function settle(turns = 12): Promise<void> {
  for (let i = 0; i < turns; i++) await new Promise((r) => setImmediate(r));
}

async function waitFor(cond: () => boolean, what: string, timeoutMs = 15_000): Promise<void> {
  const start = performance.now();
  while (!cond()) {
    if (performance.now() - start > timeoutMs) throw new Error(`timeout waiting for: ${what}`);
    await settle(4);
  }
}

const ackHandler: RequestHandler = async () => acknowledge();

/** Client transport wired to a fresh transfer session. */
async function connectClient(port: number, senderOpts?: { workingFrameBytes: number }): Promise<{
  client: SocketTransport;
  session: TransferSession;
}> {
  const session = new TransferSession(
    senderOpts ? { sender: { workingFrameBytes: senderOpts.workingFrameBytes } } : undefined,
  );
  const client = SocketTransport.connectTo(port, "127.0.0.1");
  client.onRequest(ackHandler);
  await client.connect();
  session.wire(client);
  return { client, session };
}

/** Tee proxy: sits between the client and the peer, decoding one direction for wire observation. */
interface TeeProxy {
  port: number;
  framesSeen(): Frame[];
  injectBeforePiping(bytes: Uint8Array): void;
  close(): Promise<void>;
}

async function startTeeProxy(peerPort: number): Promise<TeeProxy> {
  const listener = net.createServer();
  await new Promise<void>((resolve) => listener.listen(0, "127.0.0.1", resolve));
  const addr = listener.address() as net.AddressInfo;
  const seen: Frame[] = [];
  let injection: Uint8Array | null = null;

  listener.on("connection", (clientSock) => {
    const upstream = net.connect(peerPort, "127.0.0.1");
    const clientToPeer = new FrameDecoder();
    clientSock.on("data", (chunk: Buffer) => {
      try {
        seen.push(...clientToPeer.push(new Uint8Array(chunk)));
      } catch {
        /* observational decode only */
      }
      upstream.write(chunk);
    });
    upstream.on("data", (chunk: Buffer) => {
      clientSock.write(chunk);
    });
    const fail = () => {
      clientSock.destroy();
      upstream.destroy();
    };
    clientSock.on("error", fail);
    upstream.on("error", fail);
    upstream.on("connect", () => {
      if (injection) upstream.write(Buffer.from(injection));
    });
  });

  return {
    port: addr.port,
    framesSeen: () => seen.slice(),
    injectBeforePiping: (bytes) => {
      injection = bytes;
    },
    close: () =>
      new Promise((resolve) => {
        listener.close(() => resolve());
      }),
  };
}

const INTEROP_CAPS: Capabilities = {
  commands: [{ name: "new_activity" }, { name: "invoke_activity" }],
  produceEvents: [{ name: "content_delta" }, { name: "complete" }],
  acceptEvents: [],
  // This harness exercises the fetch family (SPEC §8.7): the declaration
  // is the application's own decision, made here by the test harness.
  fetchExt: 1,
};

// ---- Suite ----

describe.skipIf(!BIN_AVAILABLE)("xacpp interop — xacpp-ts client ↔ xacpp-rs peer", () => {
  let peer: PeerProcess;

  beforeAll(async () => {
    peer = await spawnPeer(INTEROP_BIN);
  }, 20_000);

  afterAll(async () => {
    await peer.kill();
  });

  // a. lifecycle: negotiate → establish → generic command echo.
  it("a_lifecycle_negotiate_establish_generic_echo_roundtrip", async () => {
    const client = SocketTransport.connectTo(peer.port, "127.0.0.1");
    client.onRequest(ackHandler);
    await client.connect();

    // Negotiate: the peer echoes the initiator's capabilities verbatim.
    // (The rs serde skips empty arrays on the wire — compare semantically.)
    const negotiated = await client.send(null, {
      kind: "command",
      payload: { negotiate: { capabilities: INTEROP_CAPS } },
    });
    expect(negotiated.kind).toBe("negotiated");
    const echoedCaps = (negotiated as { kind: "negotiated"; capabilities: Capabilities }).capabilities;
    expect(echoedCaps.commands).toEqual(INTEROP_CAPS.commands);
    expect(echoedCaps.produceEvents).toEqual(INTEROP_CAPS.produceEvents);
    expect(echoedCaps.acceptEvents ?? []).toEqual(INTEROP_CAPS.acceptEvents);

    // Establish (no credentials): peer answers with its fixed identity.
    const established = await client.send(null, {
      kind: "command",
      payload: { establish: {} },
    });
    expect(established).toEqual({ kind: "established", sessionId: "interop", credentials: "interop-creds" });

    // Establish with credentials: same fixed identity (peer ignores them).
    const established2 = await client.send(null, {
      kind: "command",
      payload: { establish: { credentials: "something-secret" } },
    });
    expect(established2).toEqual({ kind: "established", sessionId: "interop", credentials: "interop-creds" });

    // EstablishConfirm.
    const confirmed = await client.send(null, { kind: "command", payload: "establish_confirm" });
    expect(confirmed).toEqual({ kind: "established", sessionId: "interop", credentials: "interop-creds" });

    // Generic echo: plain, unicode, boundary values — field by field.
    const echoSamples: unknown[] = [
      null,
      { nested: { list: [1, 2, 3], flag: true } },
      { unicode: "你好世界 🌟🚀 — éviter les pièges" },
      { big: 9007199254740991, neg: -9007199254740991, small: 0.5 },
      [1, "two", null, { four: 4 }],
    ];
    for (const [i, args] of echoSamples.entries()) {
      const name = `echo-${i}`;
      const response = await client.send(null, {
        kind: "command",
        payload: genericCommand(name, args),
      });
      expect(response).toEqual({ kind: "generic", name, data: args });
    }

    await client.disconnect();
  }, 30_000);

  // b. events: faf events counted on the peer's stderr.
  it("b_event_delivery_counted_on_peer_stderr", async () => {
    const client = SocketTransport.connectTo(peer.port, "127.0.0.1");
    client.onRequest(ackHandler);
    await client.connect();

    const marker = `evt-${Date.now().toString(36)}`;
    const names = [`${marker}-a`, `${marker}-b`, `${marker}-c`, `${marker}-d`, `${marker}-e`];
    for (const [i, name] of names.entries()) {
      await client.send(null, {
        kind: "event",
        payload: newActivityEvent("act-interop", newEvent(name, { i })),
      });
    }

    // The peer prints `EVENT name=<inner name> n=<count>` per received event
    // (count is per-connection, starting at 1 — so the five names carry
    // n=1..5 in arrival order).
    await waitFor(() => {
      const lines = peer.stderrLines();
      return names.every((name) => lines.some((l) => l.startsWith(`EVENT name=${name} `)));
    }, "all five EVENT lines on stderr");

    // Sequence: same-connection counts increase monotonically.
    const lines = peer.stderrLines().filter((l) => l.startsWith("EVENT name=") && l.includes(marker));
    const counts = lines.map((l) => Number(/n=(\d+)$/.exec(l)![1]));
    expect(counts).toEqual([1, 2, 3, 4, 5]);

    await client.disconnect();
  }, 30_000);

  // c. binary echo: 2 MiB + irregular tail, bit-exact with confirmation.
  it("c_binary_echo_2mib_irregular_tail_bit_exact", async () => {
    const { client, session } = await connectClient(peer.port);

    const payload = pattern(2 * 1024 * 1024 + 137); // 3 working frames, 137-byte tail
    const echoed: { transactionId: string; payload: Uint8Array }[] = [];
    session.onBinary = (transactionId, p) => echoed.push({ transactionId, payload: p });

    const completions: { transactionId: string; confirmed: boolean }[] = [];
    const txid = session.sendBinary(payload, (r) => completions.push(r));

    // The peer echoes every fully reassembled binary transaction back.
    await waitFor(() => echoed.length === 1, "echo delivery from peer");
    expect(echoed[0].payload.length).toBe(payload.length);
    expect(Buffer.from(echoed[0].payload).equals(Buffer.from(payload))).toBe(true);

    // Our outbound transaction is confirmed by the peer's final ACK.
    await waitFor(() => completions.some((c) => c.transactionId === txid), "sender confirmation");
    expect(completions.find((c) => c.transactionId === txid)?.confirmed).toBe(true);

    // The peer logs its echo on stderr.
    await waitFor(() => peer.stderrLines().some((l) => l.includes("binary echoed")), "peer echo log line");

    await client.disconnect();
  }, 60_000);

  // d. reconnect after a fully delivered but UNCONFIRMED transaction: the
  // receiver consumed the slot (no tombstones — it cannot distinguish
  // "completed" from "never seen"), so the declaration adjudicates Unknown
  // and the holder closes by ITS OWN adjudication (SPEC §5): every frame
  // was already sent ⇒ CONFIRMED. The zero-progress resume deadlock
  // (Resumed ⇒ swallowing slot) is retired; the transfer closes in
  // bounded time with the confirmation terminal.
  it("d_reconnect_with_lost_final_ack_confirms_by_holder_side_adjudication", async () => {
    const senderSession = new TransferSession({
      sender: { workingFrameBytes: 256 * 1024, initialTimeoutMs: 300, maxTimeoutMs: 5_000 },
    });
    const client1 = SocketTransport.connectTo(peer.port, "127.0.0.1");
    client1.onRequest(ackHandler);
    await client1.connect();
    senderSession.wire(client1);

    const payload = pattern(4 * 1024 * 1024); // 16 working frames
    const completions: { transactionId: string; confirmed: boolean }[] = [];
    const txid = senderSession.sendBinary(payload, (r) => completions.push(r));

    // Kill the connection immediately after submitting: the egress flushes
    // the queued frames (the peer receives the full transaction), but the
    // peer's final ACK comes back to a dead socket — no confirmation ever
    // arrives, so the unconfirmed copy must survive the reconnect.
    await client1.disconnect();
    expect(completions, "no confirmation before reconnect").toHaveLength(0);
    expect(senderSession.copyCount).toBeGreaterThan(0); // unconfirmed copy retained

    // Same session wired into a fresh transport: the holder declares, the
    // peer no longer holds the slot (delivered = consumed), the verdict is
    // Unknown, and the copy closes CONFIRMED by holder-side adjudication
    // (fully sent).
    const client2 = SocketTransport.connectTo(peer.port, "127.0.0.1");
    client2.onRequest(ackHandler);
    await client2.connect();
    senderSession.wire(client2);

    await waitFor(() => completions.length === 1, "completion after reconnect", 30_000);
    expect(completions[0]).toEqual({ transactionId: txid, confirmed: true });
    expect(senderSession.copyCount).toBe(0);

    await client2.disconnect();
  }, 60_000);

  // e. heartbeat: the client's first hop observed on the wire via the tee.
  it("e_heartbeat_first_hop_observed_on_wire", async () => {
    const proxy = await startTeeProxy(peer.port);
    try {
      const { client } = await connectClient(proxy.port);
      // Binding clock ticks every 100 ms; the session's first tick emits the
      // first heartbeat hop immediately (peer gains liveness instantly).
      await waitFor(
        () => proxy.framesSeen().some((f) => f.frameType === FrameType.HEARTBEAT),
        "client→peer heartbeat frame on the wire",
      );

      // The connection stays alive with real traffic flowing afterwards.
      const response = await client.send(null, {
        kind: "command",
        payload: genericCommand("post-heartbeat", null),
      });
      expect(response).toEqual({ kind: "generic", name: "post-heartbeat", data: null });

      await client.disconnect();
    } finally {
      await proxy.close();
    }
  }, 30_000);

  // f. dirty stream: garbage + a valid unknown-type frame before real
  // traffic — the peer's decoder tolerates both and keeps working.
  it("f_dirty_stream_garbage_and_unknown_frame_tolerated", async () => {
    const proxy = await startTeeProxy(peer.port);
    // Injection: unaligned garbage (no magic — decoder skips it), then a
    // fully valid unknown-type frame (CRC correct — decoded and ignored,
    // SPEC §9), then more garbage. None of it may kill or derail the peer.
    const garbage1 = Buffer.from("hello there, no magic in here\n\r\x00\x01\x02");
    const unknownFrame = encodeFrame(makeFrame(0x7fff, pattern(48)));
    const garbage2 = Buffer.alloc(64, 0xee);
    const injection = Buffer.concat([garbage1, Buffer.from(unknownFrame), garbage2]);
    proxy.injectBeforePiping(new Uint8Array(injection));

    try {
      const { client, session } = await connectClient(proxy.port);

      // Negotiate + generic echo work as if nothing happened.
      const negotiated = await client.send(null, {
        kind: "command",
        payload: { negotiate: { capabilities: INTEROP_CAPS } },
      });
      expect(negotiated.kind).toBe("negotiated");

      const echoed = await client.send(null, {
        kind: "command",
        payload: genericCommand("after-dirt", { ok: true }),
      });
      expect(echoed).toEqual({ kind: "generic", name: "after-dirt", data: { ok: true } });

      // And the binary channel on the same tolerated connection still works.
      const payload = pattern(64 * 1024);
      const echoedBinary: Uint8Array[] = [];
      session.onBinary = (_txid, p) => echoedBinary.push(p);
      session.sendBinary(payload);
      await waitFor(() => echoedBinary.length === 1, "binary echo after dirty stream");
      expect(Buffer.from(echoedBinary[0]).equals(Buffer.from(payload))).toBe(true);

      await client.disconnect();
    } finally {
      await proxy.close();
    }
  }, 30_000);
});

// ---- Fetch channel interop (SPEC §8) ----
//
// PENDING 联调: the rs side of the fetch channel is being implemented in
// parallel (worktree: xacpp-rs-fetch). These cases assume the rs
// `interop_peer` example gains two fetch modes (proposed contract — adjust
// at联调 time if the rs side lands differently):
//   --fetch-serve <dir> : answer fetch(uri) with the bytes of <dir>/<uri>
//                         (mime "application/octet-stream");
//   --fetch-pull <uri>  : after the ts side establishes, the peer issues
//                         fetch(uri) back and prints
//                         `FETCHED uri=<uri> sha256=<hex> bytes=<n>` on stderr.
// The binary keeps the READY port=N stderr convention. Point
// XACPP_INTEROP_FETCH_BIN at a fetch-capable build; the suite skips
// otherwise.

const INTEROP_FETCH_BIN = process.env.XACPP_INTEROP_FETCH_BIN;
const FETCH_BIN_AVAILABLE = (() => {
  if (!INTEROP_FETCH_BIN) return false;
  try {
    fs.accessSync(INTEROP_FETCH_BIN, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
})();

function sha256Hex(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/** Full Peer+Session client stack (fetch needs the session layer, not just a transfer). */
async function connectFetchStack(
  port: number,
  onCommandOverride?: (command: XacppCommand) => Promise<XacppResponse>,
): Promise<{
  transport: SocketTransport;
  session: XacppSession;
}> {
  const transport = SocketTransport.connectTo(port, "127.0.0.1");
  const session = new XacppSession();
  const peer = new XacppPeer(
    INTEROP_CAPS,
    transport,
    { onNegotiate: async () => {} },
    {
      onEstablish: async () => {
        throw new Error("client side never accepts establish");
      },
      onEstablishConfirm: async () => {
        throw new Error("unused");
      },
    },
    session,
  );
  await peer.connect();
  await peer.negotiate();
  await peer.establish(
    undefined,
    {
      // Hand-rolled serving exists only when an override is supplied.
      isFetchResponder: onCommandOverride !== undefined,
      onCommand: async (command) => {
        if (onCommandOverride) return onCommandOverride(command);
        return acknowledge();
      },
      onEvent: async () => acknowledge(),
    },
    () => {
      throw new Error("challenge path not used");
    },
  );
  return { transport, session };
}

describe.skipIf(!FETCH_BIN_AVAILABLE)("fetch kernel interop — xacpp-ts ↔ xacpp-rs (PENDING 联调)", () => {
  // rs requests nothing here; it SERVES: ts pulls a fixture file from rs.
  // Stateless kernel: fetch() resolves with the declared metadata only;
  // the bytes are claimed through the stream receive path by txid.
  it("g_ts_fetches_from_rs_serving_peer", async () => {
    const fixtureDir = fs.mkdtempSync("/tmp/xacpp-fetch-interop-");
    const payload = pattern(3 * 1024 * 1024 + 91); // multi-frame + irregular tail
    fs.writeFileSync(`${fixtureDir}/fixture.bin`, payload);

    const serving = await spawnFetchPeer(["--fetch-serve", fixtureDir]);
    try {
      const { transport, session } = await connectFetchStack(serving.port);
      const claims = new Map<string, { chunks: Uint8Array[]; ends: string[] }>();
      session.onBinaryStream((stream) => {
        const claim = { chunks: [] as Uint8Array[], ends: [] as string[] };
        claims.set(stream.transactionId, claim);
        stream.onChunk((chunk) => claim.chunks.push(chunk));
        stream.onEnd((reason) => claim.ends.push(reason));
      });

      const ready = await session.fetch("fixture.bin");
      expect(ready.sha256).toBe(sha256Hex(payload));
      expect(ready.sizeBytes).toBe(payload.length);

      await waitFor(() => claims.get(ready.txid)?.ends.includes("completed") === true, "stream completes", 30_000);
      const claim = claims.get(ready.txid)!;
      expect(Buffer.concat(claim.chunks.map((c) => Buffer.from(c)))).toEqual(Buffer.from(payload));
      await transport.disconnect();
    } finally {
      await serving.kill();
      fs.rmSync(fixtureDir, { recursive: true, force: true });
    }
  }, 60_000);

  // ts SERVES: rs pulls a resource from the ts session's consumer handler,
  // which answers fetch_ready and streams the bytes with the SAME txid
  // (SPEC §8.1/§8.2) through openBinarySink.
  it("h_rs_fetches_from_ts_serving_session", async () => {
    const payload = pattern(1024 * 1024 + 7);
    const pulling = await spawnFetchPeer(["--fetch-pull", "res:fixture"]);
    try {
      const { transport, session } = await connectFetchStack(pulling.port, async (command) => {
        if (typeof command === "object" && "fetch" in command) {
          return serveInteropFetch(session, payload);
        }
        return acknowledge();
      });
      await waitFor(
        () =>
          pulling
            .stderrLines()
            .some((l) => l.startsWith("FETCHED uri=res:fixture ") && l.includes(`sha256=${sha256Hex(payload)}`) && l.includes(`bytes=${payload.length}`)),
        "rs peer fetched the ts-served resource",
        30_000,
      );
      await transport.disconnect();
    } finally {
      await pulling.kill();
    }
  }, 60_000);
});

/** Answer a fetch command with fetch_ready + a same-txid binary stream (SPEC §8.1). */
async function serveInteropFetch(session: XacppSession, payload: Uint8Array): Promise<XacppResponse> {
  const txid = newTransactionId();
  const sink = session.openBinarySink({ totalLen: payload.length }, txid);
  // SPEC §8.4: complete the fetch_ready response before the binary frames —
  // defer the stream one macrotask past the response write.
  setImmediate(() => {
    void sink
      .write(payload)
      .then(() => sink.finish())
      .catch(() => {});
  });
  return { kind: "fetch_ready", txid, sha256: sha256Hex(payload), sizeBytes: payload.length, mime: "application/octet-stream" };
}

/** spawnPeer variant carrying extra argv (fetch modes) before --port. */
async function spawnFetchPeer(extraArgs: string[]): Promise<PeerProcess> {
  const child = spawn(INTEROP_FETCH_BIN!, [...extraArgs, "--port", "0"], { stdio: ["ignore", "ignore", "pipe"] });
  child.stderr!.setEncoding("utf8");
  const lines: string[] = [];
  const port = await new Promise<number>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`fetch interop_peer READY timeout (stderr so far: ${lines.join(" | ")})`)), 10_000);
    const lineReader = readline.createInterface({ input: child.stderr! });
    lineReader.on("line", (line: string) => {
      lines.push(line);
      const m = /READY port=(\d+)/.exec(line);
      if (m) {
        clearTimeout(timer);
        resolve(Number(m[1]));
      }
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`fetch interop_peer exited early with code ${code}`));
    });
  });
  let killed = false;
  return {
    port,
    stderrLines: () => lines.slice(),
    kill: async () => {
      if (killed || child.exitCode !== null) return;
      killed = true;
      const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
      child.kill("SIGTERM");
      const gone = await Promise.race([exited.then(() => true), new Promise<false>((r) => setTimeout(() => r(false), 500))]);
      if (!gone && child.exitCode === null) child.kill("SIGKILL");
      await Promise.race([exited, new Promise<void>((r) => setTimeout(r, 2_000))]);
    },
  };
}
