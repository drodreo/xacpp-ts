/**
 * Protocol performance benchmark (release acceptance, item D).
 *
 * Mirrors the rs-side protocol bench shape and the repo bench precedent
 * (`examples/egress-bench.mts`): imports the dist bundle, uses
 * `XACPP_BENCH_N` for scale, and prints raw tables. Three groups:
 *
 *   1. Encoding layer — JSONL baseline (stringify/parse + newline framing)
 *      vs binary frame codec (encodeFrame/FrameDecoder + stringify/parse)
 *      over the same envelope set at ~200 B and ~2 KB per envelope.
 *   2. Full chain — real TCP loopback, ts<->ts SocketTransport pair with
 *      reliability bound on both sides:
 *        a) small-message request/response RTT distribution + throughput;
 *        b) one 64 MiB sendBinary end-to-end (MB/s, frame count, bit-exact);
 *        c) 4 concurrent 16 MiB sendBinary aggregate (MB/s, bit-exact).
 *   3. Control-frame insertion latency — HEARTBEAT frames emitted on a
 *      100 ms cadence, timestamped enqueue (heartbeat `senderEpochMs`,
 *      same-process clock) -> peer arrival (passive socket sniffer), with
 *      vs without a bulk binary transfer as background traffic. This
 *      quantifies the egress control-priority discipline: a control frame
 *      queued behind a bulk drain must not wait behind the whole queue.
 *      Sniffing caveat: the passive sniffer decodes a second copy of the
 *      inbound stream (CPU overhead), so bulk-phase throughput is NOT
 *      reported from this setup — latency percentiles only, and the same
 *      sniffer is present in both the idle and bulk phases.
 *
 * Usage:
 * ```text
 * pnpm build && node --experimental-strip-types examples/bench-protocol.mts
 * XACPP_BENCH_N=5000 node --experimental-strip-types examples/bench-protocol.mts
 * ```
 * or simply: `pnpm bench:protocol`
 *
 * Scale env:
 * - `XACPP_BENCH_N`            — envelope count (group 1, per tier) and
 *                                round-trip count (group 2a). Default 10000.
 * - `XACPP_BENCH_BULK_ROUNDS`  — group 3 bulk rounds (default 8). Bulk size
 *                                and working-frame size are fixed (64 MiB,
 *                                8 KiB) — they shape the background, not the
 *                                measured quantity.
 * - `XACPP_BENCH_ONLY`         — comma list of groups to run, e.g. "1,3".
 */

import * as net from "node:net";
import { randomBytes } from "node:crypto";

import {
  SocketTransport,
  TransferSession,
  FrameDecoder,
  FrameType,
  encodeFrame,
  makeFrame,
  decodeHeartbeat,
  acknowledge,
  newActivityEvent,
  newEvent,
} from "../dist/esm/index.js";
import type {
  Frame,
  XacppEnvelope,
  XacppTransport,
  TransferConfig,
} from "../dist/esm/index.js";

// ---- scale / config ----

const DEFAULT_N = 10_000;
const BULK_BYTES = 64 * 1024 * 1024; // group 2b payload
const CONCURRENT_SENDS = 4; // group 2c
const CONCURRENT_BYTES = 16 * 1024 * 1024; // group 2c per-send payload
const CTRL_BULK_BYTES = 64 * 1024 * 1024; // group 3 background per round
const CTRL_WORKING_FRAME_BYTES = 8 * 1024; // group 3 background frame size
const CTRL_BULK_ROUNDS_DEFAULT = 8;
const PHASE_CAP_MS = 180_000;

function intEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const v = Number(raw.trim());
  if (!Number.isInteger(v) || v <= 0) {
    console.error(`${name} must be a positive integer`);
    process.exit(1);
  }
  return v;
}

const N = intEnv("XACPP_BENCH_N", DEFAULT_N);
const BULK_ROUNDS = intEnv("XACPP_BENCH_BULK_ROUNDS", CTRL_BULK_ROUNDS_DEFAULT);
const ONLY = (process.env["XACPP_BENCH_ONLY"] ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
function groupEnabled(g: string): boolean {
  return ONLY.length === 0 || ONLY.includes(g);
}

/** One-time correlation between the wall clock (heartbeat senderEpochMs) and
 * the monotonic clock used for arrival timestamps — same process/thread. */
const CLOCK_OFFSET = Date.now() - performance.now();

// ---- small helpers ----

const enc = new TextEncoder();
const dec = new TextDecoder();

function fmt(n: number): string {
  return n.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return NaN;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

function latencyLine(label: string, samples: number[]): string {
  const sorted = [...samples].sort((a, b) => a - b);
  const mean = samples.reduce((a, b) => a + b, 0) / samples.length;
  return `${label}: n=${samples.length}  p50=${pct(sorted, 50).toFixed(3)}ms  p95=${pct(sorted, 95).toFixed(3)}ms  p99=${pct(sorted, 99).toFixed(3)}ms  mean=${mean.toFixed(3)}ms`;
}

function median(nums: number[]): number {
  const s = [...nums].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
}

function withCap<T>(p: Promise<T>, label: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${PHASE_CAP_MS}ms`)), PHASE_CAP_MS),
    ),
  ]);
}

/** Non-null unwrap without the `!` assertion — needed where a mutable let is
 * assigned from a callback and control-flow narrowing cannot follow. */
function unwrapOrThrow<T>(v: T | null, msg: string): T {
  if (v === null) throw new Error(msg);
  return v;
}

function mb(bytes: number): number {
  return bytes / (1024 * 1024);
}

// ---- envelope builders ----

/** Request envelope carrying one activity event, padded to ~`target` bytes. */
function padEnvelope(seq: number, target: number): Extract<XacppEnvelope, { type: "request" }> {
  const base = newActivityEvent("bench-act", newEvent("content_delta", { seq, pad: "" }));
  const env: XacppEnvelope = { type: "request", id: `r${seq}`, payload: { kind: "event", payload: base } };
  const baseLen = JSON.stringify(env).length;
  const padLen = Math.max(0, target - baseLen);
  const padded = newActivityEvent("bench-act", newEvent("content_delta", { seq, pad: "x".repeat(padLen) }));
  return { type: "request", id: `r${seq}`, payload: { kind: "event", payload: padded } };
}

// =====================================================================
// Group 1 — encoding layer: JSONL baseline vs binary frame codec
// =====================================================================

interface CodecTiming {
  encodeMs: number;
  decodeMs: number;
  wireBytes: number;
}

function jsonlWire(envelopes: XacppEnvelope[]): Buffer {
  const parts: string[] = [];
  let bytes = 0;
  for (const e of envelopes) {
    const line = JSON.stringify(e);
    bytes += Buffer.byteLength(line) + 1;
    parts.push(line);
  }
  parts.push("");
  const buf = Buffer.from(parts.join("\n"), "utf8");
  if (buf.length !== bytes) throw new Error("jsonl wire size mismatch");
  return buf;
}

function binaryWire(envelopes: XacppEnvelope[]): { buf: Buffer; bytes: number } {
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  for (const e of envelopes) {
    const frame = encodeFrame(makeFrame(FrameType.DATA_JSON, enc.encode(JSON.stringify(e))));
    bytes += frame.length;
    chunks.push(frame);
  }
  return { buf: Buffer.concat(chunks.map((c) => Buffer.from(c))), bytes };
}

function benchJsonl(envelopes: XacppEnvelope[], wire: Buffer): CodecTiming {
  // encode: stringify + newline semantics
  let t0 = performance.now();
  let bytes = 0;
  for (const e of envelopes) {
    bytes += Buffer.byteLength(JSON.stringify(e)) + 1;
  }
  const encodeMs = performance.now() - t0;
  // decode: line split + parse (wire buffer prebuilt outside timing)
  t0 = performance.now();
  const lines = wire.toString("utf8").split("\n");
  let count = 0;
  for (const line of lines) {
    if (line.length === 0) continue;
    const env = JSON.parse(line) as XacppEnvelope;
    if (env.type !== "request") throw new Error("jsonl decode: wrong envelope");
    count++;
  }
  const decodeMs = performance.now() - t0;
  if (count !== envelopes.length) throw new Error("jsonl decode: lost envelopes");
  return { encodeMs, decodeMs, wireBytes: bytes };
}

/** Decode wire bytes through the frame decoder, fed in `chunkSize` slices —
 * mirrors how the transport actually consumes a TCP stream (kernel-sized
 * reads). Feeding one giant chunk instead degrades quadratically (see the
 * consume() compaction note in the report) — measured separately as a
 * mechanism footnote. */
function benchBinary(envelopes: XacppEnvelope[], wire: Buffer, chunkSize = 64 * 1024): CodecTiming {
  // encode: stringify + frame codec
  let t0 = performance.now();
  let bytes = 0;
  for (const e of envelopes) {
    bytes += encodeFrame(makeFrame(FrameType.DATA_JSON, enc.encode(JSON.stringify(e)))).length;
  }
  const encodeMs = performance.now() - t0;
  // decode: streaming frame decoder + parse (wire buffer prebuilt outside timing)
  t0 = performance.now();
  const decoder = new FrameDecoder();
  let count = 0;
  for (let off = 0; off < wire.length; off += chunkSize) {
    const frames = decoder.push(wire.subarray(off, Math.min(off + chunkSize, wire.length)));
    for (const f of frames) {
      if (f.frameType !== FrameType.DATA_JSON) throw new Error("binary decode: wrong frame type");
      const env = JSON.parse(dec.decode(f.payload)) as XacppEnvelope;
      if (env.type !== "request") throw new Error("binary decode: wrong envelope");
      count++;
    }
  }
  const decodeMs = performance.now() - t0;
  if (count !== envelopes.length) throw new Error("binary decode: lost envelopes");
  return { encodeMs, decodeMs, wireBytes: bytes };
}

async function group1(): Promise<boolean> {
  console.log("\n=== Group 1 — encoding layer: JSONL baseline vs binary frames ===");
  console.log(`# envelopes: N=${N} per tier (~200 B and ~2 KB), warmup + 3 timed passes (median); binary decode fed in 64 KiB chunks (socket-realistic)`);
  let pass = true;

  for (const target of [200, 2000]) {
    const envelopes: XacppEnvelope[] = [];
    for (let i = 0; i < N; i++) envelopes.push(padEnvelope(i, target));
    const avgEnv = Math.round(JSON.stringify(envelopes[0]).length);

    // warmup + prebuilt wire buffers (decode passes measure pure decode)
    const jw = jsonlWire(envelopes);
    const bw = binaryWire(envelopes);
    benchJsonl(envelopes.slice(0, 1000), jsonlWire(envelopes.slice(0, 1000)));
    benchBinary(envelopes.slice(0, 1000), binaryWire(envelopes.slice(0, 1000)).buf);

    const jRuns: CodecTiming[] = [];
    const bRuns: CodecTiming[] = [];
    for (let i = 0; i < 3; i++) {
      jRuns.push(benchJsonl(envelopes, jw));
      bRuns.push(benchBinary(envelopes, bw.buf));
    }
    const j = jRuns[1];
    const b = bRuns[1];

    const jEnc = N / (j.encodeMs / 1000);
    const jDec = N / (j.decodeMs / 1000);
    const bEnc = N / (b.encodeMs / 1000);
    const bDec = N / (b.decodeMs / 1000);

    console.log(`\n-- tier ~${target} B (actual avg envelope ${avgEnv} B) --`);
    console.log(
      `jsonl  : encode ${fmt(jEnc)} env/s | decode ${fmt(jDec)} env/s | wire ${fmt(j.wireBytes)} B (${Math.round(j.wireBytes / N)} B/env)`,
    );
    console.log(
      `binary : encode ${fmt(bEnc)} env/s | decode ${fmt(bDec)} env/s | wire ${fmt(b.wireBytes)} B (${Math.round(b.wireBytes / N)} B/env, +40 B fixed overhead)`,
    );
    console.log(
      `delta  : encode ${(bEnc / jEnc - 1) >= 0 ? "+" : ""}${((bEnc / jEnc - 1) * 100).toFixed(1)}% | decode ${(bDec / jDec - 1) >= 0 ? "+" : ""}${((bDec / jDec - 1) * 100).toFixed(1)}% | wire +${(((b.wireBytes - j.wireBytes) / j.wireBytes) * 100).toFixed(2)}%`,
    );
    // sanity: per-pass raw timings kept visible
    console.log(
      `raw    : jsonl enc[${jRuns.map((r) => r.encodeMs.toFixed(1)).join(" ")}] dec[${jRuns.map((r) => r.decodeMs.toFixed(1)).join(" ")}]` +
        ` | binary enc[${bRuns.map((r) => r.encodeMs.toFixed(1)).join(" ")}] dec[${bRuns.map((r) => r.decodeMs.toFixed(1)).join(" ")}] ms`,
    );
    // mechanism footnote: single-giant-chunk feed (NOT the transport shape) —
    // consume() compacts the remaining buffer per frame => quadratic total.
    {
      const t = performance.now();
      const d = new FrameDecoder();
      const all = d.push(new Uint8Array(bw.buf));
      for (const f of all) JSON.parse(dec.decode(f.payload));
      const singleMs = performance.now() - t;
      console.log(
        `footnote: binary decode fed as ONE giant chunk: ${singleMs.toFixed(1)} ms (${fmt(N / (singleMs / 1000))} env/s) — consume() compaction is O(remaining)/frame; transport feeds socket-sized chunks, so the chunked number above is the production path`,
      );
    }
  }
  return pass;
}

// =====================================================================
// TCP pair plumbing (group 2 & 3)
// =====================================================================

interface PairOptions {
  /** Register the server-side request handler before connect. */
  serverOnRequest?: Parameters<XacppTransport["onRequest"]>[0];
  /** Client reliability session constructor options. */
  clientSession?: TransferConfig;
  /** Server reliability session constructor options. */
  serverSession?: TransferConfig;
  /** Passive sniffer attached to the accepted socket BEFORE the server
   * transport starts reading (runs first on every chunk). */
  sniffer?: (socket: net.Socket) => void;
}

interface Pair {
  client: InstanceType<typeof SocketTransport>;
  server: InstanceType<typeof SocketTransport>;
  /** Client reliability session — the 1.1 binary send API lives here
   * (`TransferSession.sendBinary`), not on the raw transport. */
  clientSession: InstanceType<typeof TransferSession>;
  /** Complete inbound binary transactions, by txid (both sides). A receiver
   * without a registered onBinary handler retains the completed slot and
   * never emits the final ACK — so the pair always registers collectors. */
  clientBinaries: Map<string, Uint8Array>;
  serverBinaries: Map<string, Uint8Array>;
  close: () => Promise<void>;
}

async function createPair(opts: PairOptions = {}): Promise<Pair> {
  const listener = net.createServer();
  let port = 0;
  let server: InstanceType<typeof SocketTransport> | null = null;
  const clientBinaries = new Map<string, Uint8Array>();
  const serverBinaries = new Map<string, Uint8Array>();

  listener.on("connection", (socket: net.Socket) => {
    opts.sniffer?.(socket);
    const s = new TransferSession(opts.serverSession);
    const t = new SocketTransport(socket);
    if (opts.serverOnRequest) t.onRequest(opts.serverOnRequest);
    s.onBinary = (txid, p) => void serverBinaries.set(txid, p);
    server = t;
    void t.connect().then(() => s.wire(t));
  });
  await new Promise<void>((resolve) => listener.listen(0, "127.0.0.1", resolve));
  port = (listener.address() as net.AddressInfo).port;

  const clientSession = new TransferSession(opts.clientSession);
  const client = SocketTransport.connectTo(port, "127.0.0.1");
  clientSession.onBinary = (txid, p) => void clientBinaries.set(txid, p);
  await client.connect();
  clientSession.wire(client);

  // wait for the server side to finish wiring
  for (let i = 0; i < 2000 && server === null; i++) await new Promise((r) => setTimeout(r, 1));
  // `server` is a mutable let assigned from the listener callback, so a plain
  // null-check narrowing would not carry into the close() closure (and would
  // collapse to `never` here) — pin the non-null value via a const binding.
  const serverTransport = unwrapOrThrow<InstanceType<typeof SocketTransport>>(server, "server transport did not start");

  return {
    client,
    server: serverTransport,
    clientSession,
    clientBinaries,
    serverBinaries,
    close: async () => {
      await client.disconnect();
      await serverTransport.disconnect();
      listener.close();
    },
  };
}

// =====================================================================
// Group 2 — full chain: RTT, 64 MiB single, 4x16 MiB concurrent
// =====================================================================

async function group2(): Promise<boolean> {
  console.log("\n=== Group 2 — full chain: real TCP loopback, ts<->ts, reliability bound ===");
  let pass = true;

  // ---- 2a: sequential request/response RTT ----
  {
    const pair = await createPair({ serverOnRequest: async () => acknowledge() });
    const latencies: number[] = [];
    const req = padEnvelope(0, 200);
    const t0 = performance.now();
    await withCap(
      (async () => {
        for (let i = 0; i < N; i++) {
          const s = performance.now();
          const resp = await pair.client.send(null, req.payload);
          const e = performance.now();
          if (resp.kind !== "generic") throw new Error("rtt: unexpected response");
          latencies.push(e - s);
        }
      })(),
      "group 2a rtt loop",
    );
    const total = performance.now() - t0;
    console.log(`\n-- 2a: request/response RTT (${N} sequential round trips, ~200 B envelopes) --`);
    console.log(latencyLine("rtt   ", latencies));
    console.log(`throughput: ${fmt(N / (total / 1000))} req/s end-to-end (${(total / 1000).toFixed(3)}s total)`);
    await pair.close();
  }

  // ---- 2b: single 64 MiB sendBinary ----
  {
    const pair = await createPair({});
    const payload = randomBytes(BULK_BYTES);
    const expectedFrames = Math.ceil(BULK_BYTES / (1024 * 1024)); // 1 MiB working frames (default)
    const confirmed = new Map<string, boolean>();

    const done = new Promise<void>((resolve, reject) => {
      pair.clientSession.sendBinary(payload, (result) => {
        confirmed.set(result.transactionId, result.confirmed);
        resolve();
      });
      setTimeout(() => reject(new Error("2b: sender onComplete timeout")), PHASE_CAP_MS);
    });
    const recvDone = new Promise<void>((resolve, reject) => {
      const check = () => (pair.serverBinaries.size >= 1 ? resolve() : setTimeout(check, 5));
      check();
      setTimeout(() => reject(new Error("2b: receiver timeout")), PHASE_CAP_MS);
    });

    const t0 = performance.now();
    await withCap(Promise.all([done, recvDone]), "group 2b bulk transfer");
    const ms = performance.now() - t0;
    const txid = [...confirmed.keys()][0];
    const got = pair.serverBinaries.get(txid);
    const exact = got !== undefined && Buffer.compare(Buffer.from(got), payload) === 0;
    const mbs = BULK_BYTES / (1024 * 1024) / (ms / 1000);
    console.log(`\n-- 2b: sendBinary ${mb(BULK_BYTES).toFixed(0)} MiB end-to-end --`);
    console.log(
      `time ${(ms / 1000).toFixed(3)}s | payload ${mbs.toFixed(1)} MiB/s | frames ${expectedFrames} x 1 MiB working | confirmed=${confirmed.get(txid)} | bit-exact=${exact}`,
    );
    if (!exact || !confirmed.get(txid)) pass = false;
    await pair.close();
  }

  // ---- 2c: 4 concurrent 16 MiB sendBinary ----
  {
    const pair = await createPair({});
    const payload = randomBytes(CONCURRENT_BYTES);
    const confirmed = new Map<string, boolean>();

    const t0 = performance.now();
    await withCap(
      Promise.all(
        Array.from({ length: CONCURRENT_SENDS }, () => {
          return new Promise<void>((resolve, reject) => {
            pair.clientSession.sendBinary(payload, (result) => {
              confirmed.set(result.transactionId, result.confirmed);
              resolve();
            });
            setTimeout(() => reject(new Error("2c: sender onComplete timeout")), PHASE_CAP_MS);
          });
        }),
      ),
      "group 2c concurrent bulk",
    );
    const recvDone = new Promise<void>((resolve, reject) => {
      const check = () => (pair.serverBinaries.size >= CONCURRENT_SENDS ? resolve() : setTimeout(check, 5));
      check();
      setTimeout(() => reject(new Error("2c: receiver timeout")), PHASE_CAP_MS);
    });
    await recvDone;
    const ms = performance.now() - t0;

    let exact = confirmed.size === CONCURRENT_SENDS;
    for (const [txid, p] of pair.serverBinaries) {
      if (!confirmed.get(txid) || Buffer.compare(Buffer.from(p), payload) !== 0) exact = false;
    }
    const totalBytes = CONCURRENT_SENDS * CONCURRENT_BYTES;
    const agg = totalBytes / (1024 * 1024) / (ms / 1000);
    console.log(`\n-- 2c: ${CONCURRENT_SENDS} concurrent sendBinary x ${mb(CONCURRENT_BYTES).toFixed(0)} MiB --`);
    console.log(
      `time ${(ms / 1000).toFixed(3)}s | aggregate ${agg.toFixed(1)} MiB/s (${mb(totalBytes).toFixed(0)} MiB total) | confirmed ${confirmed.size}/${CONCURRENT_SENDS} | bit-exact=${exact}`,
    );
    if (!exact) pass = false;
    await pair.close();
  }

  return pass;
}

// =====================================================================
// Group 3 — control-frame insertion latency (heartbeat under bulk)
// =====================================================================

interface HbSink {
  latencies: number[];
  count: () => number;
}

/** Passive HEARTBEAT sniffer on the accepted socket. Timestamps are taken at
 * chunk arrival (before any decode work); latency = arrival - enqueue where
 * enqueue comes from the heartbeat payload's senderEpochMs (sender encodes it
 * inside the tick pump immediately before priority-inserting into egress). */
function attachHeartbeatSniffer(socket: net.Socket): HbSink {
  const latencies: number[] = [];
  const decoder = new FrameDecoder();
  socket.on("data", (chunk: Buffer) => {
    const arrival = performance.now();
    let frames: Frame[];
    try {
      frames = decoder.push(new Uint8Array(chunk));
    } catch {
      return;
    }
    for (const f of frames) {
      if (f.frameType !== FrameType.HEARTBEAT) continue;
      try {
        const hb = decodeHeartbeat(f.payload);
        const enqueuePerf = hb.senderEpochMs - CLOCK_OFFSET;
        if (Number.isFinite(enqueuePerf)) latencies.push(arrival - enqueuePerf);
      } catch {
        /* malformed heartbeat — ignore for bench purposes */
      }
    }
  });
  return { latencies, count: () => latencies.length };
}

async function group3(): Promise<boolean> {
  console.log("\n=== Group 3 — control frame insertion latency (HEARTBEAT @100ms cadence) ===");
  console.log(
    `# background: ${BULK_ROUNDS} rounds x ${mb(CTRL_BULK_BYTES).toFixed(0)} MiB sendBinary @ ${CTRL_WORKING_FRAME_BYTES / 1024} KiB working frames`,
  );

  let sniffer: HbSink | null = null;
  const pair = await createPair({
    // heartbeat every binding tick (100 ms); small working frames keep the
    // bulk drain saturated for the whole background phase
    clientSession: {
      heartbeatIntervalMs: 100,
      sender: { workingFrameBytes: CTRL_WORKING_FRAME_BYTES },
    },
    sniffer: (socket) => {
      sniffer = attachHeartbeatSniffer(socket);
    },
  });
  const sink = sniffer as HbSink | null;
  if (sink === null) throw new Error("group 3: sniffer missing");

  // ---- phase A: idle background ----
  await new Promise((r) => setTimeout(r, 3500));
  const idle = [...sink.latencies];
  console.log(`\n-- idle connection --`);
  console.log(latencyLine("ctrl  ", idle));
  console.log(
    "# zero-point: enqueue/arrival clocks are correlated in-process and drift ±0.5 ms — read the idle median as zero and compare phases by delta",
  );

  // ---- phase B: bulk background ----
  const payload = randomBytes(CTRL_BULK_BYTES);
  for (let round = 0; round < BULK_ROUNDS; round++) {
    await withCap(
      new Promise<void>((resolve, reject) => {
        pair.clientSession.sendBinary(payload, (result) => {
          if (!result.confirmed) reject(new Error("group 3: bulk round released unconfirmed"));
          resolve();
        });
        setTimeout(() => reject(new Error("group 3: bulk round timeout")), PHASE_CAP_MS);
      }),
      `group 3 round ${round}`,
    );
    const recvDone = new Promise<void>((resolve, reject) => {
      const check = () => (pair.serverBinaries.size >= round + 1 ? resolve() : setTimeout(check, 5));
      check();
      setTimeout(() => reject(new Error("group 3: receiver timeout")), PHASE_CAP_MS);
    });
    await recvDone;
  }
  const bulk = sink.latencies.slice(idle.length);
  console.log(`\n-- bulk background (${BULK_ROUNDS} x ${mb(CTRL_BULK_BYTES).toFixed(0)} MiB transferred) --`);
  console.log(latencyLine("ctrl  ", bulk));

  if (idle.length < 5 || bulk.length < 5) {
    console.log(`WARN: thin samples (idle=${idle.length}, bulk=${bulk.length}) — raise XACPP_BENCH_BULK_ROUNDS`);
  }
  await pair.close();
  return true;
}

// ---- main ----

async function main(): Promise<void> {
  console.log(
    `# xacpp protocol bench | N=${N} (XACPP_BENCH_N) | bulk rounds=${BULK_ROUNDS} | node ${process.version}`,
  );

  let pass = true;
  if (groupEnabled("1")) pass = (await group1()) && pass;
  if (groupEnabled("2")) pass = (await group2()) && pass;
  if (groupEnabled("3")) pass = (await group3()) && pass;

  console.log(`\nresult: ${pass ? "PASS" : "FAIL"}`);
  process.exit(pass ? 0 : 1);
}

void main();
