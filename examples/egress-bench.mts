/**
 * Egress throughput / ordering benchmark over a real TCP socket pair.
 *
 * Mirrors the wire-level form of `tests/egress-wire.test.ts` and the rs-side
 * `examples/egress_bench.rs`: the receiving side is a raw TCP peer (no xacpp
 * transport), so what is measured is the true on-the-wire behaviour of the
 * ordered egress queue + fire-and-forget send. The default scale of 40k
 * events matches the typical stream length previously captured in
 * high-latency scenarios.
 *
 * Usage (build first — the example imports the dist bundle):
 * ```text
 * pnpm build && node --experimental-strip-types examples/egress-bench.mts
 * XACPP_BENCH_N=100000 node --experimental-strip-types examples/egress-bench.mts
 * ```
 * or simply: `pnpm bench:egress`
 *
 * Verdict: PASS requires the peer to receive exactly `N` event seqs in
 * strictly increasing order starting at 0 — no loss, no reorder.
 */

import * as net from "node:net";
import * as readline from "node:readline";

import { SocketTransport, newActivityEvent, newEvent } from "../dist/esm/index.mjs";

/** Typical stream length captured in high-latency scenarios (batch-1 baseline). */
const DEFAULT_N = 40_000;
/** Generous wall-clock cap for the whole drain phase (ms). */
const DRAIN_TIMEOUT_MS = 120_000;

function benchN(): number {
  const raw = process.env["XACPP_BENCH_N"];
  if (raw === undefined) return DEFAULT_N;
  const v = Number(raw.trim());
  if (!Number.isInteger(v) || v <= 0) {
    console.error("XACPP_BENCH_N must be a positive integer");
    process.exit(1);
  }
  return v;
}

/** Extract the event sequence number from a raw JSONL frame. */
function frameSeq(line: string): number | null {
  let v: Record<string, unknown>;
  try {
    v = JSON.parse(line);
  } catch {
    return null;
  }
  if (v["type"] !== "request") return null;
  const payload = v["payload"] as Record<string, unknown> | undefined;
  const inner = payload?.["payload"] as Record<string, unknown> | undefined;
  const event = inner?.["event"] as Record<string, unknown> | undefined;
  const data = event?.["data"] as Record<string, unknown> | undefined;
  const seq = data?.["seq"];
  return typeof seq === "number" ? seq : null;
}

interface Collected {
  seqs: number[];
  bytes: number;
}

/**
 * Raw TCP peer collecting event seqs until `expected` frames arrive.
 * Resolves as soon as the Nth frame lands (mirrors the rs collector handle).
 */
function rawSeqPeer(expected: number): { port: number; collected: Promise<Collected>; close: () => void } {
  const listener = net.createServer();
  let port = 0;
  let collected: Promise<Collected> | undefined;

  listener.listen(0, "127.0.0.1", () => {
    port = (listener.address() as net.AddressInfo).port;
  });

  listener.on("connection", (socket: net.Socket) => {
    const rl = readline.createInterface({ input: socket });
    const seqs: number[] = [];
    let bytes = 0;
    collected = new Promise<Collected>((resolve) => {
      rl.on("line", (line: string) => {
        bytes += Buffer.byteLength(line) + 1; // + '\n'
        const seq = frameSeq(line);
        if (seq !== null) {
          seqs.push(seq);
          if (seqs.length >= expected) resolve({ seqs, bytes });
        }
      });
      rl.on("close", () => resolve({ seqs, bytes }));
    });
  });

  return {
    get port() {
      return port;
    },
    get collected() {
      if (!collected) throw new Error("raw peer: no connection yet");
      return collected;
    },
    close: () => listener.close(),
  };
}

/** Client transport connected to a raw peer (the peer never sends requests,
 * so the handler is never invoked; it acknowledges anything just in case). */
async function rawClient(port: number): Promise<InstanceType<typeof SocketTransport>> {
  const client = SocketTransport.connectTo(port, "127.0.0.1");
  client.onRequest(async (_sessionId, _payload) => ({
    kind: "generic",
    name: "acknowledge",
    data: null,
  }));
  await client.connect();
  return client;
}

function seqEvent(seq: number) {
  return { kind: "event" as const, payload: newActivityEvent("act-1", newEvent("content_delta", { seq })) };
}

async function main(): Promise<void> {
  const envSet = process.env["XACPP_BENCH_N"] !== undefined;
  const n = benchN();
  const scaleSource = envSet ? "env:XACPP_BENCH_N" : "default";

  const peer = rawSeqPeer(n);
  // Wait for the listener to bind.
  await new Promise<void>((resolve) => {
    const check = () => (peer.port > 0 ? resolve() : setTimeout(check, 1));
    check();
  });
  const client = await rawClient(peer.port);

  console.log(
    `# xacpp egress bench | events=${n} (${scaleSource}) | transport=tcp socket-pair (raw peer) | path=sendFaf -> ordered egress`,
  );

  const started = Date.now();
  for (let seq = 0; seq < n; seq++) {
    await client.sendFaf(null, seqEvent(seq));
  }
  const issueMs = Date.now() - started;

  const collected = await Promise.race([
    peer.collected,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error("wire drain timed out")), DRAIN_TIMEOUT_MS),
    ),
  ]);
  const drainMs = Date.now() - started;

  const issueRate = n / (issueMs / 1000);
  const throughput = n / (drainMs / 1000);
  const avgFrame = collected.seqs.length === 0 ? 0 : Math.floor(collected.bytes / collected.seqs.length);

  console.log(
    `issue : ${n} events enqueued in ${(issueMs / 1000).toFixed(3)}s (${issueRate.toFixed(0)} events/s, enqueue-only)`,
  );
  console.log(
    `drain : peer received last frame at ${(drainMs / 1000).toFixed(3)}s since issue start (${throughput.toFixed(0)} events/s end-to-end)`,
  );
  console.log(
    `wire  : ${collected.seqs.length} frames on the wire, ${collected.bytes} bytes total (avg ${avgFrame} bytes/frame)`,
  );

  // Correctness: exactly N seqs, strictly increasing from 0 — no loss, no
  // reorder (wire order must equal submission order).
  const received = collected.seqs.length;
  let inOrder = true;
  for (let i = 0; i < collected.seqs.length; i++) {
    if (collected.seqs[i] !== i) {
      inOrder = false;
      break;
    }
  }
  const pass = received === n && inOrder;

  console.log(`verify: received ${received}/${n}, strictly-increasing-from-0=${inOrder}`);
  console.log(`result: ${pass ? "PASS" : "FAIL"}`);

  client.disconnect();
  peer.close();
  process.exit(pass ? 0 : 1);
}

void main();
