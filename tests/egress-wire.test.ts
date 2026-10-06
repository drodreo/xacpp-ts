/**
 * Wire-level egress tests: a raw TCP peer (no transport on the receiving
 * side) observes frames, asserting true on-the-wire order and throughput.
 *
 * These complement the transport-level tests: SocketTransport's server side
 * spawns one handler task per request, so handler-observed order is subject
 * to task scheduling; wire order is the actual invariant the egress
 * guarantees.
 *
 * Aligned with xacpp-rs tests/egress_wire_tests.rs.
 */

import { describe, it, expect } from "vitest";
import * as net from "node:net";
import * as readline from "node:readline";

import { genericResponse } from "../src/message";
import type { XacppRequest } from "../src/message";
import { newActivityEvent, newEvent } from "../src/events";
import { SocketTransport } from "../src/socket-transport";
import type { RequestHandler } from "../src/transport";

// ---- Helpers ----

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

interface RawPeer {
  /** Resolves once the listener is bound and `port` is valid. */
  ready: Promise<void>;
  /** Resolves once the peer connection is accepted (collected becomes valid). */
  connected: Promise<void>;
  /** Resolves once `expected` frames are collected (or the connection closes). */
  collected: Promise<number[]>;
  port: number;
  close: () => void;
}

/**
 * Start a raw TCP peer collecting event seqs until `expected` frames arrive
 * (or the connection closes).
 */
function rawSeqPeer(expected: number): RawPeer {
  const listener = net.createServer();
  let port = 0;
  let connected: (() => void) | undefined;
  let collected: Promise<number[]> | undefined;

  const ready = new Promise<void>((resolve) => {
    listener.listen(0, "127.0.0.1", () => {
      port = (listener.address() as net.AddressInfo).port;
      resolve();
    });
  });
  const connectedP = new Promise<void>((resolve) => {
    connected = resolve;
  });

  listener.on("connection", (socket: net.Socket) => {
    const rl = readline.createInterface({ input: socket });
    const seqs: number[] = [];
    collected = new Promise<number[]>((resolve) => {
      rl.on("line", (line: string) => {
        const seq = frameSeq(line);
        if (seq !== null) {
          seqs.push(seq);
          if (seqs.length >= expected) resolve([...seqs]);
        }
      });
      rl.on("close", () => resolve([...seqs]));
    });
    connected?.();
  });

  return {
    ready,
    connected: connectedP,
    get collected() {
      if (!collected) throw new Error("raw peer: no connection yet");
      return collected;
    },
    get port() {
      return port;
    },
    close: () => listener.close(),
  };
}

/** Client transport connected to a raw peer (handler acknowledges anything). */
async function rawClient(port: number): Promise<SocketTransport> {
  const client = SocketTransport.connectTo(port, "127.0.0.1");
  const handler: RequestHandler = async (_sessionId, _payload) => genericResponse("acknowledge", null);
  client.onRequest(handler);
  await client.connect();
  return client;
}

function seqEvent(seq: number): XacppRequest {
  return { kind: "event", payload: newActivityEvent("act-1", newEvent("content_delta", { seq })) };
}

/** Await the peer's collection with a hard timeout (mirrors the rs-side 10s bound). */
async function drain(peer: RawPeer, timeoutMs = 15000): Promise<number[]> {
  return Promise.race([
    peer.collected,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error("wire drain timed out")), timeoutMs),
    ),
  ]);
}

// ---- Tests ----

describe("egress wire-level", () => {
  it("1000 faf events arrive exactly in submission order", async () => {
    const N = 1000;
    const peer = rawSeqPeer(N);
    await peer.ready;
    const client = await rawClient(peer.port);
    await peer.connected;

    for (let seq = 0; seq < N; seq++) {
      client.sendFaf(null, seqEvent(seq));
    }

    const seqs = await drain(peer);
    expect(seqs).toEqual(Array.from({ length: N }, (_, i) => i));

    client.disconnect();
    peer.close();
  }, 30000);

  it("faf issue path is enqueue-only and the wire drain keeps pace", async () => {
    const N = 1000;
    const peer = rawSeqPeer(N);
    await peer.ready;
    const client = await rawClient(peer.port);
    await peer.connected;

    const started = Date.now();
    for (let seq = 0; seq < N; seq++) {
      client.sendFaf(null, seqEvent(seq));
    }
    const issueCost = Date.now() - started;

    const seqs = await drain(peer);
    const drainCost = Date.now() - started;

    expect(seqs.length).toBe(N);
    // Generous bounds: the point is orders of magnitude, not micro-benchmarks.
    expect(issueCost).toBeLessThan(500);
    expect(drainCost).toBeLessThan(5000);

    client.disconnect();
    peer.close();
  }, 30000);

  it("8 concurrent producers lose no frames and duplicate none", async () => {
    const PRODUCERS = 8;
    const PER = 125;
    const N = PRODUCERS * PER;
    const peer = rawSeqPeer(N);
    await peer.ready;
    const client = await rawClient(peer.port);
    await peer.connected;

    await Promise.all(
      Array.from({ length: PRODUCERS }, (_v, p) =>
        (async () => {
          for (let i = 0; i < PER; i++) {
            const seq = p * PER + i;
            client.sendFaf(null, seqEvent(seq));
          }
        })(),
      ),
    );

    const seqs = await drain(peer);
    seqs.sort((a, b) => a - b);
    expect(seqs).toEqual(Array.from({ length: N }, (_, i) => i));

    client.disconnect();
    peer.close();
  }, 30000);
});
