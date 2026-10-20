/**
 * Transfer session integration tests — transport level.
 *
 * Two SocketTransports (client + server mode) over real localhost TCP,
 * each wired to its own TransferSession, exercising the full wire
 * path: DATA_BINARY framing, ACK/NACK/RESUME control frames, heartbeat,
 * completion callbacks, and coexistence with the envelope channel.
 *
 * Time: `setInterval` / `clearInterval` / `Date` are faked so the
 * binding's clock is deterministic; real socket IO stays real
 * (`setTimeout` / `setImmediate` / `performance` are left untouched) —
 * IO waits poll with real time, clock advances go through vitest.
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import * as net from "node:net";

import { SocketTransport } from "../src/carrier/socket";
import { TransferSession } from "../src/transfer";
import type { BinarySendResult } from "../src/transfer";
import { newActivityEvent, newEvent } from "../src/wire/events";
import { genericResponse } from "../src/wire/message";
import type { RequestHandler } from "../src/carrier/transport";

// ---- Helpers ----

function filler(length: number, seed = 0x9e37): Uint8Array {
  const out = new Uint8Array(length);
  let s = seed >>> 0;
  for (let i = 0; i < length; i++) {
    s = (s * 1664525 + 1013904223) >>> 0;
    out[i] = s & 0xff;
  }
  return out;
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

interface Delivered {
  transactionId: string;
  payload: Uint8Array;
}

/** Receiver-side sink. */
function sink(): { delivered: Delivered[] } {
  const delivered: Delivered[] = [];
  return { delivered };
}

/** Accept loop: one SocketTransport per connection, all sharing one wired session. */
function startServer(session: TransferSession): {
  port: () => number;
  stop: () => void;
  current: () => SocketTransport | null;
  connectedCount: () => number;
} {
  const listener = net.createServer();
  let port = 0;
  let current: SocketTransport | null = null;
  let connectedCount = 0;
  listener.on("connection", (socket: net.Socket) => {
    const transport = new SocketTransport(socket);
    const handler: RequestHandler = async (_sessionId, _payload) => genericResponse("acknowledge", null);
    transport.onRequest(handler);
    current = transport;
    transport
      .connect()
      .then(() => {
        session.wire(transport);
        connectedCount++;
      })
      .catch(() => {});
  });
  listener.listen(0, "127.0.0.1", () => {
    port = (listener.address() as net.AddressInfo).port;
  });
  return {
    port: () => port,
    stop: () => listener.close(),
    current: () => current,
    connectedCount: () => connectedCount,
  };
}

/** Client transport, connected then wired to the given session. */
async function connectClient(port: number, session: TransferSession): Promise<SocketTransport> {
  const client = SocketTransport.connectTo(port, "127.0.0.1");
  await client.connect();
  session.wire(client);
  return client;
}

afterEach(() => {
  vi.useRealTimers();
});

// ---- Tests ----

describe("transfer session — large content end to end", () => {
  it("delivers a 1.5 MiB payload bit-exact and confirms on the sender", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });

    const { delivered } = sink();
    const sessionServer = new TransferSession();
    sessionServer.onBinary = (transactionId, payload) => delivered.push({ transactionId, payload });

    const server = startServer(sessionServer);
    await waitFor(() => server.port() !== 0, "server listen");

    const sessionClient = new TransferSession();
    const completions: BinarySendResult[] = [];
    const client = await connectClient(server.port(), sessionClient);
    await waitFor(() => server.connectedCount() >= 1, "server side connected");

    const payload = filler(1024 * 1024 + 512 * 1024);
    const txid = sessionClient.sendBinary(payload, (r) => completions.push(r));

    await waitFor(() => delivered.length === 1, "receiver delivery");
    expect(delivered[0].transactionId).toBe(txid);
    expectBytesEqual(delivered[0].payload, payload);

    // Final ACK flows back on the wire and releases the copy.
    await waitFor(() => completions.length === 1, "sender completion");
    expect(completions[0]).toEqual({ transactionId: txid, confirmed: true });
    expect(sessionClient.copyCount).toBe(0);
    expect(sessionServer.pendingCount).toBe(0);

    await client.disconnect();
    server.stop();
  }, 30_000);
});

describe("transfer session — RESUME across connections", () => {
  it("completes a transfer after the sender's connection is replaced mid-flight", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });

    const { delivered } = sink();
    const sessionServer = new TransferSession();
    sessionServer.onBinary = (transactionId, payload) => delivered.push({ transactionId, payload });

    const server = startServer(sessionServer);
    await waitFor(() => server.port() !== 0, "server listen");

    const sessionClient = new TransferSession();
    const client1 = await connectClient(server.port(), sessionClient);
    await waitFor(() => server.connectedCount() >= 1, "server side connected");

    // Multi-frame transfer (4 × 1 MiB); let the first frame land, then kill the connection.
    const payload = filler(4 * 1024 * 1024);
    const txid = sessionClient.sendBinary(payload);
    await waitFor(() => sessionServer.pendingCount === 1, "receiver slot created");

    await client1.disconnect();
    await settle(); // server-side reader observes the close; the slot persists

    // Real reconnect: the same session wired into a fresh transport — on
    // wire the holder declares its pending send (RESUME), the receiver
    // adjudicates it still open, and the verdict re-arms retransmission.
    const client2 = await connectClient(server.port(), sessionClient);
    await waitFor(() => server.connectedCount() >= 2, "replacement server transport");
    await settle();

    await waitFor(() => delivered.length === 1, "delivery after resume", 20_000);
    expect(delivered[0].transactionId).toBe(txid);
    expectBytesEqual(delivered[0].payload, payload);

    await client2.disconnect();
    server.stop();
  }, 40_000);
});

describe("transfer session — envelope channel coexistence", () => {
  it("send/sendFaf keep working alongside a wired transfer session", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });

    const sessionServer = new TransferSession();
    const server = startServer(sessionServer);
    await waitFor(() => server.port() !== 0, "server listen");

    const sessionClient = new TransferSession();
    const client = await connectClient(server.port(), sessionClient);
    await waitFor(() => server.connectedCount() >= 1, "server side connected");

    const response = await client.send(null, {
      kind: "event",
      payload: newActivityEvent("act-1", newEvent("content_delta", { seq: 0 })),
    });
    expect(response.kind).toBe("generic");

    client.sendFaf(null, {
      kind: "event",
      payload: newActivityEvent("act-1", newEvent("content_delta", { seq: 1 })),
    });
    await settle();

    await client.disconnect();
    server.stop();
  }, 30_000);
});

describe("transfer session — heartbeats keep the connection alive", () => {
  it("survives 95 s of idle when both sides send heartbeats", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });

    const sessionServer = new TransferSession();
    const server = startServer(sessionServer);
    await waitFor(() => server.port() !== 0, "server listen");

    const sessionClient = new TransferSession();
    const client = await connectClient(server.port(), sessionClient);
    await waitFor(() => server.connectedCount() >= 1, "server side connected");

    let clientClosed = 0;
    let serverClosed = 0;
    client.onClosed(() => clientClosed++);
    server.current()!.onClosed(() => serverClosed++);

    // 95 s of clock: heartbeats every 30 s per side flow over the real wire.
    await vi.advanceTimersByTimeAsync(95_000);
    await settle();

    expect(clientClosed).toBe(0);
    expect(serverClosed).toBe(0);

    await client.disconnect();
    server.stop();
  }, 30_000);
});

describe("transfer session — session-carried sending", () => {
  it("sendBinary works with a zero-configuration session right after wire", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });

    const { delivered } = sink();
    const sessionServer = new TransferSession();
    sessionServer.onBinary = (transactionId, payload) => delivered.push({ transactionId, payload });

    const server = startServer(sessionServer);
    await waitFor(() => server.port() !== 0, "server listen");

    // Zero-configuration session, wired to a fresh transport.
    const sessionClient = new TransferSession();
    const client = SocketTransport.connectTo(server.port(), "127.0.0.1");
    await client.connect();
    sessionClient.wire(client);
    await waitFor(() => server.connectedCount() >= 1, "server side connected");

    const payload = filler(256 * 1024);
    const completions: BinarySendResult[] = [];
    const txid = sessionClient.sendBinary(payload, (r) => completions.push(r));

    await waitFor(() => delivered.length === 1, "receiver delivery");
    expect(delivered[0].transactionId).toBe(txid);
    expectBytesEqual(delivered[0].payload, payload);
    await waitFor(() => completions.length === 1, "sender completion");
    expect(completions[0]).toEqual({ transactionId: txid, confirmed: true });

    await client.disconnect();
    server.stop();
  }, 30_000);

  it("session.sendBinary sends directly from the session handle", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });

    const { delivered } = sink();
    const sessionServer = new TransferSession();
    sessionServer.onBinary = (transactionId, payload) => delivered.push({ transactionId, payload });

    const server = startServer(sessionServer);
    await waitFor(() => server.port() !== 0, "server listen");

    const sessionClient = new TransferSession();
    const client = await connectClient(server.port(), sessionClient);
    await waitFor(() => server.connectedCount() >= 1, "server side connected");

    const payload = filler(128 * 1024);
    const completions: BinarySendResult[] = [];
    const txid = sessionClient.sendBinary(payload, (r) => completions.push(r));

    await waitFor(() => delivered.length === 1, "receiver delivery");
    expect(delivered[0].transactionId).toBe(txid);
    expectBytesEqual(delivered[0].payload, payload);
    await waitFor(() => completions.length === 1, "sender completion");
    expect(completions[0]).toEqual({ transactionId: txid, confirmed: true });

    await client.disconnect();
    server.stop();
  }, 30_000);

  it("unregistered onBinary: final ACK still flows, payload retained until registration", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });

    const { delivered } = sink();
    // Server side carries a session but no onBinary handler yet.
    const sessionServer = new TransferSession();

    const server = startServer(sessionServer);
    await waitFor(() => server.port() !== 0, "server listen");

    const sessionClient = new TransferSession();
    const client = await connectClient(server.port(), sessionClient);
    await waitFor(() => server.connectedCount() >= 1, "server side connected");

    const payload = filler(64 * 1024);
    const completions: BinarySendResult[] = [];
    const txid = sessionClient.sendBinary(payload, (r) => completions.push(r));

    // Final ACK attests reassembly completion — sent regardless of handler presence.
    await waitFor(() => completions.length === 1, "final ACK without handler");
    expect(completions[0]).toEqual({ transactionId: txid, confirmed: true });
    // Payload is retained, not dropped.
    await settle();
    expect(sessionServer.pendingCount).toBe(1);
    expect(delivered.length).toBe(0);

    // Late registration delivers the retained payload exactly once.
    sessionServer.onBinary = (transactionId, p) => delivered.push({ transactionId, payload: p });
    await settle();
    expect(delivered.length).toBe(1);
    expect(delivered[0].transactionId).toBe(txid);
    expectBytesEqual(delivered[0].payload, payload);
    expect(sessionServer.pendingCount).toBe(0);

    await client.disconnect();
    server.stop();
  }, 30_000);
});
