/**
 * Socket (real TCP) recovery tests — SPEC §5 transfer recovery semantics
 * (v1.1.1 revision): disconnect detach, slotless frames, verdict
 * adjudication, declaration retransmission.
 *
 * Real TCP only: the egress-flush window (a dead connection's queued bytes
 * keep arriving at the peer's reader) is the substance these tests pin —
 * see xacpp-tcp-resume-coverage-gap.md. Aligned with
 * xacpp-rs tests/socket_resume_tests.rs.
 */

import { describe, it, expect } from "vitest";
import * as net from "node:net";

import { SocketTransport } from "../src/carrier/socket";
import { TransferSession } from "../src/transfer";
import type { BinarySendResult, BinaryStream, TransferConfig } from "../src/transfer";
import { FrameDecoder, FrameType } from "../src/wire/frame";

const settle = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

async function waitFor(cond: () => boolean, what: string, timeoutMs = 10_000): Promise<void> {
  const start = performance.now();
  while (!cond()) {
    if (performance.now() - start > timeoutMs) throw new Error(`timeout waiting for: ${what}`);
    await settle(4);
  }
}

function pattern(len: number): Uint8Array {
  const out = new Uint8Array(len);
  for (let i = 0; i < len; i++) out[i] = (i * 31 + (i >> 7)) & 0xff;
  return out;
}

function testConfig(): TransferConfig {
  return {
    sender: {
      workingFrameBytes: 1024,
      initialTimeoutMs: Number.MAX_SAFE_INTEGER, // verdicts and disconnects drive everything
      maxTimeoutMs: Number.MAX_SAFE_INTEGER,
      backpressureCapBytes: 64 * 1024 * 1024,
    },
    heartbeatIntervalMs: Number.MAX_SAFE_INTEGER,
    silenceTimeoutMs: 30_000,
    ackBatchFrames: 64,
    ackBatchIntervalMs: 50,
    resumeVerdictTimeoutMs: 300,
  };
}

async function spawnListener(): Promise<{ listener: net.Server; port: number }> {
  const listener = net.createServer();
  await new Promise<void>((resolve) => listener.listen(0, "127.0.0.1", resolve));
  const port = (listener.address() as net.AddressInfo).port;
  return { listener, port };
}

/**
 * One connected transport pair over real TCP.
 *
 * Timing contract: the accept listener MUST be registered before the
 * client connects — Node emits the server's "connection" event once per
 * accepted socket, and an event fired before any listener is registered
 * is lost (the client-side handshake still completes, so connecting
 * first and accepting afterwards hangs forever).
 */
async function connectPair(
  listener: net.Server,
  port: number,
): Promise<{ client: SocketTransport; server: SocketTransport; rawServerSocket: net.Socket }> {
  const accepted = new Promise<net.Socket>((resolve) => listener.once("connection", resolve));
  const client = SocketTransport.connectTo(port, "127.0.0.1");
  await client.connect();
  const rawServerSocket = await accepted;
  const server = new SocketTransport(rawServerSocket);
  await server.connect();
  return { client, server, rawServerSocket };
}

/** Count DATA_BINARY frames arriving on a raw socket (frame-level probe). */
function countBinaryFrames(socket: net.Socket): () => number {
  const decoder = new FrameDecoder();
  let binary = 0;
  socket.on("data", (buf: Buffer) => {
    for (const frame of decoder.push(new Uint8Array(buf))) {
      if (frame.frameType === FrameType.DATA_BINARY) binary += 1;
    }
  });
  return () => binary;
}

/**
 * The receiver-side deterministic disconnect scenario (SPEC §5 disconnect
 * discipline): the receiver's mount detaches at the disconnect instant, so
 * frames that keep arriving afterwards find no mount and are dropped at
 * the frame level (arriving ≠ reassembled). The transfer then closes
 * through the RESUME handshake (`Resumed` — the open incomplete slot from
 * the delivered prefix — lifts the gate; the owed frames flow over the NEW
 * connection). Deterministic — the outcome does not depend on how fast
 * the re-establish is processed (`establishDelay`).
 */
async function runReceiverDisconnectScenario(establishDelay: number): Promise<void> {
  const { listener, port } = await spawnListener();

  const sessionA = new TransferSession(testConfig());
  const sessionB = new TransferSession(testConfig());

  // Receiver delivery surface: the STREAM face — chunk-level delivery as
  // the contiguous prefix advances, so partial absorption is observable
  // (the aggregate `onBinary` face only delivers whole transactions).
  let delivered = 0;
  sessionB.onBinaryStream = (stream: BinaryStream) => {
    stream.onChunk((chunk) => {
      delivered += chunk.length;
    });
  };

  const c1 = await connectPair(listener, port);
  sessionB.wire(c1.server);
  sessionA.wire(c1.client);

  // Delivered prefix while the connection is alive (the receiver's mount
  // is up — these bytes are assembled normally).
  const totalLen = 2 * 1024 * 1024;
  const deliveredPrefix = 256 * 1024;
  const payload = pattern(totalLen);
  const sink = sessionA.openBinarySink({ totalLen });
  await sink.write(payload.subarray(0, deliveredPrefix));
  await settle(300);
  expect(delivered).toBe(deliveredPrefix);

  // THE DISCONNECT: the receiver detaches its transport — per SPEC §5 the
  // transfer mount detaches with it, immediately. Its transport-level
  // listeners are gone, but the raw socket stays observable: a frame-level
  // probe counts what still arrives.
  await c1.server.disconnect();
  const arrivedBinary = countBinaryFrames(c1.rawServerSocket);

  // The full remainder enters the holder's copy and its egress queue while
  // the holder is still wired (the copy must cover the declared total —
  // the verdict-driven retransmission resends exactly these frames).
  await sink.write(payload.subarray(deliveredPrefix));

  // The holder tears its connection down: the egress flushes its queued
  // frames FIFO across the dying link (graceful close — see Egress.close).
  await c1.client.disconnect();

  // The residual frames physically arrive at the peer's dead transport…
  await settle(700);
  expect(arrivedBinary()).toBeGreaterThan(0);
  // …and are dropped at the frame level: no new slot, no reassembly, no
  // delivery — the mount was detached at the disconnect instant.
  expect(sessionB.pendingCount).toBe(1); // the delivered prefix's open slot only
  expect(delivered).toBe(deliveredPrefix);

  // Reconnection: same sessions, fresh transport. The receiver wires
  // first (its processing may be arbitrarily slow — `establishDelay`),
  // then the holder: the RESUME declaration goes out at wire-up.
  const c2 = await connectPair(listener, port);
  sessionB.wire(c2.server);
  await settle(establishDelay);
  sessionA.wire(c2.client);

  // Deterministic closure: `Resumed` ⇒ the owed frames flow over the new
  // connection ⇒ byte-count completion ⇒ final ACK ⇒ confirmed — in
  // bounded time, whatever the establish speed was.
  const result = await Promise.race([
    sink.finish(),
    settle(5_000).then(() => {
      throw new Error("closure within bounded time — no resume deadlock");
    }),
  ]);
  expect(result.confirmed).toBe(true);
  // The receiver saw exactly the transaction's bytes: the prefix while the
  // connection was alive, the rest through the verdict-driven
  // retransmission — nothing from the dead connection, nothing twice.
  await settle(300);
  expect(delivered).toBe(totalLen);

  await c2.server.disconnect();
  await c2.client.disconnect();
  listener.close();
}

describe("socket recovery — SPEC §5 transfer recovery revision (1.1.1)", () => {
  it("receiver disconnect drops residual frames, then the resumed transfer confirms (fast establish)", async () => {
    await runReceiverDisconnectScenario(0);
  }, 30_000);

  it("slow establish reaches the same deterministic outcome", async () => {
    await runReceiverDisconnectScenario(600);
  }, 30_000);

  it("holder disconnect: residual egress flushes across the dead link, the bare receiver drops it, RESUME answers Unknown, the fully-sent holder confirms", async () => {
    const { listener, port } = await spawnListener();

    const sessionA = new TransferSession(testConfig());
    const sessionB = new TransferSession(testConfig());
    // No onBinary/onBinaryStream handler on B: had the residual frames been
    // reassembled, the completed slot would stay retained (visible in
    // pendingCount) — the frame-level drop assertion below pins that this
    // never happens.

    const c1 = await connectPair(listener, port);
    sessionB.wire(c1.server);
    sessionA.wire(c1.client);

    // THE DEADLOCK WINDOW (report-faithful shape): the whole transaction is
    // queued into the sender's egress in one call — a 16 MiB payload is
    // 16384 working frames, none of which have been written when the very
    // next statement tears the connection down — then the disconnect: the
    // egress flushes ALL of it FIFO across the dying link, and the frames
    // arrive at the peer on their own, after both mounts are detached.
    const completions: BinarySendResult[] = [];
    const txid = sessionA.sendBinary(pattern(16 * 1024 * 1024), (r) => completions.push(r));
    await c1.client.disconnect();
    await c1.server.disconnect();

    // The residual frames crossed the wire into a detached peer: no slot
    // was opened, nothing was answered (SPEC §3/§5 — the holder's closure
    // signal is the RESUME verdict, never a data-frame reply).
    await settle(1_500);
    expect(sessionB.pendingCount).toBe(0);

    // Reconnection: same sessions, fresh transport; the receiver wires
    // first, the holder's wire-up declares the pending send.
    const c2 = await connectPair(listener, port);
    sessionB.wire(c2.server);
    sessionA.wire(c2.client);

    // Bounded closure: Unknown verdict (the receiver never saw the
    // transaction — and a completed slot must NOT answer `Resumed`, that
    // would deadlock the holder on the swallowing slot), fully-sent holder
    // ⇒ confirmed by holder-side adjudication.
    await waitFor(() => completions.length === 1, "closure within bounded time — no resume deadlock", 5_000);
    expect(completions[0]).toEqual({ transactionId: txid, confirmed: true });

    await c2.server.disconnect();
    await c2.client.disconnect();
    listener.close();
  }, 30_000);

  it("an unanswered RESUME declaration is retransmitted past the verdict timeout", async () => {
    const { listener, port } = await spawnListener();

    const sessionA = new TransferSession(testConfig()); // resumeVerdictTimeoutMs = 300

    // Connection 1: full session pair, nothing pending (empty declaration).
    const c1 = await connectPair(listener, port);
    sessionA.wire(c1.client);
    await c1.server.disconnect();

    // Queue a transaction on the live connection 1, then tear it down:
    // the transaction stays pending (no ACK, retransmission disabled).
    sessionA.sendBinary(pattern(4096));
    await settle(100);
    await c1.client.disconnect();

    // Connection 2: the receiver side is a BARE frame reader — frames are
    // decoded and counted, never answered. The holder wires (wire-up
    // declaration #1 goes out) and stays gated; the verdict timeout must
    // produce declaration #2. The bare socket is accepted through the same
    // register-before-connect contract.
    const bareAccepted = new Promise<net.Socket>((resolve) => listener.once("connection", resolve));
    const ta2 = SocketTransport.connectTo(port, "127.0.0.1");
    await ta2.connect();
    const bare = await bareAccepted;
    sessionA.wire(ta2);

    let declarations = 0;
    const decoder = new FrameDecoder();
    bare.on("data", (buf: Buffer) => {
      for (const frame of decoder.push(new Uint8Array(buf))) {
        if (frame.frameType === FrameType.RESUME) declarations += 1;
      }
    });
    await waitFor(() => declarations >= 2, "declaration retransmission past the verdict timeout", 5_000);

    bare.destroy();
    await ta2.disconnect();
    listener.close();
  }, 30_000);
});
