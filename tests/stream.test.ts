/**
 * Transport-layer streaming tests — BinarySink (sender) + BinaryStream
 * (receiver) + TRANSACTION_PAUSE / TRANSACTION_ABORT control frames
 * (SPEC §4/§5, docs/streams.md).
 *
 * Harness: two TransferSessions wired to in-memory mock wirings; frames
 * are pumped between the captured outbound buffers and the peer's inbound
 * path through the real codecs, so every crossing is byte-faithful.
 *
 * Time: the sink/stream API is user-facing (Date.now()-anchored); tests
 * capture `t0` after wiring and drive `tick`/inbound frames with offsets
 * from it — no fake timers, no interval pumping (the known vitest
 * fake-clock pitfall is sidestepped by manual pumping).
 */

import { describe, it, expect } from "vitest";

import { FrameDecoder, FrameType, encodeFrame, makeFrame } from "../src/wire/frame";
import type { Frame } from "../src/wire/frame";
import { TransferSession } from "../src/transfer";
import type {
  BinarySendResult,
  BinaryStream,
  StreamEndReason,
  TransferActivityHandlers,
  TransferWiring,
} from "../src/transfer";
import {
  CONTROL_PAYLOAD_FRAME_TYPES,
  ErrorCode,
  decodeTransactionAbort,
  decodeTransactionCompleted,
  decodeTransactionPause,
  encodeAck,
  encodeError,
  encodeResumeRequest,
  encodeTransactionAbort,
  encodeTransactionCompleted,
  encodeTransactionPause,
} from "../src/wire/control";
import { SenderError } from "../src/reliability/transaction";
import type { RequestHandler } from "../src/carrier/transport";
import { XacppError } from "../src/wire/message";

// ---- Helpers ----

/** Deterministic filler with distinct byte values per position. */
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

async function settle(turns = 8): Promise<void> {
  for (let i = 0; i < turns; i++) await new Promise((r) => setImmediate(r));
}

/** Decode wire bytes back into frames. */
function decodeWire(bytes: Uint8Array): Frame[] {
  const decoder = new FrameDecoder();
  return decoder.push(bytes);
}

const TX = "ab".repeat(16); // 32 lowercase hex chars

/**
 * In-memory XacppTransport & TransferWiring: captures every outbound
 * frame (data + encoded control) and lets tests inject inbound frames
 * through the production dispatch path (onFrame → binding).
 */
class MockWiring {
  readonly writtenData: Frame[] = [];
  readonly writtenControl: Uint8Array[] = [];
  private activity: TransferActivityHandlers | null = null;
  private readonly closeListeners: (() => void)[] = [];
  closed = false;
  requestHandler: RequestHandler | null = null;

  async connect(): Promise<void> {}
  async disconnect(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    for (const l of this.closeListeners) l();
  }
  onClosed(listener: () => void): () => void {
    this.closeListeners.push(listener);
    return () => {
      const i = this.closeListeners.indexOf(listener);
      if (i >= 0) this.closeListeners.splice(i, 1);
    };
  }
  async send(): Promise<never> {
    throw XacppError.noHandler();
  }
  async sendFaf(): Promise<void> {}
  onRequest(handler: RequestHandler): void {
    this.requestHandler = handler;
  }
  writeControlFrame(bytes: Uint8Array): void {
    this.writtenControl.push(bytes);
  }
  writeDataFrame(frame: Frame): void {
    this.writtenData.push(frame);
  }
  onTransferActivity(handlers: TransferActivityHandlers | null): void {
    this.activity = handlers;
  }

  /** Inject one inbound frame through the production dispatch path. */
  deliverFrame(frame: Frame): void {
    this.activity?.onFrame(frame);
  }

  /** Snapshot of every control frame decoded so far. */
  decodedControl(): Frame[] {
    return this.writtenControl.flatMap((bytes) => decodeWire(bytes));
  }
}

interface WiredSession {
  session: TransferSession;
  mock: MockWiring;
}

function wiredSession(config?: ConstructorParameters<typeof TransferSession>[0]): WiredSession {
  const session = new TransferSession(config);
  const mock = new MockWiring();
  session.wire(mock as unknown as MockWiring & TransferWiring);
  return { session, mock };
}

/** Move every captured outbound frame (data + control) of `from` into `to`'s inbound path. */
function drain(from: MockWiring, to: MockWiring): void {
  for (const frame of from.writtenData.splice(0)) to.deliverFrame(frame);
  for (const bytes of from.writtenControl.splice(0)) {
    for (const frame of decodeWire(bytes)) to.deliverFrame(frame);
  }
}

/**
 * The order the transport's egress actually writes: control frames jump
 * ahead of the data frames queued for the wire (SPEC §5 invariant 4),
 * data frames keep their FIFO order. The mock keeps the two channels in
 * separate buckets — this is their faithful merge.
 */
function wireOrder(mock: MockWiring): Frame[] {
  return [
    ...mock.writtenControl.flatMap((bytes) => decodeWire(bytes)),
    ...mock.writtenData,
  ];
}

/** Move every captured outbound frame of `from` into `to`'s inbound path, in wire order. */
function drainWireOrder(from: MockWiring, to: MockWiring): void {
  const order = wireOrder(from);
  from.writtenControl.length = 0;
  from.writtenData.length = 0;
  for (const frame of order) to.deliverFrame(frame);
}

/** Build an inbound ACK control frame. */
function ackFrame(transactionId: string, highestContiguousSeq: number, receivedFrames: number): Frame {
  const bytes = encodeFrame(
    makeFrame(
      CONTROL_PAYLOAD_FRAME_TYPES.ack,
      encodeAck([{ transactionId, highestContiguousSeq, receivedFrames }]),
    ),
  );
  const [frame] = decodeWire(bytes);
  return frame!;
}

interface StreamRecord {
  stream: BinaryStream;
  chunks: Uint8Array[];
  ends: StreamEndReason[];
}

/** Register a stream handler collecting every stream's chunks/ends. */
function collectStreams(session: TransferSession): Map<string, StreamRecord> {
  const records = new Map<string, StreamRecord>();
  session.onBinaryStream = (stream) => {
    const record: StreamRecord = { stream, chunks: [], ends: [] };
    records.set(stream.transactionId, record);
    stream.onChunk((chunk) => record.chunks.push(chunk));
    stream.onEnd((reason) => record.ends.push(reason));
  };
  return records;
}

const QUIET = { initialTimeoutMs: 1_000_000 };

// ---- BinarySink: streaming send ----

describe("BinarySink — multi-write streaming", () => {
  it("sinks several writes as working frames, finish() resolves confirmed exactly once, receiver assembles bit-exact", async () => {
    const { session: sender, mock: senderMock } = wiredSession({ sender: { workingFrameBytes: 10, ...QUIET } });
    const { session: receiver, mock: receiverMock } = wiredSession();

    const payload = filler(25); // → frames of 10 / 10 / 5
    const sink = sender.openBinarySink({ totalLen: payload.length });
    expect(senderMock.writtenData).toHaveLength(0); // nothing egresses before a write

    await sink.write(payload.subarray(0, 10));
    await sink.write(payload.subarray(10, 20));
    await sink.write(payload.subarray(20));
    expect(senderMock.writtenData).toHaveLength(3);
    expect(senderMock.writtenData.map((f) => f.seq)).toEqual([0, 1, 2]);
    expect(senderMock.writtenData.every((f) => f.transactionId === sink.transactionId)).toBe(true);

    let resolved = 0;
    const finishPromise = sink.finish().then((r) => {
      resolved += 1;
      return r;
    });
    await settle();
    expect(resolved).toBe(0); // finish resolves only on the final ACK

    // Receiver assembles the transaction bit-exact and emits the final ACK.
    drain(senderMock, receiverMock);
    drain(receiverMock, senderMock);

    const result = await finishPromise;
    expect(result.confirmed).toBe(true);
    expect(result.transactionId).toBe(sink.transactionId);
    expect(resolved).toBe(1); // exactly once
    expect(sender.copyCount).toBe(0);
  });

  it("write() pends under backpressure and resolves when ACKs release the cap", async () => {
    const { session: sender, mock: senderMock } = wiredSession({
      sender: { workingFrameBytes: 10, backpressureCapBytes: 10, ...QUIET },
    });

    const sink = sender.openBinarySink({ totalLen: 30 });
    await sink.write(filler(10)); // entry unpaused (0 ≤ cap) → f0 egresses; usage → 10
    let secondDone = false;
    const second = sink.write(filler(10)).then(() => {
      secondDone = true;
    }); // entry unpaused (10 ≤ cap) → f1 egresses; usage → 20
    let thirdDone = false;
    const third = sink.write(filler(10)).then(() => {
      thirdDone = true;
    }); // entry paused (20 > cap) → deferred, pends
    await settle();
    expect(secondDone).toBe(true);
    expect(thirdDone).toBe(false); // backpressure holds the write open
    expect(senderMock.writtenData).toHaveLength(2); // in-flight frames egressed

    // ACK the two in-flight frames: unacked drops to 10 ≤ cap → flush → third resolves.
    senderMock.deliverFrame(ackFrame(sink.transactionId, 1, 2));
    await third;
    expect(thirdDone).toBe(true);
    expect(senderMock.writtenData).toHaveLength(3); // the deferred frame flushed
  });

  it("releases copy memory per acknowledged fragment, not only on the final ACK", async () => {
    const { session: sender, mock: senderMock } = wiredSession({ sender: { workingFrameBytes: 10, ...QUIET } });

    const sink = sender.openBinarySink({ totalLen: 30 });
    await sink.write(filler(10));
    await sink.write(filler(10));
    await sink.write(filler(10));
    sink.finish(); // close the stream: the copy becomes releasable on the final ACK
    expect(sender.retainedPayloadBytes).toBe(30);
    expect(sender.copyCount).toBe(1);

    // Incremental ACK covering frame 0 only: the fragment's payload is released,
    // the copy itself stays live for the rest of the stream.
    senderMock.deliverFrame(ackFrame(sink.transactionId, 0, 1));
    expect(sender.retainedPayloadBytes).toBe(20);
    expect(sender.copyCount).toBe(1);

    // Final ACK: copy gone.
    senderMock.deliverFrame(ackFrame(sink.transactionId, 2, 3));
    expect(sender.copyCount).toBe(0);
    expect(sender.retainedPayloadBytes).toBe(0);
  });

  it("honors a caller-supplied txid on the wire; a live collision throws", async () => {
    const { session: sender, mock: senderMock } = wiredSession();
    const sink = sender.openBinarySink({ totalLen: 10 }, TX);
    await sink.write(filler(10));
    expect(senderMock.writtenData[0]!.transactionId).toBe(TX);

    expect(() => sender.openBinarySink({ totalLen: 10 }, TX)).toThrow(SenderError);
    // A distinct txid opens fine while the first is in flight.
    const other = sender.openBinarySink({ totalLen: 10 });
    await other.write(filler(10));
    expect(other.transactionId).not.toBe(TX);
  });

  it("openBinarySink requires a live connection and a positive integer totalLen", () => {
    const unwired = new TransferSession();
    expect(() => unwired.openBinarySink({ totalLen: 10 })).toThrow(XacppError);
    const { session } = wiredSession();
    expect(() => session.openBinarySink({ totalLen: 0 })).toThrow(RangeError);
  });

  it("rejects writes beyond the declared totalLen and finish() before the total is written", async () => {
    const { session: sender } = wiredSession({ sender: { workingFrameBytes: 10 } });
    const sink = sender.openBinarySink({ totalLen: 15 });
    await expect(sink.write(filler(16))).rejects.toBeInstanceOf(SenderError);

    const short = sender.openBinarySink({ totalLen: 30 });
    await short.write(filler(10));
    await expect(short.finish()).rejects.toBeInstanceOf(SenderError); // 10/30 bytes
  });
});

// ---- abort ----

describe("BinarySink — abort", () => {
  it("abort releases the copy, emits TRANSACTION_ABORT, and the receiver discards its partial reassembly", async () => {
    const { session: sender, mock: senderMock } = wiredSession({ sender: { workingFrameBytes: 10 } });
    const { session: receiver, mock: receiverMock } = wiredSession();

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10));
    await sink.write(filler(10));
    drain(senderMock, receiverMock); // receiver holds both frames, incomplete
    expect(receiver.pendingCount).toBe(1);

    sink.abort();
    expect(sender.copyCount).toBe(0);

    // The holder MUST emit TRANSACTION_ABORT (SPEC §5) carrying the txid.
    expect(senderMock.writtenControl).toHaveLength(1);
    const [abortFrame] = decodeWire(senderMock.writtenControl[0]!);
    expect(abortFrame!.frameType).toBe(FrameType.TRANSACTION_ABORT);
    expect(abortFrame!.transactionId).toHaveLength(32); // header identity unused
    expect(decodeTransactionAbort(abortFrame!.payload)).toBe(TX);

    // Receiver captures the abort: partial reassembly is discarded (SPEC §5).
    drain(senderMock, receiverMock);
    expect(receiver.pendingCount).toBe(0);
  });

  it("an aborted txid is free for reuse on the connection (no tombstones)", async () => {
    const { session: sender } = wiredSession();
    const sink = sender.openBinarySink({ totalLen: 10 }, TX);
    await sink.write(filler(10));
    sink.abort(); // releases the copy from pending, leaving no trace
    expect(() => sender.openBinarySink({ totalLen: 10 }, TX)).not.toThrow();
  });

  it("abort rejects a pending finish() and pending writes", async () => {
    const { session: sender } = wiredSession({
      sender: { workingFrameBytes: 10, backpressureCapBytes: 10, ...QUIET },
    });
    const sink = sender.openBinarySink({ totalLen: 30 });
    await sink.write(filler(10));
    await sink.write(filler(10)); // usage 20 > cap → global pause active
    const pendingWrite = sink.write(filler(10)); // pends
    const finishPromise = sink.finish();
    await settle();

    sink.abort();
    await expect(pendingWrite).rejects.toBeInstanceOf(XacppError);
    await expect(finishPromise).rejects.toBeInstanceOf(XacppError);
  });

  it("a parked frame keeps the abort behind the transaction's own frames: the peer builds the stream, then cancels it exactly once", async () => {
    const { session: sender, mock: senderMock } = wiredSession({
      sender: { workingFrameBytes: 10, backpressureCapBytes: 5, ...QUIET },
    });
    const { session: receiver, mock: receiverMock } = wiredSession();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10)); // frame 0 egresses (admitted while unpaused)
    const parked = sink.write(filler(10)); // usage 10 > cap 5 → this frame is parked
    await settle();
    expect(senderMock.writtenData).toHaveLength(1); // only frame 0 went out

    sink.abort();
    await expect(parked).rejects.toBeInstanceOf(XacppError);

    // The transaction still has a parked frame → the abort rides the DATA
    // FIFO: it must not overtake the transaction's own first frame (the peer
    // keeps no tombstone — an abort arriving before the first byte would be
    // dropped, and the stream would hang).
    const aborts = senderMock.writtenData.filter((f) => f.frameType === FrameType.TRANSACTION_ABORT);
    expect(aborts).toHaveLength(1);
    expect(decodeTransactionAbort(aborts[0]!.payload)).toBe(TX);
    expect(senderMock.writtenControl).toHaveLength(0); // not the expedited channel

    // Wire order: frame 0 first, then the abort.
    drainWireOrder(senderMock, receiverMock);
    expect(records.get(TX)!.chunks).toHaveLength(1); // the peer saw the first frame
    expect(records.get(TX)!.ends).toEqual(["cancelled"]); // then the cancel
    expect(records.get(TX)!.ends).toHaveLength(1); // exactly one terminal event
    expect(receiver.pendingCount).toBe(0); // the partial reassembly is released
  });
});

// ---- pause / resume ----

describe("BinarySink — pause / resume", () => {
  it("pause emits TRANSACTION_PAUSE, holds new writes, and skips the retransmission clock while heartbeats keep flowing", async () => {
    const t0 = Date.now();
    const { session: sender, mock: senderMock } = wiredSession({
      heartbeatIntervalMs: 1_000,
      sender: { workingFrameBytes: 10, initialTimeoutMs: 100 },
    });

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10));
    await sink.write(filler(10));
    sender.tick(t0 + 1); // opening heartbeat (first tick of the connection)

    sink.pause();
    // Holder MUST emit TRANSACTION_PAUSE (SPEC §5).
    const pauseFrame = senderMock.decodedControl().find((f) => f.frameType === FrameType.TRANSACTION_PAUSE);
    expect(pauseFrame).toBeDefined();
    expect(decodeTransactionPause(pauseFrame!.payload)).toBe(TX);

    // Writes during pause are held (pend until resume).
    let heldDone = false;
    const held = sink.write(filler(10)).then(() => {
      heldDone = true;
    });
    await settle();
    expect(heldDone).toBe(false);
    expect(senderMock.writtenData).toHaveLength(2); // nothing new egressed

    // Past the initial retransmission timeout: the paused transaction's clock is
    // skipped — no retransmission frames — and no heartbeat is due yet (interval 1 s).
    const mid = sender.tick(t0 + 1 + 150); // 150 ms past the first tick, timeout 100 ms
    expect(mid.dataFrames).toHaveLength(0);
    expect(mid.controlFrames).toHaveLength(0);

    // At the heartbeat deadline the heartbeat still flows: liveness is
    // connection-level and unaffected by the transaction pause (SPEC §5).
    const later = sender.tick(t0 + 1 + 1_100);
    expect(later.dataFrames).toHaveLength(0); // still no retransmission
    const heartbeats = later.controlFrames
      .flatMap((bytes) => decodeWire(bytes))
      .filter((f) => f.frameType === FrameType.HEARTBEAT);
    expect(heartbeats).toHaveLength(1);

    // Resume: held frame egresses (write resolves).
    sink.resume();
    await held;
    expect(heldDone).toBe(true);
    expect(senderMock.writtenData).toHaveLength(3);

    // The restarted retransmission clock fires again for unconfirmed frames.
    const swept = sender.tick(t0 + 1 + 1_100 + 100);
    expect(swept.dataFrames).toHaveLength(3); // all unacked frames retransmit
  });

  it("no wire frame announces resume — resumption is implicit (SPEC §5)", () => {
    const { session: sender, mock: senderMock } = wiredSession({ sender: { workingFrameBytes: 10 } });
    const sink = sender.openBinarySink({ totalLen: 10 }, TX);
    sink.pause();
    senderMock.writtenControl.length = 0;
    sink.resume();
    expect(senderMock.writtenControl).toHaveLength(0);
  });
});

// ---- onBinaryStream: receiver side ----

describe("onBinaryStream — receiver streaming", () => {
  function fullPair(): {
    sender: TransferSession;
    senderMock: MockWiring;
    receiver: TransferSession;
    receiverMock: MockWiring;
  } {
    const senderSide = wiredSession({ sender: { workingFrameBytes: 10, ...QUIET } });
    const receiverSide = wiredSession();
    return {
      sender: senderSide.session,
      senderMock: senderSide.mock,
      receiver: receiverSide.session,
      receiverMock: receiverSide.mock,
    };
  }

  it("invokes the handler at the first frame, streams ordered chunks, and ends 'completed' exactly once", async () => {
    const { sender, senderMock, receiver, receiverMock } = fullPair();
    const records = collectStreams(receiver);
    const legacyDelivered: string[] = [];
    receiver.onBinary = (txid) => { legacyDelivered.push(txid); }; // streams own their transaction

    const payload = filler(25);
    const txid = sender.sendBinary(payload);
    drain(senderMock, receiverMock);

    const record = records.get(txid);
    expect(record).toBeDefined();
    expect(record!.stream.totalLen).toBe(25);
    expect(record!.stream.startOffset).toBe(0);
    expect(record!.chunks.map((c) => c.length)).toEqual([10, 10, 5]); // ordered
    expectBytesEqual(Buffer.concat(record!.chunks.map((c) => Buffer.from(c))), payload);
    expect(record!.ends).toEqual(["completed"]);
    expect(legacyDelivered).toEqual([]); // no double delivery through onBinary

    // The final ACK flows back and confirms the sender (full loop).
    // settle(): sendBinary's finish() registers the completion one microtask
    // after write() resolves — the copy only becomes releasable then.
    await settle();
    drain(receiverMock, senderMock);
    expect(sender.copyCount).toBe(0);
  });

  it("makes meta readable synchronously inside the handler invocation (first-frame callback)", () => {
    const { sender, senderMock, receiver, receiverMock } = fullPair();
    let seen: { txid: string; totalLen: number } | null = null;
    receiver.onBinaryStream = (stream) => {
      // No chunk callbacks registered yet: meta must already be readable here.
      seen = { txid: stream.transactionId, totalLen: stream.totalLen };
    };

    const sink = sender.openBinarySink({ totalLen: 10 }, TX);
    void sink.write(filler(10));
    expect(seen).toBeNull(); // nothing delivered yet

    drain(senderMock, receiverMock); // first frame lands → handler fires synchronously
    expect(seen).toEqual({ txid: TX, totalLen: 10 });
  });

  it("buffers chunks until onChunk registers, and fires late onEnd immediately", () => {
    const { sender, senderMock, receiver, receiverMock } = fullPair();
    let stream: BinaryStream | null = null;
    receiver.onBinaryStream = (s) => {
      stream = s;
    };

    sender.sendBinary(filler(20));
    drain(senderMock, receiverMock);

    const s = stream!;
    expect(s).toBeDefined();
    const chunks: Uint8Array[] = [];
    s.onChunk((chunk) => chunks.push(chunk)); // late registration flushes the queue
    expect(chunks.map((c) => c.length)).toEqual([10, 10]);

    const ends: StreamEndReason[] = [];
    s.onEnd((reason) => ends.push(reason)); // already ended
    expect(ends).toEqual(["completed"]);
    expect(chunks).toHaveLength(2); // no re-delivery
  });

  it("captures TRANSACTION_PAUSE as a visible stream state without breaking chunk flow", async () => {
    const { sender, senderMock, receiver, receiverMock } = fullPair();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 20 }, TX);
    await sink.write(filler(10));
    drain(senderMock, receiverMock); // first chunk streams
    expect(records.get(TX)!.stream.paused).toBe(false);

    sink.pause();
    drain(senderMock, receiverMock); // deliver the pause frame
    expect(records.get(TX)!.stream.paused).toBe(true);

    // Retransmitted in-flight frames still reassemble and stream while paused.
    const swept = sender.tick(Date.now() + 1_000_000); // timeout sweep → retransmission of frame 0
    for (const frame of swept.dataFrames) receiverMock.deliverFrame(frame);
    expect(records.get(TX)!.chunks).toHaveLength(1); // duplicate — no re-emission

    // Resumption is implicit: the next data frame clears the paused state.
    sink.resume();
    await sink.write(filler(10));
    drain(senderMock, receiverMock);
    expect(records.get(TX)!.stream.paused).toBe(false);
    expect(records.get(TX)!.chunks).toHaveLength(2);
  });

  it("captures TRANSACTION_ABORT: the stream ends 'cancelled' and partial reassembly is dropped", async () => {
    const { sender, senderMock, receiver, receiverMock } = fullPair();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10));
    drain(senderMock, receiverMock); // one chunk streamed, transaction incomplete
    expect(records.get(TX)!.ends).toEqual([]);
    expect(receiver.pendingCount).toBe(1);

    sink.abort();
    drain(senderMock, receiverMock);
    expect(records.get(TX)!.ends).toEqual(["cancelled"]);
    expect(receiver.pendingCount).toBe(0); // partial reassembly discarded
  });

  it("keeps live streams alive but stalled across a disconnect; the transaction slot survives for RESUME", async () => {
    const { sender, senderMock, receiver, receiverMock } = fullPair();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10));
    drain(senderMock, receiverMock);
    expect(receiver.pendingCount).toBe(1); // slot exists

    receiver.unwire();
    // v3 stream lifecycle: the stream object stays alive (session-scoped),
    // surfaced as `stalled`; no terminal event fires.
    expect(records.get(TX)!.stream.stalled).toBe(true);
    expect(records.get(TX)!.ends).toEqual([]);
    // Transaction state survives the connection (RESUME anchor).
    expect(receiver.pendingCount).toBe(1);
  });

  it("streams only the contiguous prefix above the registration-time frontier (startOffset)", async () => {
    const { sender, senderMock, receiver, receiverMock } = fullPair();

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10));
    // Frame 0 lands BEFORE any stream handler is registered: no stream is created.
    drain(senderMock, receiverMock);

    const records = collectStreams(receiver); // handler registers now
    await sink.write(filler(10)); // frame 1 arrives after registration
    drain(senderMock, receiverMock);

    const record = records.get(TX);
    expect(record).toBeDefined();
    expect(record!.stream.startOffset).toBe(10); // seq 0 predates the stream
    expect(record!.chunks.map((c) => c.length)).toEqual([10]); // only seq 1
  });

  it("out-of-order frames stream in seq order once the gap fills", () => {
    const { sender, senderMock, receiver, receiverMock } = fullPair();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    void sink.write(filler(10));
    void sink.write(filler(10));
    void sink.write(filler(10));
    const frames = senderMock.writtenData.splice(0);

    receiverMock.deliverFrame(frames[0]!);
    receiverMock.deliverFrame(frames[2]!); // lands above the gap
    expect(records.get(TX)!.chunks).toHaveLength(1);
    receiverMock.deliverFrame(frames[1]!); // fills the gap
    expect(records.get(TX)!.chunks.map((c) => c.length)).toEqual([10, 10, 10]);
    expect(records.get(TX)!.ends).toEqual(["completed"]);
  });
});

// ---- control codecs ----

describe("TRANSACTION_PAUSE / TRANSACTION_ABORT control codecs", () => {
  it("registers the SPEC §4 frame type values and control-class membership", () => {
    expect(FrameType.TRANSACTION_PAUSE).toBe(0x0009);
    expect(FrameType.TRANSACTION_ABORT).toBe(0x000a);
    expect(CONTROL_PAYLOAD_FRAME_TYPES.transactionPause).toBe(0x0009);
    expect(CONTROL_PAYLOAD_FRAME_TYPES.transactionAbort).toBe(0x000a);
  });

  it("round-trips the 16-byte txid payload strictly", () => {
    expect(decodeTransactionPause(encodeTransactionPause(TX))).toBe(TX);
    expect(decodeTransactionAbort(encodeTransactionAbort(TX))).toBe(TX);
    expect(encodeTransactionPause(TX)).toHaveLength(16);
    expect(encodeTransactionAbort(TX)).toHaveLength(16);
    expect(() => decodeTransactionPause(new Uint8Array(15))).toThrow();
    expect(() => decodeTransactionPause(new Uint8Array(17))).toThrow();
    expect(() => decodeTransactionAbort(new Uint8Array(15))).toThrow();
    expect(() => decodeTransactionAbort(new Uint8Array(17))).toThrow();
    expect(() => encodeTransactionPause("xyz")).toThrow();
  });

  it("rides the wire through the real codec", () => {
    const bytes = encodeFrame(makeFrame(CONTROL_PAYLOAD_FRAME_TYPES.transactionPause, encodeTransactionPause(TX)));
    const [frame] = decodeWire(bytes);
    expect(frame!.frameType).toBe(FrameType.TRANSACTION_PAUSE);
    expect(decodeTransactionPause(frame!.payload)).toBe(TX);
  });
});

// ---- stream lifecycle (v3 — session-scoped streams, docs/streams.md) ----

describe("stream lifecycle (v3 — session-scoped streams)", () => {
  /**
   * Pair with long silence/heartbeat windows so tests can hand-drive
   * `tick` far into the future without tripping the silence detector.
   */
  function pair(): {
    sender: TransferSession;
    senderMock: MockWiring;
    receiver: TransferSession;
    receiverMock: MockWiring;
  } {
    const senderSide = wiredSession({
      sender: {
        workingFrameBytes: 10,
        initialTimeoutMs: 1_000_000,
      },
      silenceTimeoutMs: 1_000_000_000,
      heartbeatIntervalMs: 1_000_000_000,
    });
    const receiverSide = wiredSession({
      silenceTimeoutMs: 1_000_000_000,
      heartbeatIntervalMs: 1_000_000_000,
    });
    return {
      sender: senderSide.session,
      senderMock: senderSide.mock,
      receiver: receiverSide.session,
      receiverMock: receiverSide.mock,
    };
  }

  function rewired(session: TransferSession): MockWiring {
    const fresh = new MockWiring();
    session.wire(fresh as unknown as MockWiring & TransferWiring);
    return fresh;
  }

  it("disconnect marks the stream stalled (alive, no end); rewire resumes the SAME stream object to completion", async () => {
    const { sender, senderMock, receiver, receiverMock } = pair();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10)); // frame 0
    drain(senderMock, receiverMock);
    const original = records.get(TX)!.stream;
    expect(original.stalled).toBe(false);
    expect(records.get(TX)!.ends).toEqual([]);

    // The connection drops on both sides.
    receiver.unwire();
    sender.unwire();
    expect(original.stalled).toBe(true); // visible stall flag
    expect(records.get(TX)!.ends).toEqual([]); // object alive — no terminal event
    expect(records.get(TX)!.stream).toBe(original);

    // Reconnect both sides; the holder's declaration drives the handshake.
    const senderMock2 = rewired(sender);
    const receiverMock2 = rewired(receiver);
    expect(original.stalled).toBe(false); // cleared at wire time

    drain(senderMock2, receiverMock2); // RESUME declaration → receiver adjudicates
    drain(receiverMock2, senderMock2); // RESUME_RESPONSE (Resumed) → gate lifts, frame 0 retransmits
    drain(senderMock2, receiverMock2); // retransmitted frame 0 lands (duplicate — idempotent)

    // The SAME stream object continues (identity, not a fresh stream). A
    // streaming copy only holds written bytes — the verdict's
    // retransmission covers the unconfirmed residue, and the remaining
    // bytes flow from the caller on the same sink.
    expect(records.get(TX)!.stream).toBe(original);

    await sink.write(filler(20)); // remaining bytes go out live
    drain(senderMock2, receiverMock2);
    expect(records.get(TX)!.chunks).toHaveLength(3); // frame 0 (pre-disconnect) + frames 1+2
    expect(records.get(TX)!.ends).toEqual(["completed"]);
    expect(records.get(TX)!.stream).toBe(original);
  });

  it("sink write pends across a disconnect and auto-continues on rewire", async () => {
    const { sender, senderMock, receiver, receiverMock } = pair();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10));
    drain(senderMock, receiverMock); // frame 0 delivered
    expect(records.get(TX)!.chunks).toHaveLength(1);

    sender.unwire();
    receiver.unwire();
    let settled = false;
    const pending = sink.write(filler(10)).then(() => {
      settled = true;
    });
    await settle();
    expect(settled).toBe(false); // pends while unwired — bytes live in the retained copy

    const senderMock2 = rewired(sender);
    await settle();
    expect(settled).toBe(true); // wire() settles pended writes
    const receiverMock2 = rewired(receiver);

    // The reconnect gate holds: no retransmission before the verdict —
    // the verdict is the sole retransmission trigger across a reconnect.
    expect(sender.tick(Date.now() + 2_000_000).dataFrames).toEqual([]);

    drain(senderMock2, receiverMock2); // RESUME declaration → receiver adjudicates (Resumed)
    drain(receiverMock2, senderMock2); // verdict → gate lifts, frames 0+1 retransmit
    drain(senderMock2, receiverMock2); // frame 0 duplicate idempotent, frame 1 streams
    expect(records.get(TX)!.chunks).toHaveLength(2);

    await sink.write(filler(10)); // final frame goes out live
    drain(senderMock2, receiverMock2);
    expect(records.get(TX)!.ends).toEqual(["completed"]);
    await pending;
  });

  it("sink finish survives a disconnect: no rejection, no unconfirmed settlement; the final ACK after rewire resolves confirmed", async () => {
    const { sender, senderMock, receiver, receiverMock } = pair();

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(30)); // 3 frames
    drain(senderMock, receiverMock); // all delivered — receiver slot complete
    expect(receiver.pendingCount).toBe(1);

    let result: BinarySendResult | null = null;
    let rejected = false;
    const finished = sink
      .finish()
      .then((r) => {
        result = r;
      })
      .catch(() => {
        rejected = true;
      });
    await settle();
    expect(result).toBeNull(); // final ACK not yet in

    sender.unwire(); // disconnect with finish pending
    await settle(20);
    expect(rejected).toBe(false); // no rejection across the disconnect
    expect(result).toBeNull(); // no unconfirmed settlement

    // Reconnect the sender only (the receiver never dropped); the holder
    // declares, the receiver (its complete-but-undelivered slot intact)
    // answers Resumed, and the final ACK confirms the transaction.
    const senderMock2 = rewired(sender);
    drain(senderMock2, receiverMock); // RESUME declaration → receiver adjudicates
    drain(receiverMock, senderMock2); // RESUME_RESPONSE (Resumed) → gate lifts, unconfirmed frames retransmit
    drain(senderMock2, receiverMock); // retransmitted frames land (duplicates — idempotent)
    senderMock2.deliverFrame(ackFrame(TX, 2, 3)); // final ACK reaches the holder
    await finished;
    expect(result).toEqual({ transactionId: TX, confirmed: true });
    expect(rejected).toBe(false);
  });

  it("a peer abort while offline surfaces as 'cancelled' when the buffered abort frame reaches the receiver on rewire", async () => {
    const { sender, senderMock, receiver, receiverMock } = pair();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10));
    drain(senderMock, receiverMock); // frame 0 → stream created
    expect(records.get(TX)!.ends).toEqual([]);

    receiver.unwire(); // receiver drops offline — stream stalls, stays alive
    expect(records.get(TX)!.stream.stalled).toBe(true);

    sink.abort(); // peer aborts while the receiver is offline

    // Receiver reconnects; the queued TRANSACTION_ABORT frame (written at
    // abort time, still buffered on the sender's mock) is drained and takes
    // the terminal state. The receiver declares nothing (it has no pending
    // sends). Note: the holder keeps no aborted ledger — if the abort frame
    // were lost with a dead connection, the holder's next RESUME
    // declaration cannot cover the released txid, and the receiver's
    // open-but-undeclared cross-check cancels the stream then.
    const receiverMock2 = rewired(receiver);
    drain(senderMock, receiverMock2); // the buffered TRANSACTION_ABORT frame reaches the receiver

    // Post-hoc cancellation: the offline abort surfaces as `cancelled`.
    expect(records.get(TX)!.ends).toEqual(["cancelled"]);
    expect(receiver.pendingCount).toBe(0); // slot released
  });

  it("an open stream the holder did not declare is cancelled by the declaration cross-check", async () => {
    const { sender, senderMock, receiver, receiverMock } = pair();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10)); // frame 0
    drain(senderMock, receiverMock);
    expect(records.get(TX)!.chunks).toHaveLength(1);
    expect(records.get(TX)!.ends).toEqual([]);

    // The holder's RESUME declaration covers an unrelated transaction only —
    // the open stream's txid is NOT declared (the holder aborted or released
    // its copy while the link was down), so the stream can never complete:
    // streams close only on cancellation or completion, and the cross-check
    // cancels it here.
    receiver.onControlFrame(
      makeFrame(
        FrameType.RESUME,
        encodeResumeRequest([
          { transactionId: "cd".repeat(16), highestContiguousSeq: 0, receivedFrames: 1 },
        ]),
      ),
      Date.now(),
    );
    expect(records.get(TX)!.ends).toEqual(["cancelled"]);
    expect(receiver.pendingCount).toBe(0); // slot released
  });

  it("a long pause (beyond the retired retention windows) keeps the transaction alive and completable", async () => {
    const { sender, senderMock, receiver, receiverMock } = pair();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10));
    drain(senderMock, receiverMock); // frame 0 streams
    expect(sender.copyCount).toBe(1);

    sink.pause(); // TRANSACTION_PAUSE announced; retransmission clock suspended
    drain(senderMock, receiverMock);
    expect(records.get(TX)!.stream.paused).toBe(true);

    // Far beyond both retired windows (copy retention 120s / stale sweep 120s):
    // the sweep skips paused copies and nothing expires the transaction.
    const swept = sender.tick(Date.now() + 2_000_000);
    expect(swept.dataFrames).toEqual([]); // paused — no retransmission
    expect(sender.copyCount).toBe(1); // alive — no expiry sweep

    sink.resume();
    await sink.write(filler(20)); // remaining bytes go out
    const finished = sink.finish();
    drain(senderMock, receiverMock);
    drain(receiverMock, senderMock); // final ACK flows back — copy released
    await finished;
    expect(records.get(TX)!.ends).toEqual(["completed"]);
    expect(sender.copyCount).toBe(0);
  });

  it("transaction_expired ERROR ends the live stream as 'cancelled' and releases the slot", () => {
    const { sender, senderMock, receiver, receiverMock } = pair();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    void sink.write(filler(10));
    drain(senderMock, receiverMock); // frame 0 → stream alive
    expect(records.get(TX)!.ends).toEqual([]);

    const errorFrame = makeFrame(
      FrameType.ERROR,
      encodeError({ code: ErrorCode.TransactionExpired, message: `${TX} expired` }),
    );
    receiverMock.deliverFrame(errorFrame);

    // v3: the structured release surfaces on the stream as `cancelled`.
    expect(records.get(TX)!.ends).toEqual(["cancelled"]);
    expect(receiver.pendingCount).toBe(0); // slot released (existing semantics)
  });
});

// ---- TRANSACTION_COMPLETED: the completion announcement ----

describe("TRANSACTION_COMPLETED — the completion announcement", () => {
  /** Control frames the sender announced, whatever egress path they took. */
  function announced(mock: MockWiring, frameType: number): Frame[] {
    return [
      ...mock.writtenData.filter((f) => f.frameType === frameType),
      ...mock.decodedControl().filter((f) => f.frameType === frameType),
    ];
  }

  it("announces the completion on the wire right after the tail frame; the receiver's stream completes", async () => {
    const { session: sender, mock: senderMock } = wiredSession({ sender: { workingFrameBytes: 10, ...QUIET } });
    const { session: receiver, mock: receiverMock } = wiredSession();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 25 }, TX);
    await sink.write(filler(10));
    await sink.write(filler(10));
    await sink.write(filler(5));
    void sink.finish(); // resolves on the final ACK; the announcement is immediate

    // Exactly one completion announcement, carrying the transaction id.
    const completions = announced(senderMock, FrameType.TRANSACTION_COMPLETED);
    expect(completions).toHaveLength(1);
    expect(decodeTransactionCompleted(completions[0]!.payload)).toBe(TX);

    // Wire order: the announcement follows the tail data frame (it must
    // never overtake it — a peer reading "completed" first would call the
    // transaction short).
    const tailIndex = senderMock.writtenData.findIndex(
      (f) => f.frameType === FrameType.DATA_BINARY && f.seq === 2,
    );
    const announceIndex = senderMock.writtenData.findIndex(
      (f) => f.frameType === FrameType.TRANSACTION_COMPLETED,
    );
    expect(announceIndex).toBeGreaterThan(tailIndex);

    drain(senderMock, receiverMock);
    expect(records.get(TX)!.ends).toEqual(["completed"]);
    expect(records.get(TX)!.ends).toHaveLength(1); // the late announcement is idempotent
  });

  it("the whole-package path announces the completion too — no exception for sendBinary", async () => {
    const { session: sender, mock: senderMock } = wiredSession({ sender: { workingFrameBytes: 10, ...QUIET } });

    const payload = filler(30);
    const txid = sender.sendBinary(payload); // open + write + finish
    await settle(); // let the fire-and-forget finish() chain run

    // One completion announcement, carrying the transaction id.
    const completions = announced(senderMock, FrameType.TRANSACTION_COMPLETED);
    expect(completions).toHaveLength(1);
    expect(decodeTransactionCompleted(completions[0]!.payload)).toBe(txid);

    // Wire order: the announcement follows the tail data frame.
    const tailIndex = senderMock.writtenData.findIndex(
      (f) => f.frameType === FrameType.DATA_BINARY && f.seq === 2,
    );
    const announceIndex = senderMock.writtenData.findIndex(
      (f) => f.frameType === FrameType.TRANSACTION_COMPLETED,
    );
    expect(announceIndex).toBeGreaterThan(tailIndex);
  });

  it("rejects a short finish: an abort is announced (never a completion), the copy is released, the peer terminates", async () => {
    const { session: sender, mock: senderMock } = wiredSession({ sender: { workingFrameBytes: 10, ...QUIET } });
    const { session: receiver, mock: receiverMock } = wiredSession();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10)); // 10 of the 30 declared bytes

    // Byte-count consistency: the frames' payload sum is the written bytes.
    const dataFrames = senderMock.writtenData.filter((f) => f.frameType === FrameType.DATA_BINARY);
    expect(dataFrames.reduce((n, f) => n + f.payload.length, 0)).toBe(10);

    drain(senderMock, receiverMock); // the peer holds a partial transaction
    expect(records.get(TX)!.ends).toEqual([]);

    // The short finish is rejected (existing error form).
    await expect(sink.finish()).rejects.toBeInstanceOf(SenderError);

    // No completion is announced; the abort is.
    expect(announced(senderMock, FrameType.TRANSACTION_COMPLETED)).toHaveLength(0);
    const aborts = announced(senderMock, FrameType.TRANSACTION_ABORT);
    expect(aborts).toHaveLength(1);
    expect(decodeTransactionAbort(aborts[0]!.payload)).toBe(TX);

    // No state pollution: the copy is released (no retransmission storm).
    expect(sender.copyCount).toBe(0);

    // The peer terminates explicitly — it never waits for the missing bytes.
    drain(senderMock, receiverMock);
    expect(records.get(TX)!.ends).toEqual(["cancelled"]);
    expect(receiver.pendingCount).toBe(0);
  });

  it("ends the stream as 'incomplete' when the peer announces completion with bytes missing", async () => {
    const { session: sender, mock: senderMock } = wiredSession({ sender: { workingFrameBytes: 10, ...QUIET } });
    const { session: receiver, mock: receiverMock } = wiredSession();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10));
    drain(senderMock, receiverMock);
    expect(records.get(TX)!.ends).toEqual([]);

    // A completion announcement for a transaction whose bytes are missing:
    // the stream fails explicitly, and the gap is readable.
    receiverMock.deliverFrame(
      makeFrame(FrameType.TRANSACTION_COMPLETED, encodeTransactionCompleted(TX)),
    );
    const record = records.get(TX)!;
    expect(record.ends).toEqual(["incomplete"]);
    expect(record.stream.receivedBytes).toBe(10);
    expect(record.stream.expectedBytes).toBe(30);
    expect(receiver.pendingCount).toBe(0); // the partial reassembly is released
  });

  it("ignores a late completion announcement: one terminal event, healthy session", async () => {
    const { session: sender, mock: senderMock } = wiredSession({ sender: { workingFrameBytes: 10, ...QUIET } });
    const { session: receiver, mock: receiverMock } = wiredSession();
    const records = collectStreams(receiver);

    const sink = sender.openBinarySink({ totalLen: 10 }, TX);
    await sink.write(filler(10));
    drain(senderMock, receiverMock);
    expect(records.get(TX)!.ends).toEqual(["completed"]);

    // The announcement arrives after the transaction is already terminal.
    receiverMock.deliverFrame(
      makeFrame(FrameType.TRANSACTION_COMPLETED, encodeTransactionCompleted(TX)),
    );
    expect(records.get(TX)!.ends).toEqual(["completed"]); // exactly once
    expect(receiver.pendingCount).toBe(0);

    // The session is healthy: the next transaction still completes.
    const other = sender.openBinarySink({ totalLen: 10 });
    await other.write(filler(10));
    drain(senderMock, receiverMock);
    expect(records.get(other.transactionId)!.ends).toEqual(["completed"]);
  });
});

describe("legacy sendBinary over the streaming core", () => {
  it("keeps the existing contract: sync txid, immediate egress, confirmed completion", async () => {
    const { session: sender, mock: senderMock } = wiredSession({
      sender: { workingFrameBytes: 10, backpressureCapBytes: 10, ...QUIET },
    });
    const { session: receiver, mock: receiverMock } = wiredSession();
    const delivered: { txid: string; payload: Uint8Array }[] = [];
    receiver.onBinary = (txid, payload) => { delivered.push({ txid, payload }); };

    const completions: BinarySendResult[] = [];
    const payload = filler(30);
    const txid = sender.sendBinary(payload, (r) => completions.push(r)); // sync, admits past the cap
    expect(txid).toHaveLength(32);
    expect(senderMock.writtenData).toHaveLength(3); // frames went out immediately
    await settle(); // let the fire-and-forget finish() chain register the completion

    drain(senderMock, receiverMock);
    drain(receiverMock, senderMock);
    await settle(); // the completion promise chain resolves one microtask after the final ACK

    expect(delivered).toHaveLength(1);
    expectBytesEqual(delivered[0]!.payload, payload);
    expect(completions).toEqual([{ transactionId: txid, confirmed: true }]);
    expect(sender.copyCount).toBe(0);
  });
});

// ---- control-frame scheduling: expedite vs FIFO ----

describe("transaction control frames — scheduling (expedite vs FIFO)", () => {
  it("with nothing parked, pause and abort take the expedited control channel — they overtake the queued data frames", async () => {
    const { session: sender, mock: senderMock } = wiredSession({ sender: { workingFrameBytes: 10, ...QUIET } });

    const sink = sender.openBinarySink({ totalLen: 30 }, TX);
    await sink.write(filler(10)); // frame 0 is queued for the wire; nothing is parked

    sink.pause();
    const pauses = senderMock.decodedControl().filter((f) => f.frameType === FrameType.TRANSACTION_PAUSE);
    expect(pauses).toHaveLength(1);
    expect(decodeTransactionPause(pauses[0]!.payload)).toBe(TX);
    expect(senderMock.writtenData.some((f) => f.frameType === FrameType.TRANSACTION_PAUSE)).toBe(false);

    sink.abort();
    const aborts = senderMock.decodedControl().filter((f) => f.frameType === FrameType.TRANSACTION_ABORT);
    expect(aborts).toHaveLength(1);
    expect(decodeTransactionAbort(aborts[0]!.payload)).toBe(TX);
    expect(senderMock.writtenData.some((f) => f.frameType === FrameType.TRANSACTION_ABORT)).toBe(false);

    // The expedited channel jumps ahead of the data frame queued for the
    // wire (SPEC §5 invariant 4): the control frames are written first.
    expect(wireOrder(senderMock).map((f) => f.frameType)).toEqual([
      FrameType.TRANSACTION_PAUSE,
      FrameType.TRANSACTION_ABORT,
      FrameType.DATA_BINARY,
    ]);
  });
});
