/**
 * Reliability integration tests — TransferSession unit level
 * (docs/SPEC.md §5, batch 3b).
 *
 * The session is time-injected: tests drive the clock by passing `now`
 * explicitly — no fake timers needed at this level. Two sessions
 * (sender side, receiver side) exchange Frame objects directly; encoded
 * control frames travel through the real wire codec (encodeFrame →
 * FrameDecoder) so every crossing is byte-faithful.
 *
 * Transport-level integration (real TCP + fake interval/Date) lives in
 * reliability-transport.test.ts.
 */

import { describe, it, expect, vi } from "vitest";

import { FrameDecoder, FrameType, makeFrame } from "../src/wire/frame";
import type { Frame } from "../src/wire/frame";
import {
  decodeAck,
  decodeError,
  decodeHeartbeat,
  decodeNack,
  decodeResumeResponse,
  encodeError,
  encodeResumeRequest,
  encodeResumeResponse,
  ErrorCode,
  RESUME_STATUS,
} from "../src/wire/control";
import { TransferSession } from "../src/transfer";
import type { BinarySendResult } from "../src/transfer";

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

/** Decode wire bytes back into frames (control frames travel encoded). */
function decodeWire(bytes: Uint8Array): Frame[] {
  const decoder = new FrameDecoder();
  return decoder.push(bytes);
}

/** Decode a single encoded control frame and return it. */
function decodeControl(bytes: Uint8Array): Frame {
  const frames = decodeWire(bytes);
  expect(frames).toHaveLength(1);
  return frames[0];
}

interface Delivered {
  transactionId: string;
  payload: Uint8Array;
}

/** Receiver session with a delivery sink. */
function receivingSession(): { session: TransferSession; delivered: Delivered[] } {
  const delivered: Delivered[] = [];
  const session = new TransferSession();
  session.onBinary = (transactionId, payload) => {
    delivered.push({ transactionId, payload });
  };
  return { session, delivered };
}

/** Feed data frames into a receiver in order; returns the control frames from the last push. */
function feed(receiver: TransferSession, frames: Frame[], now: number): Uint8Array[] {
  let out: Uint8Array[] = [];
  for (const frame of frames) out = receiver.acceptDataFrame(frame, now);
  return out;
}

/** Long-but-benign timers so timeout retransmission never interferes. */
const QUIET_SENDER = { initialTimeoutMs: 1_000_000 };

/** Find the first frame of the given type among encoded control frames. */
function controlOf(frames: Uint8Array[], frameType: number): Frame {
  const match = frames.map(decodeControl).find((f) => f.frameType === frameType);
  expect(match, `expected a 0x${frameType.toString(16)} frame`).toBeDefined();
  return match!;
}

// ---- Tests ----

describe("TransferSession — large content round trip", () => {
  it("delivers a >1MiB payload bit-exact across a 1MiB frame + tail frame, final ACK releases the copy", () => {
    const completions: BinarySendResult[] = [];
    const sender = new TransferSession({ sender: QUIET_SENDER });
    const { session: receiver, delivered } = receivingSession();

    const payload = filler(1024 * 1024 + 512 * 1024); // 1.5 MiB → 2 frames (1 MiB + 0.5 MiB tail)
    const { transactionId, frames } = sender.openSend(payload, 0, (r) => completions.push(r));
    expect(frames).toHaveLength(2);
    expect(frames[0].payload.length).toBe(1024 * 1024);
    expect(frames[1].payload.length).toBe(512 * 1024); // tail frame
    expect(frames[1].seq).toBe(1);

    expect(receiver.acceptDataFrame(frames[0], 1)).toEqual([]); // nothing complete yet
    const acks = feed(receiver, [frames[1]], 2);
    expect(delivered).toHaveLength(1);
    expect(delivered[0].transactionId).toBe(transactionId);
    expectBytesEqual(delivered[0].payload, payload);

    // Immediate final ACK on completion (SPEC §5).
    expect(acks).toHaveLength(1);
    const ackFrame = decodeControl(acks[0]);
    expect(ackFrame.frameType).toBe(FrameType.ACK);
    const entries = decodeAck(ackFrame.payload);
    expect(entries).toEqual([
      { transactionId, highestContiguousSeq: 1, receivedFrames: 2 },
    ]);

    // Sender applies the final ACK: copy released, completion confirmed.
    expect(sender.copyCount).toBe(1);
    const outcome = sender.onControlFrame(ackFrame, 3);
    expect(sender.copyCount).toBe(0);
    expect(outcome.controlFrames).toEqual([]);
    expect(completions).toEqual([{ transactionId, confirmed: true }]);
  });
});

describe("TransferSession — loss recovery", () => {
  it("immediate NACK on a frame above a gap → retransmission → bit-exact delivery", () => {
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { session: receiver, delivered } = receivingSession();

    const payload = filler(300); // 3 frames of 100 bytes
    const { transactionId, frames } = sender.openSend(payload, 0);

    expect(receiver.acceptDataFrame(frames[0], 1)).toEqual([]);
    // frames[1] is lost on the wire; frames[2] lands above the gap.
    const out = receiver.acceptDataFrame(frames[2], 2);
    expect(out).toHaveLength(1);
    const nackFrame = decodeControl(out[0]);
    expect(nackFrame.frameType).toBe(FrameType.NACK);
    const nacks = decodeNack(nackFrame.payload);
    expect(nacks).toEqual([{ transactionId, missingSeqs: [1] }]);

    // Sender retransmits the listed frames immediately.
    const outcome = sender.onControlFrame(nackFrame, 3);
    expect(outcome.dataFrames).toEqual([frames[1]]);

    // Retransmission completes the transaction; the final ACK releases the copy.
    const acks = feed(receiver, outcome.dataFrames, 4);
    expect(delivered).toHaveLength(1);
    expectBytesEqual(delivered[0].payload, payload);
    expect(sender.onControlFrame(decodeControl(acks[0]), 5).dataFrames).toEqual([]);
    expect(sender.copyCount).toBe(0);
  });

  it("timeout sweep retransmits unconfirmed frames with ×2 backoff", () => {
    const sender = new TransferSession({
      sender: {
        workingFrameBytes: 100,
        initialTimeoutMs: 2_000,
        maxTimeoutMs: 30_000,
      },
    });
    const { frames } = sender.openSend(filler(300), 0); // opened at t=0

    // Sweep 1 at the initial timeout (2 s); next window = 2 s × 2 = 4 s → t=6 s.
    expect(sender.tick(1_999).dataFrames).toEqual([]);
    expect(sender.tick(2_000).dataFrames).toEqual(frames);
    // Sweep 2 at t=6 s; next window = 2 s × 4 = 8 s → t=14 s.
    expect(sender.tick(3_999).dataFrames).toEqual([]);
    expect(sender.tick(6_000).dataFrames).toEqual(frames);
    expect(sender.tick(13_999).dataFrames).toEqual([]);
  });
});

describe("TransferSession — incremental ACK batching", () => {
  it("first batch fires on the first tick, then throttles to the 500 ms window", () => {
    const receiver = new TransferSession();
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { transactionId, frames } = sender.openSend(filler(500), 0); // 5 frames

    // Receive 2 frames; the first tick of the connection emits a batch
    // immediately (the 500 ms window started at connection setup). The
    // same tick carries the opening heartbeat.
    receiver.acceptDataFrame(frames[0], 1);
    receiver.acceptDataFrame(frames[1], 2);
    let outcome = receiver.tick(3);
    let entries = decodeAck(controlOf(outcome.controlFrames, FrameType.ACK).payload);
    expect(entries).toEqual([
      { transactionId, highestContiguousSeq: 1, receivedFrames: 2 },
    ]);

    // One more frame inside the window: no new ACK yet.
    receiver.acceptDataFrame(frames[2], 4);
    const quietTick = receiver.tick(100).controlFrames.map(decodeControl);
    expect(quietTick.filter((f) => f.frameType === FrameType.ACK)).toEqual([]);
    const throttleTick = receiver.tick(503).controlFrames.map(decodeControl); // 500 ms since the batch at t=3
    expect(throttleTick.some((f) => f.frameType === FrameType.ACK)).toBe(true);
  });

  it("frame-count threshold (64) fires before the time window", () => {
    const receiver = new TransferSession();
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { transactionId, frames } = sender.openSend(filler(7_000), 0); // 70 frames

    // 64 new distinct frames cross the threshold instantly.
    for (let i = 0; i < 64; i++) receiver.acceptDataFrame(frames[i], 1);
    const outcome = receiver.tick(1); // same instant — threshold, not time
    const entries = decodeAck(controlOf(outcome.controlFrames, FrameType.ACK).payload);
    expect(entries).toEqual([
      { transactionId, highestContiguousSeq: 63, receivedFrames: 64 },
    ]);
  });
});

describe("TransferSession — RESUME across connections", () => {
  it("the holder declares, the receiver adjudicates, and the verdict resumes the transfer bit-exact", () => {
    const completions: BinarySendResult[] = [];
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { session: receiver, delivered } = receivingSession();

    const payload = filler(300);
    const { transactionId, frames } = sender.openSend(payload, 0, (r) => completions.push(r));

    // Connection 1: only seq 0 arrives, then the connection dies.
    receiver.acceptDataFrame(frames[0], 1);
    receiver.onConnectionGone();

    // Connection 2: the holder declares its pending sends on wire-up.
    const declarationBytes = sender.buildResumeDeclaration(2);
    expect(declarationBytes).not.toBeNull();
    const declaration = decodeControl(declarationBytes!);
    expect(declaration.frameType).toBe(FrameType.RESUME);

    // The declaration gates the copy: no blind retransmission before the
    // verdict (the retransmission clock skips gated copies).
    expect(sender.tick(1_000_000).dataFrames).toEqual([]);

    // The receiver adjudicates: the txid is still open → `Resumed` (the
    // resume instruction); the answer rides a RESUME_RESPONSE frame.
    const answer = receiver.onControlFrame(declaration, 3);
    expect(answer.dataFrames).toEqual([]);
    expect(answer.controlFrames).toHaveLength(1);
    const responseFrame = decodeControl(answer.controlFrames[0]);
    expect(responseFrame.frameType).toBe(FrameType.RESUME_RESPONSE);
    const response = decodeResumeResponse(responseFrame.payload);
    expect(response).toEqual([{ transactionId, status: RESUME_STATUS.Resumed }]);

    // The verdict lifts the gate and retransmits every unconfirmed frame
    // (frame 0's ACK never arrived — it retransmits and is absorbed
    // idempotently by the receiver's incomplete window).
    const outcome = sender.onControlFrame(responseFrame, 4);
    expect(outcome.controlFrames).toEqual([]);
    expect(outcome.dataFrames).toEqual(frames);

    // Retransmission completes the receive; the final ACK confirms on the sender.
    const acks = feed(receiver, outcome.dataFrames, 5);
    expect(delivered).toHaveLength(1);
    expectBytesEqual(delivered[0].payload, payload);
    expect(sender.onControlFrame(decodeControl(acks[0]), 6).dataFrames).toEqual([]);
    expect(sender.copyCount).toBe(0);
    expect(completions).toEqual([{ transactionId, confirmed: true }]);
  });

  it("a receiver `unknown` verdict fails the transfer: copy released, completion unconfirmed", () => {
    const completions: BinarySendResult[] = [];
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { transactionId, frames } = sender.openSend(filler(300), 0, (r) => completions.push(r));
    expect(frames).toHaveLength(3);

    const unknown = makeFrame(
      FrameType.RESUME_RESPONSE,
      encodeResumeResponse([{ transactionId, status: RESUME_STATUS.Unknown }]),
    );
    const outcome = sender.onControlFrame(unknown, 1);

    expect(sender.copyCount).toBe(0);
    expect(completions).toEqual([{ transactionId, confirmed: false }]);
    expect(outcome.controlFrames).toEqual([]);
  });

  it("an empty pending set declares nothing on wire-up", () => {
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    expect(sender.buildResumeDeclaration(1)).toBeNull();
  });

describe("TransferSession — recovery adjudication (receiver side)", () => {
  it("adjudicates each declared txid against its open slots", () => {
    const { session: receiver } = receivingSession();
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { transactionId, frames } = sender.openSend(filler(300), 0);
    receiver.acceptDataFrame(frames[0], 1); // open, incomplete
    const stranger = "ab".repeat(16);

    const outcome = receiver.onControlFrame(
      makeFrame(
        FrameType.RESUME,
        encodeResumeRequest([
          { transactionId, highestContiguousSeq: 0, receivedFrames: 1 },
          { transactionId: stranger, highestContiguousSeq: 0, receivedFrames: 1 },
        ]),
      ),
      2,
    );
    expect(outcome.dataFrames).toEqual([]);
    const resp = decodeResumeResponse(decodeControl(outcome.controlFrames[0]).payload);
    expect(resp).toEqual([
      { transactionId, status: RESUME_STATUS.Resumed },
      { transactionId: stranger, status: RESUME_STATUS.Unknown },
    ]);
    expect(receiver.pendingCount).toBe(1); // the declared slot is untouched
  });

  it("cancels an open-but-undeclared slot (the holder no longer holds it)", () => {
    const { session: receiver } = receivingSession();
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { transactionId, frames } = sender.openSend(filler(300), 0);
    receiver.acceptDataFrame(frames[0], 1);
    expect(receiver.pendingCount).toBe(1);

    // The holder's declaration covers an unrelated transaction only.
    const stranger = "ab".repeat(16);
    const outcome = receiver.onControlFrame(
      makeFrame(
        FrameType.RESUME,
        encodeResumeRequest([{ transactionId: stranger, highestContiguousSeq: 0, receivedFrames: 1 }]),
      ),
      2,
    );
    const resp = decodeResumeResponse(decodeControl(outcome.controlFrames[0]).payload);
    expect(resp).toEqual([{ transactionId: stranger, status: RESUME_STATUS.Unknown }]);
    expect(receiver.pendingCount).toBe(0); // the orphaned slot is cross-checked out
    expect(transactionId).not.toBe(stranger);
  });

  it("answers every declaration exactly once; a repeated declaration disturbs nothing", () => {
    const { session: receiver, delivered } = receivingSession();
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { transactionId, frames } = sender.openSend(filler(300), 0);
    receiver.acceptDataFrame(frames[0], 1);

    const declare = () =>
      makeFrame(
        FrameType.RESUME,
        encodeResumeRequest([{ transactionId, highestContiguousSeq: 0, receivedFrames: 1 }]),
      );
    for (const now of [2, 3]) {
      const outcome = receiver.onControlFrame(declare(), now);
      expect(outcome.controlFrames).toHaveLength(1); // never silently swallowed
      expect(decodeResumeResponse(decodeControl(outcome.controlFrames[0]).payload)).toEqual([
        { transactionId, status: RESUME_STATUS.Resumed },
      ]);
    }
    expect(receiver.pendingCount).toBe(1); // declared slot undisturbed

    // The transaction completes normally afterwards.
    feed(receiver, [frames[1], frames[2]], 4);
    expect(delivered).toHaveLength(1);
    expectBytesEqual(delivered[0].payload, filler(300));
  });

  it("an empty declaration is not answered", () => {
    const { session: receiver } = receivingSession();
    const outcome = receiver.onControlFrame(makeFrame(FrameType.RESUME, encodeResumeRequest([])), 1);
    expect(outcome.controlFrames).toEqual([]);
    expect(outcome.dataFrames).toEqual([]);
  });

  it("a completed-but-undelivered slot survives the cross-check (it needs nothing from the holder)", () => {
    const receiver = new TransferSession(); // no handler: delivery is pending
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { frames } = sender.openSend(filler(300), 0);
    feed(receiver, frames, 1); // complete, but undelivered (no onBinary)
    expect(receiver.pendingCount).toBe(1);

    const stranger = "ab".repeat(16);
    const outcome = receiver.onControlFrame(
      makeFrame(
        FrameType.RESUME,
        encodeResumeRequest([{ transactionId: stranger, highestContiguousSeq: 0, receivedFrames: 1 }]),
      ),
      2,
    );
    expect(decodeResumeResponse(decodeControl(outcome.controlFrames[0]).payload)).toEqual([
      { transactionId: stranger, status: RESUME_STATUS.Unknown },
    ]);
    expect(receiver.pendingCount).toBe(1); // complete slots are NOT cross-checked out
  });

  it("both directions reconcile independently over crossed declarations", () => {
    const a = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const b = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const deliveredA: Delivered[] = [];
    const deliveredB: Delivered[] = [];
    a.onBinary = (transactionId, payload) => deliveredA.push({ transactionId, payload });
    b.onBinary = (transactionId, payload) => deliveredB.push({ transactionId, payload });
    const completionsA: BinarySendResult[] = [];
    const completionsB: BinarySendResult[] = [];

    const txnAB = a.openSend(filler(300), 0, (r) => completionsA.push(r)); // a → b
    const txnBA = b.openSend(filler(200, 7), 0, (r) => completionsB.push(r)); // b → a
    b.acceptDataFrame(txnAB.frames[0], 1); // b holds a partial slot
    a.acceptDataFrame(txnBA.frames[0], 1); // a holds a partial slot

    // Both declare their own pending sends (wire-up on the new connection).
    const declareA = decodeControl(a.buildResumeDeclaration(2)!);
    const declareB = decodeControl(b.buildResumeDeclaration(2)!);

    // Crossed adjudication: each answers the peer's declaration.
    const answerB = b.onControlFrame(declareA, 3);
    const answerA = a.onControlFrame(declareB, 3);
    expect(decodeResumeResponse(decodeControl(answerB.controlFrames[0]).payload)).toEqual([
      { transactionId: txnAB.transactionId, status: RESUME_STATUS.Resumed },
    ]);
    expect(decodeResumeResponse(decodeControl(answerA.controlFrames[0]).payload)).toEqual([
      { transactionId: txnBA.transactionId, status: RESUME_STATUS.Resumed },
    ]);

    // Verdicts applied: gates lift, every unconfirmed frame retransmits.
    const outcomeA = a.onControlFrame(decodeControl(answerB.controlFrames[0]), 4);
    const outcomeB = b.onControlFrame(decodeControl(answerA.controlFrames[0]), 4);
    expect(outcomeA.dataFrames).toEqual(txnAB.frames);
    expect(outcomeB.dataFrames).toEqual(txnBA.frames);

    // Both transfers complete bit-exact; the final ACKs confirm both.
    const acksB = feed(b, outcomeA.dataFrames, 5);
    const acksA = feed(a, outcomeB.dataFrames, 5);
    expectBytesEqual(deliveredB[0].payload, filler(300));
    expectBytesEqual(deliveredA[0].payload, filler(200, 7));
    a.onControlFrame(decodeControl(acksB[0]), 6);
    b.onControlFrame(decodeControl(acksA[0]), 6);
    expect(completionsA).toEqual([{ transactionId: txnAB.transactionId, confirmed: true }]);
    expect(completionsB).toEqual([{ transactionId: txnBA.transactionId, confirmed: true }]);
  });
});

  it("a peer RESUME rejection (RESUME_RESPONSE frame) releases the copy and reports unconfirmed", () => {
    const completions: BinarySendResult[] = [];
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { transactionId, frames } = sender.openSend(filler(300), 0, (r) => completions.push(r));
    expect(frames).toHaveLength(3);

    const rejected = makeFrame(
      FrameType.RESUME_RESPONSE,
      encodeResumeResponse([{ transactionId, status: RESUME_STATUS.Rejected }]),
    );
    const outcome = sender.onControlFrame(rejected, 1);

    expect(sender.copyCount).toBe(0);
    expect(completions).toEqual([{ transactionId, confirmed: false }]);
    expect(outcome.controlFrames).toEqual([]);
  });

  it("a RESUME_RESPONSE with `resumed` lifts the gate, retransmits at once, and rearms the schedule", () => {
    const completions: BinarySendResult[] = [];
    const sender = new TransferSession({
      sender: { workingFrameBytes: 100, initialTimeoutMs: 2_000 },
    });
    const { transactionId, frames } = sender.openSend(filler(300), 0, (r) => completions.push(r));

    // Wire-up gates the copy; the verdict is the sole retransmission trigger.
    sender.buildResumeDeclaration(1);
    const resumed = makeFrame(
      FrameType.RESUME_RESPONSE,
      encodeResumeResponse([{ transactionId, status: RESUME_STATUS.Resumed }]),
    );
    const outcome = sender.onControlFrame(resumed, 2);
    expect(outcome.dataFrames).toEqual(frames); // every unconfirmed frame
    expect(sender.copyCount).toBe(1);
    expect(completions).toEqual([]);
    // The retransmission window rearmed at the verdict.
    expect(sender.tick(2 + 1_999).dataFrames).toEqual([]);
    expect(sender.tick(2 + 2_000).dataFrames).toEqual(frames);
  });

  it("dispatch is frame-type driven: a RESUME_RESPONSE payload on a RESUME frame is rejected, never guessed", () => {
    const completions: BinarySendResult[] = [];
    const sender = new TransferSession({ sender: { ...QUIET_SENDER, workingFrameBytes: 100 } });
    const { transactionId } = sender.openSend(filler(300), 0, (r) => completions.push(r));

    // Response payload (1 + 17 bytes/entry) under the wrong frame type:
    // decode-as-request fails on shape; the frame is dropped — no state change.
    const wrongType = makeFrame(
      FrameType.RESUME,
      encodeResumeResponse([{ transactionId, status: RESUME_STATUS.Rejected }]),
    );
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const outcome = sender.onControlFrame(wrongType, 1);
    errSpy.mockRestore();

    expect(outcome.controlFrames).toEqual([]);
    expect(outcome.dataFrames).toEqual([]);
    expect(sender.copyCount).toBe(1);
    expect(completions).toEqual([]);
  });
});

describe("TransferSession — backpressure", () => {
  it("pauses new transactions past the cap, holds their frames, flushes on recovery", () => {
    const sender = new TransferSession({
      sender: { workingFrameBytes: 100, backpressureCapBytes: 250, ...QUIET_SENDER },
    });
    const { session: receiver, delivered } = receivingSession();

    // Transaction 1 opens while unpaused: in flight, goes out even though
    // it pushes usage past the cap (guarantees the wire keeps flowing).
    const first = sender.openSend(filler(400), 0); // 4 frames, 400 unacked > 250
    expect(first.frames).toHaveLength(4);
    expect(sender.paused).toBe(true);

    // Transaction 2 opens while paused: frames held, caller never blocks.
    const second = sender.openSend(filler(200), 1);
    expect(second.frames).toEqual([]);

    // Timeout retransmissions also hold while paused.
    expect(sender.tick(2).dataFrames).toEqual([]);

    // The peer ACKs transaction 1 fully → usage 0 → unpaused → flush.
    receiver.acceptDataFrame(first.frames[0], 3);
    const acks = feed(receiver, first.frames.slice(1), 4);
    const finalAck = decodeControl(acks[0]);
    const outcome = sender.onControlFrame(finalAck, 5);
    expect(sender.paused).toBe(false);
    expect(outcome.dataFrames.map((f) => [f.transactionId, f.seq])).toEqual([
      [second.transactionId, 0],
      [second.transactionId, 1],
    ]);

    // Flushed frames complete on the receiver.
    feed(receiver, outcome.dataFrames, 6);
    expect(delivered.map((d) => d.transactionId)).toEqual([first.transactionId, second.transactionId]);
  });
});

describe("TransferSession — heartbeat and silence", () => {
  it("sends heartbeats on the configured interval with a monotonic sequence", () => {
    const sender = new TransferSession({ heartbeatIntervalMs: 30_000 });
    sender.noteActivity(1_000);

    // First tick of the connection emits the opening heartbeat immediately
    // (the peer gets liveness evidence + its silence clock starts fed).
    const opening = sender.tick(1_000);
    expect(opening.controlFrames).toHaveLength(1);
    const first = decodeHeartbeat(decodeControl(opening.controlFrames[0]).payload);
    expect(first.monotonicSeq).toBe(1);
    expect(first.senderEpochMs).toBe(1_000);

    expect(sender.tick(30_999).controlFrames).toEqual([]); // not due
    const outcome = sender.tick(31_000);
    expect(outcome.controlFrames).toHaveLength(1);
    const heartbeat = decodeHeartbeat(decodeControl(outcome.controlFrames[0]).payload);
    expect(heartbeat.monotonicSeq).toBe(2);
    expect(heartbeat.senderEpochMs).toBe(31_000);

    expect(sender.tick(50_000).controlFrames).toEqual([]); // not due yet
    const next = sender.tick(61_000);
    expect(decodeHeartbeat(decodeControl(next.controlFrames[0]).payload).monotonicSeq).toBe(3);
  });

  it("flags the connection dead after the silence timeout, resetting per connection", () => {
    const sender = new TransferSession({ silenceTimeoutMs: 90_000 });

    // No activity baseline yet: the first tick starts the clock.
    expect(sender.tick(0).peerSilent).toBe(false);
    expect(sender.tick(89_999).peerSilent).toBe(false);
    expect(sender.tick(90_000).peerSilent).toBe(true);

    // Fresh connection: baseline resets.
    sender.onConnectionGone();
    expect(sender.tick(90_100).peerSilent).toBe(false);

    // Inbound activity defers the verdict.
    sender.noteActivity(100_000);
    expect(sender.tick(150_000).peerSilent).toBe(false);
    expect(sender.tick(190_000).peerSilent).toBe(true);
  });
});

describe("TransferSession — protocol ERROR", () => {
  it("releases the matching receiver slot on transaction_expired with a leading txid (structural)", () => {
    const sender = new TransferSession({ sender: QUIET_SENDER });
    const receiver = new TransferSession();
    const { transactionId, frames } = sender.openSend(filler(300), 0);

    receiver.acceptDataFrame(frames[0], 1);
    expect(receiver.pendingCount).toBe(1);

    // SPEC §5 convention: the message starts with the affected txid
    // (32 lowercase hex); the remainder is free-form.
    const errorFrame = makeFrame(
      FrameType.ERROR,
      encodeError({
        code: ErrorCode.TransactionExpired,
        message: `${transactionId} expired after 120s retention`,
      }),
    );
    const outcome = receiver.onControlFrame(errorFrame, 2);
    expect(receiver.pendingCount).toBe(0);
    expect(outcome.controlFrames).toEqual([]);
  });

  it("does NOT parse a txid from mid-message (structural parse only)", () => {
    const sender = new TransferSession({ sender: QUIET_SENDER });
    const receiver = new TransferSession();
    const { transactionId, frames } = sender.openSend(filler(300), 0);

    receiver.acceptDataFrame(frames[0], 1);
    expect(receiver.pendingCount).toBe(1);

    // Same txid, but not at the head of the message — not a structural
    // reference; the slot survives.
    const errorFrame = makeFrame(
      FrameType.ERROR,
      encodeError({
        code: ErrorCode.TransactionExpired,
        message: `transaction ${transactionId} expired`,
      }),
    );
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    receiver.onControlFrame(errorFrame, 2);
    expect(receiver.pendingCount).toBe(1);
    warnSpy.mockRestore();
  });

  it("logs undecodable ERROR payloads without crashing", () => {
    const receiver = new TransferSession();
    const garbage = makeFrame(FrameType.ERROR, new Uint8Array([0x00])); // < 2 bytes
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const outcome = receiver.onControlFrame(garbage, 0);
    expect(outcome.controlFrames).toEqual([]);
    expect(errSpy).toHaveBeenCalled();
    errSpy.mockRestore();
  });

  it("handles a valid non-expiry ERROR code without crashing", () => {
    const receiver = new TransferSession();
    const frame = makeFrame(FrameType.ERROR, encodeError({ code: ErrorCode.BackpressureStall, message: "" }));
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const outcome = receiver.onControlFrame(frame, 0);
    expect(outcome.controlFrames).toEqual([]);
    expect(errSpy).toHaveBeenCalledTimes(1);
    errSpy.mockRestore();
  });
});

describe("TransferSession — delivery without a registered handler", () => {
  it("emits the final ACK immediately on completion even without a handler; the payload is retained", () => {
    const sender = new TransferSession({ sender: QUIET_SENDER });
    const receiver = new TransferSession(); // no handler registered

    const payload = filler(300);
    const { transactionId, frames } = sender.openSend(payload, 0);

    const acks = feed(receiver, frames, 1);
    // The final ACK reflects reassembly, not the handler — it goes out now.
    expect(acks).toHaveLength(1);
    const ackFrame = decodeControl(acks[0]);
    expect(ackFrame.frameType).toBe(FrameType.ACK);
    expect(decodeAck(ackFrame.payload)).toEqual([
      { transactionId, highestContiguousSeq: 0, receivedFrames: 1 },
    ]);
    // The completed slot (payload included) is retained.
    expect(receiver.pendingCount).toBe(1);
  });

  it("delivers each retained payload exactly once when the handler registers late", () => {
    const sender = new TransferSession({ sender: QUIET_SENDER });
    const receiver = new TransferSession();

    const payload = filler(300);
    const { transactionId, frames } = sender.openSend(payload, 0);
    expect(feed(receiver, frames, 1)).toHaveLength(1); // immediate final ACK, no handler
    expect(receiver.pendingCount).toBe(1);

    const delivered: Delivered[] = [];
    receiver.onBinary = (id, p) => delivered.push({ transactionId: id, payload: p });
    expect(delivered).toHaveLength(1);
    expect(delivered[0].transactionId).toBe(transactionId);
    expectBytesEqual(delivered[0].payload, payload);
    // Delivery consumed the slot.
    expect(receiver.pendingCount).toBe(0);

    // Re-registering the handler must not re-deliver (no retained slots).
    receiver.onBinary = (id, p) => delivered.push({ transactionId: id, payload: p });
    expect(delivered).toHaveLength(1);
  });

  it("duplicate frames on a retained slot re-issue the ACK but do not deliver twice", () => {
    const sender = new TransferSession({ sender: QUIET_SENDER });
    const receiver = new TransferSession();

    const payload = filler(300);
    const { transactionId, frames } = sender.openSend(payload, 0);
    feed(receiver, frames, 1); // completed, no handler → retained
    expect(receiver.pendingCount).toBe(1);

    // A duplicate frame while the slot is retained (still no handler):
    // the final ACK is re-issued (idempotent), nothing is delivered.
    const dupAcks = receiver.acceptDataFrame(frames[0], 2);
    expect(dupAcks).toHaveLength(1);
    expect(decodeControl(dupAcks[0]).frameType).toBe(FrameType.ACK);
    expect(receiver.pendingCount).toBe(1);

    // The late registration delivers the payload exactly once.
    const delivered: Delivered[] = [];
    receiver.onBinary = (id, p) => delivered.push({ transactionId: id, payload: p });
    expect(delivered).toHaveLength(1);
    expect(delivered[0].transactionId).toBe(transactionId);
    expectBytesEqual(delivered[0].payload, payload);
    expect(receiver.pendingCount).toBe(0);
  });

  it("retro-delivers multiple retained transactions in slot order", () => {
    const sender = new TransferSession({ sender: QUIET_SENDER });
    const receiver = new TransferSession();

    const t1 = sender.openSend(filler(300, 1), 0);
    const t2 = sender.openSend(filler(300, 2), 1);
    feed(receiver, t1.frames, 2);
    feed(receiver, t2.frames, 3);
    expect(receiver.pendingCount).toBe(2);

    const delivered: Delivered[] = [];
    receiver.onBinary = (id, p) => delivered.push({ transactionId: id, payload: p });
    expect(delivered.map((d) => d.transactionId)).toEqual([t1.transactionId, t2.transactionId]);
    expect(receiver.pendingCount).toBe(0);
  });

  it("mixes retro-delivery with normal delivery without duplication and releases the sender copies", () => {
    const sender = new TransferSession({ sender: QUIET_SENDER });
    const receiver = new TransferSession();

    // t1 completes with no handler: final ACK out, payload retained.
    const t1 = sender.openSend(filler(300, 1), 0);
    const ack1 = decodeControl(feed(receiver, t1.frames, 1)[0]);
    expect(receiver.pendingCount).toBe(1);

    // Registering the handler delivers t1; t2 then flows the normal path.
    const delivered: Delivered[] = [];
    receiver.onBinary = (id, p) => delivered.push({ transactionId: id, payload: p });
    const t2 = sender.openSend(filler(300, 2), 2);
    const ack2 = decodeControl(feed(receiver, t2.frames, 3)[0]);

    expect(delivered.map((d) => d.transactionId)).toEqual([t1.transactionId, t2.transactionId]);
    expect(receiver.pendingCount).toBe(0);
    // Both final ACKs release the sender's copies.
    sender.onControlFrame(ack1, 4);
    sender.onControlFrame(ack2, 5);
    expect(sender.copyCount).toBe(0);
  });
});
