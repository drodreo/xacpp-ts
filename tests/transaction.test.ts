/**
 * Transaction sender tests (docs/SPEC.md §5).
 *
 * Coverage:
 * 1. Chunking into DATA_BINARY working frames (1 MiB default, tail remainder,
 *    seq/total_len header fields) and byte-exact reassembly of the frame list
 * 2. Copy lifecycle: registration, watermark advance, release on final ACK
 * 3. Timeout retransmission: initial window, ×2 exponential backoff, cap
 * 4. NACK-driven immediate retransmission lists (dedup, out-of-range ignored)
 * 5. Backpressure: paused above the cap, recovers when ACKs bring usage back
 * 6. Recovery discipline (holder side): reconnect declaration gates
 *    pending copies; verdicts lift the gate + retransmit (resumed) or
 *    fail the transfer (unknown/rejected); no retention ceiling (copy
 *    lifetime == transaction lifetime, SPEC §5 v3)
 */

import { describe, it, expect } from "vitest";
import { FrameType, encodeFrame, FrameDecoder } from "../src/wire/frame";
import { TransactionSender, SenderError, DEFAULT_WORKING_FRAME_BYTES } from "../src/reliability/transaction";
import { RESUME_STATUS } from "../src/wire/control";

function filler(length: number, seed = 0x51ed): Uint8Array {
  const out = new Uint8Array(length);
  let s = seed >>> 0;
  for (let i = 0; i < length; i++) {
    s = (s * 1664525 + 1013904223) >>> 0;
    out[i] = s & 0xff;
  }
  return out;
}

/** Small working frames keep tests fast; reassembly proves wire equality. */
function makeSender(overrides?: Partial<ConstructorParameters<typeof TransactionSender>[0]>) {
  return new TransactionSender({
    workingFrameBytes: 8,
    initialTimeoutMs: 100,
    maxTimeoutMs: 800,
    backpressureCapBytes: 32,
    ...overrides,
  });
}

function ackEntry(transactionId: string, highestContiguousSeq: number, receivedFrames: number) {
  return { transactionId, highestContiguousSeq, receivedFrames };
}

function reassembleFrames(frames: ReturnType<TransactionSender["open"]>["frames"]): Uint8Array {
  const decoder = new FrameDecoder();
  const received = frames.flatMap((f) => decoder.push(encodeFrame(f)));
  expect(received.length).toBe(frames.length);
  const sorted = [...received].sort((a, b) => a.seq - b.seq);
  const out = new Uint8Array(sorted.reduce((n, f) => n + f.payload.length, 0));
  let offset = 0;
  for (const frame of sorted) {
    out.set(frame.payload, offset);
    offset += frame.payload.length;
  }
  return out;
}

describe("chunking", () => {
  it("slices the payload into working frames with a tail remainder", () => {
    const sender = makeSender(); // working frame = 8 bytes
    const payload = filler(20);
    const { transactionId, frames } = sender.open(payload, { now: 0 });
    expect(frames.length).toBe(3); // 8 + 8 + 4
    frames.forEach((frame, i) => {
      expect(frame.frameType).toBe(FrameType.DATA_BINARY);
      expect(frame.transactionId).toBe(transactionId);
      expect(frame.totalLen).toBe(20);
      expect(frame.seq).toBe(i);
    });
    expect(frames[2].payload.length).toBe(4);
    expect(reassembleFrames(frames)).toEqual(payload);
  });

  it("emits a single frame when the payload fits one working frame", () => {
    const sender = makeSender();
    const payload = filler(8);
    const { frames } = sender.open(payload, { now: 0 });
    expect(frames.length).toBe(1);
    expect(frames[0].payload).toEqual(payload);
  });

  it("defaults the working frame size to 1 MiB", () => {
    expect(DEFAULT_WORKING_FRAME_BYTES).toBe(1024 * 1024);
  });

  it("copies the payload so later caller mutation cannot corrupt retransmissions", () => {
    const sender = makeSender();
    const payload = filler(16);
    const { transactionId } = sender.open(payload, { now: 0 });
    const expected = payload.slice();
    payload.fill(0xff);
    const resent = sender.onNack({ transactionId, missingSeqs: [0] }, 0);
    expect(resent[0].payload).toEqual(expected.subarray(0, 8));
  });

  it("rejects empty payloads and duplicate transaction ids", () => {
    const sender = makeSender();
    expect(() => sender.open(new Uint8Array(0), { now: 0 })).toThrow(SenderError);
    const txid = "ab".repeat(16);
    sender.open(filler(4), { now: 0, transactionId: txid });
    expect(() => sender.open(filler(4), { now: 0, transactionId: txid })).toThrow(SenderError);
  });
});

describe("ACK processing and copy lifecycle", () => {
  it("advances the watermark monotonically and releases on the final ACK", () => {
    const sender = makeSender();
    const payload = filler(16); // 2 frames of 8
    const { transactionId } = sender.open(payload, { now: 0 });
    expect(sender.copyCount).toBe(1);
    expect(sender.unackedBytes).toBe(16);

    expect(sender.onAck(ackEntry(transactionId, 0, 1), 10)).toEqual({ known: true, released: false });
    expect(sender.unackedBytes).toBe(8);
    // Older watermark is ignored (monotonic).
    expect(sender.onAck(ackEntry(transactionId, 0, 1), 20)).toEqual({ known: true, released: false });
    expect(sender.unackedBytes).toBe(8);
    // Final ACK (seq 1 is the last frame) releases the copy.
    expect(sender.onAck(ackEntry(transactionId, 1, 2), 30)).toEqual({ known: true, released: true });
    expect(sender.copyCount).toBe(0);
    expect(sender.unackedBytes).toBe(0);
    expect(sender.onAck(ackEntry(transactionId, 1, 2), 40)).toEqual({ known: false, released: false });
  });

  it("clamps watermarks beyond the final frame to completion", () => {
    const sender = makeSender();
    const { transactionId } = sender.open(filler(8), { now: 0 });
    expect(sender.onAck(ackEntry(transactionId, 99, 1), 0).released).toBe(true);
  });
});

describe("timeout retransmission", () => {
  it("fires after the initial window and backs off ×2 up to the cap", () => {
    const sender = makeSender(); // initial 100, cap 800
    const payload = filler(16);
    const { transactionId, frames } = sender.open(payload, { now: 0 });

    expect(sender.dueRetransmissions(50)).toEqual([]); // before the window
    expect(sender.dueRetransmissions(100)).toEqual(frames); // at the window
    // Window after fire 1: 100 * 2^1 = 200 → next due at 300.
    expect(sender.dueRetransmissions(299)).toEqual([]);
    expect(sender.dueRetransmissions(300)).toEqual(frames);
    // After fire 2: 100 * 2^2 = 400 → due at 700; after fire 3: 800 → capped.
    expect(sender.dueRetransmissions(700)).toEqual(frames);
    expect(sender.dueRetransmissions(1500)).toEqual(frames); // capped at 800 from 700
  });

  it("retransmits only unconfirmed frames above the watermark", () => {
    const sender = makeSender();
    const payload = filler(24); // 3 frames
    const { transactionId, frames } = sender.open(payload, { now: 0 });
    sender.onAck(ackEntry(transactionId, 0, 1), 10); // progress resets the window → due at 110
    expect(sender.dueRetransmissions(100)).toEqual([]);
    expect(sender.dueRetransmissions(110)).toEqual([frames[1], frames[2]]);
  });

  it("progress resets the backoff window", () => {
    const sender = makeSender();
    const payload = filler(24);
    const { transactionId } = sender.open(payload, { now: 0 });
    expect(sender.dueRetransmissions(100).length).toBe(3);
    sender.onAck(ackEntry(transactionId, 1, 2), 150); // progress at t=150
    expect(sender.dueRetransmissions(249)).toEqual([]); // window restarted: due at 250
    expect(sender.dueRetransmissions(250).length).toBe(1); // only seq 2 above the watermark
  });
});

describe("NACK-driven retransmission", () => {
  it("returns the listed frames immediately, deduped and range-checked", () => {
    const sender = makeSender();
    const payload = filler(24);
    const { transactionId, frames } = sender.open(payload, { now: 0 });
    const resent = sender.onNack({ transactionId, missingSeqs: [2, 0, 2, 99] }, 5);
    expect(resent).toEqual([frames[2], frames[0]]); // 99 out of range, 2 deduped
    expect(sender.copyCount).toBe(1); // NACK never releases the copy
  });

  it("returns nothing for unknown transactions", () => {
    const sender = makeSender();
    expect(sender.onNack({ transactionId: "cd".repeat(16), missingSeqs: [0] }, 0)).toEqual([]);
  });
});

describe("backpressure", () => {
  it("pauses above the cap and recovers as ACKs bring usage back", () => {
    const sender = makeSender({ backpressureCapBytes: 16 }); // cap: 16 bytes
    const first = sender.open(filler(16), { now: 0 });
    expect(sender.paused).toBe(false); // 16 unacked, not above the cap
    const second = sender.open(filler(8, 7), { now: 1 });
    expect(sender.paused).toBe(true); // 24 > 16
    sender.onAck(ackEntry(first.transactionId, 1, 2), 2); // first fully acked
    expect(sender.unackedBytes).toBe(8);
    expect(sender.paused).toBe(false); // fell back
    sender.onAck(ackEntry(second.transactionId, 0, 1), 3);
    expect(sender.copyCount).toBe(0);
  });
});

describe("recovery discipline (holder side)", () => {
  it("declarePending gates every pending copy and declares the written progress", () => {
    const sender = makeSender();
    const first = sender.open(filler(24), { now: 0 });
    const second = sender.open(filler(8, 7), { now: 0 });
    const entries = sender.declarePending(10);
    expect(entries).toEqual([
      { transactionId: first.transactionId, highestContiguousSeq: 2, receivedFrames: 3 },
      { transactionId: second.transactionId, highestContiguousSeq: 0, receivedFrames: 1 },
    ]);
    expect(sender.isAwaitingResume(first.transactionId)).toBe(true);
    expect(sender.isAwaitingResume(second.transactionId)).toBe(true);
    // The retransmission clock skips gated copies (no blind retransmission
    // across a reconnect — the verdict is the sole trigger).
    expect(sender.dueRetransmissions(1_000_000)).toEqual([]);
    // An unknown id reads as ungated.
    expect(sender.isAwaitingResume("ef".repeat(16))).toBe(false);
  });

  it("Resumed lifts the gate and retransmits every unconfirmed frame", () => {
    const sender = makeSender();
    const { transactionId, frames } = sender.open(filler(24), { now: 0 });
    sender.onAck(ackEntry(transactionId, 0, 1), 5); // frame 0 confirmed
    sender.declarePending(10);
    const outcome = sender.applyResumeVerdict({ transactionId, status: RESUME_STATUS.Resumed }, 20);
    expect(outcome).toEqual({ known: true, confirmed: false, failed: false, retransmit: [frames[1], frames[2]] });
    expect(sender.isAwaitingResume(transactionId)).toBe(false);
    // The retransmission clock is rearmed (fresh window from the verdict).
    expect(sender.dueRetransmissions(20 + 99)).toEqual([]);
    expect(sender.dueRetransmissions(20 + 100)).toEqual([frames[1], frames[2]]);
    // The copy stays live; the final ACK still completes it.
    expect(sender.onAck(ackEntry(transactionId, 2, 3), 600).released).toBe(true);
  });

  it("Unknown fails an UNFINISHED transfer (frames still owed): the copy is released", () => {
    const sender = makeSender();
    const { transactionId } = sender.openStream({ now: 0, totalLen: 16 });
    sender.appendChunk(transactionId, filler(8), { now: 1 }); // half written — frames still owed
    sender.declarePending(10);
    expect(sender.applyResumeVerdict({ transactionId, status: RESUME_STATUS.Unknown }, 20)).toEqual({
      known: true,
      confirmed: false,
      failed: true,
      retransmit: [],
    });
    expect(sender.copyCount).toBe(0);
    // A replayed verdict for the released id fires nothing twice.
    expect(sender.applyResumeVerdict({ transactionId, status: RESUME_STATUS.Unknown }, 30)).toEqual({
      known: false,
      confirmed: false,
      failed: false,
      retransmit: [],
    });
  });

  it("Unknown CONFIRMS a fully-sent transfer (holder-side adjudication, SPEC §5)", () => {
    const sender = makeSender();
    // open() frames the whole payload: tail frame plus the completion
    // announcement went out — the holder's own knowledge says fully sent.
    const { transactionId } = sender.open(filler(16), { now: 0 });
    sender.declarePending(10);
    expect(sender.applyResumeVerdict({ transactionId, status: RESUME_STATUS.Unknown }, 20)).toEqual({
      known: true,
      confirmed: true,
      failed: false,
      retransmit: [],
    });
    expect(sender.copyCount).toBe(0);
  });

  it("Rejected adjudicates exactly like Unknown (wire-compat value): fully sent ⇒ confirmed", () => {
    const sender = makeSender();
    const { transactionId } = sender.open(filler(16), { now: 0 });
    sender.declarePending(10);
    expect(sender.applyResumeVerdict({ transactionId, status: RESUME_STATUS.Rejected }, 20)).toEqual({
      known: true,
      confirmed: true,
      failed: false,
      retransmit: [],
    });
    expect(sender.copyCount).toBe(0);
  });

  it("an aborted copy is never declared; the txid is free for reuse (no tombstones)", () => {
    const sender = makeSender();
    const { transactionId } = sender.openStream({ now: 0, totalLen: 16 });
    sender.abortStream(transactionId, { now: 10 });
    // No terminal ledger: abort releases the copy from pending and leaves
    // no trace, so a declaration does not carry it (the receiver answers
    // `unknown` exactly like for a never-seen transaction).
    expect(sender.declarePending(20)).toEqual([]);
    // The released txid is free again (uniqueness is guaranteed by UUID
    // generation; the pending table self-checks live conflicts).
    expect(() => sender.openStream({ now: 30, transactionId, totalLen: 16 })).not.toThrow();
    sender.abortStream(transactionId, { now: 40 }); // release it again
    expect(() => sender.open(filler(16), { now: 50, transactionId })).not.toThrow();
  });

  it("a paused copy only lifts the gate on Resumed — the pause intent dominates", () => {
    const sender = makeSender();
    const { transactionId } = sender.openStream({ now: 0, totalLen: 16 });
    sender.setStreamPaused(transactionId, true, { now: 1 });
    sender.appendChunk(transactionId, filler(8), { now: 2 }); // held while paused
    sender.declarePending(10);
    const outcome = sender.applyResumeVerdict({ transactionId, status: RESUME_STATUS.Resumed }, 20);
    expect(outcome).toEqual({ known: true, confirmed: false, failed: false, retransmit: [] });
    expect(sender.isAwaitingResume(transactionId)).toBe(false);
    // Still paused: the retransmission clock stays suspended.
    expect(sender.dueRetransmissions(1_000_000)).toEqual([]);
    // Unpausing rearms the window and flushes the held frames.
    const held = sender.setStreamPaused(transactionId, false, { now: 30 });
    expect(held).toHaveLength(1);
    expect(sender.dueRetransmissions(30 + 100)).toEqual(held);
  });
});

describe("option validation", () => {
  it("rejects non-positive or fractional options", () => {
    expect(() => new TransactionSender({ workingFrameBytes: 0 })).toThrow(RangeError);
    expect(() => new TransactionSender({ initialTimeoutMs: -1 })).toThrow(RangeError);
    expect(() => new TransactionSender({ maxTimeoutMs: 1.5 })).toThrow(RangeError);
    expect(() => new TransactionSender({ backpressureCapBytes: 0 })).toThrow(RangeError);
  });

  it("validates the now parameter on timing-bearing methods", () => {
    const sender = makeSender();
    expect(() => sender.open(filler(4), { now: -1 })).toThrow(RangeError);
    const { transactionId } = sender.open(filler(4), { now: 0 });
    expect(() => sender.onAck(ackEntry(transactionId, 0, 1), 1.5)).toThrow(RangeError);
    expect(() => sender.dueRetransmissions(-1)).toThrow(RangeError);
  });
});
