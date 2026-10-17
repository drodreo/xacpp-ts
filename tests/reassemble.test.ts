/**
 * Transaction reassembler tests (docs/SPEC.md §3/§5).
 *
 * Coverage:
 * 1. In-order assembly and bit-exact delivery of the concatenated payload
 * 2. Out-of-order arrival: every permutation of small frame counts completes
 * 3. Duplicate frames are idempotent (receivedFrames / frontier untouched)
 * 4. Gap detection + NACK export; missing lists capped to the control-frame budget
 * 5. Multi-transaction concurrency and interleaving
 * 6. Protocol violations: total_len mismatch, byte overrun, duplicate length mismatch
 * 7. Delivery consumes the slot; take() on unknown/incomplete throws
 * 8. ACK export: cumulative frontier + distinct received counts; zero-progress slots omitted
 * 9. Stale cleanup: only incomplete, past-retention slots are reaped
 */

import { describe, it, expect } from "vitest";
import { makeFrame, FrameType, newTransactionId } from "../src/frame";
import type { Frame } from "../src/frame";
import { TransactionReassembler, ReassemblyError, NACK_MISSING_SEQS_MAX } from "../src/reassemble";

/** Split `payload` into `chunkSize`-byte DATA_BINARY frames of one transaction. */
function chunk(
  payload: Uint8Array,
  chunkSize: number,
  opts?: { transactionId?: string; totalLen?: number },
): Frame[] {
  const transactionId = opts?.transactionId ?? newTransactionId();
  const frames: Frame[] = [];
  for (let offset = 0, seq = 0; offset < payload.length; offset += chunkSize, seq++) {
    frames.push(
      makeFrame(FrameType.DATA_BINARY, payload.subarray(offset, offset + chunkSize), {
        transactionId,
        totalLen: opts?.totalLen ?? payload.length,
        seq,
      }),
    );
  }
  return frames;
}

function bytesOf(...byteRows: number[][]): Uint8Array {
  const out = new Uint8Array(byteRows.reduce((n, row) => n + row.length, 0));
  let offset = 0;
  for (const row of byteRows) {
    out.set(row, offset);
    offset += row.length;
  }
  return out;
}

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

function permutations<T>(items: T[]): T[][] {
  if (items.length <= 1) return [items];
  const out: T[][] = [];
  for (let i = 0; i < items.length; i++) {
    const rest = [...items.slice(0, i), ...items.slice(i + 1)];
    for (const perm of permutations(rest)) out.push([items[i], ...perm]);
  }
  return out;
}

describe("assembly and delivery", () => {
  it("assembles in-order frames and delivers a bit-exact payload", () => {
    const payload = filler(10_000);
    const frames = chunk(payload, 1024);
    expect(frames.length).toBe(10);
    const reassembler = new TransactionReassembler();
    const txid = frames[0].transactionId;
    let completed = false;
    for (const frame of frames) {
      completed = reassembler.push(frame, 0).completed;
    }
    expect(completed).toBe(true);
    expect(reassembler.isComplete(txid)).toBe(true);
    expect(reassembler.take(txid)).toEqual(payload);
    expect(reassembler.pendingCount).toBe(0);
  });

  it("completes from every permutation of 4 out-of-order frames", () => {
    const payload = bytesOf(
      [1, 2, 3, 4],
      [5, 6, 7, 8],
      [9, 10, 11, 12],
      [13, 14, 15, 16],
    );
    for (const perm of permutations([0, 1, 2, 3])) {
      const frames = chunk(payload, 4);
      const reassembler = new TransactionReassembler();
      let completedOn: number | null = null;
      perm.forEach((seq, i) => {
        const outcome = reassembler.push(frames[seq], 0);
        if (outcome.completed) completedOn = i;
      });
      // Contiguity + total bytes requires every frame: completion lands on the final push.
      expect(completedOn).toBe(3);
      expect(reassembler.take(frames[0].transactionId)).toEqual(payload);
    }
  });

  it("handles zero-length final frames and single-frame transactions", () => {
    const reassembler = new TransactionReassembler();
    const single = makeFrame(FrameType.DATA_BINARY, new Uint8Array(0), { totalLen: 0, seq: 0 });
    const outcome = reassembler.push(single, 0);
    expect(outcome.completed).toBe(true);
    expect(reassembler.take(single.transactionId).length).toBe(0);
  });
});

describe("duplicates", () => {
  it("treats duplicate frames as idempotent no-ops", () => {
    const payload = filler(90);
    const frames = chunk(payload, 30); // exactly 3 frames
    const reassembler = new TransactionReassembler();
    reassembler.push(frames[0], 0);
    reassembler.push(frames[1], 0);
    const dup = reassembler.push(frames[0], 0);
    expect(dup.duplicate).toBe(true);
    expect(dup.completed).toBe(false);
    const ack = reassembler.ackEntries()[0];
    expect(ack.receivedFrames).toBe(2);
    expect(ack.highestContiguousSeq).toBe(1);
    reassembler.push(frames[2], 0);
    expect(reassembler.ackEntries()[0].receivedFrames).toBe(3);
    expect(reassembler.isComplete(frames[0].transactionId)).toBe(true);
  });

  it("rejects a duplicate seq carrying a different length", () => {
    const reassembler = new TransactionReassembler();
    const txid = newTransactionId();
    reassembler.push(makeFrame(FrameType.DATA_BINARY, new Uint8Array(4), { transactionId: txid, totalLen: 8, seq: 0 }), 0);
    expect(() =>
      reassembler.push(
        makeFrame(FrameType.DATA_BINARY, new Uint8Array(5), { transactionId: txid, totalLen: 8, seq: 0 }),
        0,
      ),
    ).toThrow(ReassemblyError);
  });
});

describe("gap detection and NACK export", () => {
  it("exports missing seqs below the highest received frame", () => {
    const payload = filler(10);
    const frames = chunk(payload, 2); // 5 frames
    const reassembler = new TransactionReassembler();
    const txid = frames[0].transactionId;
    reassembler.push(frames[0], 0); // seq 0
    reassembler.push(frames[2], 0); // seq 2 — gap at 1
    reassembler.push(frames[4], 0); // seq 4 — still missing 1, 3
    const nacks = reassembler.nackEntries();
    expect(nacks.length).toBe(1);
    expect(nacks[0].transactionId).toBe(txid);
    expect(nacks[0].missingSeqs).toEqual([1, 3]);
    // Fill seq 1: contiguous prefix advances; seq 3 is still missing below highest (4).
    reassembler.push(frames[1], 0);
    expect(reassembler.nackEntries()[0].missingSeqs).toEqual([3]);
    reassembler.push(frames[3], 0);
    expect(reassembler.nackEntries()).toEqual([]);
    expect(reassembler.isComplete(txid)).toBe(true);
  });

  it("caps NACK missing lists at the control-frame budget", () => {
    const reassembler = new TransactionReassembler();
    const txid = newTransactionId();
    // seq 0 and seq 5000 of a 5001-byte transaction → 4999 missing, above the cap.
    reassembler.push(makeFrame(FrameType.DATA_BINARY, new Uint8Array(1), { transactionId: txid, totalLen: 5001, seq: 0 }), 0);
    reassembler.push(makeFrame(FrameType.DATA_BINARY, new Uint8Array(1), { transactionId: txid, totalLen: 5001, seq: 5000 }), 0);
    const [entry] = reassembler.nackEntries();
    expect(entry.missingSeqs.length).toBe(NACK_MISSING_SEQS_MAX);
    expect(entry.missingSeqs[0]).toBe(1); // lowest seqs kept
    expect(NACK_MISSING_SEQS_MAX).toBe(4091); // (16384 - 1 - 16 - 2) / 4
  });

  it("omits slots without a contiguous prefix from ACK export; their gaps are real", () => {
    const reassembler = new TransactionReassembler();
    const txid = newTransactionId();
    reassembler.push(
      makeFrame(FrameType.DATA_BINARY, new Uint8Array(4), { transactionId: txid, totalLen: 8, seq: 0 }),
      0,
    );
    const fresh = newTransactionId();
    // First arrival is seq 1: no contiguous prefix yet, and seq 0 is a real gap.
    reassembler.push(
      makeFrame(FrameType.DATA_BINARY, new Uint8Array(4), { transactionId: fresh, totalLen: 8, seq: 1 }),
      0,
    );
    expect(reassembler.nackEntries()).toEqual([{ transactionId: fresh, missingSeqs: [0] }]);
    expect(reassembler.ackEntries().map((e) => e.transactionId)).toEqual([txid]);
    // Filling seq 0 promotes the slot into the ACK export.
    reassembler.push(
      makeFrame(FrameType.DATA_BINARY, new Uint8Array(4), { transactionId: fresh, totalLen: 8, seq: 0 }),
      0,
    );
    expect(reassembler.ackEntries().map((e) => e.transactionId)).toEqual([txid, fresh]);
  });
});

describe("concurrent transactions", () => {
  it("reassembles interleaved transactions independently", () => {
    const payloads = [filler(50, 1), filler(120, 2), filler(10, 3)];
    const reassembler = new TransactionReassembler();
    const chunks = payloads.map((p) => chunk(p, 20));
    // Interleave round-robin, reversed frame order within each round.
    const maxLen = Math.max(...chunks.map((c) => c.length));
    for (let i = 0; i < maxLen; i++) {
      for (const frames of chunks) {
        const idx = frames.length - 1 - i;
        if (idx >= 0) reassembler.push(frames[idx], 0);
      }
    }
    chunks.forEach((frames, i) => {
      expect(reassembler.isComplete(frames[0].transactionId)).toBe(true);
      expect(reassembler.take(frames[0].transactionId)).toEqual(payloads[i]);
    });
    expect(reassembler.pendingCount).toBe(0);
  });
});

describe("protocol violations", () => {
  it("rejects total_len mismatches within one transaction", () => {
    const reassembler = new TransactionReassembler();
    const txid = newTransactionId();
    reassembler.push(makeFrame(FrameType.DATA_BINARY, new Uint8Array(4), { transactionId: txid, totalLen: 8, seq: 0 }), 0);
    expect(() =>
      reassembler.push(
        makeFrame(FrameType.DATA_BINARY, new Uint8Array(4), { transactionId: txid, totalLen: 9, seq: 1 }),
        0,
      ),
    ).toThrow(ReassemblyError);
  });

  it("rejects byte overruns beyond total_len", () => {
    const reassembler = new TransactionReassembler();
    const txid = newTransactionId();
    reassembler.push(makeFrame(FrameType.DATA_BINARY, new Uint8Array(4), { transactionId: txid, totalLen: 6, seq: 0 }), 0);
    expect(() =>
      reassembler.push(
        makeFrame(FrameType.DATA_BINARY, new Uint8Array(4), { transactionId: txid, totalLen: 6, seq: 1 }),
        0,
      ),
    ).toThrow(ReassemblyError);
  });

  it("rejects frames pushed after the transaction already completed", () => {
    const reassembler = new TransactionReassembler();
    const txid = newTransactionId();
    reassembler.push(makeFrame(FrameType.DATA_BINARY, new Uint8Array(4), { transactionId: txid, totalLen: 4, seq: 0 }), 0);
    expect(() =>
      reassembler.push(
        makeFrame(FrameType.DATA_BINARY, new Uint8Array(2), { transactionId: txid, totalLen: 4, seq: 1 }),
        0,
      ),
    ).toThrow(ReassemblyError);
  });
});

describe("delivery consumes the slot", () => {
  it("throws on unknown or incomplete take() and after the slot is consumed", () => {
    const reassembler = new TransactionReassembler();
    const payload = filler(9);
    const frames = chunk(payload, 3);
    expect(() => reassembler.take(frames[0].transactionId)).toThrow(ReassemblyError);
    reassembler.push(frames[0], 0);
    expect(() => reassembler.take(frames[0].transactionId)).toThrow(ReassemblyError);
    reassembler.push(frames[1], 0);
    reassembler.push(frames[2], 0);
    reassembler.take(frames[0].transactionId);
    expect(() => reassembler.take(frames[0].transactionId)).toThrow(ReassemblyError);
    expect(reassembler.has(frames[0].transactionId)).toBe(false);
  });

  it("supports explicit discard", () => {
    const reassembler = new TransactionReassembler();
    const frames = chunk(filler(9), 3);
    reassembler.push(frames[0], 0);
    expect(reassembler.discard(frames[0].transactionId)).toBe(true);
    expect(reassembler.discard(frames[0].transactionId)).toBe(false);
    expect(reassembler.pendingCount).toBe(0);
  });
});

describe("stale cleanup", () => {
  it("reaps only incomplete slots past retention and refreshes on new frames", () => {
    const reassembler = new TransactionReassembler({ staleRetentionMs: 100 });
    const stale = chunk(filler(9), 3);
    const fresh = chunk(filler(9, 7), 3);
    reassembler.push(stale[0], 0); // last activity t=0
    reassembler.push(fresh[0], 90); // last activity t=90
    expect(reassembler.reapStale(50)).toEqual([]); // nothing past 100ms
    expect(reassembler.reapStale(150)).toEqual([stale[0].transactionId]); // 150-0 > 100
    expect(reassembler.has(stale[0].transactionId)).toBe(false);
    expect(reassembler.has(fresh[0].transactionId)).toBe(true);
  });

  it("never auto-reaps complete-but-undelivered slots", () => {
    const reassembler = new TransactionReassembler({ staleRetentionMs: 100 });
    const frames = chunk(filler(9), 3);
    frames.forEach((f) => reassembler.push(f, 0));
    expect(reassembler.reapStale(1_000_000)).toEqual([]);
    expect(reassembler.isComplete(frames[0].transactionId)).toBe(true);
  });

  it("validates the now parameter", () => {
    const reassembler = new TransactionReassembler();
    const frames = chunk(filler(3), 3);
    expect(() => reassembler.push(frames[0], -1)).toThrow(RangeError);
    expect(() => reassembler.push(frames[0], 1.5)).toThrow(RangeError);
    expect(() => reassembler.reapStale(-5)).toThrow(RangeError);
  });
});

describe("ACK export", () => {
  it("reports the cumulative frontier and distinct received counts", () => {
    const payload = filler(100);
    const frames = chunk(payload, 20); // 5 frames
    const reassembler = new TransactionReassembler();
    const txid = frames[0].transactionId;
    reassembler.push(frames[0], 0);
    reassembler.push(frames[2], 0);
    reassembler.push(frames[1], 0);
    const [entry] = reassembler.ackEntries();
    expect(entry).toEqual({ transactionId: txid, highestContiguousSeq: 2, receivedFrames: 3 });
    // Complete-but-undelivered keeps exporting the final ACK.
    reassembler.push(frames[3], 0);
    reassembler.push(frames[4], 0);
    expect(reassembler.ackEntries()).toEqual([
      { transactionId: txid, highestContiguousSeq: 4, receivedFrames: 5 },
    ]);
    reassembler.take(txid);
    expect(reassembler.ackEntries()).toEqual([]);
  });
});
