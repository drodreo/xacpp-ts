/**
 * Transaction reassembler (docs/SPEC.md §3 "Reassembly criteria" and
 * §5 "Reliability semantics", receiver side).
 *
 * One slot per `transactionId`; frames may arrive in any order.
 * Reassembly criteria (SPEC §3): frames contiguous from seq 0 AND
 * `sum(frame_len) == total_len`. Duplicates are idempotent; overruns
 * (bytes beyond `total_len`) and `total_len` mismatches are protocol
 * violations.
 *
 * Time-injected: `push` takes `now` for validation symmetry only —
 * slots live until delivered (`take`) or explicitly discarded
 * (`discard`); there is no stale sweep (SPEC §5: incomplete slots are
 * the recovery adjudication surface and are never reaped by time).
 */

import { CONTROL_FRAME_MAX, TRANSACTION_ID_SIZE } from "../wire/frame";
import type { Frame } from "../wire/frame";
import type { AckEntry, NackEntry } from "../wire/control";

/** Error raised on transaction-level protocol violations (receiver side). */
export class ReassemblyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReassemblyError";
  }
}

/**
 * Max missing seqs per NACK entry so that one entry always fits the
 * 16 KiB control-frame cap (SPEC §4):
 * count(1) + txid(16) + missing_count(2) + 4·n ≤ 16384.
 */
export const NACK_MISSING_SEQS_MAX = Math.floor(
  (CONTROL_FRAME_MAX - 1 - TRANSACTION_ID_SIZE - 2) / 4,
);

/** Result of feeding one frame into the reassembler. */
export interface PushOutcome {
  transactionId: string;
  /** True when this seq was already held — the frame is ignored (idempotent). */
  duplicate: boolean;
  /** True when this frame completed the transaction (exactly once per transaction). */
  completed: boolean;
  /**
   * Seq of the highest contiguously-received frame after this push
   * (-1 when seq 0 is absent). Integration layers build cumulative ACK
   * entries from it without a second frontier walk.
   */
  frontierSeq: number;
}

interface Slot {
  transactionId: string;
  totalLen: number;
  /** seq → payload (distinct frames only). */
  frames: Map<number, Uint8Array>;
  /** Sum of distinct received frame lengths. */
  receivedBytes: number;
  /** Highest seq seen so far; -1 when nothing received. */
  highestReceivedSeq: number;
}

/** Contiguous frontier walk: returns the first missing seq and the byte count of the contiguous prefix. */
function frontier(slot: Slot): { next: number; bytes: number } {
  let seq = 0;
  let bytes = 0;
  for (;;) {
    const payload = slot.frames.get(seq);
    if (payload === undefined) break;
    bytes += payload.length;
    seq++;
  }
  return { next: seq, bytes };
}

function isComplete(slot: Slot): boolean {
  const { bytes } = frontier(slot);
  return bytes === slot.totalLen;
}

export class TransactionReassembler {
  private readonly slots = new Map<string, Slot>();

  /** Number of live slots (complete-but-undelivered included). */
  get pendingCount(): number {
    return this.slots.size;
  }

  /** Whether a slot exists for the transaction. */
  has(transactionId: string): boolean {
    return this.slots.has(transactionId);
  }

  /** Whether the transaction currently satisfies both reassembly criteria (SPEC §3). */
  isComplete(transactionId: string): boolean {
    const slot = this.slots.get(transactionId);
    return slot !== undefined && isComplete(slot);
  }

  /**
   * Contiguous-frontier accessor for the streaming receive path (read-only):
   * `seq` is the next missing seq (== count of the contiguous prefix),
   * `bytes` the prefix's byte total. `null` for unknown transactions.
   */
  frontierOf(transactionId: string): { seq: number; bytes: number } | null {
    const slot = this.slots.get(transactionId);
    if (slot === undefined) return null;
    const { next, bytes } = frontier(slot);
    return { seq: next, bytes };
  }

  /** Declared transaction total of a live slot; `null` when unknown. */
  totalLenOf(transactionId: string): number | null {
    return this.slots.get(transactionId)?.totalLen ?? null;
  }

  /**
   * Peek one held frame by seq without consuming anything (read-only,
   * streaming receive path). `null` when the transaction is unknown or the
   * seq was never received.
   */
  frameAt(transactionId: string, seq: number): Uint8Array | null {
    return this.slots.get(transactionId)?.frames.get(seq) ?? null;
  }

  /**
   * Feed one data frame (SPEC §3/§5). Duplicates are idempotent.
   * Throws {@link ReassemblyError} on:
   * - `total_len` mismatch against the slot's declared total;
   * - a duplicate seq carrying a different payload length;
   * - byte overrun (sum of distinct frame lengths exceeding `total_len`).
   */
  push(frame: Frame, now: number): PushOutcome {
    if (!Number.isSafeInteger(now) || now < 0) {
      throw new RangeError(`now must be a non-negative safe integer, got: ${now}`);
    }
    let slot = this.slots.get(frame.transactionId);
    if (slot === undefined) {
      slot = {
        transactionId: frame.transactionId,
        totalLen: frame.totalLen,
        frames: new Map(),
        receivedBytes: 0,
        highestReceivedSeq: -1,
      };
      this.slots.set(frame.transactionId, slot);
    } else if (slot.totalLen !== frame.totalLen) {
      throw new ReassemblyError(
        `total_len mismatch for transaction ${frame.transactionId}: header says ${frame.totalLen}, slot declared ${slot.totalLen}`,
      );
    }

    const existing = slot.frames.get(frame.seq);
    if (existing !== undefined) {
      if (existing.length !== frame.payload.length) {
        throw new ReassemblyError(
          `duplicate seq ${frame.seq} of transaction ${frame.transactionId} carries a different length (${frame.payload.length} != ${existing.length})`,
        );
      }
      return {
        transactionId: frame.transactionId,
        duplicate: true,
        completed: isComplete(slot),
        frontierSeq: frontier(slot).next - 1,
      };
    }

    if (slot.receivedBytes + frame.payload.length > slot.totalLen) {
      throw new ReassemblyError(
        `byte overrun for transaction ${frame.transactionId}: ${slot.receivedBytes + frame.payload.length} > total_len ${slot.totalLen}`,
      );
    }
    slot.frames.set(frame.seq, frame.payload);
    slot.receivedBytes += frame.payload.length;
    if (frame.seq > slot.highestReceivedSeq) slot.highestReceivedSeq = frame.seq;

    return {
      transactionId: frame.transactionId,
      duplicate: false,
      completed: isComplete(slot),
      frontierSeq: frontier(slot).next - 1,
    };
  }

  /**
   * Deliver the complete payload (seq-ordered concatenation) and release
   * the slot (delivery consumes the transaction, SPEC §5). Throws
   * {@link ReassemblyError} when the transaction is unknown or incomplete.
   */
  take(transactionId: string): Uint8Array {
    const slot = this.slots.get(transactionId);
    if (slot === undefined) {
      throw new ReassemblyError(`unknown transaction ${transactionId}`);
    }
    const { next, bytes } = frontier(slot);
    if (bytes !== slot.totalLen) {
      throw new ReassemblyError(
        `transaction ${transactionId} is incomplete (contiguous ${bytes}/${slot.totalLen} bytes, frontier seq ${next})`,
      );
    }
    const out = new Uint8Array(slot.totalLen);
    let offset = 0;
    for (let seq = 0; seq < next; seq++) {
      const payload = slot.frames.get(seq);
      if (payload === undefined) {
        throw new ReassemblyError(
          `transaction ${transactionId} lost frame ${seq} while assembling (frontier seq ${next})`,
        );
      }
      out.set(payload, offset);
      offset += payload.length;
    }
    this.slots.delete(transactionId);
    return out;
  }

  /**
   * Enumerate completed-but-undelivered transaction ids (slot order).
   * These slots hold their payload until `take` delivers it — delivery
   * is the caller's duty (SPEC §5).
   */
  completedUntaken(): string[] {
    const out: string[] = [];
    for (const slot of this.slots.values()) {
      if (isComplete(slot)) out.push(slot.transactionId);
    }
    return out;
  }

  /**
   * Export cumulative ACK entries for all live transactions that hold a
   * contiguous prefix (seq 0 present — without it there is no cumulative
   * value a `u32` could carry). Completed-but-undelivered slots are
   * included so the final ACK is re-exportable until `take`.
   */
  ackEntries(): AckEntry[] {
    const out: AckEntry[] = [];
    for (const slot of this.slots.values()) {
      const { next } = frontier(slot);
      if (next === 0) continue; // seq 0 absent: nothing contiguous to report
      out.push({
        transactionId: slot.transactionId,
        highestContiguousSeq: next - 1,
        receivedFrames: slot.frames.size,
      });
    }
    return out;
  }

  /**
   * Export gap lists for incomplete transactions: every missing seq below
   * the transaction's highest received seq, per entry capped at
   * {@link NACK_MISSING_SEQS_MAX} (lowest seqs kept) so one entry always
   * fits the 16 KiB control-frame cap. Completed transactions and slots
   * without received frames produce no entry.
   */
  nackEntries(): NackEntry[] {
    const out: NackEntry[] = [];
    for (const slot of this.slots.values()) {
      if (isComplete(slot) || slot.highestReceivedSeq < 0) continue;
      const missingSeqs: number[] = [];
      for (let seq = 0; seq <= slot.highestReceivedSeq; seq++) {
        if (missingSeqs.length >= NACK_MISSING_SEQS_MAX) break;
        if (!slot.frames.has(seq)) missingSeqs.push(seq);
      }
      if (missingSeqs.length > 0) {
        out.push({ transactionId: slot.transactionId, missingSeqs });
      }
    }
    return out;
  }

  /**
   * Explicitly discard a slot: the recovery cross-check (the holder's
   * declaration did not cover this still-open slot), a peer ERROR
   * `transaction_expired`, or a local TRANSACTION_ABORT.
   */
  discard(transactionId: string): boolean {
    return this.slots.delete(transactionId);
  }

  /**
   * Ids of all open (incomplete) receive slots — the recovery
   * cross-check surface: an incomplete slot the holder did not declare can
   * never complete and is cancelled. Completed-but-undelivered slots are
   * excluded (they need nothing more from the holder).
   */
  incompleteTransactions(): string[] {
    const out: string[] = [];
    for (const slot of this.slots.values()) {
      if (!isComplete(slot)) out.push(slot.transactionId);
    }
    return out;
  }
}
