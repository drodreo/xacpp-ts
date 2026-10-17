/**
 * Transaction sender — reliability copy manager for outbound data
 * transactions (docs/SPEC.md §5, sender side).
 *
 * Responsibilities:
 * - Slice a transaction payload into DATA_BINARY working frames
 *   (~1 MiB, SPEC §4 recommendation) for the integration layer to write
 *   to egress;
 * - Retain unacknowledged copies until the final ACK, a peer RESUME
 *   rejection, or copy-retention expiry (default 120 s, SPEC §5);
 * - Advance the confirmed watermark on ACK; release on completion;
 * - Produce retransmission lists: timeout-driven (initial 2 s, ×2
 *   exponential backoff capped at 30 s — both configurable) and
 *   NACK-driven (immediate, listed seqs);
 * - Backpressure: unacknowledged bytes vs a cap (default 64 MiB) —
 *   `paused` flips on when the cap is exceeded and off once usage falls
 *   back;
 * - RESUME holder queries: [txid + peer progress] → status + retransmit
 *   list; expired copies answer `rejected` (SPEC §5).
 *
 * Time-injected: no internal timers. Every timing-bearing method takes
 * `now` (ms, caller clock); the integration layer drives the
 * clock and the retransmission sweeps.
 */

import { FrameType, makeFrame, newTransactionId } from "./frame";
import type { Frame } from "./frame";
import { RESUME_STATUS } from "./control";
import type { AckEntry, NackEntry, ResumeRequestEntry, ResumeResponseEntry } from "./control";

/** Error raised by the transaction sender on invalid usage. */
export class SenderError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SenderError";
  }
}

/** Working DATA_BINARY frame size (~1 MiB keeps control-frame insertion latency bounded, SPEC §4). */
export const DEFAULT_WORKING_FRAME_BYTES = 1024 * 1024;
/** Copy retention without peer progress (SPEC §5 recommendation: 120 s). */
export const DEFAULT_COPY_RETENTION_MS = 120_000;
/** Initial retransmission timeout (SPEC §5 recommendation: 2 s). */
export const DEFAULT_INITIAL_TIMEOUT_MS = 2_000;
/** Retransmission backoff cap (SPEC §5 recommendation: 30 s). */
export const DEFAULT_MAX_TIMEOUT_MS = 30_000;
/** Backpressure cap for unacknowledged bytes (SPEC §3/§5 recommendation: 64 MiB). */
export const DEFAULT_BACKPRESSURE_CAP_BYTES = 64 * 1024 * 1024;

export interface SenderOptions {
  /** DATA_BINARY working frame size; default {@link DEFAULT_WORKING_FRAME_BYTES}. */
  workingFrameBytes?: number;
  /** Copy retention without peer progress; default {@link DEFAULT_COPY_RETENTION_MS}. */
  copyRetentionMs?: number;
  /** Initial retransmission timeout; default {@link DEFAULT_INITIAL_TIMEOUT_MS}. */
  initialTimeoutMs?: number;
  /** Backoff cap; default {@link DEFAULT_MAX_TIMEOUT_MS}. */
  maxTimeoutMs?: number;
  /** Unacknowledged-bytes cap; default {@link DEFAULT_BACKPRESSURE_CAP_BYTES}. */
  backpressureCapBytes?: number;
}

export interface OpenResult {
  transactionId: string;
  /** DATA_BINARY frames, seq 0..N-1, ready for egress. */
  frames: Frame[];
}

export interface AckOutcome {
  known: boolean;
  /** True when this ACK completed the transaction and the copy was released. */
  released: boolean;
}

export interface ResumeResponseOutcome {
  known: boolean;
  /** True when the peer's `rejected` released the copy (SPEC §5). */
  released: boolean;
}

/** Holder's answer to one RESUME request entry (SPEC §5). */
export interface ResumeAnswer {
  /** RESUME_STATUS.Resumed / Rejected / Unknown. */
  status: number;
  /** Retransmission list (frames above the peer's frontier); empty unless resumed. */
  frames: Frame[];
}

interface Copy {
  transactionId: string;
  totalLen: number;
  /** DATA_BINARY frames, seq 0..N-1; payload views into the owned payload copy. */
  frames: Frame[];
  /** Contiguous confirmed frontier (highest confirmed seq; -1 = nothing confirmed). */
  ackedThrough: number;
  /** Bytes covered by the contiguous confirmed frontier. */
  ackedBytes: number;
  /** Completed timeout retransmission rounds (drives the ×2 backoff). */
  attempts: number;
  /** Earliest caller-clock ms at which a timeout retransmission may fire. */
  nextRetryAt: number;
  /** Last peer progress signal (ack / nack / resume), drives retention expiry. */
  lastPeerActivityAt: number;
}

function requireNow(now: number): void {
  if (!Number.isSafeInteger(now) || now < 0) {
    throw new RangeError(`now must be a non-negative safe integer, got: ${now}`);
  }
}

function requirePositiveInt(name: string, value: number): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(`${name} must be an integer >= 1, got: ${value}`);
  }
}

export class TransactionSender {
  private readonly copies = new Map<string, Copy>();
  /** Tombstones for expired copies: txid → expiry (so RESUME answers `rejected`, not `unknown`). */
  private readonly expired = new Map<string, number>();
  private readonly workingFrameBytes: number;
  private readonly copyRetentionMs: number;
  private readonly initialTimeoutMs: number;
  private readonly maxTimeoutMs: number;
  private readonly backpressureCapBytes: number;

  constructor(opts?: SenderOptions) {
    this.workingFrameBytes = opts?.workingFrameBytes ?? DEFAULT_WORKING_FRAME_BYTES;
    this.copyRetentionMs = opts?.copyRetentionMs ?? DEFAULT_COPY_RETENTION_MS;
    this.initialTimeoutMs = opts?.initialTimeoutMs ?? DEFAULT_INITIAL_TIMEOUT_MS;
    this.maxTimeoutMs = opts?.maxTimeoutMs ?? DEFAULT_MAX_TIMEOUT_MS;
    this.backpressureCapBytes = opts?.backpressureCapBytes ?? DEFAULT_BACKPRESSURE_CAP_BYTES;
    requirePositiveInt("workingFrameBytes", this.workingFrameBytes);
    requirePositiveInt("copyRetentionMs", this.copyRetentionMs);
    requirePositiveInt("initialTimeoutMs", this.initialTimeoutMs);
    requirePositiveInt("maxTimeoutMs", this.maxTimeoutMs);
    requirePositiveInt("backpressureCapBytes", this.backpressureCapBytes);
  }

  /** Number of live copies (unreleased transactions). */
  get copyCount(): number {
    return this.copies.size;
  }

  /**
   * Sum of unacknowledged bytes across all live copies: each copy
   * contributes `total_len` minus its contiguously confirmed bytes.
   */
  get unackedBytes(): number {
    let total = 0;
    for (const copy of this.copies.values()) {
      total += copy.totalLen - copy.ackedBytes;
    }
    return total;
  }

  /**
   * Backpressure signal (SPEC §5): true while unacknowledged bytes exceed
   * the cap — the integration layer must hold DATA frames; flips back to
   * false once ACKs bring usage back to (or below) the cap. Control
   * frames are exempt from this signal (SPEC §5).
   */
  get paused(): boolean {
    return this.unackedBytes > this.backpressureCapBytes;
  }

  /**
   * Slice `payload` into DATA_BINARY working frames and register the
   * reliability copy. The payload is copied — later caller-side mutation
   * cannot corrupt retransmissions.
   *
   * Throws {@link SenderError} on an empty payload or a colliding
   * transaction id; throws RangeError when `now` is invalid.
   */
  open(payload: Uint8Array, opts: { now: number; transactionId?: string }): OpenResult {
    requireNow(opts.now);
    if (payload.length === 0) {
      throw new SenderError("cannot open an empty transaction (payload length 0)");
    }
    const transactionId = opts.transactionId ?? newTransactionId();
    if (this.copies.has(transactionId) || this.expired.has(transactionId)) {
      throw new SenderError(`transaction ${transactionId} already exists (live copy or expired tombstone)`);
    }
    // Own the bytes: retransmission must replay exactly what was opened.
    const owned = payload.slice();
    const frames: Frame[] = [];
    for (let offset = 0, seq = 0; offset < owned.length; offset += this.workingFrameBytes, seq++) {
      const end = Math.min(offset + this.workingFrameBytes, owned.length);
      frames.push(
        makeFrame(FrameType.DATA_BINARY, owned.subarray(offset, end), {
          transactionId,
          totalLen: owned.length,
          seq,
        }),
      );
    }
    this.copies.set(transactionId, {
      transactionId,
      totalLen: owned.length,
      frames,
      ackedThrough: -1,
      ackedBytes: 0,
      attempts: 0,
      nextRetryAt: opts.now + this.initialTimeoutMs,
      lastPeerActivityAt: opts.now,
    });
    return { transactionId, frames };
  }

  /**
   * Apply a cumulative ACK entry: advance the confirmed watermark
   * (monotonic), reset the backoff window on progress, and release the
   * copy once the final frame is confirmed (final ACK, SPEC §5).
   */
  onAck(entry: AckEntry, now: number): AckOutcome {
    requireNow(now);
    const copy = this.copies.get(entry.transactionId);
    if (copy === undefined) return { known: false, released: false };
    copy.lastPeerActivityAt = now;
    if (entry.highestContiguousSeq > copy.ackedThrough) {
      const target = Math.min(entry.highestContiguousSeq, copy.frames.length - 1);
      for (let seq = copy.ackedThrough + 1; seq <= target; seq++) {
        copy.ackedBytes += copy.frames[seq].payload.length;
      }
      copy.ackedThrough = target;
      // Progress restarts the retransmission window (storm guard reset).
      copy.attempts = 0;
      copy.nextRetryAt = now + this.initialTimeoutMs;
    }
    if (copy.ackedThrough >= copy.frames.length - 1) {
      this.copies.delete(entry.transactionId); // final ACK releases the copy
      return { known: true, released: true };
    }
    return { known: true, released: false };
  }

  /**
   * Apply a NACK entry: immediate retransmission of the listed seqs we
   * hold (unknown/out-of-range seqs are ignored, duplicates deduped).
   * Does not touch the timeout schedule — the peer's next ACK refreshes it.
   */
  onNack(entry: NackEntry, now: number): Frame[] {
    requireNow(now);
    const copy = this.copies.get(entry.transactionId);
    if (copy === undefined) return [];
    copy.lastPeerActivityAt = now;
    const out: Frame[] = [];
    const seen = new Set<number>();
    for (const seq of entry.missingSeqs) {
      if (!Number.isInteger(seq) || seq < 0 || seq >= copy.frames.length || seen.has(seq)) continue;
      seen.add(seq);
      out.push(copy.frames[seq]);
    }
    return out;
  }

  /**
   * Timeout sweep (caller clock): collect unconfirmed frames from every
   * copy whose retransmission window has elapsed, advancing each copy's
   * ×2 backoff (capped at `maxTimeoutMs`). Frames come back in copy
   * registration order; interleaving across copies is the integration
   * layer's scheduling decision.
   */
  dueRetransmissions(now: number): Frame[] {
    requireNow(now);
    const out: Frame[] = [];
    for (const copy of this.copies.values()) {
      if (now < copy.nextRetryAt) continue;
      copy.attempts += 1;
      copy.nextRetryAt = now + Math.min(this.initialTimeoutMs * 2 ** copy.attempts, this.maxTimeoutMs);
      for (let seq = copy.ackedThrough + 1; seq < copy.frames.length; seq++) {
        out.push(copy.frames[seq]);
      }
    }
    return out;
  }

  /**
   * RESUME holder query (receiver → holder, SPEC §5): compare the peer's
   * declared progress against the held copy.
   *
   * - Copy held and fresh → `resumed` + retransmission of all frames
   *   above the peer's contiguous frontier (sparse residue above the
   *   frontier is covered by the resend; precise gaps arrive via NACK);
   * - Copy held but past the retention window → `rejected`; the copy's
   *   frames are dropped and a tombstone remembers the expiry;
   * - Never held → `unknown`; expired (tombstoned) → `rejected`.
   */
  onResumeRequest(entry: ResumeRequestEntry, now: number): ResumeAnswer {
    requireNow(now);
    const copy = this.copies.get(entry.transactionId);
    if (copy === undefined) {
      const expiredAt = this.expired.get(entry.transactionId);
      if (expiredAt !== undefined) {
        if (now - expiredAt > this.copyRetentionMs) {
          this.expired.delete(entry.transactionId); // tombstone aged out
          return { status: RESUME_STATUS.Unknown, frames: [] };
        }
        return { status: RESUME_STATUS.Rejected, frames: [] };
      }
      return { status: RESUME_STATUS.Unknown, frames: [] };
    }
    if (now - copy.lastPeerActivityAt > this.copyRetentionMs) {
      this.copies.delete(entry.transactionId);
      this.expired.set(entry.transactionId, now);
      return { status: RESUME_STATUS.Rejected, frames: [] };
    }
    copy.lastPeerActivityAt = now;
    const frontier = Math.min(entry.highestContiguousSeq, copy.frames.length - 1);
    // A fresh resume restarts the retransmission window.
    copy.attempts = 0;
    copy.nextRetryAt = now + this.initialTimeoutMs;
    return {
      status: RESUME_STATUS.Resumed,
      frames: copy.frames.slice(Math.max(frontier + 1, 0)),
    };
  }

  /**
   * Apply a RESUME response (holder → receiver): a peer `rejected`
   * releases the held copy (SPEC §5: "Senders release transaction copies
   * on final ACK, or on RESUME rejection by the peer"). `resumed` and
   * `unknown` keep the copy; retransmission continues per schedule.
   */
  onResumeResponse(entry: ResumeResponseEntry, now: number): ResumeResponseOutcome {
    requireNow(now);
    const copy = this.copies.get(entry.transactionId);
    if (copy === undefined) return { known: false, released: false };
    copy.lastPeerActivityAt = now;
    if (entry.status === RESUME_STATUS.Rejected) {
      this.copies.delete(entry.transactionId);
      return { known: true, released: true };
    }
    return { known: true, released: false };
  }

  /**
   * Drop copies past the retention window (no peer progress for
   * `copyRetentionMs`, caller clock). Expired copies move to tombstones
   * so later RESUME queries answer `rejected` (SPEC §5); tombstones
   * themselves age out after the same window (later queries degrade to
   * `unknown`). Returns the expired transaction ids.
   */
  reapExpired(now: number): string[] {
    requireNow(now);
    const removed: string[] = [];
    for (const [transactionId, copy] of this.copies) {
      if (now - copy.lastPeerActivityAt > this.copyRetentionMs) {
        this.copies.delete(transactionId);
        this.expired.set(transactionId, now);
        removed.push(transactionId);
      }
    }
    for (const [transactionId, expiredAt] of this.expired) {
      if (now - expiredAt > this.copyRetentionMs) this.expired.delete(transactionId);
    }
    return removed;
  }
}
