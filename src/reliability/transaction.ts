/**
 * Transaction sender — reliability copy manager for outbound data
 * transactions (docs/SPEC.md §5, sender side).
 *
 * Responsibilities:
 * - Slice a transaction payload into DATA_BINARY working frames
 *   (~1 MiB, SPEC §4 recommendation) for the integration layer to write
 *   to egress;
 * - Retain unacknowledged copies until the final ACK or a peer RESUME
 *   rejection (SPEC §5 v3: copy lifetime == transaction lifetime — no
 *   retention ceiling, no expiry sweep);
 * - Advance the confirmed watermark on ACK; release on completion;
 * - Produce retransmission lists: timeout-driven (initial 2 s, ×2
 *   exponential backoff capped at 30 s — both configurable) and
 *   NACK-driven (immediate, listed seqs);
 * - Backpressure: unacknowledged bytes vs a cap (default 64 MiB) —
 *   `paused` flips on when the cap is exceeded and off once usage falls
 *   back;
 * - Reconnect recovery (holder side): on wire-up the holder declares its
 *   pending sends (`declarePending`, gating them against retransmission);
 *   the receiver's per-transaction verdict (`applyResumeVerdict`) either
 *   lifts the gate and retransmits the unconfirmed frames (`resumed`) or
 *   closes the transfer by HOLDER-SIDE ADJUDICATION (`unknown`/`rejected`
 *   — no tombstone exists on the receiver): every frame already sent ⇒
 *   confirmed; frames still owed ⇒ failed. The copy is released either
 *   way (no terminal trace).
 *
 * Time-injected: no internal timers. Every timing-bearing method takes
 * `now` (ms, caller clock); the integration layer drives the
 * clock and the retransmission sweeps.
 */

import { FrameType, makeFrame, newTransactionId } from "../wire/frame";
import type { Frame } from "../wire/frame";
import { RESUME_STATUS } from "../wire/control";
import type { AckEntry, NackEntry, ResumeRequestEntry, ResumeResponseEntry } from "../wire/control";

/** Error raised by the transaction sender on invalid usage. */
export class SenderError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SenderError";
  }
}

/** Working DATA_BINARY frame size (~1 MiB keeps control-frame insertion latency bounded, SPEC §4). */
export const DEFAULT_WORKING_FRAME_BYTES = 1024 * 1024;
/** Initial retransmission timeout (SPEC §5 recommendation: 2 s). */
export const DEFAULT_INITIAL_TIMEOUT_MS = 2_000;
/** Retransmission backoff cap (SPEC §5 recommendation: 30 s). */
export const DEFAULT_MAX_TIMEOUT_MS = 30_000;
/** Backpressure cap for unacknowledged bytes (SPEC §3/§5 recommendation: 64 MiB). */
export const DEFAULT_BACKPRESSURE_CAP_BYTES = 64 * 1024 * 1024;

export interface SenderOptions {
  /** DATA_BINARY working frame size; default {@link DEFAULT_WORKING_FRAME_BYTES}. */
  workingFrameBytes?: number;
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

/** Outcome of applying one receiver resume verdict (recovery discipline). */
export interface ResumeVerdictOutcome {
  /** False when the transaction is not in pending (replay-safe no-op). */
  known: boolean;
  /**
   * True when the verdict closed the transfer CONFIRMED by holder-side
   * adjudication (Unknown/Rejected + every frame already sent — tail frame
   * plus the completion announcement went out): copy released, waiters
   * fire with a confirmation terminal.
   */
  confirmed: boolean;
  /** True when the verdict closed the transfer FAILED (Unknown/Rejected + frames still owed): copy released. */
  failed: boolean;
  /** Unconfirmed frames to retransmit (Resumed only; empty when paused or closed). */
  retransmit: Frame[];
}

interface Copy {
  transactionId: string;
  totalLen: number;
  /**
   * DATA_BINARY frames, seq 0..N-1. Entries at or below the confirmed
   * frontier are nulled out (fragment-level retention — the payload is
   * released on ACK); retransmission surfaces only read entries above the
   * frontier, which are never null.
   */
  frames: (Frame | null)[];
  /** Contiguous confirmed frontier (highest confirmed seq; -1 = nothing confirmed). */
  ackedThrough: number;
  /** Bytes covered by the contiguous confirmed frontier. */
  ackedBytes: number;
  /** Completed timeout retransmission rounds (drives the ×2 backoff). */
  attempts: number;
  /** Earliest caller-clock ms at which a timeout retransmission may fire. */
  nextRetryAt: number;
  /** Streaming copies only: sink-side pause (no egress, retransmission clock skipped). */
  paused: boolean;
  /** Streaming copies only: frames produced while paused, held for egress until resume. */
  heldFrames: Frame[];
  /** Streaming copies only: bytes appended so far (integrity guard vs `totalLen`). */
  writtenBytes: number;
  /**
   * Reconnect gate (recovery discipline): set when the holder declares its
   * pending sends on wire-up; the retransmission clock skips gated copies
   * and sink writes hold their frames until the receiver's verdict arrives.
   */
  awaitingResume: boolean;
  /**
   * Caller-clock ms the RESUME declaration was last sent at (`null` while
   * not gated). A declaration unanswered past the verdict timeout is
   * retransmitted (SPEC §5: the handshake is the sole recovery entry, so
   * its own reliability is mandatory).
   */
  declaredAt: number | null;
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
  private readonly workingFrameBytes: number;
  private readonly initialTimeoutMs: number;
  private readonly maxTimeoutMs: number;
  private readonly backpressureCapBytes: number;

  constructor(opts?: SenderOptions) {
    this.workingFrameBytes = opts?.workingFrameBytes ?? DEFAULT_WORKING_FRAME_BYTES;
    this.initialTimeoutMs = opts?.initialTimeoutMs ?? DEFAULT_INITIAL_TIMEOUT_MS;
    this.maxTimeoutMs = opts?.maxTimeoutMs ?? DEFAULT_MAX_TIMEOUT_MS;
    this.backpressureCapBytes = opts?.backpressureCapBytes ?? DEFAULT_BACKPRESSURE_CAP_BYTES;
    requirePositiveInt("workingFrameBytes", this.workingFrameBytes);
    requirePositiveInt("initialTimeoutMs", this.initialTimeoutMs);
    requirePositiveInt("maxTimeoutMs", this.maxTimeoutMs);
    requirePositiveInt("backpressureCapBytes", this.backpressureCapBytes);
  }

  /** Number of live copies (unreleased transactions). */
  get copyCount(): number {
    return this.copies.size;
  }

  /** Whether a live copy exists for the transaction. */
  hasCopy(transactionId: string): boolean {
    return this.copies.has(transactionId);
  }

  /**
   * Sum of unacknowledged bytes across all live copies: each copy
   * contributes its written bytes minus the contiguously confirmed ones.
   * (Streaming copies track `writtenBytes` separately from the declared
   * `totalLen` — only bytes actually appended occupy the pipe.)
   */
  get unackedBytes(): number {
    let total = 0;
    for (const copy of this.copies.values()) {
      total += copy.writtenBytes - copy.ackedBytes;
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
   * Sum of retained (not yet released) frame payload bytes across all live
   * copies. With fragment-level release the acknowledged frames' payloads
   * are dropped eagerly, so this tracks the copy memory actually held.
   */
  get retainedPayloadBytes(): number {
    let total = 0;
    for (const copy of this.copies.values()) {
      for (let seq = copy.ackedThrough + 1; seq < copy.frames.length; seq++) {
        const frame = copy.frames.at(seq);
        if (frame === null || frame === undefined) {
          // Fragment-level release only nulls entries at or below the
          // confirmed frontier — a hole above it is an internal invariant
          // violation, not a number to report.
          throw new SenderError(
            `transaction ${copy.transactionId} frame ${seq} above the confirmed frontier is missing`,
          );
        }
        total += frame.payload.length;
      }
    }
    return total;
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
    if (this.copies.has(transactionId)) {
      throw new SenderError(`transaction ${transactionId} already exists (live copy)`);
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
      paused: false,
      heldFrames: [],
      writtenBytes: owned.length,
      awaitingResume: false,
      declaredAt: null,
    });
    return { transactionId, frames };
  }

  /**
   * Open an outbound **streaming** transaction (docs/streams.md): the
   * copy starts empty and grows frame by frame through {@link appendChunk}.
   * `totalLen` is the caller-declared transaction total — it rides every
   * frame header (SPEC §3) and gates the receiver's reassembly criteria
   * unchanged.
   *
   * Throws {@link SenderError} when `totalLen` is not a positive integer or
   * the transaction id collides with a live copy in pending.
   */
  openStream(opts: { now: number; transactionId?: string; totalLen: number }): { transactionId: string } {
    requireNow(opts.now);
    if (!Number.isSafeInteger(opts.totalLen) || opts.totalLen < 1) {
      throw new RangeError(`totalLen must be an integer >= 1, got: ${opts.totalLen}`);
    }
    const transactionId = opts.transactionId ?? newTransactionId();
    if (this.copies.has(transactionId)) {
      throw new SenderError(`transaction ${transactionId} already exists (live copy)`);
    }
    this.copies.set(transactionId, {
      transactionId,
      totalLen: opts.totalLen,
      frames: [],
      ackedThrough: -1,
      ackedBytes: 0,
      attempts: 0,
      nextRetryAt: opts.now + this.initialTimeoutMs,
      paused: false,
      heldFrames: [],
      writtenBytes: 0,
      awaitingResume: false,
      declaredAt: null,
    });
    return { transactionId };
  }

  /**
   * Append one chunk to a streaming copy: sliced into DATA_BINARY working
   * frames (short tail frames are wire-legal — reassembly is byte-sum
   * based). While the sink-side pause is active the frames are held in the
   * copy and `held` is returned true; the egress gate (global backpressure)
   * stays with the integration layer.
   *
   * Throws {@link SenderError} on an unknown copy, a closed copy, a byte
   * count exceeding the declared `totalLen`, or an invalid `now`.
   */
  appendChunk(transactionId: string, chunk: Uint8Array, opts: { now: number }): { frames: Frame[]; held: boolean } {
    requireNow(opts.now);
    const copy = this.requireStreamCopy(transactionId);
    if (chunk.length === 0) return { frames: [], held: copy.paused || copy.awaitingResume };
    if (copy.writtenBytes + chunk.length > copy.totalLen) {
      throw new SenderError(
        `chunk overruns the declared totalLen of transaction ${transactionId}: ${copy.writtenBytes}+${chunk.length} > ${copy.totalLen}`,
      );
    }
    copy.writtenBytes += chunk.length;
    const frames: Frame[] = [];
    for (let offset = 0, seq = copy.frames.length; offset < chunk.length; offset += this.workingFrameBytes, seq++) {
      const end = Math.min(offset + this.workingFrameBytes, chunk.length);
      frames.push(
        makeFrame(FrameType.DATA_BINARY, chunk.slice(offset, end), {
          transactionId,
          totalLen: copy.totalLen,
          seq,
        }),
      );
    }
    // The copy owns every frame of the transaction (retransmission / RESUME
    // replay surface); `heldFrames` mirrors the pending ones while paused or
    // reconnect-gated as the egress gate.
    copy.frames.push(...frames);
    if (copy.paused || copy.awaitingResume) {
      copy.heldFrames.push(...frames);
      return { frames, held: true };
    }
    return { frames, held: false };
  }

  /**
   * Close a streaming copy: no further chunks are accepted and the copy
   * becomes releasable on the final ACK (SPEC §5 semantics unchanged). The
   * declared total must be written in full — an unfinished total can never
   * satisfy the receiver's reassembly criteria.
   */
  finishStream(transactionId: string, opts: { now: number }): void {
    requireNow(opts.now);
    const copy = this.requireStreamCopy(transactionId);
    if (copy.writtenBytes !== copy.totalLen) {
      throw new SenderError(
        `transaction ${transactionId} finished with ${copy.writtenBytes}/${copy.totalLen} declared bytes`,
      );
    }
  }

  /**
   * Sink-side pause (SPEC §5: suspend new frames AND retransmissions of the
   * transaction; already-transmitted frames keep receiving ACKs). Resume
   * returns the held frames for egress (in seq order) and restarts the
   * retransmission window.
   */
  setStreamPaused(transactionId: string, paused: boolean, opts: { now: number }): Frame[] {
    requireNow(opts.now);
    const copy = this.requireStreamCopy(transactionId);
    if (copy.paused === paused) return [];
    copy.paused = paused;
    if (!paused) {
      // A fresh resume restarts the retransmission window.
      copy.attempts = 0;
      copy.nextRetryAt = opts.now + this.initialTimeoutMs;
      // The reconnect gate outranks the unpause: frames stay held until
      // the receiver's verdict lifts it (they are unconfirmed copy frames,
      // so the post-verdict retransmission covers them).
      if (copy.awaitingResume) return [];
      const held = copy.heldFrames;
      copy.heldFrames = [];
      return held;
    }
    return [];
  }

  /**
   * Abort a streaming transaction: the copy is released from pending
   * immediately, leaving no trace (no terminal ledger — later RESUME
   * queries answer `unknown`, and the txid is free for reuse).
   */
  abortStream(transactionId: string, opts: { now: number }): void {
    requireNow(opts.now);
    const copy = this.copies.get(transactionId);
    if (copy === undefined) {
      throw new SenderError(`unknown streaming transaction ${transactionId}`);
    }
    this.copies.delete(transactionId);
  }

  private requireStreamCopy(transactionId: string): Copy {
    const copy = this.copies.get(transactionId);
    if (copy === undefined) {
      throw new SenderError(`unknown streaming transaction ${transactionId}`);
    }
    return copy;
  }

  /**
   * Apply a cumulative ACK entry: advance the confirmed watermark
   * (monotonic), reset the backoff window on progress, and release the copy
   * once the final frame is confirmed (final ACK, SPEC §5). Confirmed
   * fragments' payload references are dropped eagerly (fragment-level
   * retention, docs/streams.md) — an unclosed streaming copy is never
   * released, more frames may still be appended.
   */
  onAck(entry: AckEntry, now: number): AckOutcome {
    requireNow(now);
    const copy = this.copies.get(entry.transactionId);
    if (copy === undefined) return { known: false, released: false };
    if (entry.highestContiguousSeq > copy.ackedThrough) {
      const target = Math.min(entry.highestContiguousSeq, copy.frames.length - 1);
      for (let seq = copy.ackedThrough + 1; seq <= target; seq++) {
        const frame = copy.frames.at(seq);
        if (frame === null || frame === undefined) {
          throw new SenderError(
            `transaction ${copy.transactionId} frame ${seq} is missing while advancing the confirmed frontier`,
          );
        }
        copy.ackedBytes += frame.payload.length;
        copy.frames[seq] = null; // fragment-level release
      }
      copy.ackedThrough = target;
      // Progress restarts the retransmission window (storm guard reset).
      copy.attempts = 0;
      copy.nextRetryAt = now + this.initialTimeoutMs;
    }
    if (copy.writtenBytes === copy.totalLen && copy.ackedThrough >= copy.frames.length - 1) {
      this.copies.delete(entry.transactionId); // final ACK releases the copy
      return { known: true, released: true };
    }
    return { known: true, released: false };
  }

  /**
   * Apply a NACK entry: immediate retransmission of the listed seqs we
   * hold (unknown/out-of-range seqs are ignored, duplicates deduped,
   * already-released fragments cannot be re-sent). Does not touch the
   * timeout schedule — the peer's next ACK refreshes it.
   */
  onNack(entry: NackEntry, now: number): Frame[] {
    requireNow(now);
    const copy = this.copies.get(entry.transactionId);
    if (copy === undefined) return [];
    const out: Frame[] = [];
    const seen = new Set<number>();
    for (const seq of entry.missingSeqs) {
      if (!Number.isInteger(seq) || seq < 0 || seq >= copy.frames.length || seen.has(seq)) continue;
      const frame = copy.frames[seq];
      if (!frame) continue; // fragment already released by an ACK
      seen.add(seq);
      out.push(frame);
    }
    return out;
  }

  /**
   * Timeout sweep (caller clock): collect unconfirmed frames from every
   * copy whose retransmission window has elapsed, advancing each copy's
   * ×2 backoff (capped at `maxTimeoutMs`). Paused streaming copies are
   * skipped entirely — their retransmission clock is suspended (SPEC §5).
   * Frames come back in copy registration order; interleaving across
   * copies is the integration layer's scheduling decision.
   */
  dueRetransmissions(now: number): Frame[] {
    requireNow(now);
    const out: Frame[] = [];
    for (const copy of this.copies.values()) {
      if (copy.paused || copy.awaitingResume) continue;
      if (now < copy.nextRetryAt) continue;
      copy.attempts += 1;
      copy.nextRetryAt = now + Math.min(this.initialTimeoutMs * 2 ** copy.attempts, this.maxTimeoutMs);
      for (let seq = copy.ackedThrough + 1; seq < copy.frames.length; seq++) {
        const frame = copy.frames[seq];
        if (frame) out.push(frame);
      }
    }
    return out;
  }

  /**
   * Reconnect declaration (recovery discipline, holder → receiver): gate
   * every pending copy against retransmission and return the declaration
   * entries. The progress fields carry the holder's written progress
   * (informational; the receiver adjudicates on the txid alone — its own
   * ACK stream is the watermark, which is why the resume instruction
   * carries no offset). `now` anchors the verdict timeout (an unanswered
   * declaration is retransmitted — SPEC §5). An empty pending set
   * declares nothing.
   */
  declarePending(now: number): ResumeRequestEntry[] {
    requireNow(now);
    const entries: ResumeRequestEntry[] = [];
    for (const copy of this.copies.values()) {
      copy.awaitingResume = true;
      copy.declaredAt = now;
      entries.push({
        transactionId: copy.transactionId,
        highestContiguousSeq: Math.max(copy.frames.length - 1, 0),
        receivedFrames: copy.frames.length,
      });
    }
    return entries;
  }

  /**
   * Verdict-timeout sweep (SPEC §5): re-arm the gate and declare ONLY the
   * copies whose declaration has stayed unanswered past `timeoutMs` — the
   * handshake is the sole recovery entry, so its own reliability is
   * mandatory. Each returned entry's declaration clock is re-anchored at
   * `now`. Copies already adjudicated (`Resumed` lifted the gate,
   * `Unknown`/`Rejected` released the copy) are never re-declared.
   */
  timedOutDeclarations(now: number, timeoutMs: number): ResumeRequestEntry[] {
    requireNow(now);
    const entries: ResumeRequestEntry[] = [];
    for (const copy of this.copies.values()) {
      if (!copy.awaitingResume || copy.declaredAt === null) continue;
      if (now - copy.declaredAt < timeoutMs) continue;
      copy.declaredAt = now;
      entries.push({
        transactionId: copy.transactionId,
        highestContiguousSeq: Math.max(copy.frames.length - 1, 0),
        receivedFrames: copy.frames.length,
      });
    }
    return entries;
  }

  /** Whether the transaction is gated awaiting the receiver's resume verdict. */
  isAwaitingResume(transactionId: string): boolean {
    return this.copies.get(transactionId)?.awaitingResume ?? false;
  }

  /**
   * Apply one receiver verdict (recovery discipline, receiver → holder):
   *
   * - `Resumed` — the receiver still holds the transaction open: lift the
   *   gate and retransmit every unconfirmed frame (frames produced under
   *   the gate ride the same list — they are unconfirmed copy frames). A
   *   user-paused copy only lifts the gate without emitting (the pause
   *   intent dominates; unpausing rearms the clock and the timeout sweep
   *   covers the gap);
   * - `Unknown` / `Rejected` — the receiver does not recognize the
   *   transaction (no tombstone: consumed-and-delivered and never-seen
   *   are indistinguishable to it). The transfer closes by HOLDER-SIDE
   *   ADJUDICATION from the holder's own knowledge (SPEC §5): every frame
   *   already sent (tail frame plus the completion announcement —
   *   `writtenBytes` covers `totalLen`) ⇒ CONFIRMED; frames still owed ⇒
   *   FAILED. The copy is released either way; the caller releases
   *   waiters with the matching terminal.
   *
   * Unknown transaction ids are a no-op (a replayed verdict for an
   * already-released copy fires nothing twice).
   */
  applyResumeVerdict(entry: ResumeResponseEntry, now: number): ResumeVerdictOutcome {
    requireNow(now);
    const copy = this.copies.get(entry.transactionId);
    if (copy === undefined) return { known: false, confirmed: false, failed: false, retransmit: [] };
    if (entry.status === RESUME_STATUS.Resumed) {
      copy.awaitingResume = false;
      copy.declaredAt = null;
      copy.attempts = 0;
      copy.nextRetryAt = now + this.initialTimeoutMs;
      if (copy.paused) return { known: true, confirmed: false, failed: false, retransmit: [] };
      // Entries at or below the confirmed frontier are nulled out
      // (fragment-level release) and are never re-sent.
      const retransmit: Frame[] = [];
      for (let seq = copy.ackedThrough + 1; seq < copy.frames.length; seq++) {
        const frame = copy.frames.at(seq);
        if (frame === null || frame === undefined) {
          throw new SenderError(
            `transaction ${copy.transactionId} frame ${seq} above the confirmed frontier is missing`,
          );
        }
        retransmit.push(frame);
      }
      copy.heldFrames = []; // every held frame is an unconfirmed copy frame — covered above
      return { known: true, confirmed: false, failed: false, retransmit };
    }
    // Unknown / Rejected: holder-side adjudication (SPEC §5) — closure
    // comes from the holder's own knowledge, not the receiver's.
    const fullySent = copy.writtenBytes === copy.totalLen;
    this.copies.delete(entry.transactionId);
    return { known: true, confirmed: fullySent, failed: !fullySent, retransmit: [] };
  }
}
