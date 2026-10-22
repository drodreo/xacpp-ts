/**
 * Transfer session (docs/SPEC.md §5).
 *
 * Session-level reliability state for binary transfers, carried by the
 * logical session and attached to whatever connection is live:
 *
 * - {@link TransferSession} — the reliability state: receiver reassembly +
 *   sender copies + per-transaction completion callbacks + clock-driven
 *   scheduling (incremental ACKs, retransmission sweeps, heartbeats,
 *   peer-silence detection). It lives on the session,
 *   not on the connection: `wire(transport)` attaches it to a live
 *   connection (per-connection binding + clock + recovery declaration),
 *   `unwire()`
 *   detaches it — all state survives, so a transfer interrupted by a
 *   reconnect continues from the breakpoint (SPEC §5). Every session
 *   carries one from construction (default configuration).
 * - {@link ReliabilityBinding} — internal per-connection wiring created
 *   by `wire`: the clock (interval) driving `tick`, inbound frame
 *   dispatch (DATA_BINARY / control), and outbound write paths (control
 *   frames take egress priority, SPEC §5 invariant 4).
 * - {@link TransferWiring} — the write/activity surface a transport
 *   provides for attachment (implemented by SocketTransport and
 *   StdioTransport).
 *
 * ## Boundary contracts
 *
 * - The session does not manage reconnection. A dead connection makes
 *   that transport instance unusable (existing error semantics);
 *   continuity is achieved by wiring the same session into the
 *   replacement transport — on wire the holder declares its pending sends
 *   (RESUME) and the receiver adjudicates per transaction
 *   (RESUME_RESPONSE), resuming the recognized transfers (SPEC §5).
 * - The envelope channel (DATA_JSON single-frame transactions, i.e.
 *   `send` / `sendFaf` / `onRequest`) is untouched: it does not enter
 *   the transfer machinery and is not subject to backpressure.
 *
 * ## Time model
 *
 * The session is time-injected like its components: every method takes
 * `now` (ms, caller clock). The binding owns the interval that pumps the
 * clock in production; tests drive the clock by calling `tick(now)` /
 * `acceptDataFrame(frame, now)` directly, or fake the interval + Date.
 */

import { encodeFrame, makeFrame } from "../wire/frame";
import type { Frame } from "../wire/frame";
import type { XacppTransport } from "../carrier/transport";
import { XacppError } from "../wire/message";
import { TransactionReassembler } from "../reliability/reassemble";
import { TransactionSender, SenderError } from "../reliability/transaction";
import { CONTROL_PAYLOAD_FRAME_TYPES, ErrorCode, RESUME_STATUS } from "../wire/control";
import {
  decodeAck,
  decodeError,
  decodeNack,
  decodeResumeRequest,
  decodeResumeResponse,
  decodeTransactionAbort,
  decodeTransactionCompleted,
  decodeTransactionPause,
  encodeAck,
  encodeHeartbeat,
  encodeNack,
  encodeResumeRequest,
  encodeResumeResponse,
  encodeTransactionCompleted,
} from "../wire/control";
import type {
  BinaryHandler,
  BinarySendResult,
  BinarySink,
  BinarySinkMeta,
  BinaryStream,
  BinaryStreamHandler,
  StreamEndReason,
  TickOutcome,
  TransferConfig,
} from "./types";
import { ReliabilityBinding } from "./binding";
import type { PendingWrite, TransferWiring } from "./binding";
import { BinaryStreamImpl } from "./stream-impl";
import { SinkController } from "./sink-controller";

/** Heartbeat send interval (SPEC §5; configurable). */
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;
/** Peer-silence timeout: no inbound frame for this long ⇒ connection dead. */
export const DEFAULT_SILENCE_TIMEOUT_MS = 90_000;
/** Incremental ACK batch: emit an ACK once this many new frames arrived. */
export const DEFAULT_ACK_BATCH_FRAMES = 64;
/** Incremental ACK batch: … or once this much time elapsed since the last ACK. */
export const DEFAULT_ACK_BATCH_INTERVAL_MS = 500;
/** RESUME declaration verdict timeout: an unanswered declaration is retransmitted (SPEC §5). */
export const DEFAULT_RESUME_VERDICT_TIMEOUT_MS = 2_000;
/** Period of the binding's clock that drives the session. */
export const DEFAULT_RELIABILITY_TICK_MS = 100;
function requirePositiveInt(name: string, value: number): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(`${name} must be an integer >= 1, got: ${value}`);
  }
}

export class TransferSession {
  private readonly reassembler: TransactionReassembler;
  private readonly sender: TransactionSender;
  private readonly heartbeatIntervalMs: number;
  private readonly silenceTimeoutMs: number;
  private readonly ackBatchFrames: number;
  private readonly ackBatchIntervalMs: number;
  private readonly resumeVerdictTimeoutMs: number;

  /** Per-transaction completion callbacks (survive across connections). */
  private readonly completions = new Map<string, (result: BinarySendResult) => void>();
  /** Complete receiver payload handler (survives across connections). */
  private onBinaryHandler: BinaryHandler | null = null;
  /** First-frame receiver stream handler (survives across connections). */
  private onBinaryStreamHandler: BinaryStreamHandler | null = null;
  /** Live receive-side streams by transaction id (session-scoped: they survive connection drops). */
  private readonly streams = new Map<string, BinaryStreamImpl>();

  // ---- Sink write-resolution bookkeeping ----

  /** Writes whose frames sit in the global deferred queue (FIFO-aligned). */
  private readonly deferredWrites: PendingWrite[] = [];
  /** Writes whose frames sit in a transaction's pause-hold queue. */
  private readonly heldWrites = new Map<string, PendingWrite[]>();

  // ---- Per-connection state (reset by onConnectionGone) ----
  /** New distinct frames since the last emitted ACK batch. */
  private pendingAckFrames = 0;
  /** Caller-clock ms of the last emitted ACK; null = none yet this connection. */
  private lastAckSentAt: number | null = null;
  /** Caller-clock ms of the last inbound activity (any frame); null = nothing yet. */
  private lastPeerActivityAt: number | null = null;
  /** Caller-clock ms of the last heartbeat send; null = none yet this connection. */
  private lastHeartbeatAt: number | null = null;
  /** Monotonic heartbeat counter (per-sender, never resets). */
  private heartbeatSeq = 0;
  /** DATA_BINARY frames held back while paused; flushed once unpaused (dropped on disconnect — copies retransmit). */
  private deferred: Frame[] = [];
  /** Outbound data-frame writer, attached while a connection is wired. */
  private dataWriter: ((frame: Frame) => void) | null = null;
  /** Per-connection wiring (present while wired to a live transport). */
  private binding: ReliabilityBinding | null = null;
  /** Currently wired transport (null = detached). */
  private wiring: (XacppTransport & TransferWiring) | null = null;
  /** Close-subscription of the wired transport (drives auto-unwire). */
  private unwireOnClose: (() => void) | null = null;

  constructor(opts?: TransferConfig) {
    this.reassembler = new TransactionReassembler();
    this.sender = new TransactionSender(opts?.sender);
    this.heartbeatIntervalMs = opts?.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
    this.silenceTimeoutMs = opts?.silenceTimeoutMs ?? DEFAULT_SILENCE_TIMEOUT_MS;
    this.ackBatchFrames = opts?.ackBatchFrames ?? DEFAULT_ACK_BATCH_FRAMES;
    this.ackBatchIntervalMs = opts?.ackBatchIntervalMs ?? DEFAULT_ACK_BATCH_INTERVAL_MS;
    this.resumeVerdictTimeoutMs = opts?.resumeVerdictTimeoutMs ?? DEFAULT_RESUME_VERDICT_TIMEOUT_MS;
    requirePositiveInt("heartbeatIntervalMs", this.heartbeatIntervalMs);
    requirePositiveInt("silenceTimeoutMs", this.silenceTimeoutMs);
    requirePositiveInt("ackBatchFrames", this.ackBatchFrames);
    requirePositiveInt("ackBatchIntervalMs", this.ackBatchIntervalMs);
    requirePositiveInt("resumeVerdictTimeoutMs", this.resumeVerdictTimeoutMs);
  }

  /**
   * Register the complete-transaction delivery handler (single slot,
   * like `onRequest`). Transactions completed before this registration
   * are delivered here — each retained payload exactly once (delivery
   * consumes the slot).
   */
  set onBinary(handler: BinaryHandler | null) {
    this.onBinaryHandler = handler;
    if (handler === null) return;
    for (const transactionId of this.reassembler.completedUntaken()) {
      this.deliverCompleted(transactionId);
    }
  }

  get onBinary(): BinaryHandler | null {
    return this.onBinaryHandler;
  }

  /**
   * Register the streaming receive handler (single slot): invoked once per
   * inbound transaction at its first frame. While registered, streamed
   * transactions are owned by their {@link BinaryStream} and never reach
   * `onBinary`; transactions that completed before registration still go
   * through `onBinary`'s exactly-once retro-delivery.
   */
  set onBinaryStream(handler: BinaryStreamHandler | null) {
    this.onBinaryStreamHandler = handler;
  }

  get onBinaryStream(): BinaryStreamHandler | null {
    return this.onBinaryStreamHandler;
  }

  /** Retained (unreleased) frame payload bytes across all live copies (diagnostics). */
  get retainedPayloadBytes(): number {
    return this.sender.retainedPayloadBytes;
  }

  /** Backpressure signal (SPEC §5): true while unacknowledged bytes exceed the sender cap. */
  get paused(): boolean {
    return this.sender.paused;
  }

  /** Number of live sender copies (diagnostics). */
  get copyCount(): number {
    return this.sender.copyCount;
  }

  /** Number of live receiver slots, complete-but-undelivered included (diagnostics). */
  get pendingCount(): number {
    return this.reassembler.pendingCount;
  }

  // ---- Connection wiring ----

  /**
   * Attach the session to a live transport: creates the per-connection
   * binding (clock + frame dispatch + write paths) and declares pending
   * sends (RESUME, recovery discipline — gated until the receiver's
   * verdicts arrive). The session auto-unwires when the transport closes
   * — all state survives.
   *
   * Declaration timing contract (SPEC §5): wiring here DECLARES — the
   * caller only wires when the peer's transfer plane already exists
   * (direct-transport consumers, or the responder wiring inside
   * onEstablish). The initiator-at-connect path must NOT declare at
   * wire-up (the responder's transfer plane does not exist yet and a
   * dropped declaration deadlocks the holder); the peer uses
   * `wireWithoutDeclaration` there and fires `declarePendingSends` once
   * the establish handshake completes.
   */
  wire(transport: XacppTransport & TransferWiring): void {
    this.attachWiring(transport);
    this.declarePendingSends();
  }

  /**
   * @internal Wire without sending the RESUME declaration (gate still
   * armed). Peer-internal: the initiator wires at connect, before the
   * responder's transfer plane exists; the declaration is fired by the
   * peer once the establish handshake completes.
   */
  wireWithoutDeclaration(transport: XacppTransport & TransferWiring): void {
    this.attachWiring(transport);
  }

  private attachWiring(transport: XacppTransport & TransferWiring): void {
    if (this.binding !== null) throw XacppError.invalidState("transfer session already wired to a transport");
    const writeData = (frame: Frame) => transport.writeDataFrame(frame);
    const binding = new ReliabilityBinding(this, {
      writeControl: (bytes) => transport.writeControlFrame(bytes),
      writeData,
      markConnectionDead: () => {
        void transport.disconnect().catch(() => {});
      },
    });
    this.binding = binding;
    binding.start();
    this.wiring = transport;
    this.attachDataWriter(writeData);
    transport.onTransferActivity({
      onActivity: () => this.noteActivity(Date.now()),
      onFrame: (frame) => binding.handleWireFrame(frame),
    });
    this.unwireOnClose = transport.onClosed(() => this.unwire());
    // Recovery discipline: arm the reconnect gate on every pending send —
    // they retransmit nothing until the receiver's per-transaction verdict
    // arrives (declarePending is idempotent; the declaration frame itself
    // is sent by `wire` now or by `declarePendingSends` later).
    this.sender.declarePending(Date.now());
    // The connection is live again: stalled streams resume on the SAME
    // object once the RESUME handshake completes (the response clears the
    // flag too — this covers request-less stalls, e.g. pure senders).
    for (const stream of this.streams.values()) {
      if (!stream.terminated) stream.markRewired();
    }
    // Sink writes that pended across the disconnect resolve here: their
    // bytes live in the retained copies and retransmit from this connection.
    this.resolveAllPendingWrites();
  }

  /** Emit one encoded control frame on the wired connection (no-op unwired). @internal */
  emitControl(bytes: Uint8Array): void {
    this.wiring?.writeControlFrame(bytes);
  }

  /**
   * Emit one control frame in WIRE ORDER — behind the data frames already
   * queued for the wire.
   *
   * Control frames take scheduling priority (SPEC §5 invariant 4): they
   * are written ahead of the queued data frames. A frame whose MEANING
   * depends on the data frames preceding it (the transaction's
   * completion announcement) must not use that path: overtaking the
   * transaction's tail data frame would make the peer read "completed"
   * before the last bytes and call the transaction short. Such a frame
   * rides the data FIFO instead — same queue, same order. It stays a
   * control frame on the wire (its frame type carries the meaning) and
   * the peer dispatches on the frame type.
   */
  emitOrderedControl(frame: Frame): void {
    this.dataWriter?.(frame);
  }

  // ---- Sink collaboration surface (used by SinkController; @internal) ----

  /** Append a chunk to the sink's transaction. @internal */
  sinkAppend(transactionId: string, chunk: Uint8Array): { frames: Frame[]; held: boolean } {
    return this.sender.appendChunk(transactionId, chunk, { now: Date.now() });
  }

  /** Validate and close the sink's transaction. @internal */
  sinkFinish(transactionId: string): void {
    this.sender.finishStream(transactionId, { now: Date.now() });
  }

  /**
   * Announce a finished transaction (TRANSACTION_COMPLETED) in wire order.
   *
   * The announcement is skipped when the transaction still has frames
   * parked in the egress queues (pause-hold or global backpressure): the
   * tail frame has not left yet, and announcing now would tell the peer
   * "no more bytes" while bytes are still coming. The peer then completes
   * through the ordinary byte-complete path. It is likewise skipped while
   * the transaction is gated awaiting the receiver's resume verdict (SPEC
   * §5: a gated transaction SENDS NOTHING) — the closure rides the verdict
   * outcome instead (`Resumed` retransmits the frames and the byte count
   * completes it; `Unknown`/`Rejected` closes by holder-side adjudication).
   */
  sinkAnnounceCompleted(transactionId: string): void {
    if (this.sender.isAwaitingResume(transactionId)) {
      return;
    }
    if (!this.sinkFramesEgressed(transactionId)) {
      console.warn(
        "[xacpp:reliability] transaction %s finished with frames still parked — completion not announced",
        transactionId,
      );
      return;
    }
    this.emitOrderedControl(
      makeFrame(
        CONTROL_PAYLOAD_FRAME_TYPES.transactionCompleted,
        encodeTransactionCompleted(transactionId),
      ),
    );
  }

  /**
   * Whether a transaction-control frame for `transactionId` may take the
   * expedited control channel.
   *
   * The control channel jumps ahead of the data frames already queued for
   * the wire (SPEC §5 invariant 4) — safe only when the transaction has
   * nothing left in the egress queues. A control frame whose meaning
   * depends on the transaction's own data frames (abort / pause) must
   * never overtake them: the peer would read the control frame before the
   * bytes it describes. With frames still parked the frame rides the data
   * FIFO instead — same queue, same order.
   *
   * @internal
   */
  sinkMayExpediteControl(transactionId: string): boolean {
    return this.sinkFramesEgressed(transactionId);
  }

  /** Whether every frame of the transaction has been handed to the wire. */
  private sinkFramesEgressed(transactionId: string): boolean {
    for (const write of this.deferredWrites) {
      if (write.transactionId === transactionId) return false;
    }
    const held = this.heldWrites.get(transactionId);
    return held === undefined || held.length === 0;
  }

  /** Register the terminal-result callback for the sink's transaction. @internal */
  sinkRegisterCompletion(transactionId: string, onResult: (result: BinarySendResult) => void): void {
    this.completions.set(transactionId, onResult);
  }

  /** Whether the sender still holds a live copy for the transaction. @internal */
  senderHasCopy(transactionId: string): boolean {
    return this.sender.hasCopy(transactionId);
  }

  /** Remove a registered completion without firing it. @internal */
  removeCompletion(transactionId: string): void {
    this.completions.delete(transactionId);
  }

  /** Gate the transaction's egress + retransmission clock (wire: pause is announced by the sink). @internal */
  sinkSetPaused(transactionId: string, paused: boolean): Frame[] {
    return this.sender.setStreamPaused(transactionId, paused, { now: Date.now() });
  }

  /** Release the copy and bar the txid from reuse. @internal */
  sinkAbort(transactionId: string): void {
    this.sender.abortStream(transactionId, { now: Date.now() });
  }

  /**
   * Detach from the current connection: stop the clock and per-connection
   * scheduling state (copies/slots/completions survive). Idempotent.
   */
  unwire(): void {
    this.unwireOnClose?.();
    this.unwireOnClose = null;
    this.wiring?.onTransferActivity(null);
    this.wiring = null;
    this.binding?.stop();
    this.binding = null;
    this.attachDataWriter(null);
    this.onConnectionGone();
  }

  /** Whether the session is currently wired to a live transport. */
  get wired(): boolean {
    return this.binding !== null;
  }

  private attachDataWriter(writer: ((frame: Frame) => void) | null): void {
    this.dataWriter = writer;
  }

  /**
   * Send a binary payload as a reliable transaction: sliced into
   * DATA_BINARY working frames, retained until the final ACK or a peer
   * RESUME rejection, retransmitted on timeout, and resumable across
   * connections (wire the same session into the replacement transport).
   * Never blocks: while backpressure is active the frames are held and
   * flush automatically. Returns the transaction id. Throws notConnected
   * when no live connection carries this session.
   *
   * Convenience composition over {@link openBinarySink} (docs/streams.md)
   * — behavior is unchanged: sync id return, immediate egress while
   * unpaused, exactly-once completion callback.
   */
  sendBinary(payload: Uint8Array, onComplete?: (result: BinarySendResult) => void): string {
    if (payload.length === 0) {
      throw new SenderError("cannot open an empty transaction (payload length 0)");
    }
    const sink = this.openBinarySink({ totalLen: payload.length });
    void sink
      .write(payload)
      .catch(() => {})
      .then(() => sink.finish())
      .then(
        (result) => onComplete?.(result),
        () => {}, // aborted sinks have no completion to report
      );
    return sink.transactionId;
  }

  /**
   * Open a streaming send transaction (docs/streams.md): chunks flow
   * through `write` (split into working frames, retained per-fragment
   * until acknowledged), `finish` closes the stream and resolves with the
   * terminal send result, `pause`/`resume`/`abort` gate the transaction
   * (wire: TRANSACTION_PAUSE / implicit / TRANSACTION_ABORT). The
   * caller-supplied `txid` (optional — generated otherwise) MUST be unique
   * among live transactions; a collision throws.
   */
  openBinarySink(meta: BinarySinkMeta, txid?: string): BinarySink {
    if (this.dataWriter === null) throw XacppError.notConnected();
    const { transactionId } = this.sender.openStream({
      now: Date.now(),
      totalLen: meta.totalLen,
      ...(txid !== undefined ? { transactionId: txid } : {}),
    });
    return new SinkController(this, transactionId);
  }

  // ---- Sender side ----

  /**
   * Open an outbound binary transaction: slice into DATA_BINARY working
   * frames and register the reliability copy. Never blocks the caller.
   *
   * Backpressure (SPEC §5): the pause check happens **before** the copy
   * is registered — a transaction opened while unpaused is "in flight"
   * and its frames go out even if they push usage past the cap (this is
   * what guarantees the wire keeps flowing so ACKs can bring the usage
   * back down). A transaction opened while already paused is held
   * internally and flushes automatically once unpaused; its retained
   * copy retransmits on any future connection.
   *
   * Returns the transaction id and the frames to write **now** (empty
   * while paused).
   */
  openSend(
    payload: Uint8Array,
    now: number,
    onComplete?: (result: BinarySendResult) => void,
  ): { transactionId: string; frames: Frame[] } {
    const wasPaused = this.sender.paused;
    const { transactionId, frames } = this.sender.open(payload, { now });
    if (onComplete) this.completions.set(transactionId, onComplete);
    if (wasPaused) {
      this.deferred.push(...frames);
      return { transactionId, frames: [] };
    }
    return { transactionId, frames };
  }

  // ---- Receiver side ----

  /**
   * Feed one inbound DATA_BINARY frame into the reassembler. Returns the
   * control frames to write now: the immediate final ACK on completion,
   * and/or an immediate NACK when this frame landed above a gap. With a
   * stream handler registered, frames additionally stream through the
   * transaction's {@link BinaryStream} (ordered contiguous passthrough).
   */
  acceptDataFrame(frame: Frame, now: number): Uint8Array[] {
    const slotExisted = this.reassembler.has(frame.transactionId);
    // Frontier BEFORE this frame: a stream created by this arrival starts
    // at the pre-arrival watermark (anything contiguous before it predates
    // the stream and is summarized in startOffset, not re-emitted).
    const preFrontier = slotExisted ? this.reassembler.frontierOf(frame.transactionId) : null;
    const streamExistedBefore = this.streams.has(frame.transactionId);
    let outcome;
    try {
      outcome = this.reassembler.push(frame, now);
    } catch (e) {
      // Transaction-level protocol violation: the frame stream itself is
      // still CRC-valid, so the connection survives; the offending frame
      // is dropped (the sender's retransmission loop will re-face it).
      console.error(
        "[xacpp:reliability] rejected DATA_BINARY frame: %s",
        e instanceof Error ? e.message : String(e),
      );
      return [];
    }
    if (outcome.slotlessDropped) {
      // SPEC §3/§5: a slotless id with `seq > 0` — late retransmission of
      // a settled transaction or an unannounced stream. Dropped — no
      // slot, no ACK, no NACK, no stream, no reply: the holder's closure
      // signal is the RESUME verdict, never a data-frame reply.
      console.debug(
        "[xacpp:reliability] dropped slotless DATA_BINARY frame (tx %s seq %d) — not reassembled",
        frame.transactionId,
        frame.seq,
      );
      return [];
    }

    const controlFrames: Uint8Array[] = [];

    if (outcome.completed) {
      // Final ACK on completion (SPEC §5): reflects reassembly, not the
      // handler — sent immediately regardless of handler presence.
      controlFrames.push(
        encodeFrame(
          makeFrame(
            CONTROL_PAYLOAD_FRAME_TYPES.ack,
            encodeAck([
              {
                transactionId: frame.transactionId,
                highestContiguousSeq: outcome.frontierSeq,
                receivedFrames: outcome.frontierSeq + 1,
              },
            ]),
          ),
        ),
      );
    } else {
      if (!outcome.duplicate) this.pendingAckFrames += 1;
      if (!outcome.duplicate && frame.seq > outcome.frontierSeq) {
        // Landed above a gap: immediate NACK for this transaction.
        const entry = this.reassembler.nackEntries().find((e) => e.transactionId === frame.transactionId);
        if (entry) controlFrames.push(encodeFrame(makeFrame(CONTROL_PAYLOAD_FRAME_TYPES.nack, encodeNack([entry]))));
      }
    }

    // Resumption after a pause is implicit: the next data frame of the
    // transaction clears its paused state — completion frames included
    // (SPEC §5).
    this.streams.get(frame.transactionId)?.markResumed();

    // Streaming passthrough (both completion states — a transaction can
    // complete with its very first frame): create the stream at the first
    // arrival when a handler is registered, then emit contiguous chunks.
    this.ensureStream(frame.transactionId, slotExisted, preFrontier);
    this.drainStream(frame.transactionId);

    if (outcome.completed) {
      // The stream owns the transaction when it streamed it from the start
      // (existed before this frame) or the transaction arrived entirely
      // under the current handler (a newly opened slot). A slot that completed while
      // nobody streamed it stays with `onBinary`'s exactly-once
      // retro-delivery contract.
      if (this.onBinaryStreamHandler !== null && (streamExistedBefore || !slotExisted) && this.streams.has(frame.transactionId)) {
        this.endStream(frame.transactionId, "completed");
        this.reassembler.discard(frame.transactionId); // stream consumed it — no aggregate retention
      } else if (this.onBinaryHandler !== null) {
        this.deliverCompleted(frame.transactionId);
      }
      // No handler: the completed slot retains its payload until one
      // registers — the `onBinary` setter delivers it exactly once.
    }
    return controlFrames;
  }

  // ---- Control frame dispatch ----

  /** Dispatch one inbound control frame. Returns response/retransmission frames to write. */
  onControlFrame(frame: Frame, now: number): TickOutcome {
    const empty: TickOutcome = { controlFrames: [], dataFrames: [], peerSilent: false };
    switch (frame.frameType) {
      case CONTROL_PAYLOAD_FRAME_TYPES.ack:
        return this.onAckFrame(frame.payload, now);
      case CONTROL_PAYLOAD_FRAME_TYPES.nack:
        return this.onNackFrame(frame.payload, now);
      case CONTROL_PAYLOAD_FRAME_TYPES.resumeRequest:
        // RESUME declaration (holder → receiver): adjudicate each entry
        // against our open inbound slots, then answer in one frame.
        return this.guardDecode(() => this.adjudicateResumeDeclaration(decodeResumeRequest(frame.payload)));
      case CONTROL_PAYLOAD_FRAME_TYPES.resumeResponse:
        // RESUME_RESPONSE frame (receiver → holder): apply the verdicts.
        return this.guardDecode(() => this.applyResumeResponses(decodeResumeResponse(frame.payload), now));
      case CONTROL_PAYLOAD_FRAME_TYPES.heartbeat:
        // Liveness only — the binding refreshes the silence clock for every frame.
        return empty;
      case CONTROL_PAYLOAD_FRAME_TYPES.transactionPause:
        // Holder paused the named transaction: surface on its live stream (SPEC §5).
        return this.guardDecode(() => {
          const txid = decodeTransactionPause(frame.payload);
          this.streams.get(txid)?.markPaused();
          return empty;
        });
      case CONTROL_PAYLOAD_FRAME_TYPES.transactionAbort:
        // Holder abandoned the transaction: end its stream and discard the
        // partial reassembly (SPEC §5). v3: the terminal state is `cancelled`.
        return this.guardDecode(() => {
          const txid = decodeTransactionAbort(frame.payload);
          this.endStream(txid, "cancelled");
          this.reassembler.discard(txid);
          return empty;
        });
      case CONTROL_PAYLOAD_FRAME_TYPES.transactionCompleted:
        // Holder announced the end of the transaction: the bytes are either
        // complete (normal completion) or short (explicit failure — the
        // receiver never hangs on a missing tail).
        return this.guardDecode(() => {
          const txid = decodeTransactionCompleted(frame.payload);
          this.completeTransaction(txid);
          return empty;
        });
      case CONTROL_PAYLOAD_FRAME_TYPES.error:
        return this.onProtocolError(frame.payload);
      default:
        console.debug(
          "[xacpp:reliability] ignoring control frame type 0x%s",
          frame.frameType.toString(16),
        );
        return empty;
    }
  }

  /** Decode-and-dispatch wrapper: a malformed payload is logged and dropped, never guessed. */
  private guardDecode(dispatch: () => TickOutcome): TickOutcome {
    try {
      return dispatch();
    } catch (e) {
      console.error(
        "[xacpp:reliability] undecodable control payload — dropped: %s",
        e instanceof Error ? e.message : String(e),
      );
      return { controlFrames: [], dataFrames: [], peerSilent: false };
    }
  }

  private onAckFrame(payload: Uint8Array, now: number): TickOutcome {
    const dataFrames: Frame[] = [];
    for (const entry of decodeAck(payload)) {
      const outcome = this.sender.onAck(entry, now);
      if (outcome.released) this.complete(entry.transactionId, { transactionId: entry.transactionId, confirmed: true });
    }
    dataFrames.push(...this.flushDeferredIfUnpaused());
    return { controlFrames: [], dataFrames, peerSilent: false };
  }

  private onNackFrame(payload: Uint8Array, now: number): TickOutcome {
    const dataFrames: Frame[] = [];
    for (const entry of decodeNack(payload)) {
      dataFrames.push(...this.enqueueDataOrDefer(this.sender.onNack(entry, now)));
    }
    return { controlFrames: [], dataFrames, peerSilent: false };
  }

  /**
   * Receiver side (recovery discipline): adjudicate the holder's
   * declaration. An **open incomplete** slot answers `Resumed` (the
   * continuation instruction); everything else answers `Unknown` — a
   * completed-but-undelivered slot awaits local consumption and needs
   * nothing more from the holder, and with no terminal trace kept a
   * consumed-and-delivered transaction and a never-seen one are
   * indistinguishable. The holder closes those by its own adjudication
   * (SPEC §5). The mirror cross-check cancels open-but-undeclared inbound
   * streams: the holder no longer holds their copy (aborted or released
   * while disconnected), so they can never complete — and streams close
   * only on cancellation or completion. Completed-but-undelivered slots
   * are excluded (they need nothing more from the holder). Every
   * declaration is answered with exactly one verdict frame (never
   * silently swallowed); an empty declaration carries no question and is
   * not answered.
   */
  private adjudicateResumeDeclaration(entries: ReturnType<typeof decodeResumeRequest>): TickOutcome {
    if (entries.length === 0) {
      return { controlFrames: [], dataFrames: [], peerSilent: false };
    }
    const verdicts = entries.map((entry) => ({
      transactionId: entry.transactionId,
      status:
        this.reassembler.has(entry.transactionId) && !this.reassembler.isComplete(entry.transactionId)
          ? RESUME_STATUS.Resumed
          : RESUME_STATUS.Unknown,
    }));
    const declared = new Set(entries.map((entry) => entry.transactionId));
    for (const txid of this.reassembler.incompleteTransactions()) {
      if (declared.has(txid)) continue;
      this.endStream(txid, "cancelled");
      this.reassembler.discard(txid);
    }
    const controlFrames = [
      encodeFrame(makeFrame(CONTROL_PAYLOAD_FRAME_TYPES.resumeResponse, encodeResumeResponse(verdicts))),
    ];
    return { controlFrames, dataFrames: [], peerSilent: false };
  }

  /**
   * Holder side (recovery discipline): apply the receiver's verdicts.
   * `Resumed` lifts the reconnect gate and retransmits every unconfirmed
   * frame (settling writes parked on the gate); `Unknown`/`Rejected`
   * closes the transfer by HOLDER-SIDE ADJUDICATION (SPEC §5): every
   * frame already sent (tail frame plus the completion announcement) ⇒
   * confirmed — completion waiters fire with a confirmation terminal;
   * frames still owed ⇒ failed (failure terminal). Parked writes are
   * rejected on the failed branch.
   */
  private applyResumeResponses(entries: ReturnType<typeof decodeResumeResponse>, now: number): TickOutcome {
    const dataFrames: Frame[] = [];
    for (const entry of entries) {
      const outcome = this.sender.applyResumeVerdict(entry, now);
      if (!outcome.known) continue;
      if (outcome.failed) {
        this.complete(entry.transactionId, { transactionId: entry.transactionId, confirmed: false });
        this.rejectPendingWrites(entry.transactionId);
        continue;
      }
      if (outcome.confirmed) {
        // Confirmed closure (adjudicated fully-sent): same exact-once
        // channel as the final ACK — completion waiters fire confirmed.
        this.complete(entry.transactionId, { transactionId: entry.transactionId, confirmed: true });
        this.resolvePendingWritesForClosure(entry.transactionId);
        continue;
      }
      // Resumed: retransmit the unconfirmed frames and settle the writes
      // parked on the reconnect gate.
      const admitted = this.enqueueDataOrDefer(outcome.retransmit);
      dataFrames.push(...admitted);
      const parked = this.heldWrites.get(entry.transactionId);
      if (parked !== undefined) {
        this.heldWrites.delete(entry.transactionId);
        if (admitted.length === outcome.retransmit.length) {
          for (const w of parked) w.resolve();
        } else {
          // Globally paused: the frames deferred — park the writes with
          // them (FIFO-aligned).
          this.deferredWrites.push(...parked);
        }
      }
    }
    dataFrames.push(...this.flushDeferredIfUnpaused());
    return { controlFrames: [], dataFrames, peerSilent: false };
  }

  private onProtocolError(payload: Uint8Array): TickOutcome {
    let info;
    try {
      info = decodeError(payload);
    } catch (e) {
      console.error(
        "[xacpp:reliability] undecodable ERROR payload: %s",
        e instanceof Error ? e.message : String(e),
      );
      return { controlFrames: [], dataFrames: [], peerSilent: false };
    }
    console.error("[xacpp:reliability] peer protocol error 0x%s: %s", info.code.toString(16), info.message);
    if (info.code === ErrorCode.TransactionExpired) {
      // Structured convention (SPEC §5): the message field starts with the
      // affected transaction id as 32 lowercase hex chars; the remainder is
      // free-form. Parsed structurally — a txid anywhere else is not one.
      // v3 stream lifecycle: the release surfaces on a live stream as
      // `cancelled` (the holder voided the transaction).
      const match = /^([0-9a-f]{32})/.exec(info.message);
      if (match && this.reassembler.has(match[1])) {
        this.endStream(match[1], "cancelled");
        this.reassembler.discard(match[1]);
      } else if (!match) {
        console.warn(
          "[xacpp:reliability] transaction_expired ERROR without a leading transaction id — nothing to release",
        );
      }
    }
    return { controlFrames: [], dataFrames: [], peerSilent: false };
  }

  // ---- Clock ----

  /** Note inbound activity (any frame from the peer) — resets the silence clock. */
  noteActivity(now: number): void {
    this.lastPeerActivityAt = now;
  }

  /**
   * One clock pump: incremental ACK batches, timeout retransmissions,
   * heartbeats, silence detection, deferred flush. No retention reaping —
   * copies and slots live as long as their transactions do (SPEC §5 v3).
   */
  tick(now: number): TickOutcome {
    // Silence baseline: the connection's first tick starts the clock.
    if (this.lastPeerActivityAt === null) this.lastPeerActivityAt = now;

    if (now - this.lastPeerActivityAt >= this.silenceTimeoutMs) {
      return { controlFrames: [], dataFrames: [], peerSilent: true };
    }

    const controlFrames: Uint8Array[] = [];

    // Incremental ACK: 64 frames or 500 ms, whichever first (configured).
    const ackDue =
      this.pendingAckFrames >= this.ackBatchFrames ||
      (this.pendingAckFrames > 0 &&
        this.lastAckSentAt !== null &&
        now - this.lastAckSentAt >= this.ackBatchIntervalMs) ||
      (this.pendingAckFrames > 0 && this.lastAckSentAt === null);
    if (ackDue) {
      const entries = this.reassembler.ackEntries();
      if (entries.length > 0) {
        controlFrames.push(encodeFrame(makeFrame(CONTROL_PAYLOAD_FRAME_TYPES.ack, encodeAck(entries))));
        this.pendingAckFrames = 0;
        this.lastAckSentAt = now;
      }
      // No contiguous prefix anywhere: nothing a cumulative ACK could
      // carry — keep the counter for a later batch.
    }

    const dataFrames: Frame[] = [];
    dataFrames.push(...this.flushDeferredIfUnpaused());
    dataFrames.push(...this.enqueueDataOrDefer(this.sender.dueRetransmissions(now)));

    // RESUME declaration retransmission (SPEC §5 recovery discipline): a
    // declaration that stays unanswered past the verdict timeout is
    // retransmitted — the handshake is the sole recovery entry, so its own
    // reliability is mandatory. Only still-gated copies are re-declared
    // (adjudicated ones are out of the pending set or out of the gate);
    // each retransmission re-anchors its verdict clock.
    const redeclare = this.sender.timedOutDeclarations(now, this.resumeVerdictTimeoutMs);
    if (redeclare.length > 0) {
      console.debug(
        "[xacpp:reliability] retransmitting RESUME declaration for %d unanswered transaction(s)",
        redeclare.length,
      );
      controlFrames.push(
        encodeFrame(makeFrame(CONTROL_PAYLOAD_FRAME_TYPES.resumeRequest, encodeResumeRequest(redeclare))),
      );
    }

    if (this.lastHeartbeatAt === null || now - this.lastHeartbeatAt >= this.heartbeatIntervalMs) {
      this.lastHeartbeatAt = now;
      controlFrames.push(
        encodeFrame(
          makeFrame(
            CONTROL_PAYLOAD_FRAME_TYPES.heartbeat,
            encodeHeartbeat({ monotonicSeq: ++this.heartbeatSeq, senderEpochMs: now }),
          ),
        ),
      );
    }

    return { controlFrames, dataFrames, peerSilent: false };
  }

  // ---- Connection lifecycle ----

  /**
   * RESUME declaration frame for every pending send (recovery discipline:
   * the entries gate the copies against retransmission until the
   * receiver's verdicts arrive; SENT only via `declarePendingSends` — see
   * that method for the timing contract). Returns null when there is
   * nothing pending — an empty declaration is never emitted.
   */
  buildResumeDeclaration(now: number): Uint8Array | null {
    const entries = this.sender.declarePending(now);
    if (entries.length === 0) return null;
    return encodeFrame(makeFrame(CONTROL_PAYLOAD_FRAME_TYPES.resumeRequest, encodeResumeRequest(entries)));
  }

  /**
   * Recovery discipline: send the RESUME declaration for every pending
   * send (declarePending is idempotent — re-declaring re-arms the same
   * gate). Fired by the peer when the establish handshake completes (the
   * only moment BOTH transfer planes are live on an establish topology),
   * or by a consumer-driven `XacppSession.wire` — which by contract happens
   * when the peer is already wired (responder's onEstablish).
   */
  declarePendingSends(): void {
    const wiring = this.wiring;
    if (wiring === null) return;
    const declaration = this.buildResumeDeclaration(Date.now());
    if (declaration !== null) wiring.writeControlFrame(declaration);
  }

  /**
   * Connection gone: drop per-connection scheduling state. v3 stream
   * lifecycle — copies/slots/completions/streams all persist: live
   * streams are surfaced as `stalled` (object alive, buffer retained)
   * and resume on the SAME object when the session re-wires.
   */
  onConnectionGone(): void {
    this.deferred = [];
    this.pendingAckFrames = 0;
    this.lastAckSentAt = null;
    this.lastPeerActivityAt = null;
    this.lastHeartbeatAt = null;
    for (const stream of this.streams.values()) {
      if (!stream.terminated) stream.markStalled();
    }
  }

  // ---- Internals ----

  /**
   * Deliver one completed transaction: take the payload and hand it to
   * the current handler (if still registered). `take` consumes the slot,
   * so every payload is delivered exactly once. Handler failures are
   * logged and never interrupt the session.
   */
  private deliverCompleted(transactionId: string): void {
    const handler = this.onBinaryHandler;
    if (handler === null) return;
    let payload: Uint8Array;
    try {
      payload = this.reassembler.take(transactionId);
    } catch (e) {
      console.error(
        "[xacpp:reliability] cannot deliver completed transaction %s: %s",
        transactionId,
        e instanceof Error ? e.message : String(e),
      );
      return;
    }
    try {
      handler(transactionId, payload);
    } catch (e) {
      console.error(
        "[xacpp:reliability] onBinary handler failed for %s: %s",
        transactionId,
        e instanceof Error ? e.message : String(e),
      );
    }
  }

  private enqueueDataOrDefer(frames: Frame[]): Frame[] {
    if (frames.length === 0) return [];
    if (this.sender.paused) {
      this.deferred.push(...frames);
      return [];
    }
    return frames;
  }

  private flushDeferredIfUnpaused(): Frame[] {
    if (this.deferred.length === 0 || this.sender.paused) return [];
    const flushed = this.deferred;
    this.deferred = [];
    // FIFO-aligned: every deferred write's frames are flushed together.
    const writes = this.deferredWrites.splice(0);
    for (const w of writes) w.resolve();
    return flushed;
  }

  // ---- Streaming internals ----

  /**
   * Create a stream for a transaction receiving frames with a handler
   * present. A live stream object is never replaced: after a disconnect +
   * rewire the RESUME continuation lands on the SAME object (identity),
   * so this only creates when no stream exists yet.
   */
  private ensureStream(
    transactionId: string,
    slotExisted: boolean,
    preFrontier: { seq: number; bytes: number } | null,
  ): void {
    const handler = this.onBinaryStreamHandler;
    if (handler === null || this.streams.has(transactionId)) return;
    const totalLen = this.reassembler.totalLenOf(transactionId);
    if (totalLen === null) return;
    // A pre-existing slot's contiguous prefix predates the stream: it is
    // summarized in startOffset, not re-emitted (late handler registration).
    const startSeq = slotExisted ? (preFrontier?.seq ?? 0) : 0;
    const startOffset = slotExisted ? (preFrontier?.bytes ?? 0) : 0;
    const stream = new BinaryStreamImpl(transactionId, totalLen, startOffset, startSeq);
    this.streams.set(transactionId, stream);
    try {
      handler(stream);
    } catch (e) {
      console.error(
        "[xacpp:reliability] onBinaryStream handler failed for %s: %s",
        transactionId,
        e instanceof Error ? e.message : String(e),
      );
    }
  }

  /** Emit every contiguous frame above the stream's watermark, in seq order. */
  private drainStream(transactionId: string): void {
    const stream = this.streams.get(transactionId);
    if (stream === undefined || stream.terminated) return;
    for (;;) {
      const payload = this.reassembler.frameAt(transactionId, stream.nextSeq);
      if (payload === null) break;
      stream.pushChunk(payload);
    }
  }

  /** End and forget a live stream (no-op when absent or already ended). */
  private endStream(
    transactionId: string,
    reason: StreamEndReason,
    detail?: { received: number; expected: number },
  ): void {
    const stream = this.streams.get(transactionId);
    if (stream === undefined) return;
    this.streams.delete(transactionId);
    stream.terminate(reason, detail);
  }

  /**
   * Apply a TRANSACTION_COMPLETED announcement: the holder is done sending.
   *
   * - bytes complete → the stream completes (idempotent with the
   *   byte-complete path: the first verdict wins, a late announcement is
   *   ignored);
   * - bytes short → the stream fails explicitly as `incomplete`, carrying
   *   the shortfall (the receiver never waits for bytes that will not
   *   come);
   * - no live stream (already terminal, or the transaction was never
   *   streamed) → ignored.
   */
  private completeTransaction(transactionId: string): void {
    const stream = this.streams.get(transactionId);
    if (stream === undefined) return; // idempotent: already terminal / not streamed
    const expected = this.reassembler.totalLenOf(transactionId) ?? stream.expectedBytes;
    const received = this.reassembler.frontierOf(transactionId)?.bytes ?? stream.receivedBytes;
    if (received >= expected) {
      this.endStream(transactionId, "completed");
    } else {
      this.endStream(transactionId, "incomplete", { received, expected });
    }
    // The transaction is over either way: the partial reassembly goes.
    this.reassembler.discard(transactionId);
  }

  /**
   * Backpressure snapshot taken BEFORE a sink append (mirrors `openSend`'s
   * pre-registration check: a write admitted while unpaused is in-flight
   * and its frames egress even if they push usage past the cap — this is
   * what keeps the wire flowing so ACKs can bring the usage back down).
   */
  isBackpressured(): boolean {
    return this.sender.paused;
  }

  /**
   * Admit frames produced by a sink write: immediate egress for an
   * entry-unpaused write; otherwise park them in the deferred queue and
   * pend the write until ACKs flush it (while unwired, frames stay in the
   * retained copy and the write pends until wire() settles it).
   */
  admitFrames(transactionId: string, frames: Frame[], wasPaused: boolean): Promise<void> {
    if (frames.length === 0) return Promise.resolve();
    if (wasPaused || this.dataWriter === null) {
      if (this.dataWriter !== null) this.deferred.push(...frames);
      return new Promise((resolve, reject) => {
        this.deferredWrites.push({ transactionId, resolve, reject });
      });
    }
    for (const frame of frames) this.dataWriter(frame);
    return Promise.resolve();
  }

  /** Park a write whose frames sit in the transaction's pause-hold queue. */
  queueHeldWrite(transactionId: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const list = this.heldWrites.get(transactionId) ?? [];
      list.push({ transactionId, resolve, reject });
      this.heldWrites.set(transactionId, list);
    });
  }

  /** Egress a resume's held frames as one unit; settle its parked writes. */
  flushHeldFrames(transactionId: string, frames: Frame[]): void {
    const writes = this.heldWrites.get(transactionId) ?? [];
    this.heldWrites.delete(transactionId);
    if (frames.length === 0) {
      for (const w of writes) w.resolve();
      return;
    }
    const admitted = this.enqueueDataOrDefer(frames);
    if (admitted.length === frames.length) {
      for (const frame of admitted) this.dataWriter?.(frame);
      for (const w of writes) w.resolve();
      return;
    }
    // Globally paused: park everything together — frames in the deferred
    // queue, resolvers in the deferred-write queue (FIFO-aligned).
    this.deferred.push(...frames);
    this.deferredWrites.push(...writes);
  }

  /** Reject every pending write of a transaction (abort path). */
  rejectPendingWrites(transactionId: string): void {
    const err = new XacppError("transaction_aborted", `transaction ${transactionId} aborted`);
    for (let i = this.deferredWrites.length - 1; i >= 0; i--) {
      const write = this.deferredWrites.at(i);
      if (write === undefined) continue;
      if (write.transactionId !== transactionId) continue;
      this.deferredWrites.splice(i, 1);
      write.reject(err);
    }
    const held = this.heldWrites.get(transactionId);
    if (held !== undefined) {
      this.heldWrites.delete(transactionId);
      for (const w of held) w.reject(err);
    }
  }

  /**
   * Resolve every pending write of a transaction (holder-side adjudication
   * closed it CONFIRMED: the bytes are acknowledged as delivered on the
   * wire — the writes are complete).
   */
  private resolvePendingWritesForClosure(transactionId: string): void {
    for (let i = this.deferredWrites.length - 1; i >= 0; i--) {
      const write = this.deferredWrites.at(i);
      if (write === undefined) continue;
      if (write.transactionId !== transactionId) continue;
      this.deferredWrites.splice(i, 1);
      write.resolve();
    }
    const held = this.heldWrites.get(transactionId);
    if (held !== undefined) {
      this.heldWrites.delete(transactionId);
      for (const w of held) w.resolve();
    }
  }

  /** Resolve every pending write (wire time: bytes retransmit from the copy). */
  private resolveAllPendingWrites(): void {
    const deferred = this.deferredWrites.splice(0);
    for (const w of deferred) w.resolve();
    const held = [...this.heldWrites.values()].flat();
    this.heldWrites.clear();
    for (const w of held) w.resolve();
  }

  private complete(transactionId: string, result: BinarySendResult): void {
    const callback = this.completions.get(transactionId);
    if (!callback) return;
    this.completions.delete(transactionId);
    try {
      callback(result);
    } catch (e) {
      console.error(
        "[xacpp:reliability] sendBinary completion callback failed for %s: %s",
        transactionId,
        e instanceof Error ? e.message : String(e),
      );
    }
  }
}
