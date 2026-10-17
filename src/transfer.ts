/**
 * Transfer session (docs/SPEC.md §5).
 *
 * Session-level reliability state for binary transfers, carried by the
 * logical session and attached to whatever connection is live:
 *
 * - {@link TransferSession} — the reliability state: receiver reassembly +
 *   sender copies + per-transaction completion callbacks + clock-driven
 *   scheduling (incremental ACKs, retransmission sweeps, heartbeats,
 *   peer-silence detection, retention reaping). It lives on the session,
 *   not on the connection: `wire(transport)` attaches it to a live
 *   connection (per-connection binding + clock + auto-RESUME), `unwire()`
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
 *   replacement transport — on wire it automatically resumes every
 *   incomplete receive by sending RESUME requests (SPEC §5).
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

import { FrameType, encodeFrame, isControlFrameType, makeFrame } from "./frame";
import type { Frame } from "./frame";
import type { XacppTransport } from "./transport";
import { XacppError } from "./message";
import { TransactionReassembler } from "./reassemble";
import type { ReassemblerOptions } from "./reassemble";
import { TransactionSender } from "./transaction";
import type { SenderOptions } from "./transaction";
import { CONTROL_PAYLOAD_FRAME_TYPES, ErrorCode, RESUME_STATUS } from "./control";
import {
  decodeAck,
  decodeError,
  decodeHeartbeat,
  decodeNack,
  decodeResumeRequest,
  decodeResumeResponse,
  encodeAck,
  encodeHeartbeat,
  encodeNack,
  encodeResumeRequest,
  encodeResumeResponse,
} from "./control";

/** Heartbeat send interval (SPEC §5; configurable). */
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;
/** Peer-silence timeout: no inbound frame for this long ⇒ connection dead. */
export const DEFAULT_SILENCE_TIMEOUT_MS = 90_000;
/** Incremental ACK batch: emit an ACK once this many new frames arrived. */
export const DEFAULT_ACK_BATCH_FRAMES = 64;
/** Incremental ACK batch: … or once this much time elapsed since the last ACK. */
export const DEFAULT_ACK_BATCH_INTERVAL_MS = 500;
/** Period of the binding's clock that drives the session. */
export const DEFAULT_RELIABILITY_TICK_MS = 100;

/** Termination of one outbound binary transaction (final ACK or released without confirmation). */
export interface BinarySendResult {
  transactionId: string;
  /** True: final ACK confirmed delivery. False: copy released (RESUME rejected / retention expired). */
  confirmed: boolean;
}

/** Inbound binary transaction handler: invoked once per complete transaction. */
export type BinaryHandler = (transactionId: string, payload: Uint8Array) => void;

export interface TransferConfig {
  /** Sender-side tuning (frame size, retention, timeouts, backpressure cap). */
  sender?: SenderOptions;
  /** Receiver-side tuning (stale retention). */
  reassembler?: ReassemblerOptions;
  /** Heartbeat send interval; default {@link DEFAULT_HEARTBEAT_INTERVAL_MS}. */
  heartbeatIntervalMs?: number;
  /** Peer-silence timeout; default {@link DEFAULT_SILENCE_TIMEOUT_MS}. */
  silenceTimeoutMs?: number;
  /** Incremental ACK frame threshold; default {@link DEFAULT_ACK_BATCH_FRAMES}. */
  ackBatchFrames?: number;
  /** Incremental ACK time threshold; default {@link DEFAULT_ACK_BATCH_INTERVAL_MS}. */
  ackBatchIntervalMs?: number;
}

/** Frames the session asks the binding to write during one clock tick / dispatch. */
export interface TickOutcome {
  /** Encoded control frames — write with egress scheduling priority. */
  controlFrames: Uint8Array[];
  /** Data frames (retransmissions / deferred flush) — write in FIFO order. */
  dataFrames: Frame[];
  /** True when the peer-silence timeout elapsed: the connection must be treated as dead. */
  peerSilent: boolean;
}

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

  /** Per-transaction completion callbacks (survive across connections). */
  private readonly completions = new Map<string, (result: BinarySendResult) => void>();
  /** Complete receiver payload handler (survives across connections). */
  private onBinaryHandler: BinaryHandler | null = null;

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
    this.reassembler = new TransactionReassembler(opts?.reassembler);
    this.sender = new TransactionSender(opts?.sender);
    this.heartbeatIntervalMs = opts?.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
    this.silenceTimeoutMs = opts?.silenceTimeoutMs ?? DEFAULT_SILENCE_TIMEOUT_MS;
    this.ackBatchFrames = opts?.ackBatchFrames ?? DEFAULT_ACK_BATCH_FRAMES;
    this.ackBatchIntervalMs = opts?.ackBatchIntervalMs ?? DEFAULT_ACK_BATCH_INTERVAL_MS;
    requirePositiveInt("heartbeatIntervalMs", this.heartbeatIntervalMs);
    requirePositiveInt("silenceTimeoutMs", this.silenceTimeoutMs);
    requirePositiveInt("ackBatchFrames", this.ackBatchFrames);
    requirePositiveInt("ackBatchIntervalMs", this.ackBatchIntervalMs);
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
   * binding (clock + frame dispatch + write paths) and sends a RESUME
   * request for every incomplete receive (SPEC §5). The session auto-
   * unwires when the transport closes — all state survives.
   */
  wire(transport: XacppTransport & TransferWiring): void {
    if (this.binding !== null) throw XacppError.invalidState("transfer session already wired to a transport");
    const writeData = (frame: Frame) => transport.writeDataFrame(frame);
    this.binding = new ReliabilityBinding(this, {
      writeControl: (bytes) => transport.writeControlFrame(bytes),
      writeData,
      markConnectionDead: () => {
        void transport.disconnect().catch(() => {});
      },
    });
    this.binding.start();
    this.wiring = transport;
    this.attachDataWriter(writeData);
    transport.onTransferActivity({
      onActivity: () => this.noteActivity(Date.now()),
      onFrame: (frame) => this.binding!.handleWireFrame(frame),
    });
    this.unwireOnClose = transport.onClosed(() => this.unwire());
    // Auto-RESUME: ask the peer to continue every incomplete receive (SPEC §5).
    const resume = this.buildResumeFrame(Date.now());
    if (resume !== null) transport.writeControlFrame(resume);
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
   * DATA_BINARY working frames, retained until the final ACK (or a peer
   * RESUME rejection / retention expiry), retransmitted on timeout, and
   * resumable across connections (wire the same session into the
   * replacement transport). Never blocks: while backpressure is active
   * the frames are held and flush automatically. Returns the transaction
   * id. Throws notConnected when no live connection carries this session.
   */
  sendBinary(payload: Uint8Array, onComplete?: (result: BinarySendResult) => void): string {
    if (this.dataWriter === null) throw XacppError.notConnected();
    const now = Date.now();
    const { transactionId, frames } = this.openSend(payload, now, onComplete);
    for (const frame of frames) this.dataWriter(frame);
    return transactionId;
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
   * and/or an immediate NACK when this frame landed above a gap.
   */
  acceptDataFrame(frame: Frame, now: number): Uint8Array[] {
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
      if (this.onBinaryHandler !== null) this.deliverCompleted(frame.transactionId);
      // No handler: the completed slot retains its payload until one
      // registers — the `onBinary` setter delivers it exactly once.
    } else {
      if (!outcome.duplicate) this.pendingAckFrames += 1;
      if (!outcome.duplicate && frame.seq > outcome.frontierSeq) {
        // Landed above a gap: immediate NACK for this transaction.
        const entry = this.reassembler.nackEntries().find((e) => e.transactionId === frame.transactionId);
        if (entry) controlFrames.push(encodeFrame(makeFrame(CONTROL_PAYLOAD_FRAME_TYPES.nack, encodeNack([entry]))));
      }
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
        // RESUME frame (receiver → holder): answer per entry, then retransmit.
        return this.guardDecode(() => this.answerResumeRequests(decodeResumeRequest(frame.payload), now));
      case CONTROL_PAYLOAD_FRAME_TYPES.resumeResponse:
        // RESUME_RESPONSE frame (holder → receiver): apply per entry.
        return this.guardDecode(() => this.applyResumeResponses(decodeResumeResponse(frame.payload), now));
      case CONTROL_PAYLOAD_FRAME_TYPES.heartbeat:
        // Liveness only — the binding refreshes the silence clock for every frame.
        return empty;
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

  /** Holder side: answer per entry, then retransmit (SPEC §5). The answer rides a RESUME_RESPONSE frame. */
  private answerResumeRequests(entries: ReturnType<typeof decodeResumeRequest>, now: number): TickOutcome {
    const answers = entries.map((entry) => this.sender.onResumeRequest(entry, now));
    const controlFrames = [
      encodeFrame(
        makeFrame(
          CONTROL_PAYLOAD_FRAME_TYPES.resumeResponse,
          encodeResumeResponse(
            answers.map((answer, i) => ({ transactionId: entries[i].transactionId, status: answer.status })),
          ),
        ),
      ),
    ];
    const dataFrames: Frame[] = [];
    for (const answer of answers) {
      if (answer.status === RESUME_STATUS.Resumed) {
        dataFrames.push(...this.enqueueDataOrDefer(answer.frames));
      }
    }
    dataFrames.push(...this.flushDeferredIfUnpaused());
    return { controlFrames, dataFrames, peerSilent: false };
  }

  /** Receiver side: a peer `rejected` releases the held copy (SPEC §5). */
  private applyResumeResponses(entries: ReturnType<typeof decodeResumeResponse>, now: number): TickOutcome {
    for (const entry of entries) {
      const outcome = this.sender.onResumeResponse(entry, now);
      if (outcome.released) {
        this.complete(entry.transactionId, { transactionId: entry.transactionId, confirmed: false });
      }
    }
    return { controlFrames: [], dataFrames: this.flushDeferredIfUnpaused(), peerSilent: false };
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
      const match = /^([0-9a-f]{32})/.exec(info.message);
      if (match && this.reassembler.has(match[1])) {
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
   * heartbeats, silence detection, retention reaping, deferred flush.
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

    // Receiver-side stale sweep: dropping an incomplete transaction here
    // is silent — the peer's retransmission loop will restart the slot
    // if frames keep arriving.
    this.reassembler.reapStale(now);
    for (const txid of this.sender.reapExpired(now)) {
      this.complete(txid, { transactionId: txid, confirmed: false });
    }

    return { controlFrames, dataFrames, peerSilent: false };
  }

  // ---- Connection lifecycle ----

  /**
   * RESUME request frame for every incomplete receive with a contiguous
   * prefix (SPEC §5: sent on connect before accepting new flows).
   * Slots without seq 0 have nothing a cumulative u32 could express —
   * the holder's timeout retransmission covers them (≤ initial timeout).
   * Returns null when there is nothing to resume.
   */
  buildResumeFrame(now: number): Uint8Array | null {
    const entries = this.reassembler.ackEntries();
    if (entries.length === 0) return null;
    return encodeFrame(makeFrame(CONTROL_PAYLOAD_FRAME_TYPES.resumeRequest, encodeResumeRequest(entries)));
  }

  /** Connection gone: drop per-connection scheduling state (copies/slots persist for resume). */
  onConnectionGone(): void {
    this.deferred = [];
    this.pendingAckFrames = 0;
    this.lastAckSentAt = null;
    this.lastPeerActivityAt = null;
    this.lastHeartbeatAt = null;
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
    return flushed;
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

/** Per-connection write/teardown hooks a transport provides to its binding. */
interface ReliabilityBindingHooks {
  /** Write an encoded control frame (egress scheduling priority). */
  writeControl(bytes: Uint8Array): void;
  /** Write one data frame, encoded (plain FIFO). */
  writeData(frame: Frame): void;
  /** Peer-silence timeout elapsed — treat the connection as dead. */
  markConnectionDead(): void;
}

/**
 * Per-connection wiring between one transport connection and a
 * TransferSession: owns the clock interval, inbound dispatch, and the
 * outbound write paths. Lives and dies with the connection.
 */
class ReliabilityBinding {
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    readonly session: TransferSession,
    private readonly hooks: ReliabilityBindingHooks,
  ) {}

  /** Start the clock (idempotent; restarts the interval). */
  start(tickMs: number = DEFAULT_RELIABILITY_TICK_MS): void {
    this.stop();
    this.timer = setInterval(() => this.pump(), tickMs);
  }

  /** Stop the clock (idempotent). */
  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /** One clock pump: run the session tick and write the produced frames. */
  pump(now: number = Date.now()): void {
    const outcome = this.session.tick(now);
    if (outcome.peerSilent) {
      this.hooks.markConnectionDead();
      return;
    }
    this.writeOutcome(outcome);
  }

  /**
   * Dispatch one inbound non-envelope frame: refreshes the silence
   * clock, feeds DATA_BINARY into the reassembler, and routes control
   * frames through the session. DATA_JSON is the transport's envelope
   * path and never reaches here.
   */
  handleWireFrame(frame: Frame, now: number = Date.now()): void {
    this.session.noteActivity(now);
    if (frame.frameType === FrameType.DATA_BINARY) {
      this.writeControlFrames(this.session.acceptDataFrame(frame, now));
      return;
    }
    if (isControlFrameType(frame.frameType)) {
      this.writeOutcome(this.session.onControlFrame(frame, now));
      return;
    }
    console.debug("[xacpp:reliability] ignoring frame type 0x%s", frame.frameType.toString(16));
  }

  private writeOutcome(outcome: TickOutcome): void {
    this.writeControlFrames(outcome.controlFrames);
    for (const frame of outcome.dataFrames) this.hooks.writeData(frame);
  }

  private writeControlFrames(bytes: Uint8Array[]): void {
    for (const b of bytes) this.hooks.writeControl(b);
  }
}

/**
 * Write/activity surface a transport provides for transfer-session
 * attachment. Implemented by `SocketTransport` and `StdioTransport`;
 * consumers reach it through `TransferSession.wire` / `XacppSession.wire`.
 */
export interface TransferWiring {
  /** Write one encoded control frame (egress scheduling priority). */
  writeControlFrame(bytes: Uint8Array): void;
  /** Write one data frame, encoded (plain FIFO). */
  writeDataFrame(frame: Frame): void;
  /**
   * Register the connection-activity / inbound-transfer-frame handlers.
   * `onActivity` fires for envelope (DATA_JSON) frames; `onFrame` for
   * everything else (DATA_BINARY / control). Pass null to unregister.
   */
  onTransferActivity(handlers: TransferActivityHandlers | null): void;
}

/** Inbound hooks a transfer session registers on a wired transport. */
export interface TransferActivityHandlers {
  /** Envelope frame arrived — the peer is alive (silence clock refresh). */
  onActivity(): void;
  /** Non-envelope frame arrived (DATA_BINARY / control) — transfer channel. */
  onFrame(frame: Frame): void;
}
