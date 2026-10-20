import type { Frame } from "../wire/frame";
import type { SenderOptions } from "../reliability/transaction";

/** Termination of one outbound binary transaction (final ACK or released without confirmation). */
export interface BinarySendResult {
  transactionId: string;
  /** True: final ACK confirmed delivery. False: copy released without confirmation (the receiver's resume verdict failed the transaction after a reconnect, or caller abort). */
  confirmed: boolean;
}

/** Inbound binary transaction handler: invoked once per complete transaction. */
export type BinaryHandler = (transactionId: string, payload: Uint8Array) => void;

// ---- Streaming API (docs/streams.md) ----

/**
 * Caller-declared metadata for an outbound streaming transaction. `totalLen`
 * rides every DATA_BINARY frame header (SPEC §3) and gates the receiver's
 * reassembly criteria — it must be declared upfront (fetch-style flows know
 * it at response time; the wire has no trailer to amend it later).
 */
export interface BinarySinkMeta {
  totalLen: number;
}

/** Why a {@link BinaryStream} ended. Terminal states are exactly three. */
export type StreamEndReason =
  /** Final frame received — reassembly criteria met. */
  | "completed"
  /** Cancelled: local abort, peer TRANSACTION_ABORT, RESUME answered
   * `rejected` post-rewire (offline abort surfaced late), or a peer
   * `transaction_expired` release. */
  | "cancelled"
  /** Incomplete: the peer announced TRANSACTION_COMPLETED while the
   * declared bytes were still missing — an explicit failure, never a
   * hang. The gap is readable on the stream: {@link BinaryStream.receivedBytes}
   * of {@link BinaryStream.expectedBytes} arrived. */
  | "incomplete";

/**
 * Ordered receive-side view of one inbound binary transaction (design
 * anchor §2.2). Created at the first frame of a transaction when a stream
 * handler is registered; `startOffset` covers bytes reassembled before the
 * stream existed (late handler registration). The stream object is
 * session-scoped: it survives connection drops (surfaced as `stalled`)
 * and only ends via `completed`/`cancelled` — a post-RESUME continuation
 * keeps emitting on the SAME object. Exactly one of the delivery surfaces
 * owns a transaction: a streamed transaction never reaches `onBinary`.
 */
export interface BinaryStream {
  /** The transaction id (32 lowercase hex chars). */
  readonly transactionId: string;
  /** Declared transaction total from the frame headers. */
  readonly totalLen: number;
  /** Transaction bytes reassembled before this stream existed (already delivered elsewhere). */
  readonly startOffset: number;
  /**
   * Bytes of this transaction received so far (the contiguous prefix,
   * including {@link startOffset}). On an `incomplete` termination this
   * is the shortfall: the peer announced completion while bytes were
   * missing.
   */
  readonly receivedBytes: number;
  /**
   * Bytes the receiver is waiting for — the transaction's declared total
   * ({@link totalLen}). On an `incomplete` termination, `receivedBytes <
   * expectedBytes` is the gap.
   */
  readonly expectedBytes: number;
  /** True while a TRANSACTION_PAUSE for this transaction is in effect (holder intent). */
  readonly paused: boolean;
  /**
   * True while the connection is down (no data can flow). Orthogonal to
   * `paused`: paused = holder intent, stalled = connection state. A
   * stalled stream keeps its object, buffer and chunk queue; when the
   * session re-wires and the RESUME handshake completes, the SAME stream
   * resumes emitting. Never set for ended streams.
   */
  readonly stalled: boolean;
  /**
   * Register the ordered chunk callback (single slot). Chunks arriving
   * before registration are flushed to it on registration; pass null to
   * unregister.
   */
  onChunk(handler: ((chunk: Uint8Array, stream: BinaryStream) => void) | null): void;
  /**
   * Pull the next ordered chunk, or null when drained (no contiguous frame
   * pending). Chunks taken here are never re-delivered through `onChunk`.
   */
  read(): Uint8Array | null;
  /**
   * Register the termination callback (single slot). Fires immediately
   * with the recorded reason when the stream already ended.
   */
  onEnd(handler: ((reason: StreamEndReason, stream: BinaryStream) => void) | null): void;
}

/** Handler invoked once per inbound transaction, at its first frame. */
export type BinaryStreamHandler = (stream: BinaryStream) => void;

/**
 * Streaming send handle over one outbound binary transaction (design
 * anchor §2.1). `write` pends under backpressure until ACKs release the
 * cap; `pause`/`resume` gate the transaction's egress and retransmission
 * clock (wire: TRANSACTION_PAUSE, implicit resumption); `abort` releases
 * the copy and announces TRANSACTION_ABORT. `finish` resolves with the
 * terminal {@link BinarySendResult} once the final ACK confirms (or the
 * copy is otherwise released).
 */
export interface BinarySink {
  readonly transactionId: string;
  /** Append one chunk (split into working frames). Resolves on egress admission. */
  write(chunk: Uint8Array): Promise<void>;
  /** Close the stream; resolves with the terminal send result. */
  finish(): Promise<BinarySendResult>;
  /** Suspend new frames and the retransmission clock; emits TRANSACTION_PAUSE. */
  pause(): void;
  /** Resume egress and the retransmission clock (no wire frame — implicit resumption). */
  resume(): void;
  /** Release the copy and emit TRANSACTION_ABORT. Rejects pending writes/finish. */
  abort(): void;
}

export interface TransferConfig {
  /** Sender-side tuning (frame size, timeouts, backpressure cap). */
  sender?: SenderOptions;
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
