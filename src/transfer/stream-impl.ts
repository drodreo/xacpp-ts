import type { BinaryStream, StreamEndReason } from "./types";

/**
 * Receive-side stream implementation: a FIFO of ordered chunks with
 * push/pull delivery surfaces (`onChunk` single slot, `read`), a paused
 * flag (TRANSACTION_PAUSE capture), a stalled flag (connection down —
 * v3 stream lifecycle), and a recorded termination. The object is
 * session-scoped: it survives connection drops and only terminates via
 * `completed`/`cancelled`.
 */
export class BinaryStreamImpl implements BinaryStream {
  readonly transactionId: string;
  readonly totalLen: number;
  readonly startOffset: number;
  /** Next seq to emit (chunks at or above the creation-time frontier only). */
  nextSeq: number;
  private ended = false;
  private endReason: StreamEndReason | null = null;
  /** Bytes received so far (contiguous prefix, startOffset included). */
  private receivedByteCount: number;
  /** Bytes expected in total (the declared totalLen). */
  private expectedByteCount: number;
  private pausedFlag = false;
  private stalledFlag = false;
  private readonly queue: Uint8Array[] = [];
  private chunkListener: ((chunk: Uint8Array, stream: BinaryStream) => void) | null = null;
  private endListener: ((reason: StreamEndReason, stream: BinaryStream) => void) | null = null;

  constructor(transactionId: string, totalLen: number, startOffset: number, startSeq: number) {
    this.transactionId = transactionId;
    this.totalLen = totalLen;
    this.startOffset = startOffset;
    this.nextSeq = startSeq;
    this.receivedByteCount = startOffset;
    this.expectedByteCount = totalLen;
  }

  get receivedBytes(): number {
    return this.receivedByteCount;
  }

  get expectedBytes(): number {
    return this.expectedByteCount;
  }

  get paused(): boolean {
    return this.pausedFlag;
  }

  get stalled(): boolean {
    return this.stalledFlag;
  }

  get terminated(): boolean {
    return this.ended;
  }

  /** Register the chunk callback; queued chunks flush to it on registration. */
  onChunk(handler: ((chunk: Uint8Array, stream: BinaryStream) => void) | null): void {
    this.chunkListener = handler;
    if (handler === null) return;
    while (this.queue.length > 0) {
      const chunk = this.queue.shift();
      if (chunk === undefined) break;
      this.deliver(chunk);
    }
  }

  /** Pull the next ordered chunk (exactly-once across both delivery surfaces). */
  read(): Uint8Array | null {
    return this.queue.shift() ?? null;
  }

  /** Register the termination callback; fires immediately when already ended. */
  onEnd(handler: ((reason: StreamEndReason, stream: BinaryStream) => void) | null): void {
    this.endListener = handler;
    if (handler === null || !this.ended) return;
    const reason = this.endReason;
    if (reason !== null) handler(reason, this);
  }

  /** Enqueue one contiguous chunk and hand it to the listener when present. */
  pushChunk(payload: Uint8Array): void {
    if (this.ended) return;
    this.nextSeq += 1;
    this.receivedByteCount += payload.length;
    if (this.chunkListener !== null) {
      this.deliver(payload);
      return;
    }
    this.queue.push(payload);
  }

  /** TRANSACTION_PAUSE captured. */
  markPaused(): void {
    this.pausedFlag = true;
  }

  /** Implicit resumption: the next data frame of the transaction (SPEC §5). */
  markResumed(): void {
    this.pausedFlag = false;
  }

  /** Connection gone: surface the stall (object and buffer stay alive). */
  markStalled(): void {
    this.stalledFlag = true;
  }

  /** Connection live again: clear the stall (RESUME continuation lands here). */
  markRewired(): void {
    this.stalledFlag = false;
  }

  /**
   * Record the termination and fire the end listener (idempotent).
   * `detail` carries the terminal byte counts of an `incomplete` end (the
   * shortfall the peer announced over).
   */
  terminate(reason: StreamEndReason, detail?: { received: number; expected: number }): void {
    if (this.ended) return;
    if (detail !== undefined) {
      this.receivedByteCount = detail.received;
      this.expectedByteCount = detail.expected;
    }
    this.ended = true;
    this.endReason = reason;
    this.endListener?.(reason, this);
  }

  private deliver(chunk: Uint8Array): void {
    const listener = this.chunkListener;
    if (listener === null) return;
    try {
      listener(chunk, this);
    } catch (e) {
      console.error(
        "[xacpp:reliability] stream chunk listener failed for %s: %s",
        this.transactionId,
        e instanceof Error ? e.message : String(e),
      );
    }
  }
}
