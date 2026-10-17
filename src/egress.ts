/**
 * Ordered egress: single FIFO exit for all outbound frames.
 *
 * One drain loop per connection owns the write half; every outbound frame
 * (request / event / response) is queued and written in submission order.
 * Two send paths differ only at enqueue time:
 *
 * - **acked** (`sendAcked`): waits for the write result — fail fast on
 *   write error (command path, preserves historical `send` semantics);
 * - **fire-and-forget** (`sendFaf`): returns as soon as the frame is
 *   queued — the caller never blocks on the wire (event path).
 */

import { XacppError } from "./message";
import type { XacppResponse } from "./message";

/** Minimal writer surface shared by socket and stdio transports. */
export interface EgressWriter {
  write(chunk: string | Uint8Array, cb?: (err?: Error | null) => void): boolean;
  end(cb?: () => void): void;
}

/** Pending slot for request/response correlation. */
export type PendingSlot =
  | {
      /** Awaiting the response payload (caller of `send`). */
      kind: "respond";
      resolve: (response: XacppResponse) => void;
      reject: (err: XacppError) => void;
    }
  /** Fire-and-forget: the peer's ack is silently consumed on arrival. */
  | { kind: "drop" };

/** Shared pending table type (one per connection). */
export type PendingMap = Map<string, PendingSlot>;

/** Host integration points owned by the transport. */
export interface EgressHost {
  /** Shared connected flag (checked before enqueueing). */
  isConnected(): boolean;
  /** Flip the shared connected flag off (fail-fast on write error). */
  markDisconnected(): void;
  /** Drop every pending slot: respond waiters reject with Closed, faf slots are discarded. */
  clearPending(): void;
}

/** Connection-close broadcast (equivalent of the Rust-side watch channel). */
export class ClosedSignal {
  private closed = false;
  private readonly listeners = new Set<() => void>();

  /** Mark the connection closed and notify subscribers (idempotent). */
  markClosed(): void {
    if (this.closed) return;
    this.closed = true;
    for (const listener of [...this.listeners]) listener();
  }

  /** Whether the connection is known closed. */
  get isClosed(): boolean {
    return this.closed;
  }

  /**
   * Subscribe to the close notification. The listener fires immediately if
   * the connection is already closed (late-subscriber contract). Returns an
   * unsubscribe function.
   */
  subscribe(listener: () => void): () => void {
    if (this.closed) listener();
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }
}

/** Write-result receipt: present on the acked path, absent for fire-and-forget. */
interface WriteAck {
  resolve: () => void;
  reject: (err: XacppError) => void;
}

/** A queued outbound item. */
type EgressItem =
  | {
      kind: "frame";
      bytes: Uint8Array;
      writeAck: WriteAck | null;
      /**
       * Control frames take scheduling priority and interleave ahead of
       * queued data frames (SPEC §5 performance invariant 4). Envelope
       * (DATA_JSON) frames stay plain FIFO — their position is
       * order-sensitive.
       */
      priority?: boolean;
    }
  /** Graceful teardown: flush queued frames, end the writer, exit. */
  | { kind: "close" };

/**
 * Ordered egress handle: entry for enqueueing outbound frames.
 *
 * Throws/rejects with `XacppError` = connection already known dead.
 */
export class Egress {
  private queue: EgressItem[] = [];
  private draining = false;
  private ended = false;

  constructor(
    private readonly writer: EgressWriter,
    private readonly host: EgressHost,
    private readonly closedSignal: ClosedSignal,
  ) {}

  /** Enqueue a frame and wait for the write result. */
  sendAcked(bytes: Uint8Array): Promise<void> {
    if (!this.host.isConnected()) return Promise.reject(XacppError.notConnected());
    return new Promise<void>((resolve, reject) => {
      this.enqueue({ kind: "frame", bytes, writeAck: { resolve, reject } });
    });
  }

  /** Enqueue a frame without waiting for the write result (fire-and-forget). */
  sendFaf(bytes: Uint8Array): void {
    if (!this.host.isConnected()) throw XacppError.notConnected();
    this.enqueue({ kind: "frame", bytes, writeAck: null });
  }

  /**
   * Enqueue a control frame with scheduling priority (fire-and-forget).
   * The frame is written ahead of all queued data frames (DATA_JSON /
   * DATA_BINARY) but never ahead of a pending graceful close. Throws =
   * connection already known dead.
   */
  sendControl(bytes: Uint8Array): void {
    if (!this.host.isConnected()) throw XacppError.notConnected();
    this.enqueue({ kind: "frame", bytes, writeAck: null, priority: true });
  }

  /**
   * Shut the egress down: queued frames flush FIFO, then the writer is ended
   * and the drain loop exits. Subsequent sends fail. Marks the connection
   * closed for `onClosed` watchers.
   */
  close(): void {
    this.host.markDisconnected();
    this.enqueue({ kind: "close" });
    this.closedSignal.markClosed();
  }

  /** Mark the connection closed for `onClosed` watchers (idempotent). */
  markClosed(): void {
    this.closedSignal.markClosed();
  }

  private enqueue(item: EgressItem): void {
    if (this.ended) return;
    if (item.kind === "frame" && item.priority) {
      // Interleave ahead of queued data frames; never ahead of a close item.
      const idx = this.queue.findIndex((queued) => !(queued.kind === "frame" && queued.priority));
      if (idx === -1) this.queue.push(item);
      else this.queue.splice(idx, 0, item);
    } else {
      this.queue.push(item);
    }
    this.pump();
  }

  private pump(): void {
    if (this.draining) return;
    this.draining = true;
    void this.drain();
  }

  private async drain(): Promise<void> {
    while (this.queue.length > 0) {
      const item = this.queue.shift()!;
      if (item.kind === "close") {
        this.shutdownWriter();
        return;
      }
      const failed = await this.writeFrame(item.bytes);
      if (item.writeAck) {
        if (failed) item.writeAck.reject(XacppError.closed());
        else item.writeAck.resolve();
      }
      if (failed) {
        // Connection dead: fail fast everything, now and later.
        this.teardown();
        return;
      }
    }
    // Queue empty: idle until the next enqueue re-arms the pump.
    this.draining = false;
  }

  /**
   * Fail-fast teardown on write error: mark disconnected, clear pending
   * (all waiters fail), fail remaining queued acks, mark closed, stop.
   */
  private teardown(): void {
    this.ended = true;
    this.host.markDisconnected();
    this.host.clearPending();
    for (const item of this.queue.splice(0)) {
      if (item.kind === "frame" && item.writeAck) item.writeAck.reject(XacppError.closed());
    }
    this.closedSignal.markClosed();
  }

  /** Flush done: end the writer and stop the drain loop. */
  private shutdownWriter(): void {
    this.ended = true;
    try {
      this.writer.end(() => {});
    } catch {
      // Writer already destroyed — nothing left to do.
    }
  }

  /** Write one encoded binary frame to the wire. Resolves to true on error. */
  private writeFrame(bytes: Uint8Array): Promise<boolean> {
    return new Promise((resolve) => {
      try {
        this.writer.write(bytes, (err) => resolve(err != null));
      } catch {
        resolve(true);
      }
    });
  }
}
