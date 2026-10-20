import type { BinarySendResult, BinarySink } from "./types";
import { encodeFrame, makeFrame } from "../wire/frame";
import { CONTROL_PAYLOAD_FRAME_TYPES, encodeTransactionAbort, encodeTransactionPause } from "../wire/control";
import { XacppError } from "../wire/message";
import type { TransferSession } from "./core";

/**
 * Send-side controller behind {@link TransferSession.openBinarySink}: a
 * thin façade over the session's streaming internals; per-sink state is
 * just the pending finish rejection (for abort).
 */
export class SinkController implements BinarySink {
  readonly transactionId: string;
  private readonly session: TransferSession;
  private finishReject: ((err: unknown) => void) | null = null;

  constructor(session: TransferSession, transactionId: string) {
    this.session = session;
    this.transactionId = transactionId;
  }

  async write(chunk: Uint8Array): Promise<void> {
    const wasPaused = this.session.isBackpressured(); // entry snapshot (before the append)
    const { frames, held } = this.session.sinkAppend(this.transactionId, chunk);
    if (held) {
      void frames; // parked in the copy's hold queue until resume
      return this.session.queueHeldWrite(this.transactionId);
    }
    return this.session.admitFrames(this.transactionId, frames, wasPaused);
  }

  /**
   * Emit a transaction-control frame on the channel the contract demands:
   * the expedited control channel when the transaction has nothing left in
   * the egress queues, the data FIFO otherwise.
   *
   * A control frame must never overtake the transaction's own data frames:
   * the peer reads the frame types in wire order, and an abort that arrives
   * before the first byte is a frame about a transaction the peer has not
   * seen yet (it has no tombstone to remember it). The data FIFO keeps
   * the frame behind the data it describes.
   */
  private emitTransactionControl(frameType: number, payload: Uint8Array, mayExpedite: boolean): void {
    const frame = makeFrame(frameType, payload);
    if (mayExpedite) {
      this.session.emitControl(encodeFrame(frame));
      return;
    }
    this.session.emitOrderedControl(frame);
  }

  async finish(): Promise<BinarySendResult> {
    // Fast path: the final ACK may already have released the copy before
    // finish was called. Absent from the copies table = already released =
    // confirmed — finish only ever acts on a successfully opened
    // transaction, so absence can only mean the final ACK released it.
    if (!this.session.senderHasCopy(this.transactionId)) {
      return { transactionId: this.transactionId, confirmed: true };
    }
    // Close the transaction. A short write (fewer bytes than declared)
    // is rejected here — the announcement is an ABORT, never a
    // completion: the peer must learn that no more bytes are coming
    // instead of waiting for a tail that will never arrive.
    try {
      this.session.sinkFinish(this.transactionId);
    } catch (e) {
      // Scheduling judgment BEFORE the copy is released and the pending
      // writes are rejected — both clear the parked state, and the abort
      // must be judged against the state the transaction was in.
      const mayExpedite = this.session.sinkMayExpediteControl(this.transactionId);
      this.session.sinkAbort(this.transactionId);
      this.session.rejectPendingWrites(this.transactionId);
      this.emitTransactionControl(
        CONTROL_PAYLOAD_FRAME_TYPES.transactionAbort,
        encodeTransactionAbort(this.transactionId),
        mayExpedite,
      );
      throw e;
    }
    // Every declared byte is out: announce the completion right after the
    // tail data frame (wire order — never ahead of it).
    this.session.sinkAnnounceCompleted(this.transactionId);
    return new Promise<BinarySendResult>((resolve, reject) => {
      this.finishReject = reject;
      this.session.sinkRegisterCompletion(this.transactionId, (result) => {
        this.finishReject = null;
        resolve(result);
      });
      // Re-check after registering: the final ACK may have landed between
      // the fast-path check and the registration (the completion entry
      // would otherwise never fire).
      if (!this.session.senderHasCopy(this.transactionId)) {
        this.session.removeCompletion(this.transactionId); // consume own slot
        this.finishReject = null;
        resolve({ transactionId: this.transactionId, confirmed: true });
      }
    });
  }

  pause(): void {
    // Judgment before the state changes: the pause is announced against the
    // transaction's egress state at the call.
    const mayExpedite = this.session.sinkMayExpediteControl(this.transactionId);
    this.session.sinkSetPaused(this.transactionId, true);
    this.emitTransactionControl(
      CONTROL_PAYLOAD_FRAME_TYPES.transactionPause,
      encodeTransactionPause(this.transactionId),
      mayExpedite,
    );
  }

  resume(): void {
    const held = this.session.sinkSetPaused(this.transactionId, false);
    this.session.flushHeldFrames(this.transactionId, held);
  }

  abort(): void {
    // Judgment BEFORE the copy is released and the pending writes are
    // rejected — both clear the parked state, and an abort must never
    // overtake the transaction's own frames.
    const mayExpedite = this.session.sinkMayExpediteControl(this.transactionId);
    this.session.sinkAbort(this.transactionId);
    if (this.finishReject !== null) {
      const reject = this.finishReject;
      this.finishReject = null;
      reject(new XacppError("transaction_aborted", `transaction ${this.transactionId} aborted`));
    }
    this.session.rejectPendingWrites(this.transactionId);
    this.emitTransactionControl(
      CONTROL_PAYLOAD_FRAME_TYPES.transactionAbort,
      encodeTransactionAbort(this.transactionId),
      mayExpedite,
    );
  }
}
