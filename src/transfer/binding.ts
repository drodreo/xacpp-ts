import { FrameType, isControlFrameType } from "../wire/frame";
import type { Frame } from "../wire/frame";
import type { XacppTransport } from "../carrier/transport";
import { DEFAULT_RELIABILITY_TICK_MS } from "./core";
import type { TransferSession } from "./core";
import type { TickOutcome } from "./types";

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
export class ReliabilityBinding {
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
 * Whether the transport carries the transfer wiring surface — the binary
 * channel the transfer session attaches to (control/data frame writes and
 * the inbound frame hook). The transports this library ships (socket /
 * stdio) do; a custom transport may not. The guard replaces the blind
 * `as XacppTransport & TransferWiring` casts: a transport without the
 * surface simply has no binary channel (no runtime surprise), instead of
 * failing with a "not a function" deep inside the transfer session.
 */
export function isTransferWiring(
  transport: XacppTransport,
): transport is XacppTransport & TransferWiring {
  return (
    "writeControlFrame" in transport &&
    typeof transport.writeControlFrame === "function" &&
    "writeDataFrame" in transport &&
    typeof transport.writeDataFrame === "function" &&
    "onTransferActivity" in transport &&
    typeof transport.onTransferActivity === "function"
  );
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

/** A sink write parked until its frames are admitted to the egress. */
export interface PendingWrite {
  transactionId: string;
  resolve: () => void;
  reject: (err: unknown) => void;
}
