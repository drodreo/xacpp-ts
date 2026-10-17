/**
 * XACPP logical session.
 *
 * Session-level entity on the initiator side: constructed independently,
 * handed to `XacppPeer` (which wires it automatically on connect and
 * backfills the negotiated identity after `establish`). On the responder
 * side, construct it inside `onEstablish` and wire it to the transport
 * parameter. The session carries the transfer state (binary reliability),
 * so it survives across connections — wire the same session into the
 * replacement transport and interrupted transfers resume (SPEC §5).
 */

import type { XacppTransport } from "./transport";
import type { XacppCommand } from "./commands";
import type { XacppActivityEvent } from "./events";
import type { XacppRequest, XacppResponse } from "./message";
import { XacppError } from "./message";
import { TransferSession } from "./transfer";
import type {
  BinaryHandler,
  BinarySendResult,
  TransferConfig,
  TransferWiring,
} from "./transfer";

/** XACPP logical session.
 *
 * Holds the identity (session_id + credentials) and the transfer state.
 * Communication and binary transfer both require a live connection:
 * `wire(transport)` attaches the session (auto-detach on close), and the
 * same session can be wired into a replacement transport after a
 * reconnection.
 */
export class XacppSession {
  private readonly transfer: TransferSession;
  private wiredTransport: (XacppTransport & TransferWiring) | null = null;
  private unwireOnClose: (() => void) | null = null;
  private _sessionId: string | null = null;
  private _credentials: string | null = null;
  /** Event names allowed to emit (negotiated produceEvents ∩ remote acceptEvents); empty = unconstrained. */
  private emitEvents: string[] = [];

  /**
   * Creates a session with its own transfer state (default configuration,
   * optionally tuned via `config`). Identity is not part of construction:
   * the initiator's peer backfills it after `establish`
   * (`bindIdentity`); the responder constructs and binds inside
   * `onEstablish`.
   */
  constructor(config?: TransferConfig) {
    this.transfer = new TransferSession(config);
  }

  /** Session identifier (throws before the identity is bound). */
  get sessionId(): string {
    if (this._sessionId === null) throw XacppError.invalidState("session identity not bound (establish pending)");
    return this._sessionId;
  }

  /**
   * Credentials issued by the responder (throws before the identity is
   * bound). Caller can save them for use in subsequent `establish` calls.
   */
  get credentials(): string {
    if (this._credentials === null) throw XacppError.invalidState("session identity not bound (establish pending)");
    return this._credentials;
  }

  /** Whether the identity has been bound (establish completed). */
  get identityBound(): boolean {
    return this._sessionId !== null;
  }

  /** Bind the negotiated identity (peer-driven after establish; responder-side self-service). */
  bindIdentity(sessionId: string, credentials: string): void {
    this._sessionId = sessionId;
    this._credentials = credentials;
  }

  /**
   * Attach the session to a live transport: communication channel +
   * transfer state (binding, clock, auto-RESUME). Auto-detaches when the
   * transport closes; wiring the same session into a replacement
   * transport resumes interrupted transfers.
   */
  wire(transport: XacppTransport & TransferWiring): void {
    if (this.wiredTransport !== null) throw XacppError.invalidState("session already wired to a transport");
    this.wiredTransport = transport;
    this.transfer.wire(transport);
    this.unwireOnClose = transport.onClosed(() => this.unwire());
  }

  /** Detach from the current connection (idempotent; identity and transfer state survive). */
  unwire(): void {
    this.unwireOnClose?.();
    this.unwireOnClose = null;
    this.wiredTransport = null;
    this.transfer.unwire();
  }

  /** Whether the session is currently wired to a live transport. */
  get wired(): boolean {
    return this.wiredTransport !== null;
  }

  /** Send command and wait for response. */
  async requestCommand(command: XacppCommand): Promise<XacppResponse> {
    return this.requireTransport().send(this.requireSessionId(), { kind: "command", payload: command });
  }

  /** Send event and wait for response. */
  async requestEvent(event: XacppActivityEvent): Promise<XacppResponse> {
    this.assertEmitAllowed(event);
    return this.requireTransport().send(this.requireSessionId(), { kind: "event", payload: event });
  }

  /**
   * Send event fire-and-forget: returns as soon as the frame is queued for
   * writing, never waits for the ack.
   *
   * Ordering against other outbound frames is preserved by the transport's
   * single FIFO egress. Throws = connection already known dead.
   */
  async sendEvent(event: XacppActivityEvent): Promise<void> {
    this.assertEmitAllowed(event);
    return this.requireTransport().sendFaf(this.requireSessionId(), { kind: "event", payload: event });
  }

  /**
   * Send a binary payload as a reliable transaction (see
   * `TransferSession.sendBinary`). Returns the transaction id; the
   * optional callback reports the final outcome (confirmed or released).
   */
  sendBinary(payload: Uint8Array, onComplete?: (result: BinarySendResult) => void): string {
    return this.transfer.sendBinary(payload, onComplete);
  }

  /**
   * Register the handler for complete inbound binary transactions
   * (receives txid + the bit-exact payload). Pass null to unregister.
   * Payloads completing before registration are delivered exactly once.
   */
  onBinary(handler: BinaryHandler | null): void {
    this.transfer.onBinary = handler;
  }

  /**
   * Subscribe to the wired connection's close notification. The listener
   * fires immediately if the connection is already closed before the
   * subscription. Returns an unsubscribe function.
   */
  onClosed(listener: () => void): () => void {
    return this.requireTransport().onClosed(listener);
  }

  // ---- Internals ----

  /** Narrow the negotiated emit capability (peer-driven after negotiate). */
  setEmitEvents(events: string[]): void {
    this.emitEvents = events;
  }

  private requireTransport(): XacppTransport & TransferWiring {
    if (this.wiredTransport === null) throw XacppError.notConnected();
    return this.wiredTransport;
  }

  private requireSessionId(): string {
    if (this._sessionId === null) throw XacppError.invalidState("session identity not bound (establish pending)");
    return this._sessionId;
  }

  private assertEmitAllowed(event: XacppActivityEvent): void {
    const eventName = event.event.name;
    if (this.emitEvents.length > 0 && !this.emitEvents.includes(eventName)) {
      throw XacppError.invalidState(`event '${eventName}' not in negotiated emitEvents capability`);
    }
  }
}
