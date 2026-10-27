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

import type { XacppTransport } from "../carrier/transport";
import type { XacppCommand } from "../wire/commands";
import type { XacppActivityEvent } from "../wire/events";
import type { XacppResponse } from "../wire/message";
import { XacppError } from "../wire/message";
import { TransferSession } from "../transfer";
import type {
  BinaryHandler,
  BinarySendResult,
  BinarySink,
  BinarySinkMeta,
  BinaryStreamHandler,
  TransferConfig,
  TransferWiring,
} from "../transfer";
import type { FetchReady } from "../wire/fetch";
import { FETCH_UNAVAILABLE } from "../wire/fetch";
import { isFetchFamily } from "../wire/commands";

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
   * 协商后的 fetch 事实（SPEC §8.7）：peer 在 negotiate 时回填。false =
   * 本端不得向该对端发起 fetch-family 命令（出站响亮失败）。未协商 =
   * false；disconnect 由 peer 清除。
   */
  private _fetchEnabled = false;

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
   * transfer state (binding, clock, recovery declaration). Auto-detaches
   * when the transport closes; wiring the same session into a replacement
   * transport resumes interrupted transfers.
   *
   * Declaration timing contract (SPEC §5): wiring DECLARES pending sends
   * — a consumer-driven wire (the responder side inside onEstablish, or
   * direct-transport consumers) only runs when the peer's transfer plane
   * is already live. The peer's own initiator-side path wires at connect
   * via `wireWithoutDeclaration` and declares only after the establish
   * handshake completes.
   */
  wire(transport: XacppTransport & TransferWiring): void {
    if (this.wiredTransport !== null) throw XacppError.invalidState("session already wired to a transport");
    this.wiredTransport = transport;
    this.transfer.wire(transport);
    this.unwireOnClose = transport.onClosed(() => this.unwire());
  }

  /**
   * Wire WITHOUT the recovery declaration: used by the peer at connect
   * time on the initiator side — the responder's session does not exist
   * until the upcoming establish, so a declaration now would be dropped by
   * the not-yet-wired peer, deadlocking the holder. The peer fires
   * `declarePendingSends` once establish completes.
   * @internal
   */
  wireWithoutDeclaration(transport: XacppTransport & TransferWiring): void {
    if (this.wiredTransport !== null) throw XacppError.invalidState("session already wired to a transport");
    this.wiredTransport = transport;
    this.transfer.wireWithoutDeclaration(transport);
    this.unwireOnClose = transport.onClosed(() => this.unwire());
  }

  /**
   * Recovery discipline trigger for the peer: send the RESUME declaration
   * for every pending send. Fired when the establish handshake completes —
   * the only moment both transfer planes are wired on an establish
   * topology. Idempotent.
   * @internal
   */
  declarePendingSends(): void {
    this.transfer.declarePendingSends();
  }

  /** Detach from the current connection (idempotent; identity and transfer state survive). */
  unwire(): void {
    this.unwireOnClose?.();
    this.unwireOnClose = null;
    this.wiredTransport = null;
    this.transfer.unwire();
  }

  /**
   * Ensure the transfer mount is attached to the given transport
   * (idempotent no-op while attached). Used by the peer when an establish
   * re-binds this session — `connect` performs the initial mount.
   * @internal
   */
  attachTransfer(transport: XacppTransport & TransferWiring): void {
    if (this.transfer.wired) return;
    this.transfer.wire(transport);
  }

  /**
   * Detach ONLY the transfer mount (clock stopped, single-slot handler
   * registration released): the envelope identity stays wired — the
   * envelope channel routes by session_id. Used by the peer when an
   * establish yields a different session identity on this connection: an
   * orphaned transfer clock would never see inbound activity again and
   * its silence timer would tear the shared connection down.
   * @internal
   */
  detachTransfer(): void {
    this.transfer.unwire();
  }

  /** Whether the session is currently wired to a live transport. */
  get wired(): boolean {
    return this.wiredTransport !== null;
  }

  /** Send command and wait for response. */
  async requestCommand(command: XacppCommand): Promise<XacppResponse> {
    // Outbound fetch gate (SPEC §8.7): fetch is a negotiated fact. Before
    // any fetch-family command leaves this session, the negotiation must
    // have enabled fetch (both sides declared fetchExt at the supported
    // version). Not enabled ⇒ fail loudly with the registered wire code —
    // the request is never sent.
    if (isFetchFamily(command) && !this._fetchEnabled) {
      throw XacppError.application(
        FETCH_UNAVAILABLE,
        "fetch not enabled with this peer (fetchExt not negotiated)",
      );
    }
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
   * Open a streaming send transaction (see `TransferSession.openBinarySink`):
   * chunked writes with ACK-driven backpressure, pause/resume/abort gating.
   */
  openBinarySink(meta: BinarySinkMeta, txid?: string): BinarySink {
    return this.transfer.openBinarySink(meta, txid);
  }

  /**
   * Register the handler for complete inbound binary transactions
   * (receives txid + the bit-exact payload). Pass null to unregister.
   * Payloads completing before registration are delivered exactly once.
   *
   * A transaction claimed by a stream (see `onBinaryStream`) never reaches
   * this handler — exactly one delivery surface owns each transaction.
   */
  onBinary(handler: BinaryHandler | null): void {
    this.transfer.onBinary = handler;
  }

  /**
   * Register the streaming receive handler (single slot): invoked once per
   * inbound transaction at its first frame, exposing ordered chunks, live
   * pause/stall state, and a terminal reason (`completed` / `cancelled` —
   * streams survive connection drops and end only on completion or
   * cancellation). Pass null to unregister.
   */
  onBinaryStream(handler: BinaryStreamHandler | null): void {
    this.transfer.onBinaryStream = handler;
  }

  /**
   * Fetch a resource's metadata from the peer by uri (SPEC §8 — the
   * stateless kernel): sends `fetch{uri}`, awaits the `fetch_ready`
   * response, and resolves with the declared metadata ONLY. The bytes
   * travel as a binary transaction whose id IS `ready.txid`; receive them
   * through `onBinaryStream` (or `onBinary`) — the kernel imposes no
   * buffering or delivery on their behalf (SPEC §8.4/§8.6).
   *
   * Connection loss fails the command with a not-connected error — retry
   * by re-issuing the same uri (idempotent; the responder decides
   * freshness).
   */
  async fetch(uri: string): Promise<FetchReady> {
    const response = await this.requestCommand({ fetch: { uri } });
    if (response.kind === "error") {
      throw XacppError.application(response.code, response.message);
    }
    if (response.kind !== "fetch_ready") {
      throw XacppError.internal(`unexpected response to fetch: ${JSON.stringify(response)}`);
    }
    return {
      txid: response.txid,
      sha256: response.sha256,
      sizeBytes: response.sizeBytes,
      mime: response.mime,
    };
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

  /**
   * Backfill the negotiated fetch fact (SPEC §8.7). Two drivers:
   * - the peer backfills the initiator-side held session after negotiate
   *   (and any session it re-binds on establish), clearing on disconnect;
   * - the responder side constructs its session inside `onEstablish` —
   *   the application backfills it there from the fact its
   *   `NegotiateHandler` received (the same consumption pattern as
   *   `effective.emitEvents`).
   * A negotiated fact, never a responder-assembly map.
   */
  setFetchEnabled(enabled: boolean): void {
    this._fetchEnabled = enabled;
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
