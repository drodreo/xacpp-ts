/**
 * XACPP logical session.
 *
 * Created via `XacppPeer.establish` on the initiator side; on the responder
 * side, construct it directly inside `onEstablish` from the transport
 * parameter and the self-generated session_id/credentials.
 * Multiple Sessions under the same Peer share the same connection.
 */

import type { XacppTransport } from "./transport";
import type { XacppCommand } from "./commands";
import type { XacppActivityEvent } from "./events";
import type { XacppRequest, XacppResponse } from "./message";

/** XACPP logical session.
 *
 * Created via `XacppPeer.establish`, holds an independent session_id and credentials.
 * Multiple Sessions under the same Peer share the same connection.
 */
export class XacppSession {
  private transport: XacppTransport;
  private _sessionId: string;
  private _credentials: string;

  /**
   * Creates a session handle over an established transport.
   *
   * Initiator side: returned by `XacppPeer.establish`. Responder side:
   * construct directly inside `onEstablish` (transport parameter +
   * self-generated sessionId + issued credentials).
   */
  constructor(
    transport: XacppTransport,
    sessionId: string,
    credentials: string,
  ) {
    this.transport = transport;
    this._sessionId = sessionId;
    this._credentials = credentials;
  }

  /** Session identifier. */
  get sessionId(): string {
    return this._sessionId;
  }

  /**
   * Credentials issued by the responder.
   *
   * Caller can save them for use in subsequent `establish` calls.
   */
  get credentials(): string {
    return this._credentials;
  }

  /** Send command and wait for response. */
  async requestCommand(command: XacppCommand): Promise<XacppResponse> {
    return this.transport.send(this._sessionId, { kind: "command", payload: command });
  }

  /** Send event and wait for response. */
  async requestEvent(event: XacppActivityEvent): Promise<XacppResponse> {
    return this.transport.send(this._sessionId, { kind: "event", payload: event });
  }

  /**
   * Send event fire-and-forget: returns as soon as the frame is queued for
   * writing, never waits for the ack.
   *
   * Ordering against other outbound frames is preserved by the transport's
   * single FIFO egress. Throws = connection already known dead.
   */
  async sendEvent(event: XacppActivityEvent): Promise<void> {
    return this.transport.sendFaf(this._sessionId, { kind: "event", payload: event });
  }

  /**
   * Subscribe to the underlying connection's close notification.
   *
   * The listener fires immediately if the connection is already closed before
   * the subscription. Returns an unsubscribe function.
   */
  onClosed(listener: () => void): () => void {
    return this.transport.onClosed(listener);
  }
}
