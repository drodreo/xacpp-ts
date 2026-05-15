/**
 * XACPP logical session.
 *
 * Created via `XacppPeer.establish`, holds an independent session_id and credentials.
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
  private _credentials: string | null;

  /** @internal Created by XacppPeer.establish. */
  constructor(
    transport: XacppTransport,
    sessionId: string,
    credentials: string | null,
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
  get credentials(): string | null {
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
}
