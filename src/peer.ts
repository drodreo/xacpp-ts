/**
 * XACPP Peer — protocol layer endpoint.
 *
 * ## Responsibility
 *
 * Peer is a protocol layer entity representing one endpoint in the communication link. Core responsibilities:
 *
 * - **Typed operations**: encapsulate Transport's payload-layer API into semantically clear Command / Event operations
 * - **Protocol state machine**: manage connection state
 * - **Session routing**: route inbound requests to the corresponding Session's handler by session_id
 *
 * ## Boundary with Transport
 *
 * Peer holds `XacppTransport` (composition), delegating all low-level IO to Transport.
 * Peer does not care about envelope id, encoding/decoding, request-response correlation (all encapsulated by Transport).
 *
 * ```text
 * ┌──────────────────────────────────────────────────────────────┐
 * │  Peer (protocol layer)                                       │
 * │  Typed operations: request_command / request_event           │
 * │  Session routing: session_id → XacppSessionHandler          │
 * ├──────────────────────────────────────────────────────────────┤
 * │  Session (session layer)                                     │
 * │  Holds XacppSessionHandler, sends directly to Transport      │
 * │  Does not go through Peer                                    │
 * ├──────────────────────────────────────────────────────────────┤
 * │  Transport (transport layer)                                 │
 * │  Single semantic: send (request-response), caller chooses    │
 * │  whether to wait for response                                │
 * │  Internals: envelope packing/unpacking (id correlation),     │
 * │  encoding/decoding, pending matching                         │
 * │  Unaware of specific business event types                    │
 * └──────────────────────────────────────────────────────────────┘
 * ```
 */

import type { XacppTransport } from "./transport";
import type { XacppCommand } from "./commands";
import type { XacppEvent } from "./events";
import type { XacppRequest, XacppResponse } from "./message";
import { XacppError } from "./message";
import type { EstablishHandler, XacppSessionHandler } from "./handler";
import { XacppSession } from "./session";

/** Peer protocol state. */
export enum PeerState {
  /** Not connected / connection closed. */
  Disconnected = "disconnected",
  /** Communication channel established, ready to create logical sessions. */
  Connected = "connected",
}

/** XACPP protocol endpoint.
 *
 * Each communication party holds a `XacppPeer` instance, exchanging messages via a shared Transport.
 */
export class XacppPeer {
  private transport: XacppTransport;
  private _state: PeerState = PeerState.Disconnected;
  private sessions: Map<string, XacppSessionHandler> = new Map();
  private establishHandler: EstablishHandler;

  constructor(transport: XacppTransport, establishHandler: EstablishHandler) {
    this.transport = transport;
    this.establishHandler = establishHandler;
  }

  /** Current protocol state. */
  get state(): PeerState {
    return this._state;
  }

  // ---- Connection management ----

  /** Establish connection.
   *
   * Registers routing closure with Transport, then starts the underlying communication channel.
   * On success, state transitions to `Connected`; subsequent `establish` calls can create logical sessions.
   */
  async connect(): Promise<void> {
    const sessions = this.sessions;
    const establishHandler = this.establishHandler;
    const transport = this.transport;

    transport.onRequest((sessionId, payload) => {
      return Promise.resolve().then(async () => {
        if (sessionId === null) {
          // Pre-session request
          if (payload.kind === "command" && typeof payload.payload === "object" && "establish" in payload.payload) {
            // Establish request
            const credentials = payload.payload.establish.credentials;
            const result = await establishHandler.onEstablish(transport, credentials);
            sessions.set(result.sessionId, result.handler);
            return { kind: "established" as const, sessionId: result.sessionId };
          }
          throw XacppError.invalidRequest("missing session_id");
        }

        // Route to Session handler
        const handler = sessions.get(sessionId);
        if (!handler) {
          throw XacppError.internal(`unknown session: ${sessionId}`);
        }

        if (payload.kind === "command") {
          return handler.onCommand(payload.payload);
        } else {
          return handler.onEvent(payload.payload);
        }
      });
    });

    await this.transport.connect();
    if (this._state === PeerState.Disconnected) {
      this._state = PeerState.Connected;
    }
  }

  /** Establish logical session.
   *
   * Sends Establish command to peer, carrying optional auth credentials and session handler.
   * Handler is registered in Peer routing table; Session is responsible for sending.
   */
  async establish(
    credentials: string | null,
    handler: XacppSessionHandler,
  ): Promise<XacppSession> {
    const response = await this.transport.send(null, {
      kind: "command",
      payload: { establish: { credentials } },
    });

    if (response.kind === "established") {
      this.sessions.set(response.sessionId, handler);
      return new XacppSession(
        this.transport,
        response.sessionId,
        response.credentials ?? null,
      );
    }

    if (response.kind === "establish_reject") {
      throw XacppError.establishReject(response.reason);
    }

    if (response.kind === "error") {
      throw XacppError.application(response.code, response.message);
    }

    throw XacppError.internal(`unexpected response to establish: ${JSON.stringify(response)}`);
  }

  /** Disconnect. */
  async disconnect(): Promise<void> {
    await this.transport.disconnect();
    this._state = PeerState.Disconnected;
    this.sessions.clear();
  }

  // ---- Active sends (no session context) ----

  /** Send command and wait for response (no session context). */
  async requestCommand(sessionId: string | null, command: XacppCommand): Promise<XacppResponse> {
    return this.transport.send(sessionId, { kind: "command", payload: command });
  }

  /** Send interactive event and wait for response (no session context). */
  async requestEvent(sessionId: string | null, event: XacppEvent): Promise<XacppResponse> {
    return this.transport.send(sessionId, { kind: "event", payload: event });
  }
}
