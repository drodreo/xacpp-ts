/**
 * XACPP Handler type definitions.
 *
 * This module centralizes all handler-related types:
 *
 * - {@link XacppSessionHandler}: logical session handler (processes Command / Event)
 * - {@link EstablishHandler}: peer Establish request handler
 */

import type { XacppTransport } from "./transport";
import type { XacppCommand } from "./commands";
import type { XacppActivityEvent } from "./events";
import type { XacppResponse } from "./message";

// ---- Establish Decision ----

/** Decision made by the responder upon receiving an Establish request. */
export type EstablishDecision =
  /** First connection: challenge required → responder returns establish_prepare. */
  | { type: "challenge_required"; challenge: string }
  /** Credentials valid: direct establishment → responder returns established. */
  | { type: "established"; sessionId: string; handler: XacppSessionHandler };

// ---- Session Handler ----

/** XACPP Session Handler interface.
 *
 * Each logical session holds one implementation, processing Commands and Events from the peer.
 */
export interface XacppSessionHandler {
  /** Handle Command. */
  onCommand(command: XacppCommand): Promise<XacppResponse>;

  /** Handle Event. */
  onEvent(event: XacppActivityEvent): Promise<XacppResponse>;
}

// ---- Establish Handler ----

/** Peer Establish request handler — serve main function.
 *
 * Called by the responder when receiving an Establish command from the peer.
 * The developer performs credential validation, creates and holds a Session (for subsequent proactive sends),
 * creates a SessionHandler and returns it. Returning reject denies the handshake.
 *
 * ## Identity Contract
 *
 * `credentials` is an identity **anchor** — it never carries user/agent identity directly.
 * Both sides maintain their own internal identity mapping:
 *
 * - On first connection (`credentials === null`): the responder performs a trust process,
 *   internally associates this connection with a specific user and agent, then issues credentials
 *   as an opaque handle to that identity. Neither side transmits user/agent over the wire.
 * - On subsequent connections: the initiator presents saved credentials, the responder looks up
 *   the previously associated identity and routes accordingly.
 */
export interface EstablishHandler {
  /**
   * Handle Establish request.
   *
   * `transport` is passed in by Peer, for use inside `onEstablish` to create `XacppSession`.
   * Returns an {@link EstablishDecision}:
   * - `challenge_required`: first-time connection, responder issues a challenge.
   * - `established`: credentials valid, direct establishment with sessionId and handler.
   */
  onEstablish(
    transport: XacppTransport,
    credentials: string | null,
  ): Promise<EstablishDecision>;

  /** Phase 3: EstablishConfirm received (challenge path only). */
  onEstablishConfirm(
    transport: XacppTransport,
  ): Promise<{ sessionId: string; handler: XacppSessionHandler }>;
}
