/**
 * XACPP Transport abstraction.
 *
 * ## Responsibility
 *
 * Transport unifies the underlying communication channel (stdio / TCP / WebSocket) into `send` semantics:
 *
 * - **send**: send request payload, wait for response payload to return. Caller can spawn to background if response is not needed.
 * - **accept**: listen for peer requests, dispatch via `onRequest` callback,
 *   which receives session_id + payload; the return value is automatically sent back as a response.
 *
 * Transport internals:
 *
 * - **Envelope packing/unpacking**: auto-assign request id, pack into envelope for sending, unpack to return payload
 * - **Request-response correlation**: match pending sends by id upon receiving a Response
 * - **Encoding/decoding**: serialization / deserialization (JSONL)
 * - **Connection management**: establish / tear down underlying communication channel
 *
 * ## Layer boundary
 *
 * - **Transport to upper layer**: exposes `send` / `onRequest`,
 *   does not expose raw byte send/receive, envelope id, or encoding/decoding details
 * - **Peer to upper layer**: exposes typed `requestCommand` / `requestEvent`
 *   and session routing mechanism
 *
 * ## accept semantics
 *
 * Transport listens for peer input, delivering (session_id, payload) to registered callbacks.
 * Handler returning `XacppResponse` indicates successful processing;
 * throwing `XacppError` indicates processing failure (Transport auto-constructs an Error response and sends it back).
 *
 * ## Error semantics
 *
 * **Connection-level throw = connection unavailable**. All fault-tolerance logic is encapsulated inside the Transport implementation.
 * Upper layer only needs one rule: method throw means connection error.
 */

import type { XacppRequest, XacppResponse } from "./message";

/** Transport layer request handler callback type. */
export type RequestHandler = (
  sessionId: string | null,
  payload: XacppRequest,
) => Promise<XacppResponse>;

/** XACPP transport layer abstraction. */
export interface XacppTransport {
  /** Establish the underlying communication channel and start the accept loop. */
  connect(): Promise<void>;

  /** Tear down the underlying communication channel. */
  disconnect(): Promise<void>;

  /**
   * Send request payload and wait for response.
   *
   * Transport auto-assigns id, packs envelope, serializes and sends, registers pending, waits for response, unpacks and returns payload.
   * Caller can skip await if response is not needed.
   */
  send(sessionId: string | null, payload: XacppRequest): Promise<XacppResponse>;

  /**
   * Register request callback (unified for Command and Event).
   *
   * Must be called before `connect`, otherwise throws XacppError(AlreadyConnected).
   * When handler returns `Ok`, Transport auto-packs into same-id envelope and sends back;
   * when handler throws, Transport auto-constructs an Error response and sends back.
   */
  onRequest(handler: RequestHandler): void;
}
