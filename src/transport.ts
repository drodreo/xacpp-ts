/**
 * XACPP Transport abstraction.
 *
 * ## Responsibility
 *
 * Transport unifies the underlying communication channel (stdio / TCP / WebSocket) into `send` semantics:
 *
 * - **send**: send request payload, wait for response payload to return. Caller can spawn to background if response is not needed.
 * - **sendFaf**: send request payload fire-and-forget — returns as soon as the frame is queued, never waits for the response.
 * - **accept**: listen for peer requests, dispatch via `onRequest` callback,
 *   which receives session_id + payload; the return value is automatically sent back as a response.
 *
 * Transport internals:
 *
 * - **Ordered egress**: all outbound frames (requests, events, responses) go through a single
 *   FIFO egress queue — one drain loop per connection owns the write half and writes frames
 *   in submission order (see `egress` module)
 * - **Envelope packing/unpacking**: auto-assign request id, pack into envelope for sending, unpack to return payload
 * - **Request-response correlation**: match pending sends by id upon receiving a Response
 * - **Encoding/decoding**: serialization / deserialization (JSONL)
 * - **Connection management**: establish / tear down underlying communication channel
 *
 * ## Layer boundary
 *
 * - **Transport to upper layer**: exposes `send` / `sendFaf` / `onRequest` / `onClosed`,
 *   does not expose raw byte send/receive, envelope id, or encoding/decoding details
 * - **Peer to upper layer**: exposes typed `requestCommand` / `requestEvent` / `sendEvent`
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
   * Send request payload fire-and-forget: returns as soon as the frame is
   * queued for writing, never waits for the response.
   *
   * Ordering guarantee: the frame is written after all previously queued
   * outbound frames and before all subsequently queued ones (single FIFO
   * egress per connection). Throws = connection already known dead.
   */
  sendFaf(sessionId: string | null, payload: XacppRequest): Promise<void>;

  /**
   * Subscribe to connection-close notification.
   *
   * The listener fires once the connection is known dead (peer disconnect
   * observed by the reader, write failure in the egress drain, or local
   * `disconnect`). It fires immediately if the connection is already closed
   * before the subscription. Returns an unsubscribe function.
   */
  onClosed(listener: () => void): () => void;

  /**
   * Register request callback (unified for Command and Event).
   *
   * Must be called before `connect`, otherwise throws XacppError(AlreadyConnected).
   * When handler returns `Ok`, Transport auto-packs into same-id envelope and sends back;
   * when handler throws, Transport auto-constructs an Error response and sends back.
   */
  onRequest(handler: RequestHandler): void;
}
