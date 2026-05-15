/**
 * XACPP protocol messages — envelope layer design.
 *
 * Wire messages are uniformly `XacppEnvelope`, divided into two layers:
 *
 * - **Envelope layer**: `type` (routing) + `id` (correlation) + `payload` (business content)
 * - **Payload layer**: `XacppRequest` / `XacppResponse`
 *
 * Transport handles envelope packing/unpacking and id correlation; upper layers only operate on payloads.
 *
 * ## JSON format examples
 *
 * ```json
 * Request (Command): {"id":"r1","type":"request","payload":{"kind":"command","payload":{"establish":{"credentials":null}}}}
 * Response (Established): {"id":"r1","type":"response","payload":{"kind":"established","sessionId":"s1"}}
 * ```
 */

import type { XacppCommand } from "./commands";
import type {
  ActionResponse,
  QuestionResponse,
  SensitiveInfoOperationResponse,
  XacppActivityEvent,
} from "./events";

// ---- Protocol errors ----

/** XACPP error. */
export class XacppError extends Error {
  /** Machine-readable error code. */
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "XacppError";
    this.code = code;
  }

  /** Connection error: not connected. */
  static notConnected(): XacppError {
    return new XacppError("not_connected", "not connected");
  }

  /** Connection error: already connected. */
  static alreadyConnected(): XacppError {
    return new XacppError("already_connected", "already connected");
  }

  /** Connection error: connection closed. */
  static closed(): XacppError {
    return new XacppError("closed", "connection closed");
  }

  /** Processing error: no handler registered. */
  static noHandler(): XacppError {
    return new XacppError("no_handler", "no handler registered");
  }

  /** Processing error: handler internal error. */
  static internal(message: string): XacppError {
    return new XacppError("internal_error", `internal error: ${message}`);
  }

  /** Application-layer custom error. */
  static application(code: string, message: string): XacppError {
    return new XacppError(code, message);
  }

  /** Processing error: request payload unparseable. */
  static invalidRequest(message: string): XacppError {
    return new XacppError("invalid_request", `invalid request: ${message}`);
  }

  /** Handshake rejected. */
  static establishReject(reason: string): XacppError {
    return new XacppError("establish_rejected", `establish rejected: ${reason}`);
  }
}

// ---- Payload types ----

/** Request payload. Accepted by Transport's `send` method. */
export type XacppRequest =
  | { kind: "command"; payload: XacppCommand }
  | { kind: "event"; payload: XacppActivityEvent };

/** Response payload. Returned by Transport's `send` method. */
export type XacppResponse =
  /** Handshake success: issues session identifier and credentials. */
  | { kind: "established"; sessionId: string; credentials: string }
  /** Challenge issued during first-time establishment. */
  | { kind: "establish_prepare"; challenge: string }
  /** Handshake rejected. */
  | { kind: "establish_reject"; reason: string }
  /** Tool call authorization response. */
  | { kind: "action"; requestId: string } & ActionResponse
  /** User question response. */
  | { kind: "question"; requestId: string } & QuestionResponse
  /** Sensitive info operation response (flatten: response fields spread to top level). */
  | { kind: "sensitive_info_operation"; requestId: string } & SensitiveInfoOperationResponse
  /** Activity created. */
  | { kind: "activity_created"; activity: string; agent: string; title?: string }
  /** Generic acknowledge: request processed successfully, no data returned. */
  | { kind: "acknowledge" }
  /** Processing failure. */
  | { kind: "error"; code: string; message: string };

// ---- Envelope types ----

/** Wire message. Envelope layer handles routing. */
export type XacppEnvelope =
  | { type: "request"; id: string; session_id?: string; payload: XacppRequest }
  | { type: "response"; id: string; session_id?: string; payload: XacppResponse };
