/**
 * XACPP protocol messages — envelope layer design.
 */

import type { XacppCommand } from "./commands";
import type { Capabilities } from "./capability";
import type { XacppActivityEvent } from "./events";

// ---- Protocol errors ----

export class XacppError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "XacppError";
    this.code = code;
  }

  static notConnected(): XacppError {
    return new XacppError("not_connected", "not connected");
  }

  static alreadyConnected(): XacppError {
    return new XacppError("already_connected", "already connected");
  }

  static closed(): XacppError {
    return new XacppError("closed", "connection closed");
  }

  static noHandler(): XacppError {
    return new XacppError("no_handler", "no handler registered");
  }

  static internal(message: string): XacppError {
    return new XacppError("internal_error", `internal error: ${message}`);
  }

  static application(code: string, message: string): XacppError {
    return new XacppError(code, message);
  }

  static invalidRequest(message: string): XacppError {
    return new XacppError("invalid_request", `invalid request: ${message}`);
  }

  static establishReject(reason: string): XacppError {
    return new XacppError("establish_rejected", `establish rejected: ${reason}`);
  }

  static invalidState(message: string): XacppError {
    return new XacppError("invalid_state", `invalid state: ${message}`);
  }
}

// ---- Shared types ----

export interface ActivityInfo {
  activity: string;
  agent: string;
  title?: string;
}

// ---- Payload types ----

export type XacppRequest =
  | { kind: "command"; payload: XacppCommand }
  | { kind: "event"; payload: XacppActivityEvent };

export type XacppResponse =
  /** Protocol: capability negotiation response. */
  | { kind: "negotiated"; capabilities: Capabilities }
  /** Protocol: handshake success. */
  | { kind: "established"; sessionId: string; credentials: string }
  /** Protocol: challenge issued. */
  | { kind: "establish_prepare"; challenge: string }
  /** Protocol: handshake rejected. */
  | { kind: "establish_reject"; reason: string }
  /** Business: generic response. */
  | { kind: "generic"; name: string; data: unknown }
  /** Error. */
  | { kind: "error"; code: string; message: string };

// ---- Convenience constructors ----

export function acknowledge(): XacppResponse {
  return { kind: "generic", name: "acknowledge", data: null };
}

export function genericResponse(name: string, data: unknown): XacppResponse {
  return { kind: "generic", name, data };
}

export function errorResponse(code: string, message: string): XacppResponse {
  return { kind: "error", code, message };
}

// ---- Envelope types ----

export type XacppEnvelope =
  | { type: "request"; id: string; session_id?: string; payload: XacppRequest }
  | { type: "response"; id: string; session_id?: string; payload: XacppResponse };
