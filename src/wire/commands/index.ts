/**
 * XACPP protocol command types.
 *
 * Commands are request-response: the sender always expects a Response.
 *
 * Handshake commands (Negotiate/Establish/EstablishConfirm) are handled by
 * the Peer layer. The fetch family (SPEC §8) and business commands route to
 * the session's consumer handler — who serves them is the extension layer's
 * business (SPEC §8.6).
 */

import type { Capabilities } from "../capability";
import type { ActivityRef } from "../activity-ref";

export * from "./lifecycle";

/** XACPP protocol command. */
export type XacppCommand =
  /** Negotiate capabilities before session establishment. */
  | { negotiate: { capabilities: Capabilities } }
  /** Establish logical session. */
  | { establish: { credentials?: string } }
  /** Confirm establishment after challenge verification. */
  | "establish_confirm"
  /**
   * Fetch a resource by uri (SPEC §8.2). The responder answers
   * `fetch_ready` and streams the bytes as a DATA_BINARY transaction whose
   * id IS the response's `txid`. Serving is the consumer handler's business
   * (SPEC §8.6: who implements it and how is the extension layer's).
   * Wire: `{ "fetch": { "uri": "..." } }`.
   */
  | { fetch: { uri: string } }
  /**
   * Stream-start authorization for the named fetch transaction (SPEC §8.3).
   * The requester confirms readiness to receive; the holder MUST NOT emit
   * any frame of the transaction before this command. Idempotent: a repeated
   * `fetch_begin` for an in-flight transaction answers success; an unknown
   * or terminal txid answers `fetch_refused`. Wire:
   * `{ "fetch_begin": { "txid": "..." } }`.
   */
  | { fetch_begin: { txid: string } }
  /** Stop and release the named fetch transaction (SPEC §8.3; idempotent). */
  | { fetch_abort: { txid: string } }
  /** Suspend transmission of the named fetch transaction (SPEC §8.3). */
  | { fetch_pause: { txid: string } }
  /** Continue transmission of the named fetch transaction (SPEC §8.3). */
  | { fetch_resume: { txid: string } }
  /**
   * Generic business command.
   * `activity` is optional; the protocol layer does not enforce it —
   * validation belongs to the command implementation.
   */
  | { generic: { name: string; arguments: unknown; activity?: ActivityRef } };

/** Convenience constructor for generic business commands. */
export function genericCommand(name: string, args: unknown, activity?: ActivityRef): XacppCommand {
  if (activity !== undefined && activity !== null) {
    return { generic: { name, arguments: args, activity } };
  }
  return { generic: { name, arguments: args } };
}

/** Extracts the command name for capability-matching purposes. */
export function commandName(cmd: XacppCommand): string {
  if (typeof cmd === "string") return cmd;
  if ("negotiate" in cmd) return "negotiate";
  if ("establish" in cmd) return "establish";
  if ("fetch" in cmd) return "fetch";
  if ("fetch_begin" in cmd) return "fetch_begin";
  if ("fetch_abort" in cmd) return "fetch_abort";
  if ("fetch_pause" in cmd) return "fetch_pause";
  if ("fetch_resume" in cmd) return "fetch_resume";
  if ("generic" in cmd) return cmd.generic.name;
  return "unknown";
}

/**
 * True for the fetch command family (SPEC §8.2): fetch / fetch_begin /
 * fetch_pause / fetch_resume / fetch_abort. The peer gates these with
 * `fetch_unavailable` when the session's handler is not a loaded fetch
 * responder ({@link XacppSessionHandler.isFetchResponder}).
 */
export function isFetchFamily(cmd: XacppCommand): boolean {
  return (
    typeof cmd !== "string" &&
    ("fetch" in cmd ||
      "fetch_begin" in cmd ||
      "fetch_abort" in cmd ||
      "fetch_pause" in cmd ||
      "fetch_resume" in cmd)
  );
}
