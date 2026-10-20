/**
 * XACPP fetch kernel — stateless resource pull (SPEC §8).
 *
 * The kernel is a *convention*, not a state machine: one side sends a
 * `fetch{uri}` command, the other answers `fetch_ready{txid,sha256,
 * sizeBytes,mime}` (or a registered error) and then streams the bytes as
 * an ordinary binary transaction whose id IS the `txid`. Everything after
 * the metadata round-trip — buffering, disk writes, integrity checks,
 * progress — is application-layer business (SPEC §8.6 firewall); who
 * answers the commands and how is the extension layer's business.
 *
 * Wire (byte-identical with xacpp-rs, SPEC §8.2/§8.3):
 *   command:  { "fetch": { "uri": "..." } }
 *   response: { "kind": "fetch_ready", "txid": "<32 hex>", "sha256": "<64 hex>",
 *               "sizeBytes": N, "mime": "..." }
 *   failure:  the generic Error response surface with a registered code
 */

import { XacppError } from "./message";

// ---- Error code registry (both libraries agree on these three) ----

/** uri does not exist / resource already cleaned up (handler-declared). */
export const FETCH_NOT_FOUND = "fetch_not_found";
/** uri outside the servable scope / pause-resume not implemented (handler-declared). */
export const FETCH_REFUSED = "fetch_refused";
/** fetch unsupported on this end (no fetch responder loaded). */
export const FETCH_UNAVAILABLE = "fetch_unavailable";

/** Convenience: handler-side "resource does not exist" rejection. */
export function fetchNotFound(message: string): XacppError {
  return new XacppError(FETCH_NOT_FOUND, message);
}

/** Convenience: handler-side "uri outside the servable scope" rejection. */
export function fetchRefused(message: string): XacppError {
  return new XacppError(FETCH_REFUSED, message);
}

/**
 * Metadata of a successful fetch round-trip (the `fetch_ready` response).
 * `txid` is the binary transaction id the bytes will travel under — claim
 * the stream through the binary receive path. The kernel resolves here:
 * the bytes themselves are the application's to receive.
 */
export interface FetchReady {
  /** 32 lowercase hex chars. */
  txid: string;
  /** Serving side's self-computed digest of the bytes it commits to send. */
  sha256: string;
  sizeBytes: number;
  mime: string;
}
