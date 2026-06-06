/**
 * XACPP Capabilities — declares what a peer can handle and what it may emit.
 */

/** Capabilities declared by one side during Negotiate. */
export interface Capabilities {
  /** JSON Schemas for commands this side can handle (tool-compatible). */
  commands?: Record<string, unknown>[];
  /** JSON Schemas for events this side may emit (contract description). */
  events?: Record<string, unknown>[];
}
