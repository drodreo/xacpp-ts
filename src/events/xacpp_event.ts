/**
 * XACPP Event type (generic).
 *
 * Events are one-way notifications. The transport auto-acknowledges receipt.
 * All business events use a generic `{ name, data }` structure.
 */

/** XACPP protocol event (generic). */
export interface XacppEvent {
  /** Event type identifier. */
  name: string;
  /** Event-specific JSON payload. */
  data: unknown;
}

/** Convenience constructor. */
export function newEvent(name: string, data: unknown): XacppEvent {
  return { name, data };
}
