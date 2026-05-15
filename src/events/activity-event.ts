import type { XacppEvent } from "./xacpp_event";

/**
 * Activity-scoped event envelope.
 *
 * Wraps an `XacppEvent` with an `activity` so the consumer can
 * identify which activity within a session produced the event.
 */
export interface XacppActivityEvent {
  activity: string;
  event: XacppEvent;
}
