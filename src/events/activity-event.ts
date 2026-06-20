import type { XacppEvent } from "./xacpp_event";

/** Activity-scoped event envelope. */
export interface XacppActivityEvent {
  activity: string;
  event: XacppEvent;
}

/** Convenience constructor. */
export function newActivityEvent(activity: string, event: XacppEvent): XacppActivityEvent {
  return { activity, event };
}
