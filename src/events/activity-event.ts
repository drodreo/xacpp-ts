import type { XacppEvent } from "./xacpp_event";
import type { ActivityRef } from "../activity-ref";

/** Activity-scoped event envelope. */
export interface XacppActivityEvent {
  activity: ActivityRef;
  event: XacppEvent;
}

/** Convenience constructor. */
export function newActivityEvent(activity: string, event: XacppEvent): XacppActivityEvent {
  return { activity: { id: activity }, event };
}
