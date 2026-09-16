/**
 * Lifecycle command payloads.
 *
 * The seven lifecycle commands (new_activity / last_activity / switch_activity /
 * list_activity / invoke_activity / cancel_activity / compact_activity) are
 * business commands sent as Generic commands (`genericCommand(name, arguments)`).
 * These types are the serialization targets for their `arguments` and for
 * response `data`.
 *
 * Wire contract:
 * - Envelope `activity` = source (the activity a command originates from).
 * - Payload `activity` = operation target (business parameter, plain string).
 * - Response name mapping:
 *   - `activity_ready` → data is `ActivityInfo` (resp-only; no event-side form)
 *   - `activity_not_found` → data is an empty object
 *   - `available_activities` → data is `AvailableActivitiesResponse`
 *   - invoke / cancel / compact → `acknowledge` (see `message.ts`)
 */

import type { ContentPart } from "../events/content";
import type { ActivityInfo } from "../message";

// ---- Request payloads ----

/** new_activity: create a new activity. */
export interface NewActivityPayload {
  title?: string;
}

/** last_activity: resume the most recent activity. No fields. */
export type LastActivityPayload = Record<string, never>;

/** switch_activity: switch to an existing activity. */
export interface SwitchActivityPayload {
  activity: string;
}

/** list_activity: list activities with optional query filter and paging. */
export interface ListActivityPayload {
  query?: string;
  pageNum?: number;
  pageSize?: number;
}

/** invoke_activity: send messages to an activity. */
export interface InvokeActivityPayload {
  activity: string;
  messages: ContentPart[];
}

/** cancel_activity: cancel a running activity. */
export interface CancelActivityPayload {
  activity: string;
  reason?: string;
}

/** compact_activity: compact an activity's context. */
export interface CompactActivityPayload {
  activity: string;
}

// ---- Response payloads ----

/** available_activities response data. */
export interface AvailableActivitiesResponse {
  total: number;
  activities: ActivityInfo[];
}
