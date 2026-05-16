/**
 * ACPP protocol event union type.
 *
 * Standardized event stream between the initiator and the peer.
 * The only terminal signal is `complete`; all other events carry no implicit termination semantics.
 */

import type { ActivityInfo } from "../message";
import type { ContentPart } from "./content";
import type { ActionRequestEvent, NotifyEvent, QuestionEvent, SensitiveInfoOperationEvent } from "./interaction";
import type { ActivityStartEvent, ContentDeltaEvent, ContentPartEvent, SecurityAlertEvent, ToolResultEvent, ToolUseEvent, TraceableEvent } from "./payload";
import type { TokenUsage, UploadEvent } from "./upload";

// ---- Content output ----

/** Unified multimodal content delta event. */
export interface ContentDeltaXacppEvent {
  type: "content_delta";
  round: string;
  pair: string;
  payload: ContentPart;
}

/** Unified multimodal content part event. */
export interface ContentPartXacppEvent {
  type: "content_part";
  round: string;
  pair: string;
  payload: ContentPart;
}

/** Thinking text output (delta). */
export interface ThinkXacppEvent {
  type: "think";
  content: string;
}

// ---- System signals ----

/** System info output. */
export interface InfoXacppEvent extends TraceableEvent {
  type: "info";
}

/** System warning output. */
export interface WarnXacppEvent extends TraceableEvent {
  type: "warn";
}

/** Structured error event (non-terminal). */
export interface ErrorXacppEvent extends TraceableEvent {
  type: "error";
}

// ---- Interaction (request-response pattern) ----

/** Tool call authorization request. */
export interface ActionRequestXacppEvent extends ActionRequestEvent {
  type: "action_request";
}

/** User notification (one-way push). */
export interface NotifyXacppEvent extends NotifyEvent {
  type: "notify";
}

/** User question. */
export interface QuestionXacppEvent extends QuestionEvent {
  type: "question";
}

/** Sensitive information operation request. */
export interface SensitiveInfoOperationXacppEvent extends SensitiveInfoOperationEvent {
  type: "sensitive_info_operation";
}

// ---- Activity lifecycle ----

/** SubActivity waiting for command. */
export interface WaitingCommandXacppEvent {
  type: "waiting_command";
}

/** SubActivity started. */
export interface ActivityStartXacppEvent extends ActivityStartEvent {
  type: "activity_start";
}

/** Activity metadata updated. */
export interface ActivityUpdatesXacppEvent extends ActivityInfo {
  type: "activity_updates";
}

/** SubActivity completed. */
export interface ActivityDoneXacppEvent {
  type: "activity_done";
  activity: string;
}

/** SubActivity aborted. */
export interface ActivityAbortedXacppEvent {
  type: "activity_aborted";
  activity: string;
  reason: string;
}

// ---- Tool invocation ----

/** Tool call started. */
export interface ToolUseXacppEvent extends ToolUseEvent {
  type: "tool_use";
}

/** Tool call completed. */
export interface ToolResultXacppEvent extends ToolResultEvent {
  type: "tool_result";
}

// ---- Security ----

/** Security alert. */
export interface SecurityAlertXacppEvent extends SecurityAlertEvent {
  type: "security_alert";
}

// ---- Upload ----

/** File upload event. */
export type UploadXacppEvent = UploadEvent & { type: "upload" };

// ---- Engine signals ----

/** ReAct loop single iteration completed. */
export interface PairCompleteXacppEvent {
  type: "pair_complete";
  contextWindow: number;
  tokenUsage: TokenUsage;
}

/** The sole terminal signal for an invoke. */
export interface CompleteXacppEvent {
  type: "complete";
  assistantReply: ContentPart[];
}

/** ACPP protocol event union type. */
export type XacppEvent =
  | ContentDeltaXacppEvent
  | ContentPartXacppEvent
  | ThinkXacppEvent
  | InfoXacppEvent
  | WarnXacppEvent
  | ErrorXacppEvent
  | ActionRequestXacppEvent
  | NotifyXacppEvent
  | QuestionXacppEvent
  | SensitiveInfoOperationXacppEvent
  | WaitingCommandXacppEvent
  | ActivityStartXacppEvent
  | ActivityUpdatesXacppEvent
  | ActivityDoneXacppEvent
  | ActivityAbortedXacppEvent
  | ToolUseXacppEvent
  | ToolResultXacppEvent
  | SecurityAlertXacppEvent
  | UploadXacppEvent
  | PairCompleteXacppEvent
  | CompleteXacppEvent;
