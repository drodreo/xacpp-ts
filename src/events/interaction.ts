/**
 * Interaction command/response payloads.
 *
 * These types serve as serialization targets for:
 * - Command `arguments` when `name` is "action_request", "question", or "sensitive_info_operation".
 * - Response `data` when `name` is "action", "question", or "sensitive_info_operation".
 *
 * The `responder` channel from previous versions has been removed.
 * Interaction requests are now Commands: the transport's request-response correlation
 * handles matching responses back to the sender.
 */

import type { AlertLevel } from "./payload";

// ---- Tool Call Authorization ----

export type ActionResponse =
  | { type: "approve" }
  | { type: "approve_always" }
  | { type: "reject"; reason: string };

export interface ActionRequestPayload {
  activity: string;
  requestId: string;
  toolName: string;
  arguments: string;
  actionId: string;
  description: string;
  alert: AlertLevel;
  intent: string;
}

// ---- Notification ----

export interface NotifyPayload {
  requestId: string;
  message: string;
}

// ---- Question ----

export type QuestionResponse =
  | { type: "answer"; content: string }
  | { type: "skip"; reason?: string };

export interface QuestionPayload {
  activity: string;
  requestId: string;
  question: string;
  options: string[];
}

// ---- Sensitive Info ----

export type SensitiveInfoType = "secret" | "env_var";

export interface SensitiveInfoItem {
  id?: string;
  key: string;
  displayText: string;
  hint: string;
  siType: SensitiveInfoType;
}

export type SensitiveInfoOperation =
  | { type: "collect"; items: SensitiveInfoItem[] }
  | { type: "delete"; items: SensitiveInfoItem[] };

export type SensitiveInfoResult =
  | { type: "provided"; key: string; value: string }
  | { type: "collect_skipped"; key: string; reason?: string }
  | { type: "deleted"; id: string }
  | { type: "delete_rejected"; id: string; reason?: string };

export interface SensitiveInfoOperationResponse {
  results: SensitiveInfoResult[];
}

export interface SensitiveInfoOperationPayload {
  activity: string;
  requestId: string;
  operation: SensitiveInfoOperation;
}
