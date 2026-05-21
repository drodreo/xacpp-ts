/**
 * 交互事件载荷（请求-响应模式）。
 *
 * 协议层不持有通道，通过 requestId 进行请求-响应关联，
 * 响应方通过 transport 发送对应的 Response 消息。
 */

import type { AlertLevel } from "./payload";

// ---- 工具调用授权 ----

/** 工具调用授权响应。 */
export type ActionResponse =
  | { type: "approve" }
  | { type: "approve_always" }
  | { type: "reject"; reason: string };

/** 工具调用授权请求事件载荷。 */
export interface ActionRequestEvent {
  requestId: string;
  toolName: string;
  arguments: string;
  actionId: string;
  description: string;
  alert: AlertLevel;
  intent: string;
}

// ---- 通知 ----

/** 用户通知事件载荷（单向推送，不阻塞等待回复）。 */
export interface NotifyEvent {
  requestId: string;
  message: string;
}

// ---- 提问 ----

/** 用户提问响应。 */
export type QuestionResponse =
  | { type: "answer"; content: string }
  | { type: "skip"; reason?: string };

/** 用户提问事件载荷。 */
export interface QuestionEvent {
  requestId: string;
  question: string;
  options: string[];
}

// ---- 敏感信息 ----

/** 敏感信息类型。 */
export type SensitiveInfoType = "secret" | "env_var";

/** 敏感信息条目（脱敏展示）。 */
export interface SensitiveInfoItem {
  id?: string;
  key: string;
  displayText: string;
  hint: string;
  siType: SensitiveInfoType;
}

/** 敏感信息操作类型。 */
export type SensitiveInfoOperation =
  | { type: "collect"; items: SensitiveInfoItem[] }
  | { type: "delete"; items: SensitiveInfoItem[] };

/** 单个敏感信息的操作结果。 */
export type SensitiveInfoResult =
  | { type: "provided"; key: string; value: string }
  | { type: "collect_skipped"; key: string; reason?: string }
  | { type: "deleted"; id: string }
  | { type: "delete_rejected"; id: string; reason?: string };

/** 敏感信息操作响应。 */
export interface SensitiveInfoOperationResponse {
  results: SensitiveInfoResult[];
}

/** 敏感信息操作请求事件载荷。 */
export interface SensitiveInfoOperationEvent {
  requestId: string;
  operation: SensitiveInfoOperation;
}
