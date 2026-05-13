/** 事件载荷类型。 */

import type { ContentPart } from "./content";

/** 内容分片事件载荷。 */
export interface ContentPartEvent {
  round: string;
  pair: string;
  payload: ContentPart;
}

/** 内容增量事件载荷。 */
export interface ContentDeltaEvent {
  round: string;
  pair: string;
  payload: ContentPart;
}

/** 可追踪事件载荷（Info/Warn/Error 共用）。 */
export interface TraceableEvent {
  title: string;
  content: string;
}

/** 授权请求 Alert 级别。 */
export type AlertLevel = "info" | "warn" | "critical";

/** 工具调用开始事件载荷。 */
export interface ToolUseEvent {
  requestId: string;
  toolName: string;
  index: number;
  arguments: string;
}

/** 工具调用结束事件载荷。 */
export interface ToolResultEvent {
  requestId: string;
  toolName: string;
  index: number;
  parts: ContentPart[];
}

/** 安全告警事件载荷。 */
export interface SecurityAlertEvent {
  eventId: string;
  toolName: string;
  alertLevel: AlertLevel;
  description: string;
  threatType?: string;
  matchedPattern?: string;
  contextSnippet?: string;
}

/** SubActivity 任务启动事件载荷。 */
export interface ActivityStartEvent {
  goal: string;
  activityId: string;
  metadata?: Record<string, string>;
}
