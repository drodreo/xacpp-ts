/** 事件载荷类型。 */

import type { ContentPart } from "./content";
import type { TokenUsage } from "./upload";

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
  intent: string;
}

/** 工具调用生命周期状态。 */
export type ToolResultStatus = "completed" | "running" | "cancelled" | "error";

/** 工具调用结束事件载荷。 */
export interface ToolResultEvent {
  requestId: string;
  toolName: string;
  index: number;
  parts: ContentPart[];
  /** 该工具结果经活动 auto 授权模式自批产生（非自批 = false）。 */
  auto: boolean;
  /** 工具调用生命周期状态：completed（成功终结）/ running（后台仍在执行）/ cancelled（完成前被取消）/ error（失败终结）；缺省 = unknown，不得推断为任何具体状态。 */
  status?: ToolResultStatus;
  /** 工具在本结果之后仍在后台继续运行（持久进程/超时转后台）；缺省 = unknown。 */
  isBackground?: boolean;
  /** 本结果为错误结果（工具异常；与 status 正交；非错误 = false）。 */
  isError: boolean;
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
  activity: string;
  metadata?: Record<string, string>;
}

/** 活动运行选定的模型档位。 */
export type ModelTier = "default" | "lite" | "expert";

/** Pair 完成事件载荷。 */
export interface PairCompleteEvent {
  /** 本次完成的 loop 迭代标识。 */
  pair: string;
  /** 服务本轮的模型上下文窗口大小（token 数）。 */
  contextWindow: number;
  /** 本轮 LLM 调用的 token 用量。 */
  tokenUsage: TokenUsage;
  /** 本轮模型档位（invoke 选定，run 开始时定格）；缺省 = unknown。 */
  tier?: ModelTier;
  /** 模型显示名；缺省 = unknown。 */
  model?: string;
}

/** 完成事件载荷。 */
export interface CompleteEvent {
  /** 引擎运行唯一标识（turn 级）。 */
  round: string;
  /** 最终助手回复内容分片。 */
  assistantReply: ContentPart[];
  /** 中止原因；仅异常中止时出现；缺省（undefined）不用于推断 canceled。 */
  abortReason?: string;
  /** 是否被用户取消（正常完成 = false）。 */
  canceled: boolean;
}
