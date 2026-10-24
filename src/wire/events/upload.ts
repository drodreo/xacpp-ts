/** 文件上传与令牌用量。 */

import type { FileRef } from "./content";

/** 文件上传事件。 */
export type UploadEvent =
  | { upload_event: "progress"; name: string; uploaded: number; total: number }
  | { upload_event: "completed"; name: string; mediaSource: FileRef }
  | { upload_event: "error"; name: string; error: string };

/** 令牌使用情况。 */
export interface TokenUsage {
  id: string;
  message: string;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  /** 缓存命中 token 数（本轮从 provider 端 prompt cache 命中读取的输入 token 数）；缺省 = provider 未报告（undefined），0 = 显式零。 */
  cacheHitTokens?: number;
}
