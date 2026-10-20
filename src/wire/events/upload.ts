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
}
