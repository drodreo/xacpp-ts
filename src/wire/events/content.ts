/** 多模态内容基础类型。 */

/** 文件引用（协议级简化版）。 */
export interface FileRef {
  remoteUrl: string;
  localUri: string;
  /** Remote URL expiration time (UTC ISO 8601). */
  remoteExpiresAt?: string;
  mimeType: string;
  /** Whether the file needs to be organized from a temp directory to a permanent location. */
  requireOrganized?: boolean;
  sizeBytes: number;
  /** SHA-256 hash of the file content. */
  sha256?: string;
}

export interface TextPart {
  type: "text";
  text: string;
  partId?: string;
  /** Content-part identity fact (x-agent LlmPartType variant name, camelCase); always present, "default" for unmarked parts. */
  partType: string;
}

export interface ImagePart {
  type: "image";
  source: FileRef;
  detail?: string;
  width?: number;
  height?: number;
  partId?: string;
  /** Content-part identity fact (x-agent LlmPartType variant name, camelCase); always present, "default" for unmarked parts. */
  partType: string;
}

export interface AudioPart {
  type: "audio";
  source: FileRef;
  sampleRate?: number;
  channels?: number;
  durationMs?: number;
  partId?: string;
  /** Content-part identity fact (x-agent LlmPartType variant name, camelCase); always present, "default" for unmarked parts. */
  partType: string;
}

export interface VideoPart {
  type: "video";
  source: FileRef;
  durationMs?: number;
  fps?: number;
  width?: number;
  height?: number;
  partId?: string;
  /** Content-part identity fact (x-agent LlmPartType variant name, camelCase); always present, "default" for unmarked parts. */
  partType: string;
}

/** File content part. */
export interface FilePart {
  type: "file";
  source: FileRef;
  partId?: string;
  /** Content-part identity fact (x-agent LlmPartType variant name, camelCase); always present, "default" for unmarked parts. */
  partType: string;
}

/** 统一多模态内容分片。 */
export type ContentPart = TextPart | ImagePart | AudioPart | VideoPart | FilePart;
