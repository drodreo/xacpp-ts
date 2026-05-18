/** 多模态内容基础类型。 */

/** 文件引用（协议级简化版）。 */
export interface FileRef {
  remoteUrl: string;
  localUri: string;
  mimeType: string;
  sizeBytes: number;
}

export interface TextPart {
  type: "text";
  text: string;
  partId?: string;
}

export interface ImagePart {
  type: "image";
  source: FileRef;
  detail?: string;
  width?: number;
  height?: number;
  partId?: string;
}

export interface AudioPart {
  type: "audio";
  source: FileRef;
  sampleRate?: number;
  channels?: number;
  durationMs?: number;
  partId?: string;
}

export interface VideoPart {
  type: "video";
  source: FileRef;
  durationMs?: number;
  fps?: number;
  width?: number;
  height?: number;
  partId?: string;
}

/** File content part. */
export interface FilePart {
  type: "file";
  source: FileRef;
  partId?: string;
}

/** 统一多模态内容分片。 */
export type ContentPart = TextPart | ImagePart | AudioPart | VideoPart | FilePart;
