/**
 * XACPP Capabilities — declares what a peer can handle and what it may emit.
 */

/** Capabilities declared by one side during Negotiate. */
export interface Capabilities {
  /** JSON Schemas for commands this side can handle (tool-compatible). */
  commands?: Record<string, unknown>[];
  /** JSON Schemas for events this side may emit (produce). */
  produceEvents?: Record<string, unknown>[];
  /** JSON Schemas for events this side can receive (accept). */
  acceptEvents?: Record<string, unknown>[];
}

/** Negotiate 完成后的生效能力（应用层可见）。 */
export interface EffectiveCapabilities {
  /** 对端能处理的命令完整 JSON Schema（来自 remote.commands）。 */
  remoteCommands: Record<string, unknown>[];
  /** 我能发给对端的事件名列表（local.produceEvents ∩ remote.acceptEvents）。 */
  emitEvents: string[];
}

/** 从 local 和 remote capabilities 计算有效能力。 */
export function computeEffectiveCapabilities(local: Capabilities, remote: Capabilities): EffectiveCapabilities {
  const remoteCommands = remote.commands ?? [];
  const localProduce = extractNames(local.produceEvents);
  const remoteAccept = extractNames(remote.acceptEvents);
  const emitEvents = localProduce.filter(name => remoteAccept.includes(name));
  return { remoteCommands, emitEvents };
}

function extractNames(schemas?: Record<string, unknown>[]): string[] {
  return (schemas ?? []).map(s => s.name as string).filter(Boolean);
}
