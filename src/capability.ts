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

// ---- Command declaration schema (typed view of `Capabilities.commands`) ----

/**
 * Command dispatch surface.
 *
 * The wire-known values are `"tool"` and `"bridge"`. Anything else (from a
 * newer peer) is kept as a plain string — never a parse failure. An absent
 * dispatcher also means bridge semantics (`undefined`).
 */
export type CommandDispatcher = "tool" | "bridge" | (string & {});

/** Evaluation policy declared alongside a command. */
export interface EvaluationPolicy {
  /** Require the model to call a specific tool. */
  requireToolCall?: RequireToolCall;
}

/** The single currently-defined evaluation entry. */
export interface RequireToolCall {
  /** Name of the tool the model must call. */
  require: string;
  /** Message used to bounce the turn back when the tool was not called. */
  onFailure: string;
}

/**
 * Typed representation of one command declaration schema (an element of
 * `Capabilities.commands` / `EffectiveCapabilities.remoteCommands`).
 *
 * Purely additive: the raw `Record<string, unknown>` carrier is unchanged. Use
 * `parseCommandDeclaration` to view a raw value in this typed form.
 *
 * Forward compatibility: unknown fields are preserved at runtime; unknown
 * `dispatcher` / `extraScopes` values are kept as plain strings.
 *
 * Wire keys are camelCase, matching the Rust side's `rename_all = "camelCase"`.
 */
export interface CommandDeclaration {
  /** Command name. Required. */
  name: string;
  /** Human-readable description. */
  description?: string;
  /** JSON Schema describing the parameters surface. */
  parameters?: Record<string, unknown>;
  /**
   * Dispatch surface. `"tool"` routes the command into the model tool surface;
   * `"bridge"` (or absent) means bridge semantics (not in the tool surface).
   */
  dispatcher?: CommandDispatcher;
  /** Evaluation policy attached by the declaring side. */
  evaluationPolicy?: EvaluationPolicy;
  /** Additional exposure scopes. Absent/empty = default conversation surface only. */
  extraScopes?: string[];
}

/**
 * Views a raw declaration value (from `remoteCommands`) as a typed
 * `CommandDeclaration`. Runtime-transparent: the original object (including
 * unknown fields) is returned as-is; this is a type-level view only. The
 * `unknown` bridge cast is intentional — the raw carrier has no fixed shape,
 * so TS cannot prove overlap; `name` presence is the caller's contract duty.
 */
export function parseCommandDeclaration(value: Record<string, unknown>): CommandDeclaration {
  return value as unknown as CommandDeclaration;
}

/**
 * True when the command should be routed into the model tool surface. Only
 * `"tool"` is tool-facing: an absent dispatcher (bridge semantics), an
 * explicit `"bridge"`, and unknown dispatcher values are all not tool-facing.
 */
export function isToolFacing(decl: CommandDeclaration): boolean {
  return decl.dispatcher === "tool";
}

/** The `requireToolCall` evaluation entry, if declared. */
export function requireToolCall(decl: CommandDeclaration): RequireToolCall | undefined {
  return decl.evaluationPolicy?.requireToolCall;
}

/** True when the command is additionally exposed in the compact scope. */
export function hasCompactScope(decl: CommandDeclaration): boolean {
  return (decl.extraScopes ?? []).includes("compact");
}
