/**
 * XACPP Capabilities — declares what a peer can handle and what it may emit.
 */

/**
 * The only fetch-extension version currently defined (SPEC §8.7). A side
 * declares `fetchExt: FETCH_EXT_VERSION` to state "I support fetch at this
 * version"; the declaration is the declaring side's own decision — the
 * library never derives it from responder assembly.
 */
export const FETCH_EXT_VERSION = 1;

/** Capabilities declared by one side during Negotiate. */
export interface Capabilities {
  /** JSON Schemas for commands this side can handle (tool-compatible). */
  commands?: Record<string, unknown>[];
  /** JSON Schemas for events this side may emit (produce). */
  produceEvents?: Record<string, unknown>[];
  /** JSON Schemas for events this side can receive (accept). */
  acceptEvents?: Record<string, unknown>[];
  /**
   * Fetch extension version (§8.7) this side supports. Absence means no
   * fetch support: accessing this side's fetch functionality MUST fail.
   * Declaration is independent of responder assembly — it is a statement
   * of protocol-level support, decided by the declaring application.
   */
  fetchExt?: number;
}

/** Negotiate 完成后的生效能力（应用层可见）。 */
export interface EffectiveCapabilities {
  /** 对端能处理的命令完整 JSON Schema（来自 remote.commands）。 */
  remoteCommands: Record<string, unknown>[];
  /** 我能发给对端的事件名列表（local.produceEvents ∩ remote.acceptEvents）。 */
  emitEvents: string[];
  /**
   * 对端声明的 fetchExt 版本（未声明 = undefined，即对端不支持 fetch）。
   * 这是 wire 上的事实记录，不是 UNKNOWN 状态、不选择 legacy 回退。
   */
  remoteFetchExt?: number;
  /**
   * 协商后的 fetch 事实：本端与对端都声明 fetchExt === FETCH_EXT_VERSION
   * 时为 true，其余情况为 false。unsupported 版本只禁用 fetch，
   * 不阻断 negotiate、不影响其他命令。
   */
  fetchEnabled: boolean;
}

/** 从 local 和 remote capabilities 计算有效能力。 */
export function computeEffectiveCapabilities(local: Capabilities, remote: Capabilities): EffectiveCapabilities {
  const remoteCommands = remote.commands ?? [];
  const localProduce = extractNames(local.produceEvents);
  const remoteAccept = extractNames(remote.acceptEvents);
  const emitEvents = localProduce.filter(name => remoteAccept.includes(name));
  // fetch is enabled only when BOTH sides declared the currently-defined
  // version. A declared-but-unsupported version (or absence) on either side
  // disables fetch; it never fails the negotiation itself (SPEC §8.7).
  const remoteFetchExt = remote.fetchExt;
  const fetchEnabled = local.fetchExt === FETCH_EXT_VERSION && remoteFetchExt === FETCH_EXT_VERSION;
  return { remoteCommands, emitEvents, remoteFetchExt, fetchEnabled };
}

function extractNames(schemas?: Record<string, unknown>[]): string[] {
  const names: string[] = [];
  for (const schema of schemas ?? []) {
    const name = schema.name;
    // The carrier is peer-supplied JSON: only a string name is a name.
    if (typeof name === "string" && name.length > 0) names.push(name);
  }
  return names;
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
  // Exemption (unchecked cast): the carrier is peer-supplied JSON with no
  // fixed shape — TS cannot prove the overlap, so the typed view needs the
  // `unknown` bridge. The raw object is returned as-is (unknown fields
  // included); `name` presence is the caller's contract duty.
  return value as unknown as CommandDeclaration; // unchecked cast: untyped wire carrier (see above)
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
