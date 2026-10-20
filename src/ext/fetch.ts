/**
 * XACPP fetch extension — assembly over the kernel (SPEC §8 contract
 * firewall).
 *
 * The kernel (SPEC §8) provides the fetch wire plus the binary stream
 * primitives. This module is convenience glue only:
 *
 * - a side: {@link FetchPuller} — fetch round-trip + begin authorization +
 *   txid dispatch over the session's single stream slot; {@link pullToVec}
 *   for the read-everything-and-verify convenience.
 * - b side: {@link FetchService} — command routing + begin gate + idempotent
 *   tables + serving lifecycle enforcement.
 * - canned providers: {@link bytesProvider} / {@link fileProvider}.
 * - free-function companions: {@link fetchVerify} / {@link fetchPause} /
 *   {@link fetchResume} / {@link fetchAbort}.
 *
 * The data plane stays on the kernel's bare objects: `pull` delivers a naked
 * {@link BinaryStream}, providers write a real {@link BinarySink}. The
 * extension never wraps stream objects (no FetchClient/FetchServer; the
 * state machine carrier IS the stream object). Consumers run with zero
 * reconnection logic — suspension absorbs disconnects (the session outlives
 * connections; streams end only on completion or cancellation).
 */

import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";

import type { XacppSession } from "../session/session";
import type { XacppSessionHandler } from "../session/handler";
import type { XacppCommand } from "../wire/commands";
import type { XacppActivityEvent } from "../wire/events";
import { XacppError, acknowledge, errorResponse } from "../wire/message";
import type { XacppResponse } from "../wire/message";
import { FETCH_NOT_FOUND, FETCH_REFUSED, FETCH_UNAVAILABLE } from "../wire/fetch";
import type { FetchReady } from "../wire/fetch";
import type { BinarySink, BinaryStream, StreamEndReason } from "../transfer";
import { newTransactionId } from "../wire/frame";

/** Serving chunk size (canned providers). */
const CHUNK_BYTES = 64 * 1024;

/** fetch companion commands carry 32-lowercase-hex transaction ids. */
const TXID_PATTERN = /^[0-9a-f]{32}$/;

/** Wire error codes the peer may answer fetch commands with. */
const FETCH_ERROR_CODES: ReadonlySet<string> = new Set([
  FETCH_NOT_FOUND,
  FETCH_REFUSED,
  FETCH_UNAVAILABLE,
]);

function sha256Hex(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

// ---- FetchError ----

export type FetchErrorKind = "refused" | "transport" | "integrity_mismatch" | "duplicate_txid" | "incomplete";

/**
 * Extension-owned error type. Never enters the wire
 * error-code registry: `refused` carries the peer's fetch_* code verbatim,
 * `transport` wraps the session/connection error, `integrity_mismatch`
 * compares the declared digest against the consumer-computed one.
 */
export class FetchError extends Error {
  readonly kind: FetchErrorKind;
  /** `refused`: the peer's fetch_* wire error code. */
  readonly code?: string;
  /** `transport`: the underlying session/connection error. */
  readonly error?: unknown;
  /** `integrity_mismatch`: declared digest vs consumer-computed digest. */
  readonly expected?: string;
  readonly actual?: string;
  readonly received?: number;
  /** `incomplete`: bytes the peer still owed (the declared resource size). */
  readonly expectedBytes?: number;

  private constructor(
    kind: FetchErrorKind,
    message: string,
    fields: Pick<FetchError, "code" | "error" | "expected" | "actual" | "received" | "expectedBytes"> = {},
  ) {
    super(message);
    this.name = "FetchError";
    this.kind = kind;
    Object.assign(this, fields);
  }

  /** Peer declined a fetch command with a registered fetch_* code. */
  static refused(code: string, message: string): FetchError {
    return new FetchError("refused", `fetch refused (${code}): ${message}`, { code });
  }

  /** Session/connection-level failure (not a fetch_* wire code). */
  static transport(error: unknown): FetchError {
    const message = error instanceof Error ? error.message : String(error);
    return new FetchError("transport", `fetch transport error: ${message}`, { error });
  }

  /** Consumer-computed digest (or received length) disagrees with the meta. */
  static integrityMismatch(expected: string, actual: string, received: number): FetchError {
    return new FetchError(
      "integrity_mismatch",
      `fetch integrity mismatch: expected sha256 ${expected}, got ${actual} (${received} bytes received)`,
      { expected, actual, received },
    );
  }

  /** Two concurrent pulls raced onto the same transaction id (pathological). */
  static duplicateTxid(): FetchError {
    return new FetchError("duplicate_txid", "duplicate fetch transaction id");
  }

  /**
   * The stream ended before the declared bytes arrived: the peer
   * announced the end of the transaction while bytes were still missing
   * (a short write). Explicit failure — never a digest verdict, never a
   * hang.
   */
  static incomplete(received: number, expectedBytes: number): FetchError {
    return new FetchError(
      "incomplete",
      `fetch stream incomplete: received ${received} of ${expectedBytes} declared bytes`,
      { received, expectedBytes },
    );
  }
}

function toFetchError(e: unknown): FetchError {
  if (e instanceof FetchError) return e;
  if (e instanceof XacppError) {
    if (FETCH_ERROR_CODES.has(e.code)) return FetchError.refused(e.code, e.message);
    return FetchError.transport(e);
  }
  return FetchError.transport(
    e instanceof Error ? new XacppError("internal_error", e.message) : new XacppError("internal_error", String(e)),
  );
}

// ---- a side: FetchPuller (dispatcher only — never in the data plane) ----

/** A promise together with its settlement functions. */
interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
}

/**
 * Create a deferred promise. The settlement functions are installed
 * synchronously by the Promise constructor; the initializers only guard
 * against a use before that (they throw instead of silently no-op).
 */
function deferred<T>(): Deferred<T> {
  let resolve: (value: T) => void = () => {
    throw new Error("deferred promise settled before it was initialized");
  };
  let reject: (reason: unknown) => void = () => {
    throw new Error("deferred promise settled before it was initialized");
  };
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

interface PendingEntry {
  meta: FetchReady;
  settled: boolean;
  resolve: (stream: BinaryStream) => void;
  reject: (err: FetchError) => void;
}

/**
 * Pull-side assembler.
 *
 * Exists because the kernel's `onBinaryStream` is a single registration
 * slot: the txid dispatch state needs an owner. The puller only performs
 * the fetch round-trip, the begin authorization, and the txid routing —
 * the data plane delivers the bare kernel {@link BinaryStream}.
 *
 * Dispatch semantics (aligned with xacpp-rs ext/fetch): a stream whose
 * txid no pull is waiting on is logged and dropped — a binary stream with
 * no protocol identity is either a peer violation or a late replay of a
 * terminal transaction; there is no passthrough escape hatch.
 *
 * Owning the slot means: after constructing a FetchPuller on a session,
 * do NOT also assign `session.onBinaryStream` elsewhere.
 */
export class FetchPuller {
  private readonly pending = new Map<string, PendingEntry>();

  constructor(private readonly session: XacppSession) {
    session.onBinaryStream((stream) => {
      const entry = this.pending.get(stream.transactionId);
      if (!entry) {
        // Unclaimed stream: no pending pull for this txid → log + drop.
        console.warn(`[xacpp:ext] dropping unclaimed binary stream ${stream.transactionId}`);
        return;
      }
      this.pending.delete(stream.transactionId);
      entry.settled = true;
      entry.resolve(stream);
    });
  }

  /**
   * One reliable fetch: `fetch{uri}` → register pending →
   * `fetch_begin{txid}` → wait for the stream (no timeout — pending survives
   * disconnects; the first frame after a rewire delivers). Returns the bare
   * kernel stream plus the declared meta.
   *
   * `signal` is the ts mapping of the anchor's Drop guard on the pending
   * entry: aborting the signal tears the registration down and rejects.
   * Without a signal the entry lives until delivery (session-scoped
   * recoverable state, reclaimed when the session dies).
   */
  async pull(uri: string, signal?: AbortSignal): Promise<{ stream: BinaryStream; meta: FetchReady }> {
    if (signal?.aborted) throw FetchError.transport(new XacppError("aborted", "pull aborted"));
    let meta: FetchReady;
    try {
      meta = await this.session.fetch(uri);
    } catch (e) {
      throw toFetchError(e);
    }
    if (this.pending.has(meta.txid)) {
      // Pathological concurrent pull on the same uri: the second
      // registration is rejected and torn down.
      throw FetchError.duplicateTxid();
    }
    const delivered = deferred<BinaryStream>();
    const entry: PendingEntry = {
      meta,
      settled: false,
      resolve: delivered.resolve,
      reject: delivered.reject,
    };
    this.pending.set(meta.txid, entry);
    const onAbort = () => {
      if (entry.settled) return;
      entry.settled = true;
      this.pending.delete(meta.txid);
      delivered.reject(FetchError.transport(new XacppError("aborted", "pull aborted")));
    };
    signal?.addEventListener("abort", onAbort, { once: true });

    const fail = (err: FetchError): never => {
      if (!entry.settled) {
        entry.settled = true;
        this.pending.delete(meta.txid);
      }
      signal?.removeEventListener("abort", onAbort);
      throw err;
    };

    let response: XacppResponse;
    try {
      response = await this.session.requestCommand({ fetch_begin: { txid: meta.txid } });
    } catch (e) {
      return fail(toFetchError(e));
    }
    if (response.kind === "error") return fail(FetchError.refused(response.code, response.message));
    if (response.kind !== "generic") {
      return fail(FetchError.transport(new XacppError("internal_error", "unexpected response to fetch_begin")));
    }
    // Suspension point: resolves on delivery, rejects on caller abort.
    try {
      const stream = await delivered.promise;
      signal?.removeEventListener("abort", onAbort);
      return { stream, meta };
    } catch (e) {
      signal?.removeEventListener("abort", onAbort);
      throw e;
    }
  }

  /**
   * pull + drain + rolling sha256 verify. The ts kernel
   * exposes a synchronous `read()` plus `onChunk`/`onEnd` callbacks, so the
   * rs blocking-read semantics are assembled here. The chunk listener is
   * registered ONCE for the stream's lifetime: re-registering per wait
   * would hand chunks landing between two awaits to the stale (settled)
   * listener, which silently drops them — the long-lived listener buffers
   * those instead.
   */
  async pullToVec(uri: string, signal?: AbortSignal): Promise<{ bytes: Uint8Array; meta: FetchReady }> {
    const { stream, meta } = await this.pull(uri, signal);
    const hash = createHash("sha256");
    const parts: Uint8Array[] = [];
    let received = 0;
    let endReason: StreamEndReason | null = null;
    const buffered: Uint8Array[] = [];
    let waiter: ((chunk: Uint8Array | null) => void) | null = null;
    stream.onChunk((chunk) => {
      if (waiter !== null) {
        const wake = waiter;
        waiter = null;
        wake(chunk);
      } else {
        buffered.push(chunk);
      }
    });
    stream.onEnd((reason) => {
      endReason = reason;
      if (waiter !== null) {
        const wake = waiter;
        waiter = null;
        wake(null);
      }
    });
    for (;;) {
      const chunk = buffered.length > 0 ? (buffered.shift() ?? null) : stream.read();
      if (chunk !== null) {
        parts.push(chunk);
        hash.update(chunk);
        received += chunk.length;
        continue;
      }
      if (endReason !== null) break;
      const next = await new Promise<Uint8Array | null>((resolve) => {
        waiter = resolve;
      });
      if (next === null) break;
      parts.push(next);
      hash.update(next);
      received += next.length;
    }
    // Terminal state first: the digest verdict is only meaningful for a
    // transaction that actually completed. A stream that ended short
    // (`incomplete`) or was cancelled (`cancelled`) fails explicitly —
    // never as an integrity mismatch, never as a hang.
    const terminal: StreamEndReason | null = endReason;
    if (terminal === "incomplete") {
      throw FetchError.incomplete(received, meta.sizeBytes);
    }
    if (terminal === "cancelled") {
      throw FetchError.transport(
        new XacppError("transaction_aborted", "fetch stream cancelled before completion"),
      );
    }
    fetchVerify(meta, hash.digest("hex"), received);
    return { bytes: concatBytes(parts), meta };
  }
}

function concatBytes(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

// ---- free-function companions ----

/**
 * Integrity comparison: the completeness promise is an
 * application-layer commitment — the extension only provides the comparison
 * piece. Throws {@link FetchError} `integrity_mismatch` on disagreement.
 */
export function fetchVerify(meta: FetchReady, actualSha256Hex: string, received?: number): void {
  if (meta.sha256 !== actualSha256Hex) {
    throw FetchError.integrityMismatch(meta.sha256, actualSha256Hex, received ?? 0);
  }
}

async function sendGateCommand(session: XacppSession, command: XacppCommand): Promise<void> {
  const response = await session.requestCommand(command);
  if (response.kind === "error") throw FetchError.refused(response.code, response.message);
  if (response.kind !== "generic") {
    throw FetchError.transport(new XacppError("internal_error", "unexpected response to a fetch gate command"));
  }
}

/** Suspend transmission of the named fetch transaction (SPEC §8.3). */
export async function fetchPause(session: XacppSession, txid: string): Promise<void> {
  return sendGateCommand(session, { fetch_pause: { txid } });
}

/** Continue transmission of the named fetch transaction (SPEC §8.3). */
export async function fetchResume(session: XacppSession, txid: string): Promise<void> {
  return sendGateCommand(session, { fetch_resume: { txid } });
}

/**
 * Stop and release the named fetch transaction (SPEC §8.3). Idempotent:
 * command failures are swallowed (best-effort announce), never thrown.
 */
export async function fetchAbort(session: XacppSession, txid: string): Promise<void> {
  try {
    await session.requestCommand({ fetch_abort: { txid } });
  } catch {
    // idempotent best-effort
  }
}

// ---- b side: FetchService (routing + gate + tables + lifecycle) ----

/** Metadata a provider commits to at resolve time (SPEC §8.2). */
export interface ResolvedResource {
  sizeBytes: number;
  sha256: string;
  mime: string;
}

/**
 * Two-phase provider. `resolve` runs on fetch (the
 * sha256 MUST be ready at fetch_ready time). `serve` runs after begin and
 * writes the real kernel sink — the extension owns the lifecycle: it calls
 * `finish` on success and `abort` on failure; the provider never does.
 */
export interface FetchProvider {
  /** Called at fetch time: produce the committed meta. An XacppError passes through verbatim; any other failure answers fetch_not_found. */
  resolve(uri: string): Promise<ResolvedResource>;
  /** Called after begin: write into the real sink (native write/pause/resume semantics). */
  serve(uri: string, sink: BinarySink): Promise<void>;
}

interface Serving {
  uri: string;
  txid: string;
  meta: FetchReady;
  sink: BinarySink;
  begun: boolean;
  /** Set when the extension (abort path) has taken the terminal state. */
  done: boolean;
}

/** In-flight resolve placeholder: serializes concurrent fetches on one uri. */
interface Resolving {
  outcome: Promise<{ ok: true; meta: FetchReady } | { ok: false; error: XacppError }>;
  settle: (outcome: { ok: true; meta: FetchReady } | { ok: false; error: XacppError }) => void;
}

function isResolving(entry: Serving | Resolving): entry is Resolving {
  return "outcome" in entry;
}

/**
 * Serve-side assembler: implements
 * {@link XacppSessionHandler}, answers the fetch command family, and
 * forwards everything else to the wrapped business handler. Frames are
 * gated: nothing is written until `fetch_begin` names the transaction.
 */
export class FetchService implements XacppSessionHandler {
  /** Loaded fetch responder: the peer lets the fetch family through. */
  readonly isFetchResponder = true;

  private readonly byUri = new Map<string, Serving | Resolving>();
  private readonly byTxid = new Map<string, Serving>();

  constructor(
    private readonly session: XacppSession,
    private readonly provider: FetchProvider,
    private readonly inner: XacppSessionHandler | null = null,
  ) {}

  async onCommand(command: XacppCommand): Promise<XacppResponse> {
    if (typeof command === "object" && "fetch" in command) return this.onFetch(command.fetch.uri);
    if (typeof command === "object" && "fetch_begin" in command) return this.onBegin(command.fetch_begin.txid);
    if (typeof command === "object" && "fetch_pause" in command) return this.onGate(command.fetch_pause.txid, "pause");
    if (typeof command === "object" && "fetch_resume" in command) {
      return this.onGate(command.fetch_resume.txid, "resume");
    }
    if (typeof command === "object" && "fetch_abort" in command) return this.onAbort(command.fetch_abort.txid);
    if (this.inner) return this.inner.onCommand(command);
    return errorResponse("unknown_command", `no handler for command`);
  }

  async onEvent(event: XacppActivityEvent): Promise<XacppResponse> {
    if (this.inner) return this.inner.onEvent(event);
    return acknowledge();
  }

  private async onFetch(uri: string): Promise<XacppResponse> {
    const existing = this.byUri.get(uri);
    if (existing) {
      if (isResolving(existing)) {
        // Concurrent fetch on the same uri: wait out the placeholder, then
        // answer the same meta (resolve is called exactly once).
        const outcome = await existing.outcome;
        return outcome.ok ? readyResponse(outcome.meta) : errorResponse(outcome.error.code, outcome.error.message);
      }
      return readyResponse(existing.meta); // idempotent, provider not re-consulted
    }
    const outcome = deferred<{ ok: true; meta: FetchReady } | { ok: false; error: XacppError }>();
    const resolving: Resolving = { outcome: outcome.promise, settle: outcome.resolve };
    this.byUri.set(uri, resolving);
    try {
      const resolved = await this.provider.resolve(uri);
      // Size gates (rs-symmetric, SPEC §3: total_len is a u32 on the wire —
      // sizes the wire cannot carry are refused, never advertised).
      if (resolved.sizeBytes === 0) {
        throw new XacppError(FETCH_REFUSED, "resource size must be non-zero");
      }
      if (resolved.sizeBytes > 0xffffffff) {
        throw new XacppError(FETCH_REFUSED, "resource exceeds protocol size limit (u32::MAX)");
      }
      const meta: FetchReady = {
        txid: newTransactionId(),
        sha256: resolved.sha256,
        sizeBytes: resolved.sizeBytes,
        mime: resolved.mime,
      };
      // One frame is NOT emitted here: the gate stays shut until begin.
      const sink = this.session.openBinarySink({ totalLen: meta.sizeBytes }, meta.txid);
      const serving: Serving = { uri, txid: meta.txid, meta, sink, begun: false, done: false };
      this.byUri.set(uri, serving);
      this.byTxid.set(meta.txid, serving);
      resolving.settle({ ok: true, meta });
      return readyResponse(meta);
    } catch (e) {
      const error =
        e instanceof XacppError ? e : new XacppError(FETCH_NOT_FOUND, `provider failed for ${uri}: ${String(e)}`);
      if (this.byUri.get(uri) === resolving) this.byUri.delete(uri);
      resolving.settle({ ok: false, error });
      return errorResponse(error.code, error.message);
    }
  }

  private onBegin(txid: string): XacppResponse {
    if (!TXID_PATTERN.test(txid)) return errorResponse(FETCH_REFUSED, `malformed transaction id: ${txid}`);
    const serving = this.byTxid.get(txid);
    if (!serving) return errorResponse(FETCH_REFUSED, `no such fetch transaction: ${txid}`);
    if (serving.begun) return acknowledge(); // idempotent — serve already running
    serving.begun = true;
    this.startServing(serving);
    return acknowledge();
  }

  private onGate(txid: string, action: "pause" | "resume"): XacppResponse {
    const serving = this.byTxid.get(txid);
    if (!serving) return errorResponse(FETCH_REFUSED, `no such fetch transaction: ${txid}`);
    // Control channel, ts mapping: the
    // single-threaded event loop makes the direct sink call equivalent —
    // the provider only sees write() suspension, never the control path.
    if (action === "pause") serving.sink.pause();
    else serving.sink.resume();
    return acknowledge();
  }

  private onAbort(txid: string): XacppResponse {
    const serving = this.byTxid.get(txid);
    if (!serving) return acknowledge(); // unknown/completed — idempotent
    this.byUri.delete(serving.uri);
    this.byTxid.delete(serving.txid);
    serving.done = true;
    // Before begin: no serving task exists, abort the sink directly.
    // After begin: the abort rejects the serving task's pending
    // write/finish, which then exits without a second terminal call.
    // The copy may already be gone: the final ACK releases it without
    // waiting for finish — then the terminal state is already taken and
    // the abort is a no-op (rs twin: handle_abort logs and continues).
    try {
      serving.sink.abort();
    } catch {
      // already released by the final ACK — nothing left to abort
    }
    return acknowledge();
  }

  /**
   * Serving task: run
   * provider.serve against the real sink, then take the terminal state —
   * finish on success, abort on failure — exactly once, and remove the
   * tables' entry either way.
   */
  private startServing(serving: Serving): void {
    void (async () => {
      try {
        await this.provider.serve(serving.uri, serving.sink);
        if (serving.done) return; // abort won the race
        try {
          await serving.sink.finish();
        } catch {
          // Two terminal races land here: an abort mid-finish (the copy is
          // already released) and a short write (the provider delivered
          // fewer bytes than declared — finish() already announced the
          // abort and released the copy). Either way the terminal state
          // is taken; this side has nothing left to do.
        }
      } catch {
        // provider.serve failed, or a pending write was rejected by a
        // concurrent abort: enforce the terminal state exactly once.
        if (!serving.done) {
          serving.done = true;
          try {
            serving.sink.abort();
          } catch {
            // ignore — nothing more this side can do
          }
        }
      } finally {
        if (this.byUri.get(serving.uri) === serving) this.byUri.delete(serving.uri);
        if (this.byTxid.get(serving.txid) === serving) this.byTxid.delete(serving.txid);
      }
    })();
  }
}

function readyResponse(meta: FetchReady): XacppResponse {
  return { kind: "fetch_ready", txid: meta.txid, sha256: meta.sha256, sizeBytes: meta.sizeBytes, mime: meta.mime };
}

// ---- canned providers ----

/** Canned bytes provider: digest at resolve, chunked writes at serve. */
export function bytesProvider(data: Uint8Array, mime: string): FetchProvider {
  return {
    async resolve() {
      return { sizeBytes: data.length, sha256: sha256Hex(data), mime };
    },
    async serve(_uri, sink) {
      for (let offset = 0; offset < data.length; offset += CHUNK_BYTES) {
        await sink.write(data.subarray(offset, Math.min(offset + CHUNK_BYTES, data.length)));
      }
    },
  };
}

/**
 * Canned file provider: streaming sha256 pre-scan at resolve (large files
 * never fully loaded), 64 KiB chunked disk reads at serve.
 */
export function fileProvider(path: string, mime: string): FetchProvider {
  return {
    async resolve() {
      const hash = createHash("sha256");
      const handle = await fs.open(path, "r");
      try {
        let sizeBytes = 0;
        const buf = Buffer.allocUnsafe(CHUNK_BYTES);
        for (;;) {
          const { bytesRead } = await handle.read(buf, 0, CHUNK_BYTES, null);
          if (bytesRead === 0) break;
          hash.update(buf.subarray(0, bytesRead));
          sizeBytes += bytesRead;
        }
        return { sizeBytes, sha256: hash.digest("hex"), mime };
      } finally {
        await handle.close();
      }
    },
    async serve(_uri, sink) {
      const handle = await fs.open(path, "r");
      try {
        const buf = Buffer.allocUnsafe(CHUNK_BYTES);
        for (;;) {
          const { bytesRead } = await handle.read(buf, 0, CHUNK_BYTES, null);
          if (bytesRead === 0) break;
          // Copy: the scratch buffer is reused across iterations.
          await sink.write(new Uint8Array(buf.subarray(0, bytesRead)));
        }
      } finally {
        await handle.close();
      }
    },
  };
}
