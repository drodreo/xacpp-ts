import * as net from "node:net";
import * as readline from "node:readline";
import type { RequestHandler, XacppTransport } from "./transport";
import type { XacppEnvelope, XacppRequest, XacppResponse } from "./message";
import { XacppError } from "./message";

export class SocketTransport implements XacppTransport {
  private socket: net.Socket | null = null;
  private handler: RequestHandler | null = null;
  private pending: Map<string, { resolve: (response: XacppResponse) => void; reject: (err: XacppError) => void }> = new Map();
  private nextId = 1;
  private _connected = false;
  private _exhausted = false;

  // Concurrency control
  private writeQueue: Promise<void> = Promise.resolve();
  private inflight: Set<AbortController> = new Set();

  // Client-mode connection parameters
  private port?: number;
  private host?: string;

  // readline interface
  private rl: readline.Interface | null = null;

  /** Client mode: connect() initiates a TCP connection to the specified address. */
  static connectTo(port: number, host?: string): SocketTransport {
    const t = new SocketTransport();
    t.port = port;
    t.host = host ?? "127.0.0.1";
    return t;
  }

  /** Server mode: use an already-accepted Socket. */
  constructor(socket?: net.Socket) {
    if (socket) {
      this.socket = socket;
    }
  }

  // ---- XacppTransport interface ----

  async connect(): Promise<void> {
    if (this._exhausted || this._connected) throw XacppError.alreadyConnected();

    if (!this.socket) {
      // Client mode: create new socket and connect
      if (this.port === undefined) throw XacppError.alreadyConnected();
      this.socket = new net.Socket();
      await new Promise<void>((resolve, reject) => {
        const sock = this.socket!;
        const onError = (err: Error) => { cleanup(); reject(new Error(`connect failed: ${err.message}`)); };
        const onConnect = () => { cleanup(); resolve(); };
        const cleanup = () => { sock.removeListener("error", onError); sock.removeListener("connect", onConnect); };
        sock.once("error", onError);
        sock.once("connect", onConnect);
        sock.connect(this.port!, this.host!, () => {});  // connect event triggers onConnect
      });
    }

    // Common to both modes: start readline receiver
    const sock = this.socket!;
    this.rl = readline.createInterface({ input: sock });
    this.rl.on("line", (line) => this.onFrame(line));
    this.rl.on("close", () => {
      console.info("[xacpp:socket] accept loop exited");
      this.cleanup();
    });

    this._connected = true;
    console.debug("[xacpp:socket] connected");
  }

  async disconnect(): Promise<void> {
    this._exhausted = true;
    this._connected = false;

    // Abort all inflight handlers
    for (const controller of this.inflight) {
      controller.abort();
    }
    this.inflight.clear();

    // Close readline
    if (this.rl) {
      this.rl.close();
      this.rl = null;
    }

    // Destroy socket
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }

    // Break write chain
    this.writeQueue = Promise.resolve();

    // Reject all pending sends
    this.rejectAllPending();
    console.debug("[xacpp:socket] disconnected");
  }

  async send(sessionId: string | null, payload: XacppRequest): Promise<XacppResponse> {
    if (!this._connected || !this.socket) throw XacppError.notConnected();

    const id = `r${this.nextId++}`;

    return new Promise<XacppResponse>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });

      const envelope: XacppEnvelope = {
        type: "request",
        id,
        ...(sessionId != null ? { session_id: sessionId } : {}),
        payload,
      };

      this.writeEnvelope(envelope).catch(() => {
        this.pending.delete(id);
        reject(XacppError.closed());
      });
    });
  }

  onRequest(handler: RequestHandler): void {
    if (this._connected) throw XacppError.alreadyConnected();
    this.handler = handler;
  }

  // ---- Internal ----

  private async onFrame(line: string): Promise<void> {
    let envelope: XacppEnvelope;
    try {
      envelope = JSON.parse(line);
    } catch {
      console.warn("[xacpp:socket] failed to parse frame: %s", line);
      return;
    }

    if (envelope.type === "request") {
      this.dispatchRequest(envelope);
    } else if (envelope.type === "response") {
      const pending = this.pending.get(envelope.id);
      if (pending) {
        this.pending.delete(envelope.id);
        pending.resolve(envelope.payload);
      } else {
        console.warn("[xacpp:socket] received response for unknown request %s", envelope.id);
      }
    }
  }

  /** Handle inbound request: spawn-per-request, does not await. */
  private dispatchRequest(envelope: XacppEnvelope & { type: "request" }): void {
    if (!this.handler) {
      // No handler, return error response
      this.writeEnvelope({
        type: "response",
        id: envelope.id,
        ...(envelope.session_id != null ? { session_id: envelope.session_id } : {}),
        payload: { kind: "error", code: "no_handler", message: "no handler registered" },
      }).catch(() => {});
      return;
    }

    const controller = new AbortController();
    this.inflight.add(controller);

    const sessionId = envelope.session_id ?? null;
    const handler = this.handler;

    handler(sessionId, envelope.payload)
      .then((responsePayload) => {
        if (controller.signal.aborted) return;
        return this.writeEnvelope({
          type: "response",
          id: envelope.id,
          ...(envelope.session_id != null ? { session_id: envelope.session_id } : {}),
          payload: responsePayload,
        });
      })
      .catch((e) => {
        if (controller.signal.aborted) return;
        const err = e instanceof XacppError ? e : XacppError.internal(e instanceof Error ? e.message : String(e));
        console.error("[xacpp:socket] handler error for request %s: %s", envelope.id, err.message);
        return this.writeEnvelope({
          type: "response",
          id: envelope.id,
          ...(envelope.session_id != null ? { session_id: envelope.session_id } : {}),
          payload: { kind: "error", code: err.code, message: err.message },
        });
      })
      .finally(() => {
        this.inflight.delete(controller);
      });
  }

  /** Serialize and send envelope (write-protected: promise chain serialization).
   *
   * Returns only the promise for the current write, not the entire chain.
   * Chain always continues (even if a write fails), ensuring subsequent writes are not blocked.
   */
  private writeEnvelope(envelope: XacppEnvelope): Promise<void> {
    if (!this.socket) return Promise.reject(XacppError.closed());

    const json = JSON.stringify(envelope) + "\n";
    const sock = this.socket;

    // Independent promise for this write: resolve/reject controlled by this write callback
    let resolveOp!: () => void;
    let rejectOp!: (err: XacppError) => void;
    const opPromise = new Promise<void>((resolve, reject) => {
      resolveOp = resolve;
      rejectOp = reject;
    });

    // Chain: serialize writes, always continue (even if previous failed)
    this.writeQueue = this.writeQueue.then(
      () =>
        new Promise<void>((chainResolve) => {
          sock.write(json, (err) => {
            if (err) {
              rejectOp(XacppError.closed());
            } else {
              resolveOp();
            }
            chainResolve(); // Chain always continues
          });
        }),
      () => {} // Swallow previous chain error, keep chain alive
    );

    return opPromise;
  }

  private rejectAllPending(): void {
    const err = XacppError.closed();
    for (const [, p] of this.pending) {
      p.reject(err);
    }
    this.pending.clear();
  }

  private cleanup(): void {
    this._connected = false;
    this.rejectAllPending();
  }
}
