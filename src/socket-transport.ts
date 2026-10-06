import * as net from "node:net";
import * as readline from "node:readline";
import type { RequestHandler, XacppTransport } from "./transport";
import type { XacppEnvelope, XacppRequest, XacppResponse } from "./message";
import { XacppError } from "./message";
import { ClosedSignal, Egress } from "./egress";
import type { EgressHost, PendingMap } from "./egress";

/**
 * Socket Transport Implementation.
 *
 * Communicates via TCP connection using JSONL frame protocol (one message per line, delimited by `\n`).
 * Key difference from StdioTransport: each inbound request spawns independent task for concurrent handling.
 *
 * All outbound frames (requests, events, responses) go through the single
 * ordered egress queue (see `egress`): one drain loop owns the write half
 * and writes frames FIFO in submission order.
 */
export class SocketTransport implements XacppTransport {
  private socket: net.Socket | null = null;
  private handler: RequestHandler | null = null;
  private pending: PendingMap = new Map();
  private nextId = 1;
  private _connected = false;
  private _exhausted = false;

  /** Connection-close broadcast (see `XacppTransport.onClosed`). */
  private readonly closedSignal = new ClosedSignal();
  /** Ordered egress entry (present after connect). */
  private egress: Egress | null = null;

  // Concurrency control: inflight handler tasks, all aborted on disconnect.
  private inflight: Set<AbortController> = new Set();

  // Client-mode connection parameters
  private port?: number;
  private host?: string;

  // readline interface
  private rl: readline.Interface | null = null;

  /** Egress host integration points. */
  private readonly egressHost: EgressHost = {
    isConnected: () => this._connected,
    markDisconnected: () => {
      this._connected = false;
    },
    clearPending: () => this.rejectAllPending(),
  };

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
      console.info("[xacpp:socket] reader task exited, cleaning up");
      this.onReaderGone();
    });

    // Ordered egress: drain loop owns the write half.
    this.egress = new Egress(sock, this.egressHost, this.closedSignal);

    this._connected = true;
    console.debug("[xacpp:socket] connected");
  }

  async disconnect(): Promise<void> {
    this._exhausted = true;
    this._connected = false;

    // Shut the egress down (queued frames flush FIFO before the writer closes).
    this.egress?.close();
    this.egress = null;

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

    // Drop all pending sends
    this.rejectAllPending();

    this.socket = null;
    console.debug("[xacpp:socket] disconnected");
  }

  async send(sessionId: string | null, payload: XacppRequest): Promise<XacppResponse> {
    const egress = this.egress;
    if (!egress || !this._connected) throw XacppError.notConnected();

    const id = `r${this.nextId++}`;

    return new Promise<XacppResponse>((resolve, reject) => {
      this.pending.set(id, { kind: "respond", resolve, reject });

      const envelope: XacppEnvelope = {
        type: "request",
        id,
        ...(sessionId != null ? { session_id: sessionId } : {}),
        payload,
      };

      egress.sendAcked(JSON.stringify(envelope) + "\n").catch(() => {
        this.pending.delete(id);
        reject(XacppError.closed());
      });
    });
  }

  async sendFaf(sessionId: string | null, payload: XacppRequest): Promise<void> {
    const egress = this.egress;
    if (!egress || !this._connected) throw XacppError.notConnected();

    const id = `r${this.nextId++}`;

    // Drop slot: the peer's ack is silently consumed on arrival.
    this.pending.set(id, { kind: "drop" });

    const envelope: XacppEnvelope = {
      type: "request",
      id,
      ...(sessionId != null ? { session_id: sessionId } : {}),
      payload,
    };

    try {
      egress.sendFaf(JSON.stringify(envelope) + "\n");
    } catch {
      this.pending.delete(id);
      throw XacppError.closed();
    }
  }

  onRequest(handler: RequestHandler): void {
    if (this._connected) throw XacppError.alreadyConnected();
    this.handler = handler;
  }

  onClosed(listener: () => void): () => void {
    return this.closedSignal.subscribe(listener);
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
      const slot = this.pending.get(envelope.id);
      if (slot) {
        this.pending.delete(envelope.id);
        if (slot.kind === "respond") {
          slot.resolve(envelope.payload);
        }
        // kind === "drop": fire-and-forget ack, silently consumed.
      } else {
        console.warn("[xacpp:socket] received response for unknown request %s", envelope.id);
      }
    }
  }

  /** Handle inbound request: spawn-per-request, does not await. */
  private dispatchRequest(envelope: XacppEnvelope & { type: "request" }): void {
    if (!this.handler) {
      // No handler, return error response
      this.sendResponse({
        type: "response",
        id: envelope.id,
        ...(envelope.session_id != null ? { session_id: envelope.session_id } : {}),
        payload: { kind: "error", code: "no_handler", message: "no handler registered" },
      }, "reader", envelope.id).catch(() => {});
      return;
    }

    const controller = new AbortController();
    this.inflight.add(controller);

    const sessionId = envelope.session_id ?? null;
    const handler = this.handler;

    handler(sessionId, envelope.payload)
      .then((responsePayload) => {
        if (controller.signal.aborted) return;
        return this.sendResponse({
          type: "response",
          id: envelope.id,
          ...(envelope.session_id != null ? { session_id: envelope.session_id } : {}),
          payload: responsePayload,
        }, "handler", envelope.id);
      })
      .catch((e) => {
        if (controller.signal.aborted) return;
        const err = e instanceof XacppError ? e : XacppError.internal(e instanceof Error ? e.message : String(e));
        console.error("[xacpp:socket] handler error for request %s: %s", envelope.id, err.message);
        return this.sendResponse({
          type: "response",
          id: envelope.id,
          ...(envelope.session_id != null ? { session_id: envelope.session_id } : {}),
          payload: { kind: "error", code: err.code, message: err.message },
        }, "handler", envelope.id);
      })
      .finally(() => {
        this.inflight.delete(controller);
      });
  }

  /** Serialize and send an envelope through the ordered egress (acked path). */
  private async sendResponse(envelope: XacppEnvelope, source: string, id: string): Promise<void> {
    if (!this.egress) {
      console.warn("[xacpp:%s] failed to send response for request %s: connection closed", source, id);
      return;
    }
    try {
      await this.egress.sendAcked(JSON.stringify(envelope) + "\n");
    } catch (e) {
      console.warn("[xacpp:%s] failed to send response for request %s: %s", source, id, e instanceof Error ? e.message : String(e));
    }
  }

  /** Cleanup on reader exit: connection dead — fail fast subsequent sends, drop pending waiters. */
  private onReaderGone(): void {
    this._connected = false;
    this.rejectAllPending();
    this.egress?.close();
    this.egress = null;
  }

  private rejectAllPending(): void {
    const err = XacppError.closed();
    for (const [, slot] of this.pending) {
      if (slot.kind === "respond") {
        slot.reject(err);
      }
    }
    this.pending.clear();
  }
}
