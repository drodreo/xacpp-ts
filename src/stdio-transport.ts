/**
 * Stdio Transport implementation.
 *
 * Communicates via stdin/stdout pipe handles using JSONL frame protocol (one message per line, separated by `\n`).
 *
 * All outbound frames (requests, events, responses) go through the single
 * ordered egress queue (see `egress`): one drain loop owns the write half
 * and writes frames FIFO in submission order.
 */

import * as readline from "node:readline";
import type { RequestHandler, XacppTransport } from "./transport";
import type { XacppEnvelope, XacppRequest, XacppResponse } from "./message";
import { XacppError } from "./message";
import { ClosedSignal, Egress } from "./egress";
import type { EgressHost, EgressWriter, PendingMap } from "./egress";

/** Stdio Transport implementation. */
export class StdioTransport implements XacppTransport {
  private writer: EgressWriter | null;
  private reader: NodeJS.ReadableStream | null;
  private rl: readline.Interface | null = null;
  private _connected = false;
  private _exhausted = false; // No reconnection after disconnect

  /** Handler registration. */
  private requestHandler: RequestHandler | null = null;

  /** Pending map: id → slot (respond waiter or fire-and-forget drop slot). */
  private pending: PendingMap = new Map();

  /** Auto-incrementing id. */
  private nextId = 1;

  /** Connection-close broadcast (see `XacppTransport.onClosed`). */
  private readonly closedSignal = new ClosedSignal();
  /** Ordered egress entry (present after connect). */
  private egress: Egress | null = null;

  /** Egress host integration points. */
  private readonly egressHost: EgressHost = {
    isConnected: () => this._connected,
    markDisconnected: () => {
      this._connected = false;
    },
    clearPending: () => this.rejectAllPending(),
  };

  constructor(
    writer: NodeJS.WritableStream,
    reader: NodeJS.ReadableStream,
  ) {
    this.writer = writer as EgressWriter;
    this.reader = reader;
  }

  private genId(): string {
    return `r${this.nextId++}`;
  }

  // ---- Transport interface ----

  async connect(): Promise<void> {
    if (this._exhausted) throw XacppError.alreadyConnected();
    if (this._connected) throw XacppError.alreadyConnected();
    if (!this.writer || !this.reader) throw XacppError.alreadyConnected();

    this.rl = readline.createInterface({ input: this.reader });

    this.rl.on("line", (line: string) => {
      this.onFrame(line);
    });

    this.rl.on("close", () => {
      console.info("[xacpp:stdio] accept loop exited");
      this.onReaderGone();
    });

    // Ordered egress: drain loop owns the write half.
    this.egress = new Egress(this.writer, this.egressHost, this.closedSignal);

    this._connected = true;
    console.debug("[xacpp:stdio] connected");
  }

  async disconnect(): Promise<void> {
    this._exhausted = true;

    // Shut the egress down (queued frames flush FIFO before the writer closes).
    this.egress?.close();
    this.egress = null;
    this._connected = false;

    if (this.rl) {
      this.rl.close();
      this.rl = null;
    }
    this.reader = null;
    this.writer = null;

    // Drop all pending sends
    this.rejectAllPending();
    console.debug("[xacpp:stdio] disconnected");
  }

  async send(sessionId: string | null, payload: XacppRequest): Promise<XacppResponse> {
    const egress = this.egress;
    if (!egress || !this._connected) throw XacppError.notConnected();

    const id = this.genId();

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

    const id = this.genId();

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
    this.requestHandler = handler;
  }

  onClosed(listener: () => void): () => void {
    return this.closedSignal.subscribe(listener);
  }

  // ---- Internal ----

  /** Process a frame received from the wire. */
  private async onFrame(line: string): Promise<void> {
    let envelope: XacppEnvelope;
    try {
      envelope = JSON.parse(line);
    } catch {
      console.debug("[xacpp::stdio::log] %s", line);
      return;
    }

    if (envelope.type === "request") {
      await this.handleRequest(envelope.id, envelope.session_id ?? null, envelope.payload);
    } else if (envelope.type === "response") {
      this.handleResponse(envelope.id, envelope.payload);
    }
  }

  /** Handle inbound request envelope: dispatch handler, send response. */
  private async handleRequest(id: string, sessionId: string | null, payload: XacppRequest): Promise<void> {
    let responsePayload: XacppResponse;

    try {
      if (!this.requestHandler) throw XacppError.noHandler();
      responsePayload = await this.requestHandler(sessionId, payload);
    } catch (e) {
      const err = e instanceof XacppError
        ? e
        : XacppError.internal(e instanceof Error ? e.message : String(e));
      console.error("[xacpp:stdio] handler error for request %s: %s", id, err.message);
      responsePayload = { kind: "error", code: err.code, message: err.message };
    }

    const response: XacppEnvelope = {
      type: "response",
      id,
      ...(sessionId != null ? { session_id: sessionId } : {}),
      payload: responsePayload,
    };
    try {
      await this.egress?.sendAcked(JSON.stringify(response) + "\n");
    } catch {
      console.warn("[xacpp:stdio] failed to send response for request %s", id);
    }
  }

  /** Handle inbound response envelope: match pending. */
  private handleResponse(id: string, payload: XacppResponse): void {
    const slot = this.pending.get(id);
    if (slot) {
      this.pending.delete(id);
      if (slot.kind === "respond") {
        slot.resolve(payload);
      }
      // kind === "drop": fire-and-forget ack, silently consumed.
    } else {
      console.warn("[xacpp:stdio] received response for unknown request %s", id);
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
    for (const [id, slot] of this.pending) {
      if (slot.kind === "respond") {
        console.warn("[xacpp:stdio] rejecting pending request %s: %s", id, err.message);
        slot.reject(err);
      }
    }
    this.pending.clear();
  }
}
