/**
 * Stdio Transport implementation.
 *
 * Communicates via stdin/stdout pipe handles using JSONL frame protocol (one message per line, separated by `\n`).
 */

import * as readline from "node:readline";
import type { RequestHandler, XacppTransport } from "./transport";
import type { XacppEnvelope, XacppRequest, XacppResponse } from "./message";
import { XacppError } from "./message";

/** Stdio Transport implementation. */
export class StdioTransport implements XacppTransport {
  private writer: NodeJS.WritableStream | null;
  private reader: NodeJS.ReadableStream | null;
  private rl: readline.Interface | null = null;
  private _connected = false;
  private _exhausted = false; // No reconnection after disconnect

  /** Handler registration. */
  private requestHandler: RequestHandler | null = null;

  /** Pending map: id → { resolve, reject }. */
  private pending: Map<string, { resolve: (response: XacppResponse) => void; reject: (err: XacppError) => void }> = new Map();

  /** Auto-incrementing id. */
  private nextId = 1;

  constructor(
    writer: NodeJS.WritableStream,
    reader: NodeJS.ReadableStream,
  ) {
    this.writer = writer;
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
      this.cleanup();
    });

    this._connected = true;
    console.debug("[xacpp:stdio] connected");
  }

  async disconnect(): Promise<void> {
    if (this.rl) {
      this.rl.close();
      this.rl = null;
    }
    if (this.writer) {
      this.writer.end();
      this.writer = null;
    }
    this.reader = null;
    this.cleanup();
    this._exhausted = true;
    console.debug("[xacpp:stdio] disconnected");
  }

  async send(sessionId: string | null, payload: XacppRequest): Promise<XacppResponse> {
    if (!this._connected || !this.writer) throw XacppError.notConnected();

    const id = this.genId();

    return new Promise<XacppResponse>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });

      const envelope: XacppEnvelope = {
        type: "request",
        id,
        ...(sessionId != null ? { session_id: sessionId } : {}),
        payload,
      };
      const json = JSON.stringify(envelope) + "\n";

      this.writer!.write(json, (err?: Error | null) => {
        if (err) {
          this.pending.delete(id);
          reject(XacppError.closed());
        }
      });
    });
  }

  onRequest(handler: RequestHandler): void {
    if (this._connected) throw XacppError.alreadyConnected();
    this.requestHandler = handler;
  }

  // ---- Internal ----

  /** Process a frame received from the wire. */
  private async onFrame(line: string): Promise<void> {
    let envelope: XacppEnvelope;
    try {
      envelope = JSON.parse(line);
    } catch {
      console.warn("[xacpp:stdio] failed to parse frame: %s", line);
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
    const ok = this.writeEnvelope(response);
    if (!ok) {
      console.warn("[xacpp:stdio] failed to send response for request %s", id);
    }
  }

  /** Handle inbound response envelope: match pending. */
  private handleResponse(id: string, payload: XacppResponse): void {
    const pending = this.pending.get(id);
    if (pending) {
      this.pending.delete(id);
      pending.resolve(payload);
    } else {
      console.warn("[xacpp:stdio] received response for unknown request %s", id);
    }
  }

  /** Serialize and send envelope. Returns whether write succeeded. */
  private writeEnvelope(envelope: XacppEnvelope): boolean {
    if (!this.writer) return false;
    const json = JSON.stringify(envelope) + "\n";
    return this.writer.write(json);
  }

  /** Cleanup on connection close. */
  private cleanup(): void {
    this._connected = false;

    // Reject all pending
    const err = XacppError.closed();
    for (const [id, pending] of this.pending) {
      console.warn("[xacpp:stdio] rejecting pending request %s: %s", id, err.message);
      pending.reject(err);
    }
    this.pending.clear();
  }
}
