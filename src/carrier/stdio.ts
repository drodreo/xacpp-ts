/**
 * Stdio Transport implementation.
 *
 * Communicates via stdin/stdout pipe handles using the XACPP binary frame
 * protocol (docs/SPEC.md §3/§4): each envelope travels as a single-frame
 * DATA_JSON transaction. Inbound garbage is tolerated by design: the frame
 * decoder resyncs by scanning for the "XACP" magic, so stray non-frame
 * bytes (e.g. child-process log lines that went to stdout instead of
 * stderr) are silently skipped instead of crashing the accept loop —
 * stdout is the protocol channel; logs belong on stderr.
 *
 * All outbound frames (requests, events, responses) go through the single
 * ordered egress queue (see `egress`): one drain loop owns the write half
 * and writes frames FIFO in submission order.
 */

import type { RequestHandler, XacppTransport } from "./transport";
import type { XacppEnvelope, XacppRequest, XacppResponse } from "../wire/message";
import { XacppError } from "../wire/message";
import { ClosedSignal, Egress } from "../session/egress";
import type { EgressHost, EgressWriter, PendingMap } from "../session/egress";
import { FrameDecoder, FrameType, encodeFrame, makeFrame } from "../wire/frame";
import type { Frame } from "../wire/frame";
import type { TransferActivityHandlers, TransferWiring } from "../transfer";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/** Stdio Transport implementation. */
export class StdioTransport implements XacppTransport, TransferWiring {
  private writer: EgressWriter | null;
  private reader: NodeJS.ReadableStream | null;
  private _connected = false;
  private _exhausted = false; // No reconnection after disconnect

  /** Registered transfer-session hooks (present while a session is wired). */
  private transferHandlers: TransferActivityHandlers | null = null;

  /** Streaming frame decoder (one per connection, present after connect). */
  private decoder: FrameDecoder | null = null;
  /** Set after a protocol-level decode error; the stream is unusable past it. */
  private decoderDead = false;
  /** Reader event listeners (kept for removal on disconnect). */
  private onData: ((chunk: Buffer | string) => void) | null = null;
  private onReaderClosed: (() => void) | null = null;
  private onReaderEnded: (() => void) | null = null;
  /** Idempotency guard: reader teardown fires once per connection. */
  private readerGone = false;

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

    // Streaming receive: raw byte chunks feed the frame decoder (no line
    // splitting — frame boundaries come from the wire format, not "\n").
    const decoder = new FrameDecoder();
    this.decoder = decoder;
    this.decoderDead = false;
    this.readerGone = false;

    this.onData = (chunk) => {
      if (this.decoderDead) return;
      const bytes = typeof chunk === "string" ? textEncoder.encode(chunk) : new Uint8Array(chunk);
      let frames: Frame[];
      try {
        frames = decoder.push(bytes);
      } catch (e) {
        // Protocol violation (version / CRC / oversize): the stream cannot
        // be resynced — connection-level throw semantics = connection dead.
        this.decoderDead = true;
        console.error(
          "[xacpp:stdio] frame decode failed, closing connection: %s",
          e instanceof Error ? e.message : String(e),
        );
        this.onReaderGone();
        return;
      }
      for (const frame of frames) this.onWireFrame(frame);
    };
    this.onReaderClosed = () => {
      console.info("[xacpp:stdio] accept loop exited");
      this.onReaderGone();
    };
    this.onReaderEnded = this.onReaderClosed;

    this.reader.on("data", this.onData);
    this.reader.on("close", this.onReaderClosed);
    this.reader.on("end", this.onReaderEnded);

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

    if (this.reader) {
      if (this.onData) this.reader.removeListener("data", this.onData);
      if (this.onReaderClosed) this.reader.removeListener("close", this.onReaderClosed);
      if (this.onReaderEnded) this.reader.removeListener("end", this.onReaderEnded);
    }
    this.onData = null;
    this.onReaderClosed = null;
    this.onReaderEnded = null;
    this.decoder = null;

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

      egress.sendAcked(this.encodeEnvelope(envelope)).catch(() => {
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
      egress.sendFaf(this.encodeEnvelope(envelope));
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

  /** Serialize an envelope into a single-frame DATA_JSON transaction (SPEC §3/§4). */
  private encodeEnvelope(envelope: XacppEnvelope): Uint8Array {
    return encodeFrame(makeFrame(FrameType.DATA_JSON, textEncoder.encode(JSON.stringify(envelope))));
  }

  /** Process one decoded wire frame: DATA_JSON carries the envelope; transfer frames go to the wired session. */
  private onWireFrame(frame: Frame): void {
    if (frame.frameType === FrameType.DATA_JSON) {
      // Any inbound frame proves the peer alive — refresh the silence clock.
      this.transferHandlers?.onActivity();
    } else if (this.transferHandlers) {
      // Transfer channel: DATA_BINARY reassembly + control dispatch.
      this.transferHandlers.onFrame(frame);
      return;
    } else {
      // No transfer session wired: nothing to route non-envelope frames to.
      console.debug("[xacpp:stdio] ignoring frame type 0x%s", frame.frameType.toString(16));
      return;
    }

    const payloadText = textDecoder.decode(frame.payload);
    let envelope: XacppEnvelope;
    try {
      envelope = JSON.parse(payloadText);
    } catch {
      // Expected with stdio carriers: garbage that survived the decoder's
      // magic resync (e.g. a stray "XACP"-prefixed log line) lands here.
      // Log-only, never fatal.
      console.debug("[xacpp::stdio::log] %s", payloadText);
      return;
    }

    if (envelope.type === "request") {
      void this.handleRequest(envelope.id, envelope.session_id ?? null, envelope.payload);
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
      await this.egress?.sendAcked(this.encodeEnvelope(response));
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
    if (this.readerGone) return;
    this.readerGone = true;
    this._connected = false;
    this.rejectAllPending();
    this.egress?.close();
    this.egress = null;
  }

  // ---- Transfer wiring (session attachment surface) ----

  /** Write one encoded control frame through the ordered egress (scheduling priority). */
  writeControlFrame(bytes: Uint8Array): void {
    if (!this.egress) throw XacppError.notConnected();
    this.egress.sendControl(bytes);
  }

  /** Write one data frame, encoded, through the ordered egress (fire-and-forget FIFO). */
  writeDataFrame(frame: Frame): void {
    if (!this.egress) throw XacppError.notConnected();
    this.egress.sendFaf(encodeFrame(frame));
  }

  /** Register/unregister the transfer-session activity hooks. */
  onTransferActivity(handlers: TransferActivityHandlers | null): void {
    this.transferHandlers = handlers;
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
