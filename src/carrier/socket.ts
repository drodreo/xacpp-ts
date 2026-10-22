import * as net from "node:net";
import type { RequestHandler, XacppTransport } from "./transport";
import type { XacppEnvelope, XacppRequest, XacppResponse } from "../wire/message";
import { XacppError } from "../wire/message";
import { ClosedSignal, Egress } from "../session/egress";
import type { EgressHost, PendingMap } from "../session/egress";
import { FrameDecoder, FrameType, encodeFrame, makeFrame } from "../wire/frame";
import type { Frame } from "../wire/frame";
import type { TransferActivityHandlers, TransferWiring } from "../transfer";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Socket Transport Implementation.
 *
 * Communicates via TCP connection using the XACPP binary frame protocol
 * (docs/SPEC.md §3/§4): each envelope travels as a single-frame DATA_JSON
 * transaction. Inbound bytes stream into the frame decoder — frame
 * boundaries come from the wire format, not line splitting.
 * Key difference from StdioTransport: each inbound request spawns independent task for concurrent handling.
 *
 * All outbound frames (requests, events, responses) go through the single
 * ordered egress queue (see `egress`): one drain loop owns the write half
 * and writes frames FIFO in submission order.
 */
export class SocketTransport implements XacppTransport, TransferWiring {
  private socket: net.Socket | null = null;
  private handler: RequestHandler | null = null;
  private pending: PendingMap = new Map();
  private nextId = 1;
  private _connected = false;
  private _exhausted = false;

  /** Registered transfer-session hooks (present while a session is wired). */
  private transferHandlers: TransferActivityHandlers | null = null;

  /** Connection-close broadcast (see `XacppTransport.onClosed`). */
  private readonly closedSignal = new ClosedSignal();
  /** Ordered egress entry (present after connect). */
  private egress: Egress | null = null;

  /** Streaming frame decoder (one per connection, present after connect). */
  private decoder: FrameDecoder | null = null;
  /** Set after a protocol-level decode error; the stream is unusable past it. */
  private decoderDead = false;
  /** Socket event listeners (kept for removal on disconnect). */
  private onData: ((chunk: Buffer) => void) | null = null;
  private onSocketClosed: (() => void) | null = null;
  private onSocketEnded: (() => void) | null = null;
  /**
   * Resident socket 'error' listener: a transport-level error (peer reset,
   * network failure) means the connection is dead. Without a listener, Node
   * escalates the event to an uncaught exception and takes the host process
   * down — a peer that dies abruptly is a normal event, not a crash.
   */
  private onSocketError: ((err: Error) => void) | null = null;
  /** Idempotency guard: reader teardown fires once per connection. */
  private readerGone = false;

  // Concurrency control: inflight handler tasks, all aborted on disconnect.
  private inflight: Set<AbortController> = new Set();

  // Client-mode connection parameters
  private port?: number;
  private host?: string;

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

    const sock = this.socket ?? (await this.connectClientSocket());
    this.socket = sock;

    // Common to both modes: start the streaming frame decoder receiver.
    const decoder = new FrameDecoder();
    this.decoder = decoder;
    this.decoderDead = false;
    this.readerGone = false;

    this.onData = (chunk) => {
      if (this.decoderDead) return;
      let frames: Frame[];
      try {
        frames = decoder.push(new Uint8Array(chunk));
      } catch (e) {
        // Protocol violation (version / CRC / oversize): the stream cannot
        // be resynced — connection-level throw semantics = connection dead.
        this.decoderDead = true;
        console.error(
          "[xacpp:socket] frame decode failed, closing connection: %s",
          e instanceof Error ? e.message : String(e),
        );
        this.onReaderGone();
        return;
      }
      for (const frame of frames) this.onWireFrame(frame);
    };
    this.onSocketClosed = () => {
      console.info("[xacpp:socket] reader task exited, cleaning up");
      this.onReaderGone();
    };
    this.onSocketEnded = this.onSocketClosed;
    // Resident error path: a socket error (peer reset, network failure) is a
    // dead connection, not a host crash. Idempotent with the close path via
    // onReaderGone's guard (Node always emits 'close' after 'error').
    this.onSocketError = (err: Error) => {
      console.warn("[xacpp:socket] socket error, connection dead: %s", err.message);
      this.onReaderGone();
    };

    sock.on("data", this.onData);
    sock.on("close", this.onSocketClosed);
    sock.on("end", this.onSocketEnded);
    sock.on("error", this.onSocketError);

    // Ordered egress: drain loop owns the write half.
    this.egress = new Egress(sock, this.egressHost, this.closedSignal);

    this._connected = true;

    console.debug("[xacpp:socket] connected");
  }

  /**
   * Client mode: create a socket and connect to the configured endpoint.
   * The socket is published on `this.socket` before the connect attempt —
   * a failed connect leaves the same state as before (a dead socket).
   */
  private async connectClientSocket(): Promise<net.Socket> {
    const port = this.port;
    if (port === undefined) throw XacppError.alreadyConnected();
    const host = this.host;
    const client = new net.Socket();
    this.socket = client;
    await new Promise<void>((resolve, reject) => {
      const onError = (err: Error) => { cleanup(); reject(new Error(`connect failed: ${err.message}`)); };
      const onConnect = () => { cleanup(); resolve(); };
      const cleanup = () => { client.removeListener("error", onError); client.removeListener("connect", onConnect); };
      client.once("error", onError);
      client.once("connect", onConnect);
      // No host configured → Node's default host (same call shape as before).
      if (host === undefined) client.connect(port, () => {});
      else client.connect(port, host, () => {});
    });
    return client;
  }

  async disconnect(): Promise<void> {
    this._exhausted = true;
    this._connected = false;

    // SPEC §5 disconnect discipline: the transfer mount detaches from the
    // dead transport immediately — frames still queued in this connection's
    // egress may keep arriving at the peer's reader, and anything the peer
    // flushes back over the dead link finds no hooks here and is dropped,
    // never routed into session state.
    this.transferHandlers = null;

    // Shut the egress down (queued frames flush FIFO before the writer closes).
    this.egress?.close();
    this.egress = null;

    // Abort all inflight handlers
    for (const controller of this.inflight) {
      controller.abort();
    }
    this.inflight.clear();

    // Detach reader listeners (data / close / end / error)
    if (this.socket) {
      if (this.onData) this.socket.removeListener("data", this.onData);
      if (this.onSocketClosed) this.socket.removeListener("close", this.onSocketClosed);
      if (this.onSocketEnded) this.socket.removeListener("end", this.onSocketEnded);
      if (this.onSocketError) {
        this.socket.removeListener("error", this.onSocketError);
        // The egress drain flushes asynchronously past disconnect(); a late
        // write-after-FIN makes Node destroy the socket and re-emit 'error'.
        // Keep a resident swallow on this intentionally dead connection so a
        // belated EPIPE/reset can never escape as an uncaught exception.
        this.socket.on("error", () => {});
      }
    }
    this.onData = null;
    this.onSocketClosed = null;
    this.onSocketEnded = null;
    this.onSocketError = null;
    this.decoder = null;

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

      egress.sendAcked(this.encodeEnvelope(envelope)).catch(() => {
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
      egress.sendFaf(this.encodeEnvelope(envelope));
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
      console.debug("[xacpp:socket] ignoring frame type 0x%s", frame.frameType.toString(16));
      return;
    }

    let envelope: XacppEnvelope;
    try {
      envelope = JSON.parse(textDecoder.decode(frame.payload));
    } catch {
      console.warn("[xacpp:socket] failed to parse frame payload as envelope JSON");
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
      await this.egress.sendAcked(this.encodeEnvelope(envelope));
    } catch (e) {
      console.warn("[xacpp:%s] failed to send response for request %s: %s", source, id, e instanceof Error ? e.message : String(e));
    }
  }

  /** Cleanup on reader exit: connection dead — fail fast subsequent sends, drop pending waiters. */
  private onReaderGone(): void {
    if (this.readerGone) return;
    this.readerGone = true;
    this._connected = false;
    // SPEC §5 disconnect discipline: the reader observed the connection's
    // death — detach the transfer hooks now (the earliest point this end
    // can know). Frames arriving afterwards find no mount and are dropped,
    // never routed into session state.
    this.transferHandlers = null;
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
    for (const [, slot] of this.pending) {
      if (slot.kind === "respond") {
        slot.reject(err);
      }
    }
    this.pending.clear();
  }
}
