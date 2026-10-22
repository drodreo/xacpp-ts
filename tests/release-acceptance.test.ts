/**
 * Release acceptance test matrix (SPEC v1.0, `docs/SPEC.md`) — xacpp-ts.
 *
 * Aligned with xacpp-rs `tests/release_acceptance_tests.rs` (28 ↔ 28 tests).
 * Three sections; every wire-level scenario drives frames through a real
 * transport peer (TCP socket or stdio duplex), never through serde
 * round-trips alone:
 *
 * - **A — functional coverage**: every command variant, negotiate
 *   capabilities, every response variant, the full event vocabulary, and
 *   the `DATA_BINARY` size matrix, each asserted field-by-field.
 * - **B — extreme scenarios**: dirty-stream triage, mid-frame disconnects,
 *   size boundaries (exact pass / +1 reject, both encode and forged-header
 *   decode), hostile decode-cap declarations, backpressure pause with
 *   control-frame exemption and recovery, CRC tamper at all three points,
 *   version refusal.
 * - **C — error-semantics ledger**: fault-tolerant behaviors (connection
 *   survives and keeps working) vs surfaced behaviors (error visible to
 *   the caller), each asserted end-to-end.
 */

import { describe, it, expect } from "vitest";
import * as net from "node:net";
import { PassThrough } from "node:stream";

import { SocketTransport } from "../src/carrier/socket";
import { StdioTransport } from "../src/carrier/stdio";
import { TransferSession } from "../src/transfer";
import {
  FrameDecoder,
  FrameError,
  FrameErrorCode,
  FrameType,
  CONTROL_FRAME_MAX,
  DATA_JSON_FRAME_MAX,
  DATA_BINARY_FRAME_MAX,
  DEFAULT_DECODE_BUFFER_CAP,
  encodeFrame,
  makeFrame,
  newTransactionId,
} from "../src/wire/frame";
import type { Frame } from "../src/wire/frame";
import { encodeAck } from "../src/wire/control";
import { newActivityEvent, newEvent } from "../src/wire/events";
import { genericCommand } from "../src/wire/commands";
import { acknowledge, genericResponse, XacppError } from "../src/wire/message";
import type { XacppEnvelope, XacppRequest, XacppResponse } from "../src/wire/message";
import type { Capabilities } from "../src/wire/capability";
import type { RequestHandler } from "../src/carrier/transport";
import type { SenderOptions } from "../src/reliability/transaction";

// =====================================================================================
// Shared helpers
// =====================================================================================

/** ASCII "XACP" frame magic (SPEC §3). */
const MAGIC = new Uint8Array([0x58, 0x41, 0x43, 0x50]);
/** Fixed header size (SPEC §3): magic 4 + ver 2 + type 2 + txid 16 + total 4 + seq 4 + len 4. */
const HEADER_SIZE = 36;

/** Deterministic payload pattern (not all-zero: exercises real bytes). */
function pattern(len: number): Uint8Array {
  const out = new Uint8Array(len);
  for (let i = 0; i < len; i++) out[i] = (i * 31 + (i >> 7)) & 0xff;
  return out;
}

/** Yield to the real event loop a few turns (socket IO callbacks are real macrotasks). */
async function settle(turns = 12): Promise<void> {
  for (let i = 0; i < turns; i++) await new Promise((r) => setImmediate(r));
}

/** Poll a condition with real time until it holds or the timeout elapses. */
async function waitFor(cond: () => boolean, what: string, timeoutMs = 10_000): Promise<void> {
  const start = performance.now();
  while (!cond()) {
    if (performance.now() - start > timeoutMs) throw new Error(`timeout waiting for: ${what}`);
    await settle(4);
  }
}

const textEncoder = new TextEncoder();

/** Wrap one envelope into a single-frame DATA_JSON wire transaction (SPEC §3/§4). */
function envelopeFrame(envelope: XacppEnvelope): Uint8Array {
  return encodeFrame(makeFrame(FrameType.DATA_JSON, textEncoder.encode(JSON.stringify(envelope))));
}

/** Convenience: request envelope → wire bytes. */
function requestFrame(id: string, payload: XacppRequest): Uint8Array {
  return envelopeFrame({ type: "request", id, payload });
}

/** Forge a 36-byte header declaring `frameLen` payload bytes (no body needed). */
function forgedHeader(frameType: number, frameLen: number, versionMajor = 1): Uint8Array {
  const out = new Uint8Array(HEADER_SIZE);
  const dv = new DataView(out.buffer);
  out.set(MAGIC, 0);
  dv.setUint8(4, versionMajor);
  dv.setUint8(5, 0);
  dv.setUint16(6, frameType, false);
  const txid = Buffer.from(newTransactionId(), "hex");
  out.set(txid, 8);
  dv.setUint32(24, frameLen, false); // totalLen
  dv.setUint32(28, 0, false); // seq
  dv.setUint32(32, frameLen, false); // frameLen
  return out;
}

/** Tamper one byte of an already-encoded wire frame. */
function tamper(wire: Uint8Array, offset: number): Uint8Array {
  const out = new Uint8Array(wire);
  out[offset] ^= 0xff;
  return out;
}

interface WireRig {
  /** Test-owned raw client socket (write half for injections). */
  raw: net.Socket;
  /** The transport under test (server side of the TCP pair). */
  server: SocketTransport;
  /** Frames decoded from the server's outbound byte stream. */
  rx: Frame[];
  isServerClosed(): boolean;
  write(chunk: Uint8Array): void;
  cleanup(): void;
}

/**
 * Wire rig: a real TCP pair where the test drives the raw client byte
 * stream and the server side is a live `SocketTransport` peer.
 */
async function wireRig(handler: RequestHandler, session?: TransferSession): Promise<WireRig> {
  const listener = net.createServer();
  await new Promise<void>((resolve) => listener.listen(0, "127.0.0.1", resolve));
  const addr = listener.address() as net.AddressInfo;
  const ready = new Promise<SocketTransport>((resolve) => {
    listener.on("connection", (sock) => {
      const t = new SocketTransport(sock);
      t.onRequest(handler);
      t.connect().then(() => {
        if (session) session.wire(t);
        resolve(t);
      });
    });
  });
  const raw = net.connect(addr.port, "127.0.0.1");
  // Fatal-scenario rigs (CRC tamper / version refusal) make the server tear
  // the socket down — the client side observes that as ECONNRESET. Swallow
  // it: the tests assert the close via onClosed, not via the raw stream.
  raw.on("error", () => {});
  await new Promise<void>((resolve) => raw.once("connect", resolve));
  const server = await ready;

  let closed = false;
  server.onClosed(() => {
    closed = true;
  });

  const decoder = new FrameDecoder();
  const rx: Frame[] = [];
  raw.on("data", (chunk: Buffer) => {
    try {
      rx.push(...decoder.push(new Uint8Array(chunk)));
    } catch {
      // Server declared the stream dead — later bytes are moot.
    }
  });

  return {
    raw,
    server,
    rx,
    isServerClosed: () => closed,
    write: (chunk) => raw.write(Buffer.from(chunk)),
    cleanup: () => {
      raw.destroy();
      listener.close();
    },
  };
}

interface ReliablePair {
  client: SocketTransport;
  server: SocketTransport;
  sessionClient: TransferSession;
  sessionServer: TransferSession;
  delivered: { transactionId: string; payload: Uint8Array }[];
  completions: { transactionId: string; confirmed: boolean }[];
  cleanup(): void;
}

/** ts↔ts TCP pair, both sides wired to transfer sessions (receiver sink + completion log). */
async function reliablePair(senderOpts?: SenderOptions): Promise<ReliablePair> {
  const listener = net.createServer();
  await new Promise<void>((resolve) => listener.listen(0, "127.0.0.1", resolve));
  const addr = listener.address() as net.AddressInfo;

  const delivered: { transactionId: string; payload: Uint8Array }[] = [];
  const sessionServer = new TransferSession();
  sessionServer.onBinary = (transactionId, payload) => delivered.push({ transactionId, payload });

  const connected = new Promise<SocketTransport>((resolve) => {
    listener.on("connection", (sock) => {
      const t = new SocketTransport(sock);
      t.onRequest(async () => acknowledge());
      t.connect().then(() => {
        sessionServer.wire(t);
        resolve(t);
      });
    });
  });

  const sessionClient = new TransferSession({ sender: senderOpts });
  const completions: { transactionId: string; confirmed: boolean }[] = [];
  const client = SocketTransport.connectTo(addr.port, "127.0.0.1");
  client.onRequest(async () => acknowledge());
  await client.connect();
  sessionClient.wire(client);
  const server = await connected;

  return {
    client,
    server,
    sessionClient,
    sessionServer,
    delivered,
    completions,
    cleanup: () => listener.close(),
  };
}

/** Send one binary transaction and wait for bit-exact delivery + sender confirmation. */
async function roundTripBinary(
  pair: ReliablePair,
  payload: Uint8Array,
  what: string,
  timeoutMs = 30_000,
): Promise<void> {
  const before = pair.delivered.length;
  const txid = pair.sessionClient.sendBinary(payload, (r) => pair.completions.push(r));
  await waitFor(() => pair.delivered.length === before + 1, `${what}: delivery`, timeoutMs);
  expect(pair.delivered[before].transactionId).toBe(txid);
  expect(Buffer.from(pair.delivered[before].payload).equals(Buffer.from(payload))).toBe(true);
  await waitFor(() => pair.completions.some((c) => c.transactionId === txid), `${what}: confirmed`, timeoutMs);
  expect(pair.completions.find((c) => c.transactionId === txid)?.confirmed).toBe(true);
}

/** Full event vocabulary (SPEC business events; contract-synced with xacpp-rs). */
const EVENT_NAMES = [
  "content_delta",
  "content_part",
  "think",
  "info",
  "warn",
  "error",
  "notify",
  "activity_start",
  "activity_updates",
  "activity_done",
  "activity_aborted",
  "tool_use",
  "tool_result",
  "security_alert",
  "upload",
  "pair_complete",
  "complete",
];

/** Capability sample shared by the negotiate/establish scenarios. */
const SAMPLE_CAPS: Capabilities = {
  commands: [{ name: "new_activity" }, { name: "invoke_activity" }],
  produceEvents: [{ name: "content_delta" }, { name: "complete" }],
  acceptEvents: [{ name: "notify" }],
};

// =====================================================================================
// A — functional coverage
// =====================================================================================

describe("A — functional coverage", () => {
  it("a_command_variants_wire_roundtrip", async () => {
    const captured: XacppRequest[] = [];
    const rig = await wireRig(async (_sid, payload) => {
      captured.push(payload);
      return acknowledge();
    });

    const requests: { id: string; payload: XacppRequest }[] = [
      { id: "c0", payload: { kind: "command", payload: { establish: {} } } },
      { id: "c1", payload: { kind: "command", payload: { establish: { credentials: "creds-✓-123" } } } },
      { id: "c2", payload: { kind: "command", payload: "establish_confirm" } },
      { id: "c3", payload: { kind: "command", payload: { negotiate: { capabilities: SAMPLE_CAPS } } } },
      { id: "c4", payload: { kind: "command", payload: genericCommand("plain_cmd", null) } },
      {
        id: "c5",
        payload: { kind: "command", payload: genericCommand("activity_cmd", { n: 1 }, { id: "act-src" }) },
      },
      {
        id: "c6",
        payload: { kind: "command", payload: genericCommand("list_activity", { pageNum: 1, pageSize: 10 }) },
      },
      {
        id: "c7",
        payload: {
          kind: "command",
          payload: genericCommand("set_activity_mode", { activity: "act-1", mode: "auto" }),
        },
      },
    ];
    for (const r of requests) rig.write(requestFrame(r.id, r.payload));

    await waitFor(() => captured.length === 8, "every command arrived exactly once");

    // Field-by-field on the receiving side.
    expect(captured[0]).toEqual({ kind: "command", payload: { establish: {} } });
    expect(captured[1]).toEqual({
      kind: "command",
      payload: { establish: { credentials: "creds-✓-123" } },
    });
    expect(captured[2]).toEqual({ kind: "command", payload: "establish_confirm" });
    expect(captured[3]).toEqual({ kind: "command", payload: { negotiate: { capabilities: SAMPLE_CAPS } } });
    expect(captured[4]).toEqual({
      kind: "command",
      payload: { generic: { name: "plain_cmd", arguments: null } },
    });
    expect(captured[5]).toEqual({
      kind: "command",
      payload: { generic: { name: "activity_cmd", arguments: { n: 1 }, activity: { id: "act-src" } } },
    });
    expect(captured[6]).toEqual({
      kind: "command",
      payload: { generic: { name: "list_activity", arguments: { pageNum: 1, pageSize: 10 } } },
    });
    expect(captured[7]).toEqual({
      kind: "command",
      payload: { generic: { name: "set_activity_mode", arguments: { activity: "act-1", mode: "auto" } } },
    });

    // Every command got its acknowledge response back over the wire.
    await waitFor(() => rig.rx.length >= 8, "responses decoded");
    const responseIds = rig.rx
      .filter((f) => f.frameType === FrameType.DATA_JSON)
      .map((f) => JSON.parse(new TextDecoder().decode(f.payload)))
      .filter((e) => e.type === "response")
      .map((e) => e.id);
    expect(responseIds.sort()).toEqual(requests.map((r) => r.id).sort());

    rig.cleanup();
  });

  it("a_negotiate_capabilities_wire_fields", async () => {
    const rig = await wireRig(async (_sid, payload) => {
      if (payload.kind === "command" && typeof payload.payload !== "string" && "negotiate" in payload.payload) {
        return { kind: "negotiated", capabilities: payload.payload.negotiate.capabilities };
      }
      return acknowledge();
    });

    rig.write(requestFrame("neg-1", { kind: "command", payload: { negotiate: { capabilities: SAMPLE_CAPS } } }));

    await waitFor(() => rig.rx.some((f) => f.frameType === FrameType.DATA_JSON), "negotiated response");
    // The connection-owned session emits heartbeats; take the first envelope frame, not rx[0].
    const env = JSON.parse(
      new TextDecoder().decode(rig.rx.find((f) => f.frameType === FrameType.DATA_JSON)!.payload),
    ) as {
      type: string;
      id: string;
      payload: XacppResponse;
    };
    expect(env.type).toBe("response");
    expect(env.id).toBe("neg-1");
    expect(env.payload.kind).toBe("negotiated");
    const caps = (env.payload as { kind: "negotiated"; capabilities: Capabilities }).capabilities;
    expect(caps.commands).toEqual(SAMPLE_CAPS.commands);
    expect(caps.produceEvents).toEqual(SAMPLE_CAPS.produceEvents);
    expect(caps.acceptEvents).toEqual(SAMPLE_CAPS.acceptEvents);

    rig.cleanup();
  });

  it("a_response_variants_wire_roundtrip", async () => {
    const rig = await wireRig(async (_sid, payload) => {
      if (payload.kind !== "command" || typeof payload.payload === "string" || !("generic" in payload.payload))
        return acknowledge();
      switch (payload.payload.generic.name) {
        case "give-negotiated":
          return { kind: "negotiated", capabilities: SAMPLE_CAPS };
        case "give-established":
          return { kind: "established", sessionId: "sess-1", credentials: "creds-1" };
        case "give-prepare":
          return { kind: "establish_prepare", challenge: "challenge-✓" };
        case "give-reject":
          return { kind: "establish_reject", reason: "not allowed ✗" };
        case "give-generic":
          return genericResponse("echoed", { nested: { list: [1, 2, 3] } });
        case "give-error":
          return { kind: "error", code: "boom", message: "exploded" };
        default:
          return acknowledge();
      }
    });

    const probes = [
      "give-negotiated",
      "give-established",
      "give-prepare",
      "give-reject",
      "give-generic",
      "give-error",
    ];
    probes.forEach((name, i) =>
      rig.write(requestFrame(`r${i}`, { kind: "command", payload: genericCommand(name, null) })),
    );

    await waitFor(
      () => rig.rx.filter((f) => f.frameType === FrameType.DATA_JSON).length >= 6,
      "all six responses decoded",
    );
    const responses = rig.rx
      .filter((f) => f.frameType === FrameType.DATA_JSON)
      .map((f) => JSON.parse(new TextDecoder().decode(f.payload)) as { type: string; id: string; payload: XacppResponse })
      .filter((e) => e.type === "response");
    expect(responses).toHaveLength(6);

    const byId = new Map(responses.map((e) => [e.id, e.payload]));
    expect(byId.get("r0")).toEqual({ kind: "negotiated", capabilities: SAMPLE_CAPS });
    expect(byId.get("r1")).toEqual({ kind: "established", sessionId: "sess-1", credentials: "creds-1" });
    expect(byId.get("r2")).toEqual({ kind: "establish_prepare", challenge: "challenge-✓" });
    expect(byId.get("r3")).toEqual({ kind: "establish_reject", reason: "not allowed ✗" });
    expect(byId.get("r4")).toEqual({
      kind: "generic",
      name: "echoed",
      data: { nested: { list: [1, 2, 3] } },
    });
    expect(byId.get("r5")).toEqual({ kind: "error", code: "boom", message: "exploded" });

    rig.cleanup();
  });

  it("a_event_vocabulary_request_event_wire", async () => {
    const captured: XacppRequest[] = [];
    const rig = await wireRig(async (_sid, payload) => {
      captured.push(payload);
      return acknowledge();
    });

    EVENT_NAMES.forEach((name, i) => {
      rig.write(
        requestFrame(`ev-${i}`, {
          kind: "event",
          payload: newActivityEvent("act-1", newEvent(name, { i, unicode: "事件-🌟" })),
        }),
      );
    });

    await waitFor(() => captured.length === EVENT_NAMES.length, "every event arrived exactly once");

    // One map per name: server-side handling is concurrent, arrival order is not asserted here.
    const byName = new Map<string, XacppRequest>();
    for (const req of captured) {
      if (req.kind !== "event") continue;
      byName.set(req.payload.event.name, req);
    }
    expect(byName.size).toBe(EVENT_NAMES.length);
    for (const [i, name] of EVENT_NAMES.entries()) {
      const req = byName.get(name);
      expect(req, `event ${name} delivered`).toBeDefined();
      if (!req || req.kind !== "event") continue;
      expect(req.payload.activity).toEqual({ id: "act-1" });
      expect(req.payload.event.data).toEqual({ i, unicode: "事件-🌟" });
    }

    await waitFor(() => rig.rx.filter((f) => f.frameType === FrameType.DATA_JSON).length >= 17, "acks decoded");
    rig.cleanup();
  });

  it("a_event_send_event_faf_wire", async () => {
    const captured: XacppRequest[] = [];
    const listener = net.createServer();
    await new Promise<void>((resolve) => listener.listen(0, "127.0.0.1", resolve));
    const addr = listener.address() as net.AddressInfo;
    const ready = new Promise<void>((resolve) => {
      listener.on("connection", (sock) => {
        const t = new SocketTransport(sock);
        t.onRequest(async (_sid, payload) => {
          captured.push(payload);
          return acknowledge();
        });
        t.connect().then(() => resolve());
      });
    });

    const client = SocketTransport.connectTo(addr.port, "127.0.0.1");
    client.onRequest(async () => acknowledge());
    await client.connect();
    await ready;

    // Fire-and-forget: enqueue-only submitter; the single FIFO egress keeps
    // wire order (server-side handler concurrency may reorder callbacks, so
    // arrival is asserted as a set — wire ordering is covered by
    // socket-concurrent.test.ts).
    for (const [i, name] of EVENT_NAMES.entries()) {
      client.sendFaf(null, { kind: "event", payload: newActivityEvent("act-1", newEvent(name, { i })) });
    }

    await waitFor(() => captured.length === EVENT_NAMES.length, "all faf events arrived");
    const names = new Set(
      captured.filter((r) => r.kind === "event").map((r) => (r.kind === "event" ? r.payload.event.name : "")),
    );
    expect(names).toEqual(new Set(EVENT_NAMES));

    await client.disconnect();
    listener.close();
  });

  it("a_binary_matrix_size_tiers_wire", async () => {
    const pair = await reliablePair();
    for (const size of [100 * 1024, 1024 * 1024, 5 * 1024 * 1024]) {
      await roundTripBinary(pair, pattern(size), `matrix ${size}B`);
    }
    expect(pair.sessionClient.copyCount).toBe(0);
    expect(pair.sessionServer.pendingCount).toBe(0);
    pair.cleanup();
  }, 60_000);

  it("a_binary_16mib_exact_ceiling_wire", async () => {
    const pair = await reliablePair();
    // Exactly the per-frame ceiling as a single-frame transaction.
    await roundTripBinary(pair, pattern(DATA_BINARY_FRAME_MAX), "16MiB exact ceiling", 60_000);
    pair.cleanup();
  }, 90_000);

  it("a_binary_tail_frame_boundary_wire", async () => {
    // Working frames of 1 KiB: 3 full frames + a 777-byte tail (non-divisible).
    const pair = await reliablePair({ workingFrameBytes: 1024 });
    await roundTripBinary(pair, pattern(3 * 1024 + 777), "tail frame boundary");
    pair.cleanup();
  }, 30_000);

  it("a_binary_oversize_frame_encode_rejected", () => {
    const oversized = pattern(DATA_BINARY_FRAME_MAX + 1);
    expect(() =>
      encodeFrame(makeFrame(FrameType.DATA_BINARY, oversized, { transactionId: newTransactionId() })),
    ).toThrow(FrameError);
    try {
      encodeFrame(makeFrame(FrameType.DATA_BINARY, oversized));
    } catch (e) {
      expect((e as FrameError).code).toBe(FrameErrorCode.FrameTooLarge);
    }
  });
});

// =====================================================================================
// B — extreme scenarios
// =====================================================================================

describe("B — extreme scenarios", () => {
  it("b_dirty_stream_garbage_prefix_recovers_wire", async () => {
    const rig = await wireRig(async () => genericResponse("acknowledge", null));

    const junk = new Uint8Array(0);
    const parts: Uint8Array[] = [];
    parts.push(textEncoder.encode("\x00\x01\x02 garbage, no magic here\n"));
    parts.push(new Uint8Array(64).fill(0xff));
    parts.push(textEncoder.encode("trailing text line\n"));
    const merged = parts.reduce((acc, p) => {
      const out = new Uint8Array(acc.length + p.length);
      out.set(acc);
      out.set(p, acc.length);
      return out;
    }, junk);
    rig.write(merged);
    await settle(6);

    expect(rig.isServerClosed()).toBe(false);

    rig.write(requestFrame("after-garbage", { kind: "command", payload: genericCommand("ping", null) }));
    await waitFor(() => rig.rx.some((f) => f.frameType === FrameType.DATA_JSON), "peer responds after garbage");
    const env = JSON.parse(
      new TextDecoder().decode(rig.rx.find((f) => f.frameType === FrameType.DATA_JSON)!.payload),
    ) as { type: string; id: string };
    expect(env.type).toBe("response");
    expect(env.id).toBe("after-garbage");

    rig.cleanup();
  });

  it("b_dirty_stream_pseudo_magic_payload_inert_wire", async () => {
    const rig = await wireRig(async () => genericResponse("acknowledge", null));

    // A DATA_BINARY transaction whose payload is stuffed with frame magic
    // bytes: framing is driven by the outer header, so the inner magic is
    // inert. (The frame reaches the session's reassembler, which drops it —
    // no transaction id match — and the connection stays alive.)
    const payload = new Uint8Array(256);
    for (let i = 0; i < payload.length; i += 4) payload.set(MAGIC, i);
    rig.write(encodeFrame(makeFrame(FrameType.DATA_BINARY, payload)));
    await settle(6);
    expect(rig.isServerClosed()).toBe(false);

    rig.write(requestFrame("after-pseudo", { kind: "command", payload: genericCommand("ping", null) }));
    await waitFor(() => rig.rx.some((f) => f.frameType === FrameType.DATA_JSON), "framing unaffected");
    const env = JSON.parse(
      new TextDecoder().decode(rig.rx.find((f) => f.frameType === FrameType.DATA_JSON)!.payload),
    ) as { id: string };
    expect(env.id).toBe("after-pseudo");

    rig.cleanup();
  });

  it("b_dirty_stream_stdio_text_interleaved_wire", async () => {
    // Stdio transport over PassThrough duplexes; text garbage injected ahead
    // of a valid frame must be skipped without killing the peer.
    const toServer = new PassThrough();
    const fromServer = new PassThrough();
    const server = new StdioTransport(fromServer, toServer);
    server.onRequest(async () => genericResponse("acknowledge", null));
    let closed = false;
    server.onClosed(() => {
      closed = true;
    });
    await server.connect();

    const decoder = new FrameDecoder();
    const rx: Frame[] = [];
    fromServer.on("data", (chunk: Buffer) => {
      try {
        rx.push(...decoder.push(new Uint8Array(chunk)));
      } catch {
        /* fatal — moot */
      }
    });

    toServer.write(textEncoder.encode("plain text line interleaved\n"));
    toServer.write(new Uint8Array(32).fill(0x00));
    await settle(6);
    expect(closed).toBe(false);

    toServer.write(requestFrame("after-text", { kind: "command", payload: genericCommand("ping", null) }));
    await waitFor(() => rx.some((f) => f.frameType === FrameType.DATA_JSON), "peer responds after text garbage");
    const env = JSON.parse(
      new TextDecoder().decode(rx.find((f) => f.frameType === FrameType.DATA_JSON)!.payload),
    ) as { id: string };
    expect(env.id).toBe("after-text");

    await server.disconnect();
  });

  it("b_half_frame_truncation_connection_fate", async () => {
    // Fate 1: a truncated frame suspends the decoder — the connection stays
    // alive waiting for the remainder, and completes once it arrives.
    {
      const rig = await wireRig(async () => genericResponse("acknowledge", null));
      const full = requestFrame("never-lost", { kind: "command", payload: genericCommand("ping", null) });
      const cut = Math.floor((full.length * 6) / 10);
      rig.write(full.subarray(0, cut));
      await settle(6);
      expect(rig.isServerClosed()).toBe(false);

      rig.write(full.subarray(cut));
      await waitFor(() => rig.rx.some((f) => f.frameType === FrameType.DATA_JSON), "frame completes after remainder");
      const env = JSON.parse(new TextDecoder().decode(rig.rx[0].payload)) as { id: string };
      expect(env.id).toBe("never-lost");
      rig.cleanup();
    }

    // Fate 2: the sender vanishes mid-frame — the half frame never
    // completes, and the peer connection dies with the stream.
    {
      const rig = await wireRig(async () => genericResponse("acknowledge", null));
      const full = requestFrame("abandoned", { kind: "command", payload: genericCommand("ping", null) });
      rig.write(full.subarray(0, Math.floor(full.length / 2)));
      await settle(6);
      rig.raw.destroy();
      await waitFor(() => rig.isServerClosed(), "half frame + vanished peer kills the connection", 5000);
      rig.cleanup();
    }
  });

  it("b_size_limits_exact_pass_and_plus_one_reject", () => {
    // Encode side: each per-type tier accepts its exact ceiling and rejects
    // one byte more.
    const tiers: { frameType: number; limit: number }[] = [
      { frameType: FrameType.NEGOTIATE, limit: CONTROL_FRAME_MAX },
      { frameType: FrameType.DATA_JSON, limit: DATA_JSON_FRAME_MAX },
      { frameType: FrameType.DATA_BINARY, limit: DATA_BINARY_FRAME_MAX },
    ];
    for (const { frameType, limit } of tiers) {
      const exact = new Uint8Array(limit); // all-zero is fine for sizing
      expect(() => encodeFrame(makeFrame(frameType, exact))).not.toThrow();
      const over = new Uint8Array(limit + 1);
      expect(() => encodeFrame(makeFrame(frameType, over))).toThrow(FrameError);
      try {
        encodeFrame(makeFrame(frameType, over));
      } catch (e) {
        expect((e as FrameError).code).toBe(FrameErrorCode.FrameTooLarge);
      }
    }

    // Decode side: a forged header declaring one byte over any per-type
    // limit is rejected at header validation — 36 bytes suffice, no body.
    for (const { frameType, limit } of tiers) {
      const decoder = new FrameDecoder();
      expect(() => decoder.push(forgedHeader(frameType, limit + 1))).toThrow(FrameError);
      try {
        decoder.push(forgedHeader(frameType, limit + 1));
      } catch (e) {
        expect((e as FrameError).code).toBe(FrameErrorCode.FrameTooLarge);
      }
    }
  });

  it("b_hostile_unknown_cap_declared_rejected_wire", async () => {
    // Header-only injection against a live peer: the decode-buffer cap fires
    // at 36 bytes, so memory exposure is bounded by construction.
    const rig = await wireRig(async () => genericResponse("acknowledge", null));
    rig.write(forgedHeader(0x7fff, DEFAULT_DECODE_BUFFER_CAP + 1));
    await waitFor(() => rig.isServerClosed(), "hostile cap declaration must kill the connection");
    rig.cleanup();

    // Decoder-level: the same header surfaces FrameTooLarge with its type.
    const decoder = new FrameDecoder();
    try {
      decoder.push(forgedHeader(0x7fff, DEFAULT_DECODE_BUFFER_CAP + 1));
      expect.unreachable("expected FrameTooLarge");
    } catch (e) {
      expect(e).toBeInstanceOf(FrameError);
      expect((e as FrameError).code).toBe(FrameErrorCode.FrameTooLarge);
    }

    // A shrunken cap rejects far smaller hostile declarations too —
    // including unknown frame types (SPEC §3: cap applies regardless of type).
    const small = new FrameDecoder({ maxDecodeBuffer: 4096 });
    expect(() => small.push(forgedHeader(0x7fff, 4096))).toThrow(FrameError);
  });

  it("b_crc_tamper_payload_kills_connection", async () => {
    const rig = await wireRig(async () => genericResponse("acknowledge", null));
    const wire = requestFrame("tampered", { kind: "command", payload: genericCommand("ping", null) });
    rig.write(tamper(wire, HEADER_SIZE)); // first payload byte
    await waitFor(() => rig.isServerClosed(), "CRC tamper (payload) must be fatal");
    rig.cleanup();
  });

  it("b_crc_tamper_header_kills_connection", async () => {
    const rig = await wireRig(async () => genericResponse("acknowledge", null));
    const wire = requestFrame("tampered", { kind: "command", payload: genericCommand("ping", null) });
    rig.write(tamper(wire, 8)); // first transaction-id byte
    await waitFor(() => rig.isServerClosed(), "CRC tamper (header) must be fatal");
    rig.cleanup();
  });

  it("b_crc_tamper_trailer_kills_connection", async () => {
    const rig = await wireRig(async () => genericResponse("acknowledge", null));
    const wire = requestFrame("tampered", { kind: "command", payload: genericCommand("ping", null) });
    rig.write(tamper(wire, wire.length - 4)); // first CRC trailer byte
    await waitFor(() => rig.isServerClosed(), "CRC tamper (trailer) must be fatal");
    rig.cleanup();
  });

  it("b_version_major_mismatch_refuses_wire", async () => {
    const rig = await wireRig(async () => genericResponse("acknowledge", null));
    const payload = textEncoder.encode(JSON.stringify({
      type: "request",
      id: "v2",
      payload: { kind: "command", payload: genericCommand("ping", null) },
    }));
    rig.write(encodeFrame(makeFrame(FrameType.DATA_JSON, payload, { versionMajor: 2 })));
    await waitFor(() => rig.isServerClosed(), "major version mismatch must refuse the connection");
    rig.cleanup();
  });

  it("b_backpressure_pause_control_exempt_and_recovery", () => {
    // Session contract surface, manual clock via explicit tick(now): small
    // cap so one transaction blows past it, RTO disabled so only ACK
    // feedback moves the sender. (The binding's write routing on top of
    // this surface is covered by reliability-transport.test.ts.)
    let now = 1_000_000;

    const session = new TransferSession({
      sender: {
        workingFrameBytes: 1024,
        backpressureCapBytes: 4096,
        initialTimeoutMs: 2 ** 40,
        maxTimeoutMs: 2 ** 41,
      },
      heartbeatIntervalMs: 30_000,
      silenceTimeoutMs: 2 ** 40,
    });
    const decodeAll = (chunks: Uint8Array[]): Frame[] => {
      const decoder = new FrameDecoder();
      return chunks.flatMap((b) => decoder.push(b));
    };

    // 1. Open while unpaused: the transaction is "in flight" — all 64 frames
    // go out even though they push usage (64 KiB) far past the 4 KiB cap.
    // This is the anti-deadlock invariant: the wire always keeps flowing so
    // ACKs can bring the usage back down.
    const payloadA = pattern(64 * 1024);
    const completionA: { transactionId: string; confirmed: boolean }[] = [];
    const opened = session.openSend(payloadA, now, (r) => completionA.push(r));
    expect(opened.frames).toHaveLength(64);
    expect(session.paused).toBe(true); // usage 64 KiB > cap 4 KiB

    // 2. A second transaction opened while paused is held (deferred), and
    // its retained copy retransmits on any future connection.
    const payloadB = pattern(2 * 1024);
    const openedB = session.openSend(payloadB, now);
    expect(openedB.frames).toHaveLength(0);
    expect(session.copyCount).toBe(2); // both copies retained

    // 3. Control-frame exemption: the clock keeps producing heartbeats while
    // the data path is paused.
    now += 30_000;
    const tickPaused = session.tick(now);
    expect(decodeAll(tickPaused.controlFrames).some((f) => f.frameType === FrameType.HEARTBEAT)).toBe(true);
    expect(tickPaused.dataFrames).toHaveLength(0); // nothing data-side while paused

    // 4. Recovery: cumulative ACK for the whole first transaction drops
    // usage to zero, unpausing the sender — and the ACK dispatch itself
    // flushes the deferred transaction in the same outcome.
    const ackFrame = makeFrame(
      FrameType.ACK,
      encodeAck([
        {
          transactionId: opened.transactionId,
          highestContiguousSeq: 63,
          receivedFrames: 64,
        },
      ]),
    );
    const ackOutcome = session.onControlFrame(ackFrame, now);
    expect(session.paused).toBe(false);
    expect(completionA).toEqual([{ transactionId: opened.transactionId, confirmed: true }]);
    expect(ackOutcome.dataFrames).toHaveLength(2); // 2 KiB / 1 KiB working frames
    expect(ackOutcome.dataFrames.every((f) => f.transactionId === openedB.transactionId)).toBe(true);

    // 5. With the queue drained, subsequent ticks produce no stray data.
    const tickSettled = session.tick(now + 1);
    expect(tickSettled.dataFrames).toHaveLength(0);

    // 6. Final ACK confirms the recovered transaction too.
    const ackB = makeFrame(
      FrameType.ACK,
      encodeAck([
        {
          transactionId: openedB.transactionId,
          highestContiguousSeq: 1,
          receivedFrames: 2,
        },
      ]),
    );
    session.onControlFrame(ackB, now + 1);
    expect(session.copyCount).toBe(0);
  });
});

// =====================================================================================
// C — error-semantics ledger
// =====================================================================================

describe("C — error-semantics ledger", () => {
  it("c_unknown_frame_type_crc_valid_ignored_wire", async () => {
    const rig = await wireRig(async () => genericResponse("acknowledge", null));

    const wire = encodeFrame(makeFrame(0x7fff, pattern(64)));
    rig.write(wire);
    await settle(6);
    expect(rig.isServerClosed()).toBe(false);

    rig.write(requestFrame("after-unknown", { kind: "command", payload: genericCommand("ping", null) }));
    await waitFor(() => rig.rx.some((f) => f.frameType === FrameType.DATA_JSON), "connection survives");
    expect((JSON.parse(new TextDecoder().decode(rig.rx[0].payload)) as { id: string }).id).toBe("after-unknown");
    rig.cleanup();
  });

  it("c_higher_minor_version_tolerated_wire", async () => {
    const rig = await wireRig(async () => genericResponse("acknowledge", null));

    const payload = textEncoder.encode(JSON.stringify({
      type: "request",
      id: "minor-99",
      payload: { kind: "command", payload: genericCommand("ping", null) },
    }));
    rig.write(encodeFrame(makeFrame(FrameType.DATA_JSON, payload, { versionMinor: 99 })));
    await waitFor(() => rig.rx.some((f) => f.frameType === FrameType.DATA_JSON), "higher minor accepted");
    const env = JSON.parse(new TextDecoder().decode(rig.rx[0].payload)) as { id: string };
    expect(env.id).toBe("minor-99");
    rig.cleanup();
  });

  it("c_duplicate_binary_frames_idempotent_incomplete_wire", async () => {
    const delivered: { transactionId: string; payload: Uint8Array }[] = [];
    const session = new TransferSession();
    session.onBinary = (transactionId, payload) => delivered.push({ transactionId, payload });
    const rig = await wireRig(async () => genericResponse("acknowledge", null), session);

    // Hand-craft a 2-frame transaction (2×1 KiB frames, totalLen 2048) and
    // inject duplicates of seq 0 before the completing frame.
    const txid = newTransactionId();
    const body = pattern(2048);
    const seq0 = encodeFrame(
      makeFrame(FrameType.DATA_BINARY, body.subarray(0, 1024), { transactionId: txid, totalLen: 2048, seq: 0 }),
    );
    const seq1 = encodeFrame(
      makeFrame(FrameType.DATA_BINARY, body.subarray(1024), { transactionId: txid, totalLen: 2048, seq: 1 }),
    );

    // Retransmission copies of seq 0 never deliver early (duplicate, idempotent).
    rig.write(seq0);
    rig.write(seq0);
    rig.write(seq0);
    await settle(20);
    expect(delivered, "replayed seq0 alone must not deliver early").toHaveLength(0);

    // The completing frame delivers the transaction exactly once.
    rig.write(seq1);
    await waitFor(() => delivered.length === 1, "exactly-once delivery");
    expect(Buffer.from(delivered[0].payload).equals(Buffer.from(body))).toBe(true);

    // The connection keeps working: cumulative ACK flows back on the wire.
    await waitFor(() => rig.rx.some((f) => f.frameType === FrameType.ACK), "final ACK visible on the wire");
    expect(rig.isServerClosed()).toBe(false);

    // The completed-transaction replay guard lives at the fetch extension
    // layer (ext-fetch contract matrix 10): the bare kernel surface makes
    // no terminal-replay promise (SPEC §8.4).

    rig.cleanup();
  });

  it("c_invalid_envelope_payload_skipped_wire", async () => {
    const rig = await wireRig(async () => genericResponse("acknowledge", null));

    rig.write(encodeFrame(makeFrame(FrameType.DATA_JSON, textEncoder.encode("this is not json at all"))));
    await settle(6);
    expect(rig.isServerClosed()).toBe(false);

    rig.write(requestFrame("after-invalid", { kind: "command", payload: genericCommand("ping", null) }));
    await waitFor(() => rig.rx.some((f) => f.frameType === FrameType.DATA_JSON), "peer still responds");
    expect((JSON.parse(new TextDecoder().decode(rig.rx[0].payload)) as { id: string }).id).toBe("after-invalid");
    rig.cleanup();
  });

  it("c_decoder_fatal_variants_surfaced", () => {
    // UnsupportedVersion: wrong major in the header.
    const badVersion = new FrameDecoder();
    expect(() => badVersion.push(forgedHeader(FrameType.DATA_JSON, 0, 2))).toThrow(FrameError);
    try {
      badVersion.push(forgedHeader(FrameType.DATA_JSON, 0, 2));
    } catch (e) {
      expect((e as FrameError).code).toBe(FrameErrorCode.UnsupportedVersion);
    }

    // CrcMismatch: valid header, flipped trailer byte.
    const wire = requestFrame("crc", { kind: "command", payload: genericCommand("ping", null) });
    const badCrc = new FrameDecoder();
    expect(() => badCrc.push(tamper(wire, wire.length - 1))).toThrow(FrameError);
    try {
      badCrc.push(tamper(wire, wire.length - 1));
    } catch (e) {
      expect((e as FrameError).code).toBe(FrameErrorCode.CrcMismatch);
    }

    // FrameTooLarge: forged header over the per-type limit.
    const badSize = new FrameDecoder();
    expect(() => badSize.push(forgedHeader(FrameType.DATA_JSON, DATA_JSON_FRAME_MAX + 1))).toThrow(FrameError);
    try {
      badSize.push(forgedHeader(FrameType.DATA_JSON, DATA_JSON_FRAME_MAX + 1));
    } catch (e) {
      expect((e as FrameError).code).toBe(FrameErrorCode.FrameTooLarge);
    }
  });

  it("c_decoder_drained_after_fatal_no_smear", () => {
    // After a fatal CRC error the decoder must never emit frames again —
    // the failed frame's body cannot smear into subsequent framing. (The
    // ts decoder is single-connection: the transport discards it together
    // with the stream; this pins the no-smear guarantee at module level.)
    const decoder = new FrameDecoder();
    const bad = tamper(requestFrame("bad", { kind: "command", payload: genericCommand("ping", null) }), HEADER_SIZE);
    expect(() => decoder.push(bad)).toThrow(FrameError);

    // Follow-up bytes (a valid frame appended behind the poisoned one) must
    // not let the parser skip ahead and produce a bogus frame.
    const good = requestFrame("good", { kind: "command", payload: genericCommand("ping", null) });
    const merged = new Uint8Array(bad.length + good.length);
    merged.set(bad);
    merged.set(good, bad.length);
    const smeared = new FrameDecoder();
    expect(() => smeared.push(merged)).toThrow(FrameError);

    // And a decoder that already failed keeps refusing (no silent recovery).
    expect(() => decoder.push(good)).toThrow(FrameError);
  });

  it("c_connection_closed_errors_after_disconnect", async () => {
    const pair = await reliablePair();
    await pair.client.disconnect();

    const sendErr = await pair.client.send(null, {
      kind: "command",
      payload: genericCommand("ping", null),
    }).then(
      () => null,
      (e) => e,
    );
    expect(sendErr).not.toBeNull();
    expect(sendErr).toBeInstanceOf(Error);

    let binaryErr: unknown = null;
    try {
      pair.sessionClient.sendBinary(new Uint8Array([1, 2, 3]));
    } catch (e) {
      binaryErr = e;
    }
    expect(binaryErr).toBeInstanceOf(Error);
    pair.cleanup();
  });

  it("c_send_binary_not_connected_reports_error", async () => {
    // Reliability is session-carried: a session that is not wired to any
    // transport has no live writer — sendBinary reports notConnected.
    const session = new TransferSession();

    let err: unknown = null;
    try {
      session.sendBinary(new Uint8Array([1, 2, 3]));
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(XacppError);
    expect((err as XacppError).code).toBe("not_connected");
  });

  it("c_peer_reset_connection_dead_no_uncaught", async () => {
    // A peer that dies abruptly (TCP RST) is a dead connection, never a
    // host-process crash: the transport's resident 'error' listener maps
    // the reset onto the same teardown path as a normal close. The test
    // reaching its end without an uncaught exception is itself the
    // host-survival assertion (vitest escalates unhandled errors to a
    // non-zero exit for the whole run).
    const rig = await wireRig(async () => genericResponse("acknowledge", null));

    // Prove the connection is live first.
    rig.write(requestFrame("pre-reset", { kind: "command", payload: genericCommand("ping", null) }));
    await waitFor(() => rig.rx.some((f) => f.frameType === FrameType.DATA_JSON), "live connection proof");

    // Deterministic RST: resetAndDestroy sends an abortive reset regardless
    // of pending buffered data.
    rig.raw.resetAndDestroy();

    await waitFor(() => rig.isServerClosed(), "reset maps to a dead connection");
    expect(rig.isServerClosed()).toBe(true);

    rig.cleanup();
  });
});
