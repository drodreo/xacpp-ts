/**
 * XACPP binary frame codec (protocol v1.0).
 *
 * Implements the wire format defined in docs/SPEC.md §3/§4:
 * 36-byte fixed header + variable payload + 4-byte CRC32 trailer,
 * big-endian byte order throughout, 40 bytes fixed overhead per frame.
 *
 * This module is transport-agnostic: it neither owns a carrier nor
 * reassembles transactions — those belong to later milestones.
 * Reassembly criteria (contiguous seq AND sum(frame_len) == total_len)
 * operate on the frames produced here.
 */

import { randomUUID } from "node:crypto";
import { crc32 as zlibCrc32 } from "node:zlib";

/** Wire protocol major version carried in every frame header (SPEC §3/§9). */
export const PROTOCOL_VERSION_MAJOR = 1;
/** Wire protocol minor version carried in every frame header (SPEC §3/§9).
 * Advances with the library minor version; fetch (1.1) and activity event
 * payload extensions (1.2) are envelope-level — frame format unchanged. */
export const PROTOCOL_VERSION_MINOR = 2;

/** ASCII "XACP" frame magic (SPEC §3). */
const MAGIC_BYTES = new Uint8Array([0x58, 0x41, 0x43, 0x50]);

/** Fixed header size in bytes (SPEC §3). */
const HEADER_SIZE = 36;
/** CRC32 trailer size in bytes (SPEC §3). */
const CRC_SIZE = 4;
/** Transaction id size in bytes: a 128-bit UUID (SPEC §3). */
export const TRANSACTION_ID_SIZE = 16;
/** Fixed per-frame overhead: header + CRC trailer (SPEC §3). */
export const FRAME_FIXED_OVERHEAD = HEADER_SIZE + CRC_SIZE;

/**
 * Numeric frame type table (SPEC §4).
 *
 * CONTRACT TABLE — numeric values MUST stay in sync with xacpp-rs
 * (contract table). Values are pinned by the cross-library interop
 * tests (tests/interop.test.ts).
 */
export const FrameType = {
  // Control class
  NEGOTIATE: 0x0001,
  ACK: 0x0002,
  NACK: 0x0003,
  RESUME: 0x0004,
  HEARTBEAT: 0x0005,
  ERROR: 0x0006,
  SECURITY_UPGRADE: 0x0007,
  RESUME_RESPONSE: 0x0008,
  TRANSACTION_PAUSE: 0x0009,
  TRANSACTION_ABORT: 0x000a,
  TRANSACTION_COMPLETED: 0x000b,
  // Data class
  DATA_JSON: 0x1001,
  DATA_BINARY: 0x1002,
} as const;

const CONTROL_FRAME_TYPES: ReadonlySet<number> = new Set([
  FrameType.NEGOTIATE,
  FrameType.ACK,
  FrameType.NACK,
  FrameType.RESUME,
  FrameType.HEARTBEAT,
  FrameType.ERROR,
  FrameType.SECURITY_UPGRADE,
  FrameType.RESUME_RESPONSE,
  FrameType.TRANSACTION_PAUSE,
  FrameType.TRANSACTION_ABORT,
  FrameType.TRANSACTION_COMPLETED,
]);

const DATA_FRAME_TYPES: ReadonlySet<number> = new Set([
  FrameType.DATA_JSON,
  FrameType.DATA_BINARY,
]);

/** Max payload size for control frames (SPEC §4). */
export const CONTROL_FRAME_MAX = 16 * 1024;
/** Max payload size for DATA_JSON frames (SPEC §4). */
export const DATA_JSON_FRAME_MAX = 1024 * 1024;
/** Max payload size for DATA_BINARY frames (SPEC §4). */
export const DATA_BINARY_FRAME_MAX = 16 * 1024 * 1024;

/**
 * Default decode buffer cap (SPEC §3): any frame whose total size
 * (header + frame_len + trailer) exceeds the cap is a protocol error,
 * regardless of frame type. Bounds memory exposure to unknown frame
 * types and hostile declarations; per-type limits (§4) remain the
 * tighter bound for known types. RECOMMENDED default: 64 MiB.
 */
export const DEFAULT_DECODE_BUFFER_CAP = 64 * 1024 * 1024;

/** True if the type belongs to the control class (SPEC §4). */
export function isControlFrameType(frameType: number): boolean {
  return CONTROL_FRAME_TYPES.has(frameType);
}

/** True if the type belongs to the data class (SPEC §4). */
export function isDataFrameType(frameType: number): boolean {
  return DATA_FRAME_TYPES.has(frameType);
}

/** True if the type is defined by this protocol version (SPEC §4). */
export function isKnownFrameType(frameType: number): boolean {
  return isControlFrameType(frameType) || isDataFrameType(frameType);
}

/**
 * Per-type payload size limit; `undefined` for unknown frame types
 * (a higher-minor peer may define larger classes — SPEC §9 tolerance).
 */
function frameTypeLimit(frameType: number): number | undefined {
  if (isControlFrameType(frameType)) return CONTROL_FRAME_MAX;
  if (frameType === FrameType.DATA_JSON) return DATA_JSON_FRAME_MAX;
  if (frameType === FrameType.DATA_BINARY) return DATA_BINARY_FRAME_MAX;
  return undefined;
}

/**
 * Protocol error kinds distinguished by the codec.
 *
 * - UnsupportedVersion: header version_major differs from
 *   PROTOCOL_VERSION_MAJOR ⇒ connection refused (SPEC §3/§8).
 * - FrameTooLarge: payload exceeds the per-type limit (SPEC §4).
 * - CrcMismatch: CRC32 trailer does not cover the frame contents —
 *   integrity is broken; the stream MUST NOT be silently resynced.
 * - MalformedFrame: structurally invalid input at the encoder boundary
 *   (e.g. malformed transaction id, fields beyond u32 range).
 */
export enum FrameErrorCode {
  UnsupportedVersion = "UnsupportedVersion",
  FrameTooLarge = "FrameTooLarge",
  CrcMismatch = "CrcMismatch",
  MalformedFrame = "MalformedFrame",
}

/** Error raised by the frame codec; `code` distinguishes the failure kind. */
export class FrameError extends Error {
  readonly code: FrameErrorCode;

  constructor(code: FrameErrorCode, message: string) {
    super(message);
    this.name = "FrameError";
    this.code = code;
  }
}

/**
 * A single protocol frame (SPEC §3).
 *
 * `transactionId` is the 128-bit UUID as 32 lowercase hex chars
 * (no hyphens) — the wire form is the 16 decoded bytes.
 * `totalLen` is transaction-level metadata carried verbatim; this
 * module does not validate it against sibling frames (reassembly is
 * a later milestone).
 */
export interface Frame {
  versionMajor: number;
  versionMinor: number;
  frameType: number;
  transactionId: string;
  totalLen: number;
  seq: number;
  payload: Uint8Array;
}

/** CRC-32/ISO-HDLC (reflected polynomial 0xEDB88320) over `data`.
 *
 * Backed by node:zlib (built-in since Node 22.2). Interop anchor:
 * crc32("123456789") === 0xCBF43926 — pinned by tests and shared with
 * xacpp-rs.
 */
export function crc32(data: Uint8Array): number {
  return zlibCrc32(data);
}

/**
 * Generate a new transaction id: `crypto.randomUUID()` with hyphens
 * stripped, yielding 32 hex chars = 16 bytes on the wire (SPEC §3).
 */
export function newTransactionId(): string {
  return randomUUID().replace(/-/g, "");
}

function hexToTransactionId(hex: string): Uint8Array {
  if (hex.length !== TRANSACTION_ID_SIZE * 2 || !/^[0-9a-f]+$/.test(hex)) {
    throw new FrameError(
      FrameErrorCode.MalformedFrame,
      `transaction id must be ${TRANSACTION_ID_SIZE * 2} lowercase hex chars, got: ${JSON.stringify(hex)}`,
    );
  }
  const out = new Uint8Array(TRANSACTION_ID_SIZE);
  for (let i = 0; i < TRANSACTION_ID_SIZE; i++) {
    out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function transactionIdToHex(bytes: Uint8Array): string {
  let hex = "";
  for (let i = 0; i < TRANSACTION_ID_SIZE; i++) {
    hex += bytes[i].toString(16).padStart(2, "0");
  }
  return hex;
}

/**
 * Convenience frame constructor: fills protocol defaults (version 1.0,
 * fresh transaction id, single-frame transaction: totalLen = payload
 * length, seq = 0).
 */
export function makeFrame(
  frameType: number,
  payload: Uint8Array,
  opts?: {
    versionMajor?: number;
    versionMinor?: number;
    transactionId?: string;
    totalLen?: number;
    seq?: number;
  },
): Frame {
  return {
    versionMajor: opts?.versionMajor ?? PROTOCOL_VERSION_MAJOR,
    versionMinor: opts?.versionMinor ?? PROTOCOL_VERSION_MINOR,
    frameType,
    transactionId: opts?.transactionId ?? newTransactionId(),
    totalLen: opts?.totalLen ?? payload.length,
    seq: opts?.seq ?? 0,
    payload,
  };
}

/**
 * Encode a frame into wire bytes: header + payload + CRC32 trailer,
 * big-endian throughout (SPEC §3).
 *
 * Enforces the per-type size limit for known frame types (SPEC §4).
 * Version fields are written verbatim (the decoder enforces major
 * compatibility; the encoder stays permissive for forward testing).
 */
export function encodeFrame(frame: Frame): Uint8Array {
  const txBytes = hexToTransactionId(frame.transactionId);
  const limit = frameTypeLimit(frame.frameType);
  if (limit !== undefined && frame.payload.length > limit) {
    throw new FrameError(
      FrameErrorCode.FrameTooLarge,
      `payload ${frame.payload.length} exceeds frame type 0x${frame.frameType.toString(16)} limit ${limit}`,
    );
  }
  if (!Number.isInteger(frame.frameType) || frame.frameType < 0 || frame.frameType > 0xffff) {
    throw new FrameError(
      FrameErrorCode.MalformedFrame,
      `frame type out of u16 range: ${frame.frameType}`,
    );
  }
  for (const [name, value] of [
    ["totalLen", frame.totalLen],
    ["seq", frame.seq],
  ] as const) {
    if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
      throw new FrameError(
        FrameErrorCode.MalformedFrame,
        `${name} out of u32 range: ${value}`,
      );
    }
  }

  const out = new Uint8Array(FRAME_FIXED_OVERHEAD + frame.payload.length);
  const dv = new DataView(out.buffer);
  out.set(MAGIC_BYTES, 0);
  out[4] = frame.versionMajor;
  out[5] = frame.versionMinor;
  dv.setUint16(6, frame.frameType, false); // big-endian
  out.set(txBytes, 8);
  dv.setUint32(24, frame.totalLen, false);
  dv.setUint32(28, frame.seq, false);
  dv.setUint32(32, frame.payload.length, false);
  out.set(frame.payload, HEADER_SIZE);
  // CRC32 covers offset 0 through end of payload (SPEC §3).
  dv.setUint32(
    HEADER_SIZE + frame.payload.length,
    crc32(out.subarray(0, HEADER_SIZE + frame.payload.length)),
    false,
  );
  return out;
}

interface DecodedHeader {
  versionMajor: number;
  versionMinor: number;
  frameType: number;
  transactionId: string;
  totalLen: number;
  seq: number;
  frameLen: number;
}

interface ParseResult {
  /** True when one frame worth of bytes was consumed (emitted or ignored). */
  consumed: boolean;
  /** Decoded frame; null when an unknown frame type was consumed and ignored (SPEC §9). */
  frame: Frame | null;
}

/**
 * Incremental streaming frame decoder.
 *
 * Feed arbitrary byte chunks via {@link push}; complete frames are
 * returned in arrival order. Handles:
 *
 * - Coalesced frames (multiple frames in one chunk) and split frames
 *   (one frame across many chunks).
 * - Dirty stream resync: leading garbage is skipped by scanning for
 *   the "XACP" magic. Once the magic matches, header field validation
 *   (protocol major version, per-type size limits) gates any further
 *   commitment, and the CRC32 trailer is the final integrity verdict —
 *   a pseudo-magic inside frame payload data can never derail framing
 *   because consumption past a locked header is driven by frame_len,
 *   not by scanning. A CRC mismatch is raised as an error, never
 *   silently resynced (SPEC §7: CRC is the primary integrity defense
 *   in plaintext mode).
 * - No pre-allocation from declared lengths: the internal buffer only
 *   grows with actually received bytes, so a hostile frame_len in a
 *   header cannot trigger a large allocation. On top of that, a
 *   configurable global decode buffer cap (default 64 MiB, SPEC §3)
 *   rejects any frame whose total size would exceed it — including
 *   unknown frame types — as a protocol error.
 *
 * Unknown frame types are decoded, CRC-verified, and silently dropped
 * ("unknown frame types are ignored", SPEC §9).
 */
export class FrameDecoder {
  private buf: Uint8Array = new Uint8Array(4096);
  private len = 0;
  private readonly maxDecodeBuffer: number;

  /**
   * @param opts.maxDecodeBuffer Global decode buffer cap (SPEC §3): any
   * frame whose total size (header + frame_len + trailer) exceeds the
   * cap is rejected as a protocol error, regardless of frame type —
   * this includes unknown frame types. Must be an integer large enough
   * to hold at least one empty frame (FRAME_FIXED_OVERHEAD).
   * Defaults to DEFAULT_DECODE_BUFFER_CAP (64 MiB).
   */
  constructor(opts?: { maxDecodeBuffer?: number }) {
    if (
      opts?.maxDecodeBuffer !== undefined &&
      (!Number.isInteger(opts.maxDecodeBuffer) ||
        opts.maxDecodeBuffer < FRAME_FIXED_OVERHEAD)
    ) {
      throw new RangeError(
        `maxDecodeBuffer must be an integer >= ${FRAME_FIXED_OVERHEAD}, got: ${opts.maxDecodeBuffer}`,
      );
    }
    this.maxDecodeBuffer = opts?.maxDecodeBuffer ?? DEFAULT_DECODE_BUFFER_CAP;
  }

  /** Number of bytes currently buffered, waiting for more input. */
  get buffered(): number {
    return this.len;
  }

  /**
   * Feed a chunk of bytes; returns every frame completed by this chunk.
   * Throws {@link FrameError} on protocol violations (version mismatch,
   * oversized known-type frames, CRC mismatch) — the decoder is not
   * usable for the same stream afterwards.
   *
   * Feeding stream-shaped chunks (socket/stdio read sizes) is the
   * intended usage; one huge non-stream chunk leaves a large remainder
   * that gets copied on every push until consumed — avoid feeding
   * multi-megabyte buffers in a loop.
   */
  push(chunk: Uint8Array): Frame[] {
    this.append(chunk);
    const out: Frame[] = [];
    for (;;) {
      const result = this.tryParseOne();
      if (!result.consumed) break;
      if (result.frame !== null) out.push(result.frame);
    }
    return out;
  }

  private append(chunk: Uint8Array): void {
    if (chunk.length === 0) return;
    const needed = this.len + chunk.length;
    if (needed > this.buf.length) {
      // Grow only to fit actually received bytes — never from declared
      // header lengths (SPEC §3: buffers are never pre-allocated from
      // declared lengths).
      let cap = this.buf.length * 2;
      while (cap < needed) cap *= 2;
      const next = new Uint8Array(cap);
      next.set(this.buf.subarray(0, this.len));
      this.buf = next;
    }
    this.buf.set(chunk, this.len);
    this.len = needed;
  }

  private consume(n: number): void {
    if (n <= 0) return;
    this.buf.copyWithin(0, n, this.len);
    this.len -= n;
    // Release oversized buffers after bulk transfers settle down.
    if (this.buf.length > (1 << 20) && this.len < 4096) {
      const next = new Uint8Array(4096);
      next.set(this.buf.subarray(0, this.len));
      this.buf = next;
    }
  }

  private magicAt(offset: number): boolean {
    for (let i = 0; i < MAGIC_BYTES.length; i++) {
      if (this.buf[offset + i] !== MAGIC_BYTES[i]) return false;
    }
    return true;
  }

  /**
   * Drop leading bytes until the buffer starts with the frame magic.
   * Keeps the last MAGIC_BYTES.length - 1 bytes when no full magic is
   * present (the magic itself may be split across chunks).
   */
  private alignMagic(): void {
    const limit = this.len - (MAGIC_BYTES.length - 1);
    let found = -1;
    for (let i = 0; i < limit; i++) {
      if (this.magicAt(i)) {
        found = i;
        break;
      }
    }
    const drop = found >= 0 ? found : Math.max(0, limit);
    this.consume(drop);
  }

  private readHeader(): DecodedHeader {
    const dv = new DataView(this.buf.buffer, this.buf.byteOffset, this.len);
    return {
      versionMajor: this.buf[4],
      versionMinor: this.buf[5],
      frameType: dv.getUint16(6, false),
      transactionId: transactionIdToHex(this.buf.subarray(8, 8 + TRANSACTION_ID_SIZE)),
      totalLen: dv.getUint32(24, false),
      seq: dv.getUint32(28, false),
      frameLen: dv.getUint32(32, false),
    };
  }

  private tryParseOne(): ParseResult {
    // 1. Resync onto the frame magic (no-op when already aligned).
    if (this.len > 0 && !this.magicAt(0)) {
      this.alignMagic();
    }
    if (this.len < HEADER_SIZE) return { consumed: false, frame: null };

    // 2. Cheap header validation before committing to the frame body.
    const header = this.readHeader();
    if (header.versionMajor !== PROTOCOL_VERSION_MAJOR) {
      throw new FrameError(
        FrameErrorCode.UnsupportedVersion,
        `version_major ${header.versionMajor} != supported ${PROTOCOL_VERSION_MAJOR} (connection refused, SPEC §9)`,
      );
    }
    const limit = frameTypeLimit(header.frameType);
    if (limit !== undefined && header.frameLen > limit) {
      throw new FrameError(
        FrameErrorCode.FrameTooLarge,
        `frame type 0x${header.frameType.toString(16)} declares ${header.frameLen} payload bytes, limit ${limit}`,
      );
    }
    // Global decode buffer cap — applies regardless of frame type,
    // including unknown frame types (SPEC §3). Checked before waiting
    // for the frame body so hostile declarations never get buffered.
    const totalSize = HEADER_SIZE + header.frameLen + CRC_SIZE;
    if (totalSize > this.maxDecodeBuffer) {
      throw new FrameError(
        FrameErrorCode.FrameTooLarge,
        `frame total size ${totalSize} (type 0x${header.frameType.toString(16)}) exceeds decode buffer cap ${this.maxDecodeBuffer}`,
      );
    }

    // 3. Wait for the whole frame before touching its contents.
    const need = HEADER_SIZE + header.frameLen + CRC_SIZE;
    if (this.len < need) return { consumed: false, frame: null };

    // 4. CRC32 verdict over offset 0 through end of payload (SPEC §3).
    const crcOffset = HEADER_SIZE + header.frameLen;
    const dv = new DataView(this.buf.buffer, this.buf.byteOffset, this.len);
    const expected = crc32(this.buf.subarray(0, crcOffset));
    const actual = dv.getUint32(crcOffset, false);
    if (expected !== actual) {
      throw new FrameError(
        FrameErrorCode.CrcMismatch,
        `crc32 mismatch at seq ${header.seq} of transaction ${header.transactionId}: expected 0x${expected.toString(16)}, got 0x${actual.toString(16)}`,
      );
    }

    // 5. Emit (or ignore unknown frame types, SPEC §9).
    const payload = this.buf.slice(HEADER_SIZE, crcOffset);
    this.consume(need);
    if (!isKnownFrameType(header.frameType)) {
      return { consumed: true, frame: null };
    }
    const { frameLen: _frameLen, ...frameFields } = header;
    return { consumed: true, frame: { ...frameFields, payload } };
  }
}
