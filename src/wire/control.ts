/**
 * Control frame payload codecs (docs/SPEC.md §5, "Control frame payload
 * layouts").
 *
 * Covers ACK / NACK / RESUME (request) / RESUME_RESPONSE / HEARTBEAT /
 * ERROR payloads. Every integer is big-endian. Payload structure is
 * validated here; the 16 KiB control-frame cap (SPEC §4) is enforced
 * by the frame codec when a payload is wrapped into a control frame.
 *
 * Payload codecs pair with frame types one-to-one (see
 * {@link CONTROL_PAYLOAD_FRAME_TYPES}): a RESUME payload rides a
 * `RESUME` frame (holder → receiver: pending-send declaration); a
 * RESUME_RESPONSE payload rides a `RESUME_RESPONSE` frame (receiver →
 * holder: per-entry verdict). The two directions never
 * share a frame type — dispatch on `frame_type`, never on payload shape.
 *
 * All failures are raised as {@link ControlError} — a channel distinct
 * from the frame codec's `FrameError`.
 */

import { FrameType } from "./frame";

/**
 * Canonical pairing of control payload codecs with their frame types
 * (SPEC §4/§5; append-only). Integration layers encode/decode by this
 * table so a payload can never ride the wrong frame type.
 */
export const CONTROL_PAYLOAD_FRAME_TYPES = {
  ack: FrameType.ACK,
  nack: FrameType.NACK,
  resumeRequest: FrameType.RESUME,
  resumeResponse: FrameType.RESUME_RESPONSE,
  heartbeat: FrameType.HEARTBEAT,
  error: FrameType.ERROR,
  transactionPause: FrameType.TRANSACTION_PAUSE,
  transactionAbort: FrameType.TRANSACTION_ABORT,
  transactionCompleted: FrameType.TRANSACTION_COMPLETED,
} as const;

/** Error kinds raised by the control payload codecs. */
export enum ControlErrorCode {
  /** Truncated payload, trailing bytes, bad entry counts, or numeric fields out of range. */
  MalformedPayload = "MalformedPayload",
  /** RESUME response status byte outside the registered values (0x01..=0x03). */
  UnsupportedResumeStatus = "UnsupportedResumeStatus",
}

/** Error raised by the control payload codecs; `code` distinguishes the failure kind. */
export class ControlError extends Error {
  readonly code: ControlErrorCode;

  constructor(code: ControlErrorCode, message: string) {
    super(message);
    this.name = "ControlError";
    this.code = code;
  }
}

const U8_MAX = 0xff;
const U16_MAX = 0xffff;
const U32_MAX = 0xffffffff;

/** Transaction id wire size: 16 bytes = 32 lowercase hex chars (SPEC §3). */
const TRANSACTION_ID_BYTES = 16;
/** Size of one ACK/RESUME-request entry on the wire: txid + 2 × u32. */
const ACK_ENTRY_SIZE = TRANSACTION_ID_BYTES + 4 + 4;
/** Size of one RESUME-response entry on the wire: txid + status u8. */
const RESUME_RESPONSE_ENTRY_SIZE = TRANSACTION_ID_BYTES + 1;

/**
 * Cumulative progress receipt for one transaction (ACK entry / RESUME
 * request entry — identical wire shape, SPEC §5).
 *
 * `transactionId` is the 32 lowercase hex chars form (no hyphens); the
 * wire form is the 16 decoded bytes.
 */
export interface AckEntry {
  /** All frames 0..=highestContiguousSeq are received. */
  highestContiguousSeq: number;
  /** Total distinct frames received; detects sparse residue above the frontier. */
  receivedFrames: number;
  transactionId: string;
}

/** Alias: RESUME request entries share the ACK entry shape (SPEC §5). */
export type ResumeRequestEntry = AckEntry;

/** Missing-frame report for one transaction (NACK entry, SPEC §5). */
export interface NackEntry {
  /** Seqs known to be missing (below the receiver's highest received seq), in ascending order. */
  missingSeqs: number[];
  transactionId: string;
}

/** RESUME response status codes (SPEC §5). */
export const RESUME_STATUS = {
  /** Copy held; retransmission follows. */
  Resumed: 0x01,
  /** Transaction finalized on the holder side (aborted); receiver discards the transaction. */
  Rejected: 0x02,
  /** Receiver asks about a transaction this side never held. */
  Unknown: 0x03,
} as const;

/** One answered entry of a RESUME response (SPEC §5). */
export interface ResumeResponseEntry {
  status: number; // RESUME_STATUS.Resumed | Rejected | Unknown (runtime-validated)
  transactionId: string;
}

/** HEARTBEAT payload (unidirectional liveness probe, SPEC §5). */
export interface Heartbeat {
  /** Per-sender monotonic sequence. */
  monotonicSeq: number;
  /** Sender wall-clock epoch milliseconds (u64 wire, Number here — safe-integer bounded). */
  senderEpochMs: number;
}

/**
 * Protocol-level error code registry (SPEC §5, append-only).
 *
 * Wire names: 0x0001 protocol_version_unsupported,
 * 0x0002 transaction_expired, 0x0003 backpressure_stall.
 */
export const ErrorCode = {
  BackpressureStall: 0x0003,
  ProtocolVersionUnsupported: 0x0001,
  TransactionExpired: 0x0002,
} as const;

/** ERROR payload: protocol-level error with a UTF-8 message (SPEC §5). */
export interface ProtocolErrorInfo {
  code: number;
  /** UTF-8; may be empty. */
  message: string;
}

function fail(code: ControlErrorCode, message: string): never {
  throw new ControlError(code, message);
}

function requireEntryCount(count: number, min: number, what: string): void {
  if (!Number.isInteger(count) || count < min || count > U8_MAX) {
    fail(
      ControlErrorCode.MalformedPayload,
      `${what} entry count ${count} out of range ${min}..=${U8_MAX}`,
    );
  }
}

function requireU32(name: string, value: number): void {
  if (!Number.isInteger(value) || value < 0 || value > U32_MAX) {
    fail(ControlErrorCode.MalformedPayload, `${name} out of u32 range: ${value}`);
  }
}

function parseTransactionId(hex: string): Uint8Array {
  if (typeof hex !== "string" || hex.length !== TRANSACTION_ID_BYTES * 2 || !/^[0-9a-f]+$/.test(hex)) {
    fail(
      ControlErrorCode.MalformedPayload,
      `transaction id must be ${TRANSACTION_ID_BYTES * 2} lowercase hex chars, got: ${JSON.stringify(hex)}`,
    );
  }
  const out = new Uint8Array(TRANSACTION_ID_BYTES);
  for (let i = 0; i < TRANSACTION_ID_BYTES; i++) {
    out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function transactionIdToHex(bytes: Uint8Array, offset: number): string {
  let hex = "";
  for (let i = 0; i < TRANSACTION_ID_BYTES; i++) {
    hex += bytes[offset + i].toString(16).padStart(2, "0");
  }
  return hex;
}

function writeAckEntry(out: Uint8Array, dv: DataView, offset: number, entry: AckEntry): void {
  out.set(parseTransactionId(entry.transactionId), offset);
  requireU32("highestContiguousSeq", entry.highestContiguousSeq);
  requireU32("receivedFrames", entry.receivedFrames);
  dv.setUint32(offset + TRANSACTION_ID_BYTES, entry.highestContiguousSeq, false);
  dv.setUint32(offset + TRANSACTION_ID_BYTES + 4, entry.receivedFrames, false);
}

function readAckEntry(payload: Uint8Array, dv: DataView, offset: number): AckEntry {
  return {
    transactionId: transactionIdToHex(payload, offset),
    highestContiguousSeq: dv.getUint32(offset + TRANSACTION_ID_BYTES, false),
    receivedFrames: dv.getUint32(offset + TRANSACTION_ID_BYTES + 4, false),
  };
}

function decodeAckShaped(payload: Uint8Array, what: string, minCount: number): AckEntry[] {
  if (payload.length < 1) {
    fail(ControlErrorCode.MalformedPayload, `${what} payload truncated: missing count byte`);
  }
  const count = payload[0];
  requireEntryCount(count, minCount, what);
  const expected = 1 + count * ACK_ENTRY_SIZE;
  if (payload.length !== expected) {
    fail(
      ControlErrorCode.MalformedPayload,
      `${what} payload length ${payload.length} != expected ${expected} for count ${count}`,
    );
  }
  const dv = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const out: AckEntry[] = [];
  for (let i = 0; i < count; i++) {
    out.push(readAckEntry(payload, dv, 1 + i * ACK_ENTRY_SIZE));
  }
  return out;
}

/**
 * Encode an ACK payload — cumulative progress receipts (SPEC §5).
 * Count is 1..=255: an ACK with nothing to report is not sent.
 */
export function encodeAck(entries: readonly AckEntry[]): Uint8Array {
  requireEntryCount(entries.length, 1, "ACK");
  const out = new Uint8Array(1 + entries.length * ACK_ENTRY_SIZE);
  const dv = new DataView(out.buffer);
  out[0] = entries.length;
  for (let i = 0; i < entries.length; i++) {
    writeAckEntry(out, dv, 1 + i * ACK_ENTRY_SIZE, entries[i]);
  }
  return out;
}

/** Decode an ACK payload; strict length check (trailing bytes rejected). */
export function decodeAck(payload: Uint8Array): AckEntry[] {
  return decodeAckShaped(payload, "ACK", 1);
}

/**
 * Encode a RESUME request payload — rides a `RESUME` frame
 * (receiver → holder, SPEC §5). Count is 0..=255; 0 entries means
 * "nothing to resume".
 */
export function encodeResumeRequest(entries: readonly ResumeRequestEntry[]): Uint8Array {
  requireEntryCount(entries.length, 0, "RESUME request");
  const out = new Uint8Array(1 + entries.length * ACK_ENTRY_SIZE);
  const dv = new DataView(out.buffer);
  out[0] = entries.length;
  for (let i = 0; i < entries.length; i++) {
    writeAckEntry(out, dv, 1 + i * ACK_ENTRY_SIZE, entries[i]);
  }
  return out;
}

/** Decode a RESUME request payload; strict length check (trailing bytes rejected). */
export function decodeResumeRequest(payload: Uint8Array): ResumeRequestEntry[] {
  return decodeAckShaped(payload, "RESUME request", 0);
}

function requireResumeStatus(status: number): void {
  if (
    status !== RESUME_STATUS.Resumed &&
    status !== RESUME_STATUS.Rejected &&
    status !== RESUME_STATUS.Unknown
  ) {
    fail(ControlErrorCode.UnsupportedResumeStatus, `RESUME status 0x${status?.toString(16)} is not registered`);
  }
}

/**
 * Encode a RESUME response payload — rides a `RESUME_RESPONSE` frame
 * (holder → receiver, SPEC §5). Count is 0..=255.
 */
export function encodeResumeResponse(entries: readonly ResumeResponseEntry[]): Uint8Array {
  requireEntryCount(entries.length, 0, "RESUME response");
  const out = new Uint8Array(1 + entries.length * RESUME_RESPONSE_ENTRY_SIZE);
  out[0] = entries.length;
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    requireResumeStatus(entry.status);
    out.set(parseTransactionId(entry.transactionId), 1 + i * RESUME_RESPONSE_ENTRY_SIZE);
    out[1 + i * RESUME_RESPONSE_ENTRY_SIZE + TRANSACTION_ID_BYTES] = entry.status;
  }
  return out;
}

/** Decode a RESUME response payload; strict length check (trailing bytes rejected). */
export function decodeResumeResponse(payload: Uint8Array): ResumeResponseEntry[] {
  if (payload.length < 1) {
    fail(ControlErrorCode.MalformedPayload, "RESUME response payload truncated: missing count byte");
  }
  const count = payload[0];
  requireEntryCount(count, 0, "RESUME response");
  const expected = 1 + count * RESUME_RESPONSE_ENTRY_SIZE;
  if (payload.length !== expected) {
    fail(
      ControlErrorCode.MalformedPayload,
      `RESUME response payload length ${payload.length} != expected ${expected} for count ${count}`,
    );
  }
  const out: ResumeResponseEntry[] = [];
  for (let i = 0; i < count; i++) {
    const offset = 1 + i * RESUME_RESPONSE_ENTRY_SIZE;
    const status = payload[offset + TRANSACTION_ID_BYTES];
    requireResumeStatus(status);
    out.push({
      transactionId: transactionIdToHex(payload, offset),
      status,
    });
  }
  return out;
}

/**
 * Encode a NACK payload — missing-frame reports (SPEC §5).
 * Count is 1..=255 entries; each entry's missing list is u16-bounded.
 */
export function encodeNack(entries: readonly NackEntry[]): Uint8Array {
  requireEntryCount(entries.length, 1, "NACK");
  let size = 1;
  for (const entry of entries) {
    const n = entry.missingSeqs.length;
    if (!Number.isInteger(n) || n < 0 || n > U16_MAX) {
      fail(ControlErrorCode.MalformedPayload, `missing_count out of u16 range: ${n}`);
    }
    size += TRANSACTION_ID_BYTES + 2 + 4 * n;
  }
  const out = new Uint8Array(size);
  const dv = new DataView(out.buffer);
  out[0] = entries.length;
  let offset = 1;
  for (const entry of entries) {
    out.set(parseTransactionId(entry.transactionId), offset);
    dv.setUint16(offset + TRANSACTION_ID_BYTES, entry.missingSeqs.length, false);
    offset += TRANSACTION_ID_BYTES + 2;
    for (const seq of entry.missingSeqs) {
      requireU32("missing seq", seq);
      dv.setUint32(offset, seq, false);
      offset += 4;
    }
  }
  return out;
}

/** Decode a NACK payload; strict length check (truncated / trailing bytes rejected). */
export function decodeNack(payload: Uint8Array): NackEntry[] {
  if (payload.length < 1) {
    fail(ControlErrorCode.MalformedPayload, "NACK payload truncated: missing count byte");
  }
  const count = payload[0];
  requireEntryCount(count, 1, "NACK");
  const dv = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const out: NackEntry[] = [];
  let offset = 1;
  for (let i = 0; i < count; i++) {
    if (payload.length - offset < TRANSACTION_ID_BYTES + 2) {
      fail(ControlErrorCode.MalformedPayload, `NACK payload truncated at entry ${i}`);
    }
    const txid = transactionIdToHex(payload, offset);
    const missingCount = dv.getUint16(offset + TRANSACTION_ID_BYTES, false);
    offset += TRANSACTION_ID_BYTES + 2;
    if (payload.length - offset < missingCount * 4) {
      fail(ControlErrorCode.MalformedPayload, `NACK payload truncated in missing seqs of entry ${i}`);
    }
    const missingSeqs: number[] = [];
    for (let k = 0; k < missingCount; k++) {
      missingSeqs.push(dv.getUint32(offset, false));
      offset += 4;
    }
    out.push({ transactionId: txid, missingSeqs });
  }
  if (offset !== payload.length) {
    fail(
      ControlErrorCode.MalformedPayload,
      `NACK payload has ${payload.length - offset} trailing byte(s) after ${count} entr${count === 1 ? "y" : "ies"}`,
    );
  }
  return out;
}

/** Encode a HEARTBEAT payload (SPEC §5): 12 bytes. */
export function encodeHeartbeat(heartbeat: Heartbeat): Uint8Array {
  requireU32("monotonicSeq", heartbeat.monotonicSeq);
  if (!Number.isSafeInteger(heartbeat.senderEpochMs) || heartbeat.senderEpochMs < 0) {
    fail(
      ControlErrorCode.MalformedPayload,
      `senderEpochMs must be a non-negative safe integer, got: ${heartbeat.senderEpochMs}`,
    );
  }
  const out = new Uint8Array(4 + 8);
  const dv = new DataView(out.buffer);
  dv.setUint32(0, heartbeat.monotonicSeq, false);
  dv.setBigUint64(4, BigInt(heartbeat.senderEpochMs), false);
  return out;
}

/** Decode a HEARTBEAT payload; u64 epochs beyond Number.MAX_SAFE_INTEGER are rejected. */
export function decodeHeartbeat(payload: Uint8Array): Heartbeat {
  if (payload.length !== 12) {
    fail(ControlErrorCode.MalformedPayload, `HEARTBEAT payload length ${payload.length} != 12`);
  }
  const dv = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const raw = dv.getBigUint64(4, false);
  if (raw > BigInt(Number.MAX_SAFE_INTEGER)) {
    fail(
      ControlErrorCode.MalformedPayload,
      `sender_epoch_ms 0x${raw.toString(16)} exceeds the safe integer range`,
    );
  }
  return { monotonicSeq: dv.getUint32(0, false), senderEpochMs: Number(raw) };
}

/** Encode an ERROR payload (SPEC §5): u16 code + UTF-8 message (may be empty). */
export function encodeError(info: ProtocolErrorInfo): Uint8Array {
  if (!Number.isInteger(info.code) || info.code < 0 || info.code > U16_MAX) {
    fail(ControlErrorCode.MalformedPayload, `error code out of u16 range: ${info.code}`);
  }
  const message = new TextEncoder().encode(info.message);
  const out = new Uint8Array(2 + message.length);
  new DataView(out.buffer).setUint16(0, info.code, false);
  out.set(message, 2);
  return out;
}

/** Decode an ERROR payload; the message bytes must be valid UTF-8. */
export function decodeError(payload: Uint8Array): ProtocolErrorInfo {
  if (payload.length < 2) {
    fail(ControlErrorCode.MalformedPayload, `ERROR payload length ${payload.length} < 2 (code is u16)`);
  }
  const dv = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const code = dv.getUint16(0, false);
  let message: string;
  try {
    message = new TextDecoder("utf-8", { fatal: true }).decode(payload.subarray(2));
  } catch {
    fail(ControlErrorCode.MalformedPayload, "ERROR message is not valid UTF-8");
  }
  return { code, message };
}

// ---- TRANSACTION_PAUSE / TRANSACTION_ABORT (SPEC §5 control layouts) ----
//
// Both frames carry the identical payload shape — a bare 16-byte transaction
// id — and differ only in frame type (dispatch on frame_type, never on
// payload shape). `TRANSACTION_PAUSE`: the holder paused transmission of the
// transaction. `TRANSACTION_ABORT`: the holder abandoned it and released its
// retained copy; the receiver discards the partial reassembly.

function encodeTransactionIdPayload(transactionId: string): Uint8Array {
  const out = new Uint8Array(TRANSACTION_ID_BYTES);
  out.set(parseTransactionId(transactionId), 0);
  return out;
}

function decodeTransactionIdPayload(what: string, payload: Uint8Array): string {
  if (payload.length !== TRANSACTION_ID_BYTES) {
    fail(
      ControlErrorCode.MalformedPayload,
      `${what} payload length ${payload.length} != ${TRANSACTION_ID_BYTES} (bare transaction id)`,
    );
  }
  return transactionIdToHex(payload, 0);
}

/** Encode a TRANSACTION_PAUSE payload — the paused transaction's id. */
export function encodeTransactionPause(transactionId: string): Uint8Array {
  return encodeTransactionIdPayload(transactionId);
}

/** Decode a TRANSACTION_PAUSE payload; strict length check. */
export function decodeTransactionPause(payload: Uint8Array): string {
  return decodeTransactionIdPayload("TRANSACTION_PAUSE", payload);
}

/** Encode a TRANSACTION_ABORT payload — the abandoned transaction's id. */
export function encodeTransactionAbort(transactionId: string): Uint8Array {
  return encodeTransactionIdPayload(transactionId);
}

/** Decode a TRANSACTION_ABORT payload; strict length check. */
export function decodeTransactionAbort(payload: Uint8Array): string {
  return decodeTransactionIdPayload("TRANSACTION_ABORT", payload);
}

/**
 * Encode a TRANSACTION_COMPLETED payload — the completed transaction's id.
 * Same shape as TRANSACTION_PAUSE / TRANSACTION_ABORT (a bare 16-byte
 * transaction id); the frame type carries the meaning.
 */
export function encodeTransactionCompleted(transactionId: string): Uint8Array {
  return encodeTransactionIdPayload(transactionId);
}

/** Decode a TRANSACTION_COMPLETED payload; strict length check. */
export function decodeTransactionCompleted(payload: Uint8Array): string {
  return decodeTransactionIdPayload("TRANSACTION_COMPLETED", payload);
}
