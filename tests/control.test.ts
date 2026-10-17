/**
 * Control frame payload codec tests (docs/SPEC.md §5).
 *
 * Coverage:
 * 1. Round-trip encode/decode for every control payload kind
 * 2. Wire layout spot checks (big-endian, documented offsets)
 * 3. Entry-count boundaries: ACK/NACK 1..=255, RESUME 0..=255
 * 4. Truncated payloads and trailing bytes are rejected
 * 5. RESUME status registry: valid values round-trip, invalid rejected
 * 6. HEARTBEAT u64 epoch: safe-integer boundary handling
 * 7. ERROR: empty and non-ASCII UTF-8 messages, invalid UTF-8 rejected
 * 8. Errors are raised as ControlError — a channel distinct from FrameError
 */

import { describe, it, expect } from "vitest";
import {
  ControlError,
  ControlErrorCode,
  ErrorCode,
  RESUME_STATUS,
  decodeAck,
  decodeError,
  decodeHeartbeat,
  decodeNack,
  decodeResumeRequest,
  decodeResumeResponse,
  encodeAck,
  encodeError,
  encodeHeartbeat,
  encodeNack,
  encodeResumeRequest,
  encodeResumeResponse,
} from "../src/control";

const TX_A = "000102030405060708090a0b0c0d0e0f";
const TX_B = "ffffffffffffffffffffffffffffffff";

function expectControlError(fn: () => unknown, code: ControlErrorCode): void {
  try {
    fn();
  } catch (err) {
    expect(err).toBeInstanceOf(ControlError);
    expect((err as ControlError).code).toBe(code);
    return;
  }
  expect.unreachable(`expected ControlError with code ${code}`);
}

function ackEntry(transactionId: string, highestContiguousSeq: number, receivedFrames: number) {
  return { transactionId, highestContiguousSeq, receivedFrames };
}

describe("ACK codec", () => {
  it("round-trips entries", () => {
    const entries = [
      ackEntry(TX_A, 41, 45),
      ackEntry(TX_B, 0, 1),
    ];
    expect(decodeAck(encodeAck(entries))).toEqual(entries);
  });

  it("writes the documented wire layout (big-endian)", () => {
    const wire = encodeAck([ackEntry(TX_A, 0x01020304, 0x05060708)]);
    expect(wire.length).toBe(1 + 24);
    expect(wire[0]).toBe(1);
    expect([...wire.subarray(1, 17)]).toEqual([
      0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
      0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    ]);
    const dv = new DataView(wire.buffer, wire.byteOffset, wire.byteLength);
    expect(dv.getUint32(17, false)).toBe(0x01020304);
    expect(dv.getUint32(21, false)).toBe(0x05060708);
  });

  it("accepts the 255-entry boundary", () => {
    const entries = Array.from({ length: 255 }, (_, i) =>
      ackEntry(i.toString(16).padStart(32, "0"), i, i + 1),
    );
    const decoded = decodeAck(encodeAck(entries));
    expect(decoded.length).toBe(255);
    expect(decoded[254].highestContiguousSeq).toBe(254);
  });

  it("rejects empty entries on encode and count 0 on decode", () => {
    expectControlError(() => encodeAck([]), ControlErrorCode.MalformedPayload);
    expectControlError(() => decodeAck(new Uint8Array([0])), ControlErrorCode.MalformedPayload);
  });

  it("rejects more than 255 entries", () => {
    const entries = Array.from({ length: 256 }, (_, i) => ackEntry(TX_A, i, i));
    expectControlError(() => encodeAck(entries), ControlErrorCode.MalformedPayload);
  });

  it("rejects truncated and trailing-byte payloads", () => {
    const wire = encodeAck([ackEntry(TX_A, 1, 2)]);
    expectControlError(() => decodeAck(wire.subarray(0, wire.length - 1)), ControlErrorCode.MalformedPayload);
    const padded = new Uint8Array(wire.length + 1);
    padded.set(wire);
    expectControlError(() => decodeAck(padded), ControlErrorCode.MalformedPayload);
  });

  it("rejects out-of-range numeric fields", () => {
    expectControlError(() => encodeAck([ackEntry(TX_A, -1, 0)]), ControlErrorCode.MalformedPayload);
    expectControlError(() => encodeAck([ackEntry(TX_A, 0, 0x1_0000_0000)]), ControlErrorCode.MalformedPayload);
    expectControlError(() => encodeAck([ackEntry("zz", 0, 0)]), ControlErrorCode.MalformedPayload);
  });
});

describe("NACK codec", () => {
  it("round-trips entries with ordered missing seqs", () => {
    const entries = [
      { transactionId: TX_A, missingSeqs: [1, 2, 7, 0xffffffff] },
      { transactionId: TX_B, missingSeqs: [] },
    ];
    expect(decodeNack(encodeNack(entries))).toEqual(entries);
  });

  it("accepts a u16-sized missing list on the wire", () => {
    const wire = new Uint8Array(1 + 16 + 2 + 0xffff * 4);
    wire[0] = 1;
    const dv = new DataView(wire.buffer);
    dv.setUint16(17, 0xffff, false);
    const decoded = decodeNack(wire);
    expect(decoded.length).toBe(1);
    expect(decoded[0].missingSeqs.length).toBe(0xffff);
  });

  it("rejects empty entries on encode and count 0 on decode", () => {
    expectControlError(() => encodeNack([]), ControlErrorCode.MalformedPayload);
    expectControlError(() => decodeNack(new Uint8Array([0])), ControlErrorCode.MalformedPayload);
  });

  it("rejects a missing list larger than u16", () => {
    const entries = [{ transactionId: TX_A, missingSeqs: new Array(0x1_0000).fill(0) }];
    expectControlError(() => encodeNack(entries), ControlErrorCode.MalformedPayload);
  });

  it("rejects truncated payloads mid-list", () => {
    const wire = encodeNack([{ transactionId: TX_A, missingSeqs: [1, 2, 3] }]);
    expectControlError(() => decodeNack(wire.subarray(0, wire.length - 2)), ControlErrorCode.MalformedPayload);
  });

  it("rejects trailing bytes after the last entry", () => {
    const wire = encodeNack([{ transactionId: TX_A, missingSeqs: [5] }]);
    const padded = new Uint8Array(wire.length + 3);
    padded.set(wire);
    expectControlError(() => decodeNack(padded), ControlErrorCode.MalformedPayload);
  });
});

describe("RESUME request codec", () => {
  it("round-trips entries sharing the ACK entry shape", () => {
    const entries = [ackEntry(TX_A, 12, 15), ackEntry(TX_B, 0, 3)];
    expect(decodeResumeRequest(encodeResumeRequest(entries))).toEqual(entries);
  });

  it("allows count 0 (nothing to resume)", () => {
    const wire = encodeResumeRequest([]);
    expect(wire.length).toBe(1);
    expect(wire[0]).toBe(0);
    expect(decodeResumeRequest(wire)).toEqual([]);
  });

  it("rejects malformed payloads", () => {
    expectControlError(() => encodeResumeRequest([ackEntry(TX_A, -1, 0)]), ControlErrorCode.MalformedPayload);
    expectControlError(
      () => decodeResumeRequest(encodeResumeRequest([ackEntry(TX_A, 1, 1)]).subarray(0, 10)),
      ControlErrorCode.MalformedPayload,
    );
  });
});

describe("RESUME response codec", () => {
  it("round-trips every registered status", () => {
    const entries = [
      { transactionId: TX_A, status: RESUME_STATUS.Resumed },
      { transactionId: TX_B, status: RESUME_STATUS.Rejected },
      { transactionId: "ab".repeat(16), status: RESUME_STATUS.Unknown },
    ];
    expect(decodeResumeResponse(encodeResumeResponse(entries))).toEqual(entries);
  });

  it("allows count 0", () => {
    expect(decodeResumeResponse(encodeResumeResponse([]))).toEqual([]);
  });

  it("rejects unregistered status values on both directions", () => {
    expectControlError(
      () => encodeResumeResponse([{ transactionId: TX_A, status: 0x04 }]),
      ControlErrorCode.UnsupportedResumeStatus,
    );
    expectControlError(
      () => encodeResumeResponse([{ transactionId: TX_A, status: 0 }]),
      ControlErrorCode.UnsupportedResumeStatus,
    );
    const wire = encodeResumeResponse([{ transactionId: TX_A, status: RESUME_STATUS.Resumed }]);
    wire[wire.length - 1] = 0x7f;
    expectControlError(() => decodeResumeResponse(wire), ControlErrorCode.UnsupportedResumeStatus);
  });

  it("rejects truncated payloads", () => {
    const wire = encodeResumeResponse([{ transactionId: TX_A, status: RESUME_STATUS.Resumed }]);
    expectControlError(() => decodeResumeResponse(wire.subarray(0, 10)), ControlErrorCode.MalformedPayload);
  });
});

describe("HEARTBEAT codec", () => {
  it("round-trips and uses the 12-byte big-endian layout", () => {
    const hb = { monotonicSeq: 0x11223344, senderEpochMs: 1728800000000 };
    const wire = encodeHeartbeat(hb);
    expect(wire.length).toBe(12);
    expect(decodeHeartbeat(wire)).toEqual(hb);
    const dv = new DataView(wire.buffer, wire.byteOffset, wire.byteLength);
    expect(dv.getUint32(0, false)).toBe(0x11223344);
    expect(dv.getBigUint64(4, false)).toBe(BigInt(1728800000000));
  });

  it("handles the u64 boundary at Number.MAX_SAFE_INTEGER", () => {
    const hb = { monotonicSeq: 0, senderEpochMs: Number.MAX_SAFE_INTEGER };
    expect(decodeHeartbeat(encodeHeartbeat(hb))).toEqual(hb);
  });

  it("rejects epochs beyond the safe integer range", () => {
    expectControlError(
      () => encodeHeartbeat({ monotonicSeq: 0, senderEpochMs: Number.MAX_SAFE_INTEGER + 1 }),
      ControlErrorCode.MalformedPayload,
    );
    const wire = new Uint8Array(12);
    new DataView(wire.buffer).setBigUint64(4, 1n << 53n, false); // 2^53 > 2^53-1
    expectControlError(() => decodeHeartbeat(wire), ControlErrorCode.MalformedPayload);
  });

  it("rejects negative / fractional epochs and wrong lengths", () => {
    expectControlError(() => encodeHeartbeat({ monotonicSeq: 0, senderEpochMs: -1 }), ControlErrorCode.MalformedPayload);
    expectControlError(() => encodeHeartbeat({ monotonicSeq: 0, senderEpochMs: 1.5 }), ControlErrorCode.MalformedPayload);
    expectControlError(() => decodeHeartbeat(new Uint8Array(11)), ControlErrorCode.MalformedPayload);
  });
});

describe("ERROR codec", () => {
  it("round-trips registry codes and messages", () => {
    const info = { code: ErrorCode.TransactionExpired, message: "transaction expired" };
    expect(decodeError(encodeError(info))).toEqual(info);
  });

  it("exposes the append-only registry anchors", () => {
    expect(ErrorCode.ProtocolVersionUnsupported).toBe(0x0001);
    expect(ErrorCode.TransactionExpired).toBe(0x0002);
    expect(ErrorCode.BackpressureStall).toBe(0x0003);
  });

  it("round-trips empty and non-ASCII UTF-8 messages", () => {
    expect(decodeError(encodeError({ code: 1, message: "" })).message).toBe("");
    const msg = "错误：backpressure stall — 停";
    expect(decodeError(encodeError({ code: ErrorCode.BackpressureStall, message: msg })).message).toBe(msg);
  });

  it("rejects invalid UTF-8 message bytes", () => {
    const wire = new Uint8Array([0x00, 0x01, 0xff, 0xfe]);
    expectControlError(() => decodeError(wire), ControlErrorCode.MalformedPayload);
  });

  it("rejects truncated payloads and out-of-range codes", () => {
    expectControlError(() => decodeError(new Uint8Array([0x00])), ControlErrorCode.MalformedPayload);
    expectControlError(() => encodeError({ code: 0x1_0000, message: "" }), ControlErrorCode.MalformedPayload);
  });
});

describe("error channel separation", () => {
  it("raises ControlError, never FrameError, from the control codecs", () => {
    try {
      decodeAck(new Uint8Array(0));
    } catch (err) {
      expect(err).toBeInstanceOf(ControlError);
      expect((err as Error).name).toBe("ControlError");
      return;
    }
    expect.unreachable("expected ControlError");
  });
});
