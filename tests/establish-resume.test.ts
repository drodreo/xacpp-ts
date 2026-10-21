/**
 * Recovery over the real establish handshake (SPEC §5).
 *
 * Pin the RESUME declaration timing against the establish topology: the
 * declaration must land only after BOTH peers' transfer planes are wired.
 * The bug these anchor: the declaration fired at wire-up, which on the
 * initiator side precedes the responder's onEstablish wire — the RESUME
 * frame was dropped by the not-yet-wired receiver and the holder
 * deadlocked awaiting a verdict that could never arrive.
 *
 * RED anchor discipline: both cases must fail by timeout before the fix
 * and pass deterministically after it (loopback resume is milliseconds;
 * the cap only distinguishes deadlock from success).
 */

import * as net from "node:net";

import { describe, expect, it } from "vitest";

import { SocketTransport } from "../src/carrier/socket";
import type { XacppTransport } from "../src/carrier/transport";
import type { TransferWiring } from "../src/transfer/binding";
import { XacppPeer } from "../src/session/peer";
import { XacppSession } from "../src/session/session";
import type {
  EstablishDecision,
  EstablishHandler,
  NegotiateHandler,
  XacppSessionHandler,
} from "../src/session/handler";
import type { Capabilities, EffectiveCapabilities } from "../src/wire/capability";
import { acknowledge, type XacppResponse } from "../src/wire/message";

const RESUME_CAP_MS = 5_000;

function caps(): Capabilities {
  return { commands: [], produceEvents: [], acceptEvents: [] };
}

class AcceptAllNegotiate implements NegotiateHandler {
  async onNegotiate(_effective: EffectiveCapabilities): Promise<void> {}
}

class AckSessionHandler implements XacppSessionHandler {
  async onCommand(): Promise<XacppResponse> {
    return acknowledge();
  }
  async onEvent(): Promise<XacppResponse> {
    return acknowledge();
  }
}

/** Never consulted on the initiator side, but the peer constructor needs one. */
class NeverEstablish implements EstablishHandler {
  async onEstablish(): Promise<EstablishDecision> {
    throw new Error("never an initiator");
  }
  async onEstablishConfirm(): Promise<never> {
    throw new Error("never an initiator");
  }
}

/**
 * Responder-side fixture following the documented consumer pattern: the
 * session is built on first establish and REUSED on reconnect, wired to
 * the live transport inside onEstablish. Completed inbound binaries are
 * collected for byte-exactness checks.
 */
class ResponderFixture implements EstablishHandler {
  private session: XacppSession | null = null;
  readonly receivedChunks: Uint8Array[] = [];
  private claimWaiters: Array<() => void> = [];
  private claimedOnce = false;

  /** Resolves once the inbound stream has been claimed (first chunk read). */
  waitStreamClaimed(): Promise<void> {
    if (this.claimedOnce) return Promise.resolve();
    return new Promise((resolve) => this.claimWaiters.push(resolve));
  }

  async onEstablish(
    transport: XacppTransport,
    _credentials: string | undefined,
  ): Promise<EstablishDecision> {
    if (this.session === null) {
      const session = new XacppSession();
      // Stream surface only: a transaction claimed by a stream never
      // reaches onBinary — collect everything through the stream for the
      // byte-exactness check.
      session.onBinaryStream((stream) => {
        stream.onChunk((chunk) => {
          this.receivedChunks.push(chunk);
          if (!this.claimedOnce) {
            this.claimedOnce = true;
            for (const w of this.claimWaiters.splice(0)) w();
          }
        });
      });
      this.session = session;
    }
    // The peer's transfer plane on the initiator side was wired at connect
    // — wiring (and declaring) here is safe.
    this.session.wire(transport as XacppTransport & TransferWiring);
    return {
      type: "established",
      sessionId: "responder-sid",
      credentials: "responder-creds",
      handler: new AckSessionHandler(),
    };
  }

  async onEstablishConfirm(): Promise<never> {
    throw new Error("no challenge in this fixture");
  }

  holderSession(): XacppSession {
    if (this.session === null) throw new Error("session built at establish");
    return this.session;
  }
}

interface SocketPairPending {
  client: SocketTransport;
  serverP: Promise<SocketTransport>;
  listener: net.Server;
}

async function socketListen(): Promise<SocketPairPending> {
  const listener = net.createServer();
  const serverP = new Promise<SocketTransport>((resolve) => {
    listener.on("connection", (socket) => resolve(new SocketTransport(socket)));
  });
  await new Promise<void>((resolve) => listener.listen(0, "127.0.0.1", resolve));
  const addr = listener.address() as net.AddressInfo;
  const client = SocketTransport.connectTo(addr.port, "127.0.0.1");
  return { client, serverP, listener };
}

async function establishRound(
  sessionA: XacppSession,
  responder: ResponderFixture,
): Promise<{ peerA: XacppPeer; peerB: XacppPeer; listener: net.Server }> {
  const { client, serverP, listener } = await socketListen();
  // Connect order matters: the peer owns transport.connect(), and the TCP
  // accept (hence the server transport) only exists once the client side
  // has connected.
  const peerA = new XacppPeer(caps(), client, new AcceptAllNegotiate(), new NeverEstablish(), sessionA);
  await peerA.connect();
  const server = await serverP;
  const peerB = new XacppPeer(caps(), server, new AcceptAllNegotiate(), responder);
  await peerB.connect();
  await peerA.negotiate();
  await peerA.establish(undefined, new AckSessionHandler(), () => {});
  return { peerA, peerB, listener };
}

function pattern(len: number): Uint8Array {
  const out = new Uint8Array(len);
  for (let i = 0; i < len; i++) out[i] = (i * 31 + (i >> 7)) & 0xff;
  return out;
}

describe("establish-topology recovery (SPEC §5 declaration timing)", () => {
  it("inflight transfer resumes across reconnect — holder = initiator", async () => {
    const sessionA = new XacppSession();
    const responder = new ResponderFixture();
    const round1 = await establishRound(sessionA, responder);

    // 16 MiB inflight. Wait until the receiver holds an OPEN inbound slot
    // (first chunk read), then tear down mid-flight — a zero-progress
    // receive is indistinguishable from "never seen" after a reconnect and
    // is correctly adjudicated unknown (no-tombstone cost).
    const payload = pattern(16 * 1024 * 1024);
    const done = new Promise<import("../src/transfer").BinarySendResult>((resolve) => {
      sessionA.sendBinary(payload, (result) => resolve(result));
    });
    await Promise.race([
      responder.waitStreamClaimed(),
      new Promise<never>((_resolve, reject) =>
        setTimeout(() => reject(new Error("receiver must claim the inbound stream before the outage")), RESUME_CAP_MS),
      ),
    ]);
    await round1.peerA.disconnect();
    await round1.peerB.disconnect();
    round1.listener.close();

    // Round 2: same initiator session (pending copies retained); the
    // responder reuses its session by identity.
    const round2 = await establishRound(sessionA, responder);

    const result = await Promise.race([
      done,
      new Promise<never>((_resolve, reject) =>
        setTimeout(
          () => reject(new Error("RESUME resume chain deadlocked (declaration lost before the peer was wired)")),
          RESUME_CAP_MS,
        ),
      ),
    ]);
    expect(result.confirmed).toBe(true);
    const got = Buffer.concat(responder.receivedChunks.map((c) => Buffer.from(c)));
    expect(got.length).toBe(payload.length);
    expect(Buffer.compare(got, Buffer.from(payload))).toBe(0);

    await round2.peerA.disconnect();
    await round2.peerB.disconnect();
    round2.listener.close();
  }, 20_000);

  it("inflight transfer resumes across reconnect — holder = responder", async () => {
    const sessionA = new XacppSession();
    const responder = new ResponderFixture();
    const round1 = await establishRound(sessionA, responder);

    // Receiver (initiator side): claim the inbound stream and collect it —
    // the receiver must hold an OPEN inbound slot before the outage, since
    // a zero-progress receive is indistinguishable from "never seen" after
    // a reconnect and is correctly adjudicated unknown (no-tombstone cost).
    let claimed: () => void = () => {};
    const claimedP = new Promise<void>((resolve) => {
      claimed = resolve;
    });
    const receivedChunks: Uint8Array[] = [];
    sessionA.onBinaryStream((stream) => {
      stream.onChunk((chunk) => {
        receivedChunks.push(chunk);
        claimed();
      });
    });

    const payload = pattern(16 * 1024 * 1024);
    const holder = responder.holderSession();
    const done = new Promise<import("../src/transfer").BinarySendResult>((resolve) => {
      holder.sendBinary(payload, (result) => resolve(result));
    });

    // Wait until the receiver has an OPEN slot (first chunk read), then
    // tear down mid-flight.
    await Promise.race([
      claimedP,
      new Promise<never>((_resolve, reject) =>
        setTimeout(() => reject(new Error("receiver must claim the inbound stream before the outage")), RESUME_CAP_MS),
      ),
    ]);
    await round1.peerB.disconnect();
    await round1.peerA.disconnect();
    round1.listener.close();

    const round2 = await establishRound(sessionA, responder);

    const result = await Promise.race([
      done,
      new Promise<never>((_resolve, reject) =>
        setTimeout(
          () => reject(new Error("RESUME resume chain deadlocked (declaration lost before the peer was wired)")),
          RESUME_CAP_MS,
        ),
      ),
    ]);
    expect(result.confirmed).toBe(true);

    await round2.peerA.disconnect();
    await round2.peerB.disconnect();
    round2.listener.close();
  }, 20_000);
});
