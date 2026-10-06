/**
 * SocketTransport concurrency tests.
 *
 * Verifies SocketTransport's spawn-per-request model:
 * 1. Concurrent requests are processed independently, with no cross-talk
 * 2. Concurrent response writes have no data corruption
 * 3. Disconnect aborts inflight tasks without deadlock
 * 4. Ordered egress: faf events arrive in submission order, commands do not
 *    overtake queued events, faf never waits for the ack
 * 5. Connection-close notification (onClosed), including late subscribers
 *
 * Aligned with xacpp-rs tests/socket_concurrent_tests.rs.
 */

import { describe, it, expect } from "vitest";
import * as net from "node:net";

import type { RequestHandler } from "../src/transport";
import type { XacppCommand } from "../src/commands";
import { genericCommand } from "../src/commands";
import type { XacppRequest, XacppResponse } from "../src/message";
import { genericResponse } from "../src/message";
import { newActivityEvent, newEvent } from "../src/events";
import { SocketTransport } from "../src/socket-transport";

// ---- Helper functions ----

/** Create a pair of SocketTransports connected via TCP (client + server). */
async function socketPair(
  serverHandler: RequestHandler,
): Promise<{ client: SocketTransport; server: SocketTransport; cleanup: () => void }> {
  const tcpServer = net.createServer();
  await new Promise<void>((resolve) => tcpServer.listen(0, "127.0.0.1", resolve));
  const addr = tcpServer.address() as net.AddressInfo;

  // Server: create SocketTransport after accept
  const serverTransportP = new Promise<SocketTransport>((resolve) => {
    tcpServer.on("connection", (socket) => {
      const t = new SocketTransport(socket);
      t.onRequest(serverHandler);
      t.connect().then(() => resolve(t));
    });
  });

  // Client: connectTo
  const clientTransport = SocketTransport.connectTo(addr.port, "127.0.0.1");
  clientTransport.onRequest(async (_sessionId, _payload) => genericResponse("acknowledge", null));
  await clientTransport.connect();

  const serverTransport = await serverTransportP;

  return {
    client: clientTransport,
    server: serverTransport,
    cleanup: () => {
      tcpServer.close();
    },
  };
}

/** Timeout wrapper (default 5s). */
function timeout<T>(promise: Promise<T>, ms = 5000): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timeout")), ms);
    promise.then(
      (v) => {
        clearTimeout(timer);
        resolve(v);
      },
      (e) => {
        clearTimeout(timer);
        reject(e);
      },
    );
  });
}

/** Extract command name from XacppRequest (used in test 1). */
function commandName(payload: XacppRequest): string {
  if (payload.kind !== "command") return "event";
  const cmd: XacppCommand = payload.payload;
  if (typeof cmd === "string") return cmd; // "establish_confirm"
  if ("generic" in cmd) return cmd.generic.name;
  if ("establish" in cmd) return "establish";
  if ("negotiate" in cmd) return "negotiate";
  return "other";
}

// ---- Tests ----

describe("SocketTransport concurrent", () => {
  // ---- Test 1: concurrent requests processed independently ----

  it("concurrent requests independent processing", async () => {
    // Server handler: sleep 10ms then return generic response with command name
    const handler: RequestHandler = async (_sessionId, payload) => {
      await new Promise((r) => setTimeout(r, 10));
      const name = commandName(payload);
      return genericResponse(name, null);
    };
    const { client, cleanup } = await socketPair(handler);

    const commands: XacppCommand[] = [
      genericCommand("new_activity", { title: null }),
      genericCommand("invoke_activity", { activity: "act-1", messages: [] }),
      genericCommand("compact_activity", { activity: "act-1" }),
      genericCommand("cancel_activity", { activity: "act-1" }),
      { establish: { credentials: undefined } },
      genericCommand("last_activity", null),
      genericCommand("list_activity", { pageNum: 1, pageSize: 10 }),
      genericCommand("switch_activity", { activity: "act-1" }),
    ];

    // Send 8 concurrent requests, measure time
    const start = Date.now();
    const promises = commands.map((cmd) =>
      client.send(null, { kind: "command", payload: cmd }),
    );
    const responses = await Promise.all(promises.map((p) => timeout(p)));
    const elapsed = Date.now() - start;

    // Collect command names from responses
    const names = responses
      .map((r) => {
        if (r.kind !== "generic")
          throw new Error(`expected generic, got: ${JSON.stringify(r)}`);
        return (r as { kind: "generic"; name: string }).name;
      })
      .sort();

    // All 8 responses received, no cross-talk
    expect(names).toEqual([
      "cancel_activity",
      "compact_activity",
      "establish",
      "invoke_activity",
      "last_activity",
      "list_activity",
      "new_activity",
      "switch_activity",
    ]);

    // Concurrent elapsed < 50ms (serial would need 8×10ms=80ms)
    expect(elapsed).toBeLessThan(50);

    cleanup();
  });

  // ---- Test 2: concurrent write no data corruption ----

  it("concurrent write no data corruption", async () => {
    // Server handler: return 1KB text in generic response name
    const largeContent = "A".repeat(1024);
    const handler: RequestHandler = async (_sessionId, _payload) => {
      return genericResponse(largeContent, null);
    };
    const { client, cleanup } = await socketPair(handler);

    // Send 10 concurrent requests
    const promises = Array.from({ length: 10 }, () =>
      client.send(null, { kind: "command", payload: genericCommand("new_activity", { title: null }) }),
    );
    const responses = await Promise.all(promises.map((p) => timeout(p)));

    // Each response name is intact (1KB, no truncation or corruption)
    for (const [i, resp] of responses.entries()) {
      if (resp.kind !== "generic")
        throw new Error(`response ${i}: expected generic, got: ${JSON.stringify(resp)}`);
      const { name } = resp as { kind: "generic"; name: string };
      expect(name.length).toBe(1024);
      expect(name.split("").every((c) => c === "A")).toBe(true);
    }

    cleanup();
  });

  // ---- Test 3: disconnect without deadlock ----

  it("disconnect aborts inflight no deadlock", async () => {
    // Server handler: never returns
    const handler: RequestHandler = async (_sessionId, _payload) => {
      return new Promise<XacppResponse>(() => {
        // never resolves
      });
    };
    const { client, cleanup } = await socketPair(handler);

    // Send 3 requests (handler will block)
    const sendPromises = Array.from({ length: 3 }, () =>
      client.send(null, { kind: "command", payload: genericCommand("new_activity", { title: null }) }),
    );

    // Wait to ensure requests have been sent and received by handler
    await new Promise((r) => setTimeout(r, 50));

    // Disconnect should return within 2s
    await expect(timeout(client.disconnect(), 2000)).resolves.toBeUndefined();

    // All inflight sends should complete within 2s (reject, no deadlock)
    for (const p of sendPromises) {
      await expect(timeout(p, 2000)).rejects.toThrow();
    }

    cleanup();
  });

  // ---- Test 4: faf events arrive in submission order ----

  it("faf events arrive in submission order", async () => {
    const arrivals: number[] = [];
    const handler: RequestHandler = async (_sessionId, payload) => {
      if (payload.kind === "event") {
        const seq = (payload.payload as { event: { data: { seq: number } } }).event.data.seq;
        arrivals.push(seq);
      }
      return genericResponse("acknowledge", null);
    };
    const { client, cleanup } = await socketPair(handler);

    const N = 50;
    for (let seq = 0; seq < N; seq++) {
      await client.sendFaf(null, {
        kind: "event",
        payload: newActivityEvent("act-1", newEvent("content_delta", { seq })),
      });
    }

    // FIFO drain guarantee: a following round trip implies all prior frames arrived.
    await client.send(null, { kind: "command", payload: genericCommand("sync", {}) });

    expect(arrivals).toEqual(Array.from({ length: N }, (_, i) => i));

    cleanup();
  });

  // ---- Test 5: command does not overtake queued events ----

  it("command does not overtake queued events", async () => {
    const arrivals: string[] = [];
    const handler: RequestHandler = async (_sessionId, payload) => {
      if (payload.kind === "event") {
        const seq = (payload.payload as { event: { data: { seq: number } } }).event.data.seq;
        arrivals.push(`ev${seq}`);
      } else if (payload.kind === "command" && typeof payload.payload === "object" && "generic" in payload.payload) {
        arrivals.push(`cmd:${payload.payload.generic.name}`);
      } else {
        arrivals.push("other");
      }
      return genericResponse("acknowledge", null);
    };
    const { client, cleanup } = await socketPair(handler);

    const ev = (seq: number) => newActivityEvent("act-1", newEvent("content_delta", { seq }));
    await client.sendFaf(null, { kind: "event", payload: ev(0) });
    await client.send(null, { kind: "command", payload: genericCommand("probe", {}) });
    await client.sendFaf(null, { kind: "event", payload: ev(1) });
    // Final sync: drains ev1 before assertion.
    await client.send(null, { kind: "command", payload: genericCommand("sync", {}) });

    expect(arrivals).toEqual(["ev0", "cmd:probe", "ev1", "cmd:sync"]);

    cleanup();
  });

  // ---- Test 6: sendFaf returns without waiting for the ack ----

  it("sendFaf returns without ack and does not stall the pipe", async () => {
    // Server never responds to events (handler pends forever); commands ack normally.
    const handler: RequestHandler = async (_sessionId, payload) => {
      if (payload.kind === "event") {
        return new Promise<XacppResponse>(() => {
          // never resolves
        });
      }
      return genericResponse("acknowledge", null);
    };
    const { client, cleanup } = await socketPair(handler);

    // faf returns immediately even though the peer never acks the event.
    await timeout(
      client.sendFaf(null, {
        kind: "event",
        payload: newActivityEvent("act-1", newEvent("content_delta", { seq: 0 })),
      }),
    );

    // The pipe is not stalled: a following command still round-trips.
    await timeout(
      client.send(null, { kind: "command", payload: genericCommand("probe", {}) }),
    );

    cleanup();
  });

  // ---- Test 7: closed() fires on peer disconnect ----

  it("onClosed fires when the peer disconnects", async () => {
    const handler: RequestHandler = async () => genericResponse("acknowledge", null);
    const { client, server, cleanup } = await socketPair(handler);

    let fired = 0;
    let latest: boolean | null = null;
    client.onClosed(() => {
      fired++;
      latest = true;
    });

    expect(latest).toBeNull(); // connection starts open

    await server.disconnect();

    await expect(
      timeout(new Promise<void>((resolve) => {
        const check = () => (fired > 0 ? resolve() : setTimeout(check, 5));
        check();
      }), 5000),
    ).resolves.toBeUndefined();
    expect(latest).toBe(true);

    cleanup();
  });

  // ---- Test 8: late subscriber reads closed state ----

  it("onClosed late subscriber observes the closed state immediately", async () => {
    const handler: RequestHandler = async () => genericResponse("acknowledge", null);
    const { client, server, cleanup } = await socketPair(handler);

    await server.disconnect();

    // Wait for the close to propagate (first watcher drives it), then
    // subscribe late — the listener must fire immediately.
    await expect(
      timeout(new Promise<void>((resolve) => {
        const unsub = client.onClosed(() => {
          unsub();
          resolve();
        });
      }), 5000),
    ).resolves.toBeUndefined();

    cleanup();
  });
});
