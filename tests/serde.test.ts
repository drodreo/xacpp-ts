/**
 * Serialization / deserialization correctness tests.
 *
 * Aligned with xacpp-rs/tests/serde_tests.rs.
 * Key verifications:
 * 1. XacppEvent JSON round-trip (generic { name, data } structure)
 * 2. XacppEnvelope envelope layer JSON round-trip (outer type routing + session_id)
 * 3. XacppResponse variant JSON format (kind field + camelCase)
 * 4. Deserialization from hand-written JSON
 * 5. Generic Command round-trip
 * 6. Convenience constructors
 * 7. Interaction payload serialization (no responder field)
 * 8. FileRef round-trip
 */

import { describe, it, expect } from "vitest";
import type { FileRef } from "../src/events/content";
import type { XacppEvent } from "../src/events/xacpp_event";
import { newEvent } from "../src/events/xacpp_event";
import type { XacppEnvelope } from "../src/message";
import { acknowledge, genericResponse, errorResponse } from "../src/message";
import type { XacppCommand } from "../src/commands";
import { genericCommand } from "../src/commands";
import type {
  ActionRequestPayload,
  QuestionPayload,
  SensitiveInfoOperationPayload,
} from "../src/events/interaction";

// ---- XacppEvent round-trip ----

describe("XacppEvent serialization", () => {
  it("generic event roundtrip", () => {
    const event: XacppEvent = {
      name: "think",
      data: { content: "thinking..." },
    };

    const json = JSON.stringify(event);
    expect(json).toContain('"name":"think"');
    expect(json).toContain('"content":"thinking..."');

    const de: XacppEvent = JSON.parse(json);
    expect(de.name).toBe("think");
    expect((de.data as { content: string }).content).toBe("thinking...");
  });

  it("event via newEvent constructor", () => {
    const event = newEvent("info", { title: "started", content: "" });

    const json = JSON.stringify(event);
    expect(json).toContain('"name":"info"');

    const de: XacppEvent = JSON.parse(json);
    expect(de.name).toBe("info");
    expect((de.data as { title: string }).title).toBe("started");
  });

  it("event with complex nested data roundtrip", () => {
    const event = newEvent("action_request", {
      activity: "act-1",
      requestId: "req-1",
      toolName: "bash",
      arguments: '{"command":"ls"}',
      actionId: "act-1",
      description: "list files",
      alert: "warn",
      intent: "list files",
    });

    const json = JSON.stringify(event);
    expect(json).toContain('"name":"action_request"');

    const de: XacppEvent = JSON.parse(json);
    expect(de.name).toBe("action_request");
    const data = de.data as ActionRequestPayload;
    expect(data.requestId).toBe("req-1");
    expect(data.toolName).toBe("bash");
  });

  it("event with null data roundtrip", () => {
    const event: XacppEvent = { name: "ping", data: null };

    const json = JSON.stringify(event);
    expect(json).toBe('{"name":"ping","data":null}');

    const de: XacppEvent = JSON.parse(json);
    expect(de.name).toBe("ping");
    expect(de.data).toBeNull();
  });
});

// ---- XacppEnvelope round-trip ----

describe("XacppEnvelope serialization", () => {
  it("wire request negotiate command roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "request",
      id: "r1",
      payload: {
        kind: "command",
        payload: { negotiate: { capabilities: { commands: [], events: [] } } },
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"request"');
    expect(json).toContain('"id":"r1"');
    expect(json).toContain('"kind":"command"');
    expect(json).toContain('"negotiate"');
    // session_id is not serialized when absent
    expect(json).not.toContain('"session_id"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("request");
    if (de.type === "request") {
      expect(de.id).toBe("r1");
      expect(de.payload.kind).toBe("command");
    }
  });

  it("wire request establish command roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "request",
      id: "r1",
      payload: {
        kind: "command",
        payload: { establish: { credentials: null } },
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"establish"');
    expect(json).toContain('"credentials":null');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("request");
    if (de.type === "request") {
      expect(de.payload.kind).toBe("command");
    }
  });

  it("wire request generic command with session_id roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "request",
      id: "r2",
      session_id: "s1",
      payload: {
        kind: "command",
        payload: { generic: { name: "new_activity", arguments: { title: "test" } } },
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"request"');
    expect(json).toContain('"id":"r2"');
    expect(json).toContain('"session_id":"s1"');
    expect(json).toContain('"kind":"command"');
    expect(json).toContain('"generic"');
    expect(json).toContain('"name":"new_activity"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("request");
    if (de.type === "request") {
      expect(de.id).toBe("r2");
      expect(de.session_id).toBe("s1");
      expect(de.payload.kind).toBe("command");
    }
  });

  it("wire request event with session_id roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "request",
      id: "r2",
      session_id: "s1",
      payload: {
        kind: "event",
        payload: { activity: "act-1", event: { name: "think", data: { content: "hi" } } },
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"request"');
    expect(json).toContain('"id":"r2"');
    expect(json).toContain('"session_id":"s1"');
    expect(json).toContain('"kind":"event"');
    expect(json).toContain('"name":"think"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("request");
    if (de.type === "request") {
      expect(de.session_id).toBe("s1");
      expect(de.payload.kind).toBe("event");
    }
  });

  it("wire response negotiated roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r1",
      payload: {
        kind: "negotiated",
        capabilities: { commands: [], events: [] },
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"response"');
    expect(json).toContain('"kind":"negotiated"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response" && de.payload.kind === "negotiated") {
      expect(de.payload.capabilities).toBeDefined();
    }
  });

  it("wire response established roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r1",
      payload: {
        kind: "established",
        sessionId: "s1",
        credentials: "test-creds",
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"response"');
    expect(json).toContain('"kind":"established"');
    // Response payload inner fields use camelCase
    expect(json).toContain('"sessionId":"s1"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response" && de.payload.kind === "established") {
      expect(de.payload.sessionId).toBe("s1");
    }
  });

  it("wire response establish_prepare roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r1",
      payload: {
        kind: "establish_prepare",
        challenge: "prove-your-identity",
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"kind":"establish_prepare"');
    expect(json).toContain('"challenge":"prove-your-identity"');

    const de: XacppEnvelope = JSON.parse(json);
    if (de.type === "response" && de.payload.kind === "establish_prepare") {
      expect(de.payload.challenge).toBe("prove-your-identity");
    }
  });

  it("wire response establish_reject roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r1",
      payload: {
        kind: "establish_reject",
        reason: "invalid credentials",
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"kind":"establish_reject"');
    expect(json).toContain('"reason":"invalid credentials"');

    const de: XacppEnvelope = JSON.parse(json);
    if (de.type === "response" && de.payload.kind === "establish_reject") {
      expect(de.payload.reason).toBe("invalid credentials");
    }
  });

  it("wire response generic roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r2",
      payload: {
        kind: "generic",
        name: "activity_list",
        data: { activities: ["act-1", "act-2"] },
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"response"');
    expect(json).toContain('"kind":"generic"');
    expect(json).toContain('"name":"activity_list"');
    expect(json).toContain('"activities"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response" && de.payload.kind === "generic") {
      expect(de.payload.name).toBe("activity_list");
      expect((de.payload.data as { activities: string[] }).activities).toEqual(["act-1", "act-2"]);
    }
  });

  it("wire response acknowledge (generic) roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r3",
      payload: {
        kind: "generic",
        name: "acknowledge",
        data: null,
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"response"');
    expect(json).toContain('"id":"r3"');
    expect(json).toContain('"kind":"generic"');
    expect(json).toContain('"name":"acknowledge"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response") {
      expect(de.id).toBe("r3");
      expect(de.payload.kind).toBe("generic");
    }
  });

  it("wire response error roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r4",
      payload: {
        kind: "error",
        code: "internal_error",
        message: "something went wrong",
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"response"');
    expect(json).toContain('"id":"r4"');
    expect(json).toContain('"kind":"error"');
    expect(json).toContain('"code":"internal_error"');
    expect(json).toContain('"message":"something went wrong"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response" && de.payload.kind === "error") {
      expect(de.payload.code).toBe("internal_error");
      expect(de.payload.message).toBe("something went wrong");
    }
  });
});

// ---- Deserialization from hand-written JSON ----

describe("JSON deserialization", () => {
  it("deserialize establish request from json", () => {
    const json = '{"type":"request","id":"r1","payload":{"kind":"command","payload":{"establish":{"credentials":null}}}}';
    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("request");
    if (de.type === "request") {
      expect(de.id).toBe("r1");
      expect(de.payload.kind).toBe("command");
    }
  });

  it("deserialize generic command request from json", () => {
    const json = '{"type":"request","id":"r1","session_id":"s1","payload":{"kind":"command","payload":{"generic":{"name":"new_activity","arguments":{"title":"test"}}}}}';
    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("request");
    if (de.type === "request") {
      expect(de.id).toBe("r1");
      expect(de.session_id).toBe("s1");
      expect(de.payload.kind).toBe("command");
    }
  });

  it("deserialize established response from json", () => {
    const json = '{"type":"response","id":"r1","payload":{"kind":"established","sessionId":"s1","credentials":"c"}}';
    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response" && de.payload.kind === "established") {
      expect(de.payload.sessionId).toBe("s1");
    }
  });

  it("deserialize generic response from json", () => {
    const json = '{"type":"response","id":"r1","payload":{"kind":"generic","name":"acknowledge","data":null}}';
    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response" && de.payload.kind === "generic") {
      expect(de.payload.name).toBe("acknowledge");
    }
  });
});

// ---- Generic Command round-trip ----

describe("Generic command serialization", () => {
  it("generic command via genericCommand() roundtrip", () => {
    const cmd = genericCommand("new_activity", { title: "test" });
    const json = JSON.stringify(cmd);
    expect(json).toContain('"generic"');
    expect(json).toContain('"name":"new_activity"');
    expect(json).toContain('"title":"test"');

    const de = JSON.parse(json) as XacppCommand;
    expect("generic" in de).toBe(true);
    if ("generic" in de) {
      expect(de.generic.name).toBe("new_activity");
      expect((de.generic.arguments as { title: string }).title).toBe("test");
    }
  });

  it("generic command literal roundtrip", () => {
    const cmd: XacppCommand = { generic: { name: "list_activities", arguments: { pageNum: 1, pageSize: 10 } } };
    const json = JSON.stringify(cmd);
    expect(json).toContain('"name":"list_activities"');
    expect(json).toContain('"pageNum":1');

    const de = JSON.parse(json) as XacppCommand;
    expect(de).toEqual(cmd);
  });

  it("negotiate command roundtrip", () => {
    const cmd: XacppCommand = { negotiate: { capabilities: { commands: [], events: [] } } };
    const json = JSON.stringify(cmd);
    expect(json).toContain('"negotiate"');

    const de = JSON.parse(json) as XacppCommand;
    expect("negotiate" in de).toBe(true);
  });

  it("establish command roundtrip", () => {
    const cmd: XacppCommand = { establish: { credentials: "test-creds" } };
    const json = JSON.stringify(cmd);
    expect(json).toContain('"establish"');
    expect(json).toContain('"credentials":"test-creds"');

    const de = JSON.parse(json) as XacppCommand;
    expect("establish" in de).toBe(true);
  });

  it("establish_confirm command roundtrip", () => {
    const cmd: XacppCommand = "establish_confirm";
    const json = JSON.stringify(cmd);
    expect(json).toBe('"establish_confirm"');

    const de = JSON.parse(json) as XacppCommand;
    expect(de).toBe("establish_confirm");
  });
});

// ---- Convenience constructors ----

describe("Convenience constructors", () => {
  it("acknowledge() returns generic acknowledge", () => {
    const resp = acknowledge();
    expect(resp.kind).toBe("generic");
    if (resp.kind === "generic") {
      expect(resp.name).toBe("acknowledge");
      expect(resp.data).toBeNull();
    }
  });

  it("genericResponse() returns generic with name and data", () => {
    const resp = genericResponse("activity_list", { total: 2 });
    expect(resp.kind).toBe("generic");
    if (resp.kind === "generic") {
      expect(resp.name).toBe("activity_list");
      expect((resp.data as { total: number }).total).toBe(2);
    }
  });

  it("errorResponse() returns error", () => {
    const resp = errorResponse("custom_error", "something failed");
    expect(resp.kind).toBe("error");
    if (resp.kind === "error") {
      expect(resp.code).toBe("custom_error");
      expect(resp.message).toBe("something failed");
    }
  });

  it("newEvent() creates event with name and data", () => {
    const event = newEvent("think", { content: "hello" });
    expect(event.name).toBe("think");
    expect((event.data as { content: string }).content).toBe("hello");
  });
});

// ---- Interaction payload serialization (no responder) ----

describe("Interaction payload serialization", () => {
  it("ActionRequestPayload roundtrip without responder", () => {
    const payload: ActionRequestPayload = {
      activity: "act-1",
      requestId: "req-r",
      toolName: "bash",
      arguments: '{"command":"ls"}',
      actionId: "act-r",
      description: "test",
      alert: "info",
      intent: "test",
    };

    const json = JSON.stringify(payload);
    // No responder field (removed in new protocol)
    expect(json).not.toContain("responder");
    expect(json).toContain('"requestId":"req-r"');
    expect(json).toContain('"toolName":"bash"');

    const de = JSON.parse(json) as ActionRequestPayload;
    expect(de.requestId).toBe("req-r");
    expect(de.toolName).toBe("bash");
  });

  it("QuestionPayload roundtrip", () => {
    const payload: QuestionPayload = {
      activity: "act-1",
      requestId: "req-2",
      question: "continue?",
      options: ["yes", "no"],
    };

    const json = JSON.stringify(payload);
    const de = JSON.parse(json) as QuestionPayload;
    expect(de.question).toBe("continue?");
    expect(de.options).toEqual(["yes", "no"]);
  });

  it("SensitiveInfoOperationPayload roundtrip", () => {
    const payload: SensitiveInfoOperationPayload = {
      activity: "act-1",
      requestId: "req-3",
      operation: {
        type: "collect",
        items: [
          {
            key: "API_KEY",
            displayText: "API Key",
            hint: "enter your key",
            siType: "secret",
          },
        ],
      },
    };

    const json = JSON.stringify(payload);
    const de = JSON.parse(json) as SensitiveInfoOperationPayload;
    expect(de.operation.type).toBe("collect");
    expect(de.operation.items).toHaveLength(1);
  });

  it("ActionRequestPayload as generic command arguments", () => {
    const cmd = genericCommand("action_request", {
      activity: "act-1",
      requestId: "req-1",
      toolName: "bash",
      arguments: "{}",
      actionId: "act-1",
      description: "test",
      alert: "info",
      intent: "test",
    } satisfies ActionRequestPayload);

    const json = JSON.stringify(cmd);
    expect(json).toContain('"name":"action_request"');
    expect(json).not.toContain("responder");

    const de = JSON.parse(json) as XacppCommand;
    if ("generic" in de) {
      expect(de.generic.name).toBe("action_request");
      const args = de.generic.arguments as ActionRequestPayload;
      expect(args.requestId).toBe("req-1");
    }
  });
});

// ---- FileRef round-trip tests ----

describe("FileRef serialization", () => {
  it("fileref full roundtrip", () => {
    const fileRef: FileRef = {
      remoteUrl: "https://example.com/file.png",
      localUri: "/tmp/file.png",
      remoteExpiresAt: "2026-05-18T12:00:00Z",
      mimeType: "image/png",
      requireOrganized: true,
      sizeBytes: 1024,
      sha256: "abc123",
    };

    const json = JSON.stringify(fileRef);
    expect(json).toContain('"remoteUrl":"https://example.com/file.png"');
    expect(json).toContain('"localUri":"/tmp/file.png"');
    expect(json).toContain('"remoteExpiresAt":"2026-05-18T12:00:00Z"');
    expect(json).toContain('"mimeType":"image/png"');
    expect(json).toContain('"requireOrganized":true');
    expect(json).toContain('"sizeBytes":1024');
    expect(json).toContain('"sha256":"abc123"');

    const de: FileRef = JSON.parse(json);
    expect(de).toEqual(fileRef);
  });

  it("fileref defaults roundtrip", () => {
    const fileRef: FileRef = {
      remoteUrl: "https://example.com/file.png",
      localUri: "/tmp/file.png",
      mimeType: "image/png",
      sizeBytes: 1024,
    };

    const json = JSON.stringify(fileRef);
    expect(json).not.toContain("remoteExpiresAt");
    expect(json).not.toContain("requireOrganized");
    expect(json).not.toContain("sha256");

    const de: FileRef = JSON.parse(json);
    expect(de.remoteUrl).toBe("https://example.com/file.png");
    expect(de.localUri).toBe("/tmp/file.png");
    expect(de.remoteExpiresAt).toBeUndefined();
    expect(de.mimeType).toBe("image/png");
    expect(de.requireOrganized).toBeUndefined();
    expect(de.sizeBytes).toBe(1024);
    expect(de.sha256).toBeUndefined();
  });

  it("fileref deserialize legacy format", () => {
    const json = '{"remoteUrl":"https://example.com/old.png","localUri":"/tmp/old.png","mimeType":"image/png","sizeBytes":512}';
    const de: FileRef = JSON.parse(json);
    expect(de.remoteUrl).toBe("https://example.com/old.png");
    expect(de.localUri).toBe("/tmp/old.png");
    expect(de.remoteExpiresAt).toBeUndefined();
    expect(de.mimeType).toBe("image/png");
    expect(de.requireOrganized).toBeUndefined();
    expect(de.sizeBytes).toBe(512);
    expect(de.sha256).toBeUndefined();
  });
});
