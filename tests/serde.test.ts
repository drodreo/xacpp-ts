/**
 * Serialization / deserialization correctness tests.
 *
 * Aligned with xacpp-rs/tests/serde_tests.rs, 15 test cases.
 * Key verifications:
 * 1. XacppEvent JSON round-trip (type field routing)
 * 2. XacppEnvelope envelope layer JSON round-trip (type field outer routing + session_id)
 * 3. XacppResponse variant JSON format (kind field + flatten field + camelCase)
 * 4. Deserialization from hand-written JSON (verifies adjacently tagged format)
 * 5. ActionRequest serialization without responder field
 */

import { describe, it, expect } from "vitest";
import type { XacppEvent } from "../src/events/xacpp_event";
import type { XacppEnvelope, XacppRequest, XacppResponse } from "../src/message";
import type { ActivityInfo } from "../src/message";

// ---- XacppEvent round-trip ----

describe("XacppEvent serialization", () => {
  it("test_event_action_request_roundtrip", () => {
    const event: XacppEvent = {
      type: "action_request",
      requestId: "req-1",
      toolName: "bash",
      arguments: '{"command":"ls"}',
      actionId: "act-1",
      description: "list files",
      alert: "warn",
    };

    const json = JSON.stringify(event);
    expect(json).toContain('"type":"action_request"');

    const de: XacppEvent = JSON.parse(json);
    expect(de.type).toBe("action_request");
    if (de.type === "action_request") {
      expect(de.requestId).toBe("req-1");
      expect(de.toolName).toBe("bash");
    }
  });

  it("test_event_think_roundtrip", () => {
    const event: XacppEvent = {
      type: "think",
      content: "thinking...",
    };

    const json = JSON.stringify(event);
    expect(json).toContain('"type":"think"');

    const de: XacppEvent = JSON.parse(json);
    expect(de.type).toBe("think");
    if (de.type === "think") {
      expect(de.content).toBe("thinking...");
    }
  });

  it("test_event_question_roundtrip", () => {
    const event: XacppEvent = {
      type: "question",
      requestId: "req-2",
      question: "continue?",
      options: ["yes", "no"],
    };

    const json = JSON.stringify(event);
    expect(json).toContain('"type":"question"');

    const de: XacppEvent = JSON.parse(json);
    expect(de.type).toBe("question");
    if (de.type === "question") {
      expect(de.requestId).toBe("req-2");
      expect(de.options).toEqual(["yes", "no"]);
    }
  });

  it("test_event_sensitive_info_roundtrip", () => {
    const event: XacppEvent = {
      type: "sensitive_info_operation",
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

    const json = JSON.stringify(event);
    expect(json).toContain('"type":"sensitive_info_operation"');

    const de: XacppEvent = JSON.parse(json);
    expect(de.type).toBe("sensitive_info_operation");
  });

  it("test_event_info_roundtrip", () => {
    const event: XacppEvent = {
      type: "info",
      title: "started",
      content: "",
    };

    const json = JSON.stringify(event);
    expect(json).toContain('"type":"info"');

    const de: XacppEvent = JSON.parse(json);
    expect(de.type).toBe("info");
    if (de.type === "info") {
      expect(de.title).toBe("started");
    }
  });
});

// ---- XacppEnvelope round-trip ----

describe("XacppEnvelope serialization", () => {
  it("test_wire_request_command_roundtrip", () => {
    // Establish command (replaces legacy Authenticate string enum), envelope has no session_id
    const wire: XacppEnvelope = {
      type: "request",
      id: "r1",
      payload: {
        kind: "command",
        payload: { establish: { credentials: null } },
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"request"');
    expect(json).toContain('"id":"r1"');
    expect(json).toContain('"kind":"command"');
    // Establish command is in object form on the wire
    expect(json).toContain('"establish"');
    expect(json).toContain('"credentials":null');
    // session_id is not serialized when absent
    expect(json).not.toContain('"session_id"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("request");
    if (de.type === "request") {
      expect(de.id).toBe("r1");
      expect(de.payload.kind).toBe("command");
      if (de.payload.kind === "command") {
        const cmd = de.payload.payload;
        expect(cmd).toEqual({ establish: { credentials: null } });
      }
    }
  });

  it("test_wire_request_event_with_session_id_roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "request",
      id: "r2",
      session_id: "s1",
      payload: {
        kind: "event",
        payload: {
          type: "action_request",
          requestId: "req-1",
          toolName: "bash",
          arguments: "{}",
          actionId: "act-1",
          description: "test",
          alert: "info",
        },
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"request"');
    expect(json).toContain('"id":"r2"');
    expect(json).toContain('"session_id":"s1"');
    expect(json).toContain('"kind":"event"');
    expect(json).toContain('"type":"action_request"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("request");
    if (de.type === "request") {
      expect(de.id).toBe("r2");
      expect(de.session_id).toBe("s1");
      expect(de.payload.kind).toBe("event");
      if (de.payload.kind === "event" && de.payload.payload.type === "action_request") {
        expect(de.payload.payload.requestId).toBe("req-1");
      }
    }
  });

  it("test_wire_response_established_roundtrip", () => {
    // Replaces legacy paring, verifies sessionId (camelCase)
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
    expect(json).toContain('"id":"r1"');
    expect(json).toContain('"kind":"established"');
    // Response payload inner fields use camelCase
    expect(json).toContain('"sessionId":"s1"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response") {
      expect(de.id).toBe("r1");
      if (de.payload.kind === "established") {
        expect(de.payload.sessionId).toBe("s1");
      }
    }
  });

  it("test_wire_response_action_roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r2",
      payload: {
        kind: "action",
        requestId: "req-1",
        type: "approve",
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"response"');
    expect(json).toContain('"kind":"action"');
    expect(json).toContain('"requestId":"req-1"');
    // flatten: type field appears directly at response level
    expect(json).toContain('"type":"approve"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response") {
      if (de.payload.kind === "action") {
        expect(de.payload.requestId).toBe("req-1");
        expect(de.payload.type).toBe("approve");
      }
    }
  });

  it("test_wire_response_sensitive_info_operation_roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r5",
      payload: {
        kind: "sensitive_info_operation",
        requestId: "req-1",
        results: [
          {
            type: "provided",
            key: "API_KEY",
            value: "secret",
          },
        ],
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"response"');
    expect(json).toContain('"kind":"sensitive_info_operation"');
    expect(json).toContain('"requestId":"req-1"');
    // flatten: results appear directly at response level
    expect(json).toContain('"results"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response") {
      if (de.payload.kind === "sensitive_info_operation") {
        expect(de.payload.requestId).toBe("req-1");
        expect(de.payload.results).toHaveLength(1);
      }
    }
  });

  it("test_wire_response_acknowledge_roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r3",
      payload: {
        kind: "acknowledge",
      },
    };

    const json = JSON.stringify(wire);
    expect(json).toContain('"type":"response"');
    expect(json).toContain('"id":"r3"');
    expect(json).toContain('"kind":"acknowledge"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response") {
      expect(de.id).toBe("r3");
      expect(de.payload.kind).toBe("acknowledge");
    }
  });

  it("test_wire_response_error_roundtrip", () => {
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
    if (de.type === "response") {
      expect(de.id).toBe("r4");
      if (de.payload.kind === "error") {
        expect(de.payload.code).toBe("internal_error");
        expect(de.payload.message).toBe("something went wrong");
      }
    }
  });
});

// ---- Deserialization from hand-written JSON ----

describe("JSON deserialization", () => {
  it("test_deserialize_request_from_json", () => {
    // Request envelope containing Establish command
    const json = '{"type":"request","id":"r1","payload":{"kind":"command","payload":{"establish":{"credentials":null}}}}';
    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("request");
    if (de.type === "request") {
      expect(de.id).toBe("r1");
      expect(de.payload.kind).toBe("command");
      if (de.payload.kind === "command") {
        expect(de.payload.payload).toEqual({ establish: { credentials: null } });
      }
    }
  });

  it("test_deserialize_response_from_json", () => {
    // Envelope containing established response
    const json = '{"type":"response","id":"r1","payload":{"kind":"established","sessionId":"s1"}}';
    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response") {
      expect(de.id).toBe("r1");
      if (de.payload.kind === "established") {
        expect(de.payload.sessionId).toBe("s1");
      }
    }
  });
});

// ---- Interactive event responder skip verification ----

describe("ActionRequest without responder", () => {
  it("test_action_request_serializes_without_responder", () => {
    // TS-side ActionRequestEvent has no responder field
    const event: XacppEvent = {
      type: "action_request",
      requestId: "req-r",
      toolName: "bash",
      arguments: "{}",
      actionId: "act-r",
      description: "test",
      alert: "info",
    };

    const json = JSON.stringify(event);
    // Verify serialization works correctly, does not include responder
    expect(json).not.toContain("responder");
    expect(json).toContain('"requestId":"req-r"');

    const de: XacppEvent = JSON.parse(json);
    expect(de.type).toBe("action_request");
  });
});

// ---- New Command / Response / Event round-trip tests ----

describe("New types serialization", () => {
  it("command last_activity roundtrip", () => {
    const cmd: XacppCommand = "last_activity";
    const json = JSON.stringify(cmd);
    expect(json).toBe('"last_activity"');

    const de: XacppCommand = JSON.parse(json);
    expect(de).toBe("last_activity");
  });

  it("command list_activity with query roundtrip", () => {
    const cmd: XacppCommand = { list_activity: { query: "test", pageNum: 1, pageSize: 10 } };
    const json = JSON.stringify(cmd);
    expect(json).toContain('"list_activity"');
    expect(json).toContain('"query":"test"');
    expect(json).toContain('"pageNum":1');
    expect(json).toContain('"pageSize":10');

    const de: XacppCommand = JSON.parse(json);
    expect(de).toEqual(cmd);
  });

  it("command list_activity without query roundtrip", () => {
    const cmd: XacppCommand = { list_activity: { pageNum: 1, pageSize: 10 } };
    const json = JSON.stringify(cmd);
    expect(json).toContain('"list_activity"');
    expect(json).not.toContain("query");
    expect(json).toContain('"pageNum":1');
    expect(json).toContain('"pageSize":10');

    const de: XacppCommand = JSON.parse(json);
    expect(de).toEqual(cmd);
  });

  it("command switch_activity roundtrip", () => {
    const cmd: XacppCommand = { switch_activity: { activity: "act-1" } };
    const json = JSON.stringify(cmd);
    expect(json).toContain('"switch_activity"');
    expect(json).toContain('"activity":"act-1"');

    const de: XacppCommand = JSON.parse(json);
    expect(de).toEqual(cmd);
  });

  it("response activity_ready roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r1",
      payload: {
        kind: "activity_ready",
        activity: "act-1",
        agent: "x-agent",
        title: "test title",
      },
    };
    const json = JSON.stringify(wire);
    expect(json).toContain('"kind":"activity_ready"');
    expect(json).toContain('"activity":"act-1"');
    expect(json).toContain('"agent":"x-agent"');
    expect(json).toContain('"title":"test title"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response" && de.payload.kind === "activity_ready") {
      expect(de.payload.activity).toBe("act-1");
      expect(de.payload.agent).toBe("x-agent");
      expect(de.payload.title).toBe("test title");
    }
  });

  it("response activity_not_found roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r1",
      payload: { kind: "activity_not_found" },
    };
    const json = JSON.stringify(wire);
    expect(json).toContain('"kind":"activity_not_found"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response") {
      expect(de.payload.kind).toBe("activity_not_found");
    }
  });

  it("response available_activities roundtrip", () => {
    const wire: XacppEnvelope = {
      type: "response",
      id: "r1",
      payload: {
        kind: "available_activities",
        total: 2,
        activities: [
          { activity: "act-1", agent: "x-agent", title: "title 1" },
          { activity: "act-2", agent: "x-agent" },
        ],
      },
    };
    const json = JSON.stringify(wire);
    expect(json).toContain('"kind":"available_activities"');
    expect(json).toContain('"total":2');
    expect(json).toContain('"activities"');

    const de: XacppEnvelope = JSON.parse(json);
    expect(de.type).toBe("response");
    if (de.type === "response" && de.payload.kind === "available_activities") {
      expect(de.payload.total).toBe(2);
      expect(de.payload.activities).toHaveLength(2);
      expect(de.payload.activities[0].activity).toBe("act-1");
      expect(de.payload.activities[1].activity).toBe("act-2");
    }
  });

  it("event activity_updates roundtrip", () => {
    const event: XacppEvent = {
      type: "activity_updates",
      activity: "act-1",
      agent: "x-agent",
      title: "updated title",
    };
    const json = JSON.stringify(event);
    expect(json).toContain('"type":"activity_updates"');
    expect(json).toContain('"activity":"act-1"');

    const de: XacppEvent = JSON.parse(json);
    expect(de.type).toBe("activity_updates");
    if (de.type === "activity_updates") {
      expect(de.activity).toBe("act-1");
      expect(de.agent).toBe("x-agent");
    }
  });
});
