/**
 * Activity reference (envelope structuring) tests.
 *
 * Aligned with xacpp-rs 0.8.0 contract (wire JSON must match both sides):
 * 1. Generic command with activity serialization (exact wire form)
 * 2. Generic command without activity omits the field
 * 3. Activity event envelope serializes activity as {id} structure
 * 4. Generic command with activity parse roundtrip
 */

import { describe, it, expect } from "vitest";
import { genericCommand } from "../src/commands";
import type { XacppCommand } from "../src/commands";
import type { ActivityRef } from "../src/activity-ref";
import type { XacppActivityEvent } from "../src/events";
import { newActivityEvent } from "../src/events";
import { newEvent } from "../src/events";

// ---- Generic command activity ----

describe("Generic command activity serialization", () => {
  it("generic command with activity serializes exact wire form", () => {
    const cmd = genericCommand("report_to_user", { content: [] }, { id: "act-1" });

    const json = JSON.stringify(cmd);
    expect(json).toBe(
      '{"generic":{"name":"report_to_user","arguments":{"content":[]},"activity":{"id":"act-1"}}}',
    );
  });

  it("generic command without activity omits the field", () => {
    const cmd = genericCommand("new_activity", { title: "t" });

    const json = JSON.stringify(cmd);
    expect(json).toBe('{"generic":{"name":"new_activity","arguments":{"title":"t"}}}');
    expect(json).not.toContain('"activity"');
  });

  it("generic command with activity parse roundtrip", () => {
    const cmd = genericCommand("report_to_user", { content: [] }, { id: "act-1" });

    const json = JSON.stringify(cmd);
    const de = JSON.parse(json) as XacppCommand;
    expect(typeof de === "object" && "generic" in de).toBe(true);
    if (typeof de === "object" && "generic" in de) {
      expect(de.generic.name).toBe("report_to_user");
      expect(de.generic.arguments).toEqual({ content: [] });
      expect(de.generic.activity).toEqual({ id: "act-1" });
      expect((de.generic.activity as ActivityRef).id).toBe("act-1");
    }
  });
});

// ---- Activity event envelope ----

describe("Activity event envelope serialization", () => {
  it("activity event envelope serializes activity as {id} structure", () => {
    const envelope = newActivityEvent("act-1", newEvent("think", { content: "hi" }));

    const json = JSON.stringify(envelope);
    expect(json).toContain('"activity":{"id":"act-1"}');
    expect(json).toContain('"name":"think"');

    const de = JSON.parse(json) as XacppActivityEvent;
    expect(de.activity.id).toBe("act-1");
    expect(de.event.name).toBe("think");
  });
});
