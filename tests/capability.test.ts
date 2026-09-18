/**
 * Command declaration schema typing tests.
 *
 * Aligned with xacpp-rs/src/capability.rs (CommandDeclaration test group).
 * Key verifications:
 * 1. Full declaration parse + helper semantics
 * 2. Minimal declaration defaults (absent dispatcher = bridge, absent scopes = conversation only)
 * 3. Unknown fields tolerated (preserved at runtime)
 * 4. Unknown dispatcher / scope values kept as plain strings
 * 5. Wire (camelCase) round-trip fidelity
 */

import { describe, it, expect } from "vitest";
import {
  parseCommandDeclaration,
  isToolFacing,
  requireToolCall,
  hasCompactScope,
} from "../src/capability";
import type { CommandDeclaration } from "../src/capability";

describe("CommandDeclaration", () => {
  it("full declaration parse and helpers", () => {
    const raw: Record<string, unknown> = {
      name: "new_activity",
      description: "Create a new activity",
      parameters: {
        type: "object",
        properties: { title: { type: "string" } },
      },
      dispatcher: "tool",
      evaluationPolicy: {
        requireToolCall: {
          require: "report_to_user",
          onFailure: "You must reply via report_to_user.",
        },
      },
      extraScopes: ["compact"],
    };

    const decl = parseCommandDeclaration(raw);
    expect(decl.name).toBe("new_activity");
    expect(decl.description).toBe("Create a new activity");
    expect(decl.parameters).toEqual({
      type: "object",
      properties: { title: { type: "string" } },
    });
    expect(isToolFacing(decl)).toBe(true);
    expect(requireToolCall(decl)?.require).toBe("report_to_user");
    expect(requireToolCall(decl)?.onFailure).toBe("You must reply via report_to_user.");
    expect(hasCompactScope(decl)).toBe(true);
  });

  it("minimal declaration defaults: absent dispatcher = bridge, absent scopes = conversation only", () => {
    const decl = parseCommandDeclaration({ name: "cmd" });
    expect(isToolFacing(decl)).toBe(false);
    expect(decl.dispatcher).toBeUndefined();
    expect(hasCompactScope(decl)).toBe(false);
    expect(decl.extraScopes).toBeUndefined();
    expect(requireToolCall(decl)).toBeUndefined();
  });

  it("unknown fields are tolerated and preserved at runtime", () => {
    const raw = { name: "cmd", futureField: { anything: true } };
    const decl = parseCommandDeclaration(raw);
    expect(decl.name).toBe("cmd");
    expect(decl.description).toBeUndefined();
    // parse is runtime-transparent: the original object (with unknown fields) is returned as-is.
    expect(decl).toBe(raw);
    expect(decl).toHaveProperty("futureField", { anything: true });
  });

  it("unknown dispatcher and scope values are kept as plain strings", () => {
    const decl = parseCommandDeclaration({
      name: "cmd",
      dispatcher: "turbo",
      extraScopes: ["compact", "sidebar"],
    });
    expect(decl.dispatcher).toBe("turbo");
    // Unknown dispatcher is conservatively not tool-facing.
    expect(isToolFacing(decl)).toBe(false);
    expect(hasCompactScope(decl)).toBe(true);
    expect((decl.extraScopes ?? []).includes("sidebar")).toBe(true);
  });

  it("explicit bridge dispatcher is a known wire value, not tool-facing", () => {
    const decl = parseCommandDeclaration({ name: "cmd", dispatcher: "bridge" });
    expect(decl.dispatcher).toBe("bridge");
    expect(isToolFacing(decl)).toBe(false);
  });

  it("wire camelCase round-trip fidelity", () => {
    const wire = {
      name: "new_activity",
      dispatcher: "tool",
      evaluationPolicy: {
        requireToolCall: { require: "report_to_user", onFailure: "bounce" },
      },
      extraScopes: ["compact"],
    };

    const decl: CommandDeclaration = JSON.parse(JSON.stringify(wire));
    expect(isToolFacing(decl)).toBe(true);
    expect(requireToolCall(decl)).toEqual({
      require: "report_to_user",
      onFailure: "bounce",
    });
    expect(hasCompactScope(decl)).toBe(true);

    const back = JSON.parse(JSON.stringify(decl));
    expect(back).toEqual(wire);
  });
});
