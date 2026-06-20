/**
 * XACPP protocol command types.
 *
 * Commands are request-response: the sender always expects a Response.
 *
 * Protocol commands (Negotiate/Establish/EstablishConfirm) are handled by the Peer layer.
 * Business commands use the Generic variant.
 */

import type { Capabilities } from "../capability";

/** XACPP protocol command. */
export type XacppCommand =
  /** Negotiate capabilities before session establishment. */
  | { negotiate: { capabilities: Capabilities } }
  /** Establish logical session. */
  | { establish: { credentials?: string } }
  /** Confirm establishment after challenge verification. */
  | "establish_confirm"
  /** Generic business command. */
  | { generic: { name: string; arguments: unknown } };

/** Convenience constructor for generic business commands. */
export function genericCommand(name: string, args: unknown): XacppCommand {
  return { generic: { name, arguments: args } };
}

/** Extracts the command name for capability-matching purposes. */
export function commandName(cmd: XacppCommand): string {
  if (typeof cmd === "string") return cmd;
  if ("negotiate" in cmd) return "negotiate";
  if ("establish" in cmd) return "establish";
  if ("generic" in cmd) return cmd.generic.name;
  return "unknown";
}
