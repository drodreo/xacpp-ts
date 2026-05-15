/**
 * XACPP protocol command types.
 *
 * Commands are transmitted via transport, driving the interaction flow between the initiator and the peer.
 *
 * Establish replaces the legacy Paring/Authenticate, unifying handshake and session establishment.
 */

import type { ContentPart } from "../events/content";

/** XACPP protocol command. */
export type XacppCommand =
  /** Establish logical session. */
  | { establish: { credentials: string | null } }
  /** Create a new Activity session. */
  | { new_activity: { title: string | null } }
  /** Invoke an existing Activity to perform an operation. */
  | { invoke_activity: { activity: string; messages: ContentPart[] } }
  /** Compact Activity (reclaim resources / generate snapshot summary). */
  | { compact_activity: { activity: string } }
  /** Cancel Activity. */
  | { cancel_activity: { activity: string } };
