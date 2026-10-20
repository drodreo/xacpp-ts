/**
 * XACPP event types.
 *
 * `XacppEvent` is a generic `{ name, data }` structure.
 * Type definitions for common event payloads are kept in submodules
 * as serialization targets.
 */

export * from "./content";
export * from "./interaction";
export * from "./payload";
export * from "./upload";
export type { XacppEvent } from "./xacpp_event";
export type { XacppActivityEvent } from "./activity-event";
export { newEvent } from "./xacpp_event";
export { newActivityEvent } from "./activity-event";
