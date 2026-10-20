export type { XacppCommand } from "./wire/commands";
export type {
  NewActivityPayload,
  LastActivityPayload,
  SwitchActivityPayload,
  ListActivityPayload,
  InvokeActivityPayload,
  CancelActivityPayload,
  CompactActivityPayload,
  SetActivityModePayload,
  AvailableActivitiesResponse,
} from "./wire/commands";
export { genericCommand, commandName } from "./wire/commands";
export type { ActivityRef } from "./wire/activity-ref";
export * from "./wire/events";
export { XacppError, acknowledge, genericResponse, errorResponse } from "./wire/message";
export type { ActivityInfo, XacppRequest, XacppResponse, XacppEnvelope } from "./wire/message";
export type { Capabilities, EffectiveCapabilities } from "./wire/capability";
export type {
  CommandDeclaration,
  CommandDispatcher,
  EvaluationPolicy,
  RequireToolCall,
} from "./wire/capability";
export {
  parseCommandDeclaration,
  isToolFacing,
  requireToolCall,
  hasCompactScope,
} from "./wire/capability";
export { XacppPeer, PeerState } from "./session/peer";
export { XacppSession } from "./session/session";

// Fetch kernel — stateless resource pull (fetch command + fetch_ready; SPEC §8)
export { FETCH_NOT_FOUND, FETCH_REFUSED, FETCH_UNAVAILABLE, fetchNotFound, fetchRefused } from "./wire/fetch";
export type { FetchReady } from "./wire/fetch";
export { SocketTransport } from "./carrier/socket";
export { StdioTransport } from "./carrier/stdio";
export type { XacppTransport, RequestHandler } from "./carrier/transport";
export type { XacppSessionHandler, EstablishHandler, EstablishDecision, NegotiateHandler } from "./session/handler";

// Binary frame codec (protocol v1.0, docs/SPEC.md §3/§4)
export type { Frame } from "./wire/frame";
export {
  FrameType,
  FrameDecoder,
  FrameError,
  FrameErrorCode,
  encodeFrame,
  makeFrame,
  newTransactionId,
  PROTOCOL_VERSION_MAJOR,
  PROTOCOL_VERSION_MINOR,
  FRAME_FIXED_OVERHEAD,
  TRANSACTION_ID_SIZE,
  CONTROL_FRAME_MAX,
  DATA_JSON_FRAME_MAX,
  DATA_BINARY_FRAME_MAX,
  DEFAULT_DECODE_BUFFER_CAP,
} from "./wire/frame";

// Control frame payload codecs (docs/SPEC.md §5)
export type {
  AckEntry,
  NackEntry,
  ResumeRequestEntry,
  ResumeResponseEntry,
  Heartbeat,
  ProtocolErrorInfo,
} from "./wire/control";
export {
  ControlError,
  ControlErrorCode,
  RESUME_STATUS,
  ErrorCode,
  CONTROL_PAYLOAD_FRAME_TYPES,
  encodeAck,
  decodeAck,
  encodeNack,
  decodeNack,
  encodeResumeRequest,
  decodeResumeRequest,
  encodeResumeResponse,
  decodeResumeResponse,
  encodeHeartbeat,
  decodeHeartbeat,
  encodeError,
  decodeError,
  encodeTransactionPause,
  decodeTransactionPause,
  encodeTransactionAbort,
  decodeTransactionAbort,
  encodeTransactionCompleted,
  decodeTransactionCompleted,
} from "./wire/control";

// Transaction reassembler — receiver-side reliability (docs/SPEC.md §3/§5)
export { TransactionReassembler, ReassemblyError, NACK_MISSING_SEQS_MAX } from "./reliability/reassemble";
export type { PushOutcome } from "./reliability/reassemble";

// Transaction sender — sender-side reliability (docs/SPEC.md §5)
export {
  TransactionSender,
  SenderError,
  DEFAULT_WORKING_FRAME_BYTES,
  DEFAULT_INITIAL_TIMEOUT_MS,
  DEFAULT_MAX_TIMEOUT_MS,
  DEFAULT_BACKPRESSURE_CAP_BYTES,
} from "./reliability/transaction";
export type {
  SenderOptions,
  OpenResult,
  AckOutcome,
  ResumeVerdictOutcome,
} from "./reliability/transaction";

// Transfer session — session-carried binary reliability (docs/SPEC.md §5)
export {
  TransferSession,
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_SILENCE_TIMEOUT_MS,
  DEFAULT_ACK_BATCH_FRAMES,
  DEFAULT_ACK_BATCH_INTERVAL_MS,
  DEFAULT_RELIABILITY_TICK_MS,
} from "./transfer";
export type {
  TransferConfig,
  TransferWiring,
  TransferActivityHandlers,
  TickOutcome,
  BinarySendResult,
  BinaryHandler,
  BinarySink,
  BinarySinkMeta,
  BinaryStream,
  BinaryStreamHandler,
  StreamEndReason,
} from "./transfer";

// Fetch extension — assembly over the kernel (SPEC §8 contract firewall).
// Not part of the wire protocol:
// the data plane stays on the kernel's bare BinaryStream/BinarySink objects.
export {
  FetchPuller,
  FetchService,
  FetchError,
  bytesProvider,
  fileProvider,
  fetchVerify,
  fetchPause,
  fetchResume,
  fetchAbort,
} from "./ext/fetch";
export type { FetchErrorKind, FetchProvider, ResolvedResource } from "./ext/fetch";
