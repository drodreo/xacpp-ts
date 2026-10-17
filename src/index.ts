export type { XacppCommand } from "./commands";
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
} from "./commands";
export { genericCommand, commandName } from "./commands";
export type { ActivityRef } from "./activity-ref";
export * from "./events";
export { XacppError, acknowledge, genericResponse, errorResponse } from "./message";
export type { ActivityInfo, XacppRequest, XacppResponse, XacppEnvelope } from "./message";
export type { Capabilities, EffectiveCapabilities } from "./capability";
export type {
  CommandDeclaration,
  CommandDispatcher,
  EvaluationPolicy,
  RequireToolCall,
} from "./capability";
export {
  parseCommandDeclaration,
  isToolFacing,
  requireToolCall,
  hasCompactScope,
} from "./capability";
export { XacppPeer, PeerState } from "./peer";
export { XacppSession } from "./session";
export { SocketTransport } from "./socket-transport";
export { StdioTransport } from "./stdio-transport";
export type { XacppTransport, RequestHandler } from "./transport";
export type { XacppSessionHandler, EstablishHandler, EstablishDecision, NegotiateHandler } from "./handler";

// Binary frame codec (protocol v1.0, docs/SPEC.md §3/§4)
export type { Frame } from "./frame";
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
} from "./frame";

// Control frame payload codecs (docs/SPEC.md §5)
export type {
  AckEntry,
  NackEntry,
  ResumeRequestEntry,
  ResumeResponseEntry,
  Heartbeat,
  ProtocolErrorInfo,
} from "./control";
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
} from "./control";

// Transaction reassembler — receiver-side reliability (docs/SPEC.md §3/§5)
export { TransactionReassembler, ReassemblyError, DEFAULT_STALE_RETENTION_MS, NACK_MISSING_SEQS_MAX } from "./reassemble";
export type { ReassemblerOptions, PushOutcome } from "./reassemble";

// Transaction sender — sender-side reliability (docs/SPEC.md §5)
export {
  TransactionSender,
  SenderError,
  DEFAULT_WORKING_FRAME_BYTES,
  DEFAULT_COPY_RETENTION_MS,
  DEFAULT_INITIAL_TIMEOUT_MS,
  DEFAULT_MAX_TIMEOUT_MS,
  DEFAULT_BACKPRESSURE_CAP_BYTES,
} from "./transaction";
export type {
  SenderOptions,
  OpenResult,
  AckOutcome,
  ResumeResponseOutcome,
  ResumeAnswer,
} from "./transaction";

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
} from "./transfer";
