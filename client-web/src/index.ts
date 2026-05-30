export {
  GROTTO_MAGIC,
  FrameType,
  HEADER_SIZE,
  encodeFrame,
  decodeFrame,
  encodeJsonPayload,
  decodeJsonPayload,
} from './framing/codec.js';

export { FrameStreamReader, type DecodedFrame } from './framing/stream.js';

export { wrapGrpcMessage, unwrapGrpcMessage } from './framing/grpc-message.js';

export {
  GrottoConnection,
  SINGLE_CALL_ID,
  type HeadersPayload,
  type TrailersPayload,
  type FrameHandlers,
} from './transport/connection.js';

export {
  GROTTO_CONTENT_TYPE,
  GROTTO_SESSION_ID_HEADER,
  methodRpcUrl,
} from './transport/method-url.js';

export { GrottoRpc, GrottoRpcError, type GrottoRpcOptions, type Rpc } from './transport/grotto-rpc.js';
