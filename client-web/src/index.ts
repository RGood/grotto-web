export {
  GROTTO_MAGIC,
  FrameType,
  encodeFrame,
  decodeFrame,
  encodeJsonPayload,
  decodeJsonPayload,
} from './framing/codec.js';

export { wrapGrpcMessage, unwrapGrpcMessage } from './framing/grpc-message.js';

export {
  GrottoConnection,
  SINGLE_CALL_ID,
  type HeadersPayload,
  type TrailersPayload,
  type FrameHandlers,
} from './transport/connection.js';

export { connectWebSocket, type WebSocketLike } from './transport/websocket.js';

export { methodWebSocketUrl } from './transport/method-url.js';

export { GrottoRpc, GrottoRpcError, type GrottoRpcOptions, type Rpc } from './transport/grotto-rpc.js';
