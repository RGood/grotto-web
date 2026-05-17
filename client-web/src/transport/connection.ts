import {
  decodeFrame,
  decodeJsonPayload,
  encodeFrame,
  encodeJsonPayload,
  FrameType,
} from '../framing/codec.js';
import { connectWebSocket, readMessageData, WS_OPEN, type WebSocketLike } from './websocket.js';

/** One logical RPC per WebSocket; call id is always 1 on the wire. */
export const SINGLE_CALL_ID = 1;

export type HeadersPayload = {
  metadata: Record<string, unknown>;
};

export type TrailersPayload = {
  code: number;
  details: string;
  metadata: Record<string, unknown>;
};

export type FrameHandlers = {
  onHeaders?: (payload: Uint8Array) => void;
  onMessage?: (payload: Uint8Array) => void;
  onTrailers?: (payload: Uint8Array) => void;
  onCancel?: () => void;
  onClose?: () => void;
};

/**
 * A single WebSocket carrying one gRPC call or stream.
 * Open a new instance per RPC; do not share across methods.
 */
export class GrottoConnection {
  private ws: WebSocketLike | null = null;
  private connectPromise: Promise<void> | null = null;
  private handlers: FrameHandlers | null = null;
  private closed = false;

  constructor(private readonly url: string) {}

  get peer(): string {
    return this.url;
  }

  isReady(): boolean {
    return this.ws?.readyState === WS_OPEN;
  }

  setHandlers(handlers: FrameHandlers): void {
    this.handlers = handlers;
  }

  async connect(): Promise<void> {
    if (this.closed) {
      throw new Error('grotto: connection is closed');
    }
    if (this.ws?.readyState === WS_OPEN) {
      return;
    }
    if (this.connectPromise) {
      return this.connectPromise;
    }
    this.connectPromise = this.connectInternal();
    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = null;
    }
  }

  private async connectInternal(): Promise<void> {
    const ws = await connectWebSocket(this.url);
    ws.binaryType = 'arraybuffer';
    this.ws = ws;

    const onMessage = (event: MessageEvent) => {
      try {
        const data = readMessageData(event);
        const { header, payload } = decodeFrame(data);
        if (header.callId !== SINGLE_CALL_ID) {
          return;
        }
        this.dispatch(header.type, payload);
      } catch {
        this.handlers?.onClose?.();
      }
    };

    const onClose = () => {
      this.ws = null;
      this.handlers?.onClose?.();
    };

    ws.addEventListener('message', onMessage as EventListener);
    ws.addEventListener('close', onClose as EventListener);
  }

  private dispatch(type: FrameType, payload: Uint8Array): void {
    const handlers = this.handlers;
    if (!handlers) {
      return;
    }
    switch (type) {
      case FrameType.HEADERS:
        handlers.onHeaders?.(payload);
        break;
      case FrameType.MESSAGE:
        handlers.onMessage?.(payload);
        break;
      case FrameType.TRAILERS:
        handlers.onTrailers?.(payload);
        break;
      case FrameType.CANCEL:
        handlers.onCancel?.();
        break;
      default:
        break;
    }
  }

  private async send(type: FrameType, payload: Uint8Array = new Uint8Array()): Promise<void> {
    await this.connect();
    if (!this.ws || this.ws.readyState !== WS_OPEN) {
      throw new Error('grotto: WebSocket is not open');
    }
    this.ws.send(encodeFrame(SINGLE_CALL_ID, type, payload));
  }

  sendHeaders(headers: HeadersPayload): Promise<void> {
    return this.send(FrameType.HEADERS, encodeJsonPayload(headers));
  }

  sendMessage(message: Uint8Array): Promise<void> {
    return this.send(FrameType.MESSAGE, message);
  }

  sendHalfClose(): Promise<void> {
    return this.send(FrameType.HALF_CLOSE);
  }

  sendCancel(code: number, details: string): Promise<void> {
    return this.send(FrameType.CANCEL, encodeJsonPayload({ code, details }));
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.handlers = null;
    this.ws?.close();
    this.ws = null;
  }
}

export { decodeJsonPayload, FrameType };
