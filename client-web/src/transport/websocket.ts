import type WebSocketType from 'ws';

export type WebSocketLike = {
  readonly readyState: number;
  binaryType?: string;
  send(data: Uint8Array | ArrayBuffer): void;
  close(code?: number, reason?: string): void;
  addEventListener(
    type: 'open' | 'message' | 'close' | 'error',
    listener: (event: WebSocketEventMap[keyof WebSocketEventMap]) => void
  ): void;
  removeEventListener(
    type: 'open' | 'message' | 'close' | 'error',
    listener: (event: WebSocketEventMap[keyof WebSocketEventMap]) => void
  ): void;
};

export const WS_OPEN = 1;

export async function connectWebSocket(url: string): Promise<WebSocketLike> {
  // Prefer the ws package on Node so failed HTTP upgrades surface as rejections.
  if (typeof process !== 'undefined' && process.versions?.node) {
    return connectNode(url);
  }
  if (typeof globalThis.WebSocket !== 'undefined') {
    return connectBrowser(url);
  }
  return connectNode(url);
}

function connectBrowser(url: string): Promise<WebSocketLike> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    ws.binaryType = 'arraybuffer';
    const onOpen = () => {
      cleanup();
      resolve(ws as unknown as WebSocketLike);
    };
    const onError = (e: any) => {
      cleanup();
      reject(new Error(`grotto: failed to connect to ${url}: ${e.message}`));
    };
    const cleanup = () => {
      ws.removeEventListener('open', onOpen);
      ws.removeEventListener('error', onError);
    };
    ws.addEventListener('open', onOpen);
    ws.addEventListener('error', onError);
  });
}

async function connectNode(url: string): Promise<WebSocketLike> {
  const { default: WS } = await import('ws');
  const ws = new WS(url) as WebSocketType;
  await new Promise<void>((resolve, reject) => {
    const fail = (err: Error) => {
      cleanup();
      reject(err);
    };
    const cleanup = () => {
      ws.off('open', onOpen);
      ws.off('error', onError);
      ws.off('unexpected-response', onUnexpectedResponse);
    };
    const onOpen = () => {
      cleanup();
      resolve();
    };
    const onError = (err: Error) => {
      fail(err);
    };
    const onUnexpectedResponse = (
      _request: unknown,
      response: { statusCode: number; statusMessage: string }
    ) => {
      fail(
        new Error(
          `grotto: WebSocket upgrade failed: HTTP ${response.statusCode} ${response.statusMessage}`
        )
      );
    };
    ws.once('open', onOpen);
    ws.once('error', onError);
    ws.once('unexpected-response', onUnexpectedResponse);
  });
  return ws as unknown as WebSocketLike;
}

export function readMessageData(event: MessageEvent): Uint8Array {
  if (event.data instanceof ArrayBuffer) {
    return new Uint8Array(event.data);
  }
  if (ArrayBuffer.isView(event.data)) {
    return new Uint8Array(event.data.buffer, event.data.byteOffset, event.data.byteLength);
  }
  if (typeof event.data === 'string') {
    return new TextEncoder().encode(event.data);
  }
  throw new Error('grotto: unsupported WebSocket message type');
}
