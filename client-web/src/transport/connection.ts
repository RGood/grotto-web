import {
  decodeJsonPayload,
  encodeFrame,
  encodeJsonPayload,
  FrameType,
} from '../framing/codec.js';
import { FrameStreamReader } from '../framing/stream.js';
import { GROTTO_CONTENT_TYPE } from './method-url.js';

/** One logical RPC per HTTP request; call id is always 1 on the wire. */
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
  onError?: (err: Error) => void;
  onClose?: () => void;
};

export type GrottoConnectionOptions = {
  /** Extra HTTP request headers (e.g. bidi session id). */
  requestHeaders?: Record<string, string>;
};

type RequestController = ReadableStreamDefaultController<Uint8Array>;

/**
 * One HTTP POST with streaming request and response bodies carrying GRTC frames.
 */
export class GrottoConnection {
  private requestController: RequestController | null = null;
  private requestClosed = false;
  private connectPromise: Promise<void> | null = null;
  private handlers: FrameHandlers | null = null;
  private closed = false;
  private abort: AbortController | null = null;
  private responseHeaders: Headers | null = null;
  private responseHeadersPromise: Promise<void> | null = null;

  constructor(
    private readonly url: string,
    private readonly options: GrottoConnectionOptions = {}
  ) {}

  get peer(): string {
    return this.url;
  }

  isReady(): boolean {
    return this.requestController !== null && !this.closed;
  }

  /** HTTP response headers; available after {@link awaitResponseHeaders} resolves. */
  getResponseHeader(name: string): string | null {
    return this.responseHeaders?.get(name) ?? null;
  }

  /** Waits until HTTP response status and headers are available (body may still stream). */
  async awaitResponseHeaders(): Promise<void> {
    await this.connect();
    if (this.responseHeaders) {
      return;
    }
    if (!this.responseHeadersPromise) {
      throw new Error('grotto: response headers not available');
    }
    await this.responseHeadersPromise;
  }

  setHandlers(handlers: FrameHandlers): void {
    this.handlers = handlers;
  }

  async connect(): Promise<void> {
    if (this.closed) {
      throw new Error('grotto: connection is closed');
    }
    if (this.requestController) {
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
    this.abort = new AbortController();
    let readyResolve!: () => void;
    let readyReject!: (err: Error) => void;
    const ready = new Promise<void>((resolve, reject) => {
      readyResolve = resolve;
      readyReject = reject;
    });

    let headersResolve!: () => void;
    let headersReject!: (err: Error) => void;
    this.responseHeadersPromise = new Promise<void>((resolve, reject) => {
      headersResolve = resolve;
      headersReject = reject;
    });
    void this.responseHeadersPromise.catch(() => undefined);

    const requestStream = new ReadableStream<Uint8Array>({
      start: (controller) => {
        this.requestController = controller;
        readyResolve();
      },
    });

    const requestHeaders: Record<string, string> = {
      'Content-Type': GROTTO_CONTENT_TYPE,
      // Avoid reusing a keep-alive socket while another Grotto response is streaming (bidi).
      Connection: 'close',
      ...this.options.requestHeaders,
    };

    const responsePromise = fetch(this.url, {
      method: 'POST',
      headers: requestHeaders,
      body: requestStream,
      signal: this.abort.signal,
      duplex: 'half',
      keepalive: false,
    } as RequestInit);

    void responsePromise
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(
            `grotto: HTTP ${response.status} ${response.statusText}`
          );
        }
        if (!response.body) {
          throw new Error('grotto: response has no body');
        }
        this.responseHeaders = response.headers;
        headersResolve();

        const reader = response.body.getReader();
        const frameReader = new FrameStreamReader();
        await this.readResponseLoop(reader, frameReader);
      })
      .catch((err) => {
        if (this.closed) {
          return;
        }
        const error = err instanceof Error ? err : new Error(String(err));
        if (!this.requestController) {
          readyReject(error);
          headersReject(error);
          return;
        }
        if (!this.responseHeaders) {
          headersReject(error);
        }
        this.handlers?.onError?.(error);
      });

    await ready;
  }

  private async readResponseLoop(
    reader: ReadableStreamDefaultReader<Uint8Array>,
    frameReader: FrameStreamReader
  ): Promise<void> {
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        if (value) {
          for (const frame of frameReader.push(value)) {
            this.dispatch(frame.header.type, frame.payload);
          }
        }
      }
    } catch (err) {
      if (!this.closed) {
        const error = err instanceof Error ? err : new Error(String(err));
        this.handlers?.onError?.(error);
      }
    } finally {
      reader.releaseLock();
      if (!this.closed) {
        this.handlers?.onClose?.();
      }
    }
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

  private enqueueFrame(type: FrameType, payload: Uint8Array = new Uint8Array()): void {
    if (!this.requestController || this.requestClosed) {
      throw new Error('grotto: request stream is not open');
    }
    this.requestController.enqueue(encodeFrame(SINGLE_CALL_ID, type, payload));
  }

  private closeRequestStream(): void {
    if (this.requestClosed || !this.requestController) {
      return;
    }
    this.requestClosed = true;
    try {
      this.requestController.close();
    } catch {
      // already closed
    }
  }

  async sendHeaders(headers: HeadersPayload): Promise<void> {
    await this.connect();
    this.enqueueFrame(FrameType.HEADERS, encodeJsonPayload(headers));
  }

  async sendMessage(message: Uint8Array): Promise<void> {
    await this.connect();
    this.enqueueFrame(FrameType.MESSAGE, message);
  }

  async sendHalfClose(): Promise<void> {
    await this.connect();
    this.enqueueFrame(FrameType.HALF_CLOSE);
    this.closeRequestStream();
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.handlers = null;
    this.abort?.abort();
    this.closeRequestStream();
    this.requestController = null;
  }
}

export { decodeJsonPayload, FrameType };
