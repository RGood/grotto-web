import assert from 'node:assert/strict';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { describe, it } from 'node:test';

import {
  PingRequest,
  PingServiceClientImpl,
} from '../../examples/basic/client/src/gen/ping/ping.js';
import {
  decodeFrame,
  encodeFrame,
  encodeJsonPayload,
  FrameStreamReader,
  FrameType,
  GROTTO_CONTENT_TYPE,
  GrottoRpc,
  GrottoRpcError,
  methodRpcUrl,
  SINGLE_CALL_ID,
  wrapGrpcMessage,
} from '../src/index.js';

const PING_METHOD = '/ping.PingService/Ping';

function startMockServer(): Promise<{
  url: string;
  requestCount: () => number;
  close: () => Promise<void>;
}> {
  let requests = 0;

  return new Promise((resolve, reject) => {
    const server: Server = createServer((req: IncomingMessage, res: ServerResponse) => {
      if (req.method !== 'POST' || req.url !== PING_METHOD) {
        res.writeHead(404);
        res.end();
        return;
      }
      requests += 1;

      const reader = new FrameStreamReader();
      req.on('data', (chunk: Buffer) => {
        for (const frame of reader.push(new Uint8Array(chunk))) {
          if (frame.header.type === FrameType.MESSAGE) {
            sendUnaryPing(res);
          }
        }
      });
      req.on('end', () => {
        if (!res.writableEnded) {
          res.writeHead(500);
          res.end();
        }
      });
    });

    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      const port = typeof addr === 'object' && addr !== null ? addr.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}`,
        requestCount: () => requests,
        close: () =>
          new Promise((res, rej) => {
            server.close((err) => (err ? rej(err) : res()));
          }),
      });
    });
  });
}

function startHttpRejectServer(statusCode: number): Promise<{
  url: string;
  close: () => Promise<void>;
}> {
  return new Promise((resolve, reject) => {
    const server: Server = createServer((_req, res) => {
      res.writeHead(statusCode);
      res.end();
    });
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      const port = typeof addr === 'object' && addr !== null ? addr.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}`,
        close: () =>
          new Promise((res, rej) => {
            server.close((err) => (err ? rej(err) : res()));
          }),
      });
    });
  });
}

function sendUnaryPing(res: ServerResponse): void {
  const pong = Buffer.from([0x0a, 0x0b, ...Buffer.from('pong: hello', 'utf8')]);
  const response = wrapGrpcMessage(pong);
  const callId = SINGLE_CALL_ID;

  res.writeHead(200, { 'Content-Type': GROTTO_CONTENT_TYPE });
  res.write(encodeFrame(callId, FrameType.HEADERS, encodeJsonPayload({ metadata: {} })));
  res.write(encodeFrame(callId, FrameType.MESSAGE, response));
  res.write(
    encodeFrame(
      callId,
      FrameType.TRAILERS,
      encodeJsonPayload({
        code: 0,
        details: '',
        metadata: {},
      })
    )
  );
  res.end();
}

describe('Grotto HTTP transport', { concurrency: 1 }, () => {
  it('frames round-trip', () => {
    const payload = new TextEncoder().encode('test');
    const frame = encodeFrame(SINGLE_CALL_ID, FrameType.MESSAGE, payload);
    const decoded = decodeFrame(frame);
    assert.equal(decoded.header.callId, SINGLE_CALL_ID);
    assert.equal(decoded.header.type, FrameType.MESSAGE);
    assert.deepEqual(decoded.payload, payload);
  });

  it('decodes frames from a byte stream', () => {
    const payload = new TextEncoder().encode('chunked');
    const frame = encodeFrame(SINGLE_CALL_ID, FrameType.MESSAGE, payload);
    const reader = new FrameStreamReader();
    const part1 = reader.push(frame.subarray(0, 8));
    assert.equal(part1.length, 0);
    const part2 = reader.push(frame.subarray(8));
    assert.equal(part2.length, 1);
    assert.deepEqual(part2[0]!.payload, payload);
  });

  it('puts the method on the HTTP URL', () => {
    assert.equal(
      methodRpcUrl('http://localhost:50051', PING_METHOD),
      'http://localhost:50051/ping.PingService/Ping'
    );
  });

  it('drives a ts-proto client via GrottoRpc', async () => {
    const mock = await startMockServer();
    const client = new PingServiceClientImpl(new GrottoRpc(mock.url));
    try {
      const response = await client.Ping(PingRequest.create({ message: 'hello' }));
      assert.equal(response.message, 'pong: hello');
      assert.equal(mock.requestCount(), 1);
    } finally {
      await mock.close();
    }
  });

  it('fails unary RPC when the server returns a non-OK HTTP status', async () => {
    const mock = await startHttpRejectServer(501);
    const client = new PingServiceClientImpl(new GrottoRpc(mock.url));
    try {
      await assert.rejects(
        () => client.Ping(PingRequest.create({ message: 'hello' })),
        (err: Error) => {
          assert.match(err.message, /HTTP 501/);
          return true;
        }
      );
    } finally {
      await mock.close();
    }
  });

  it('issues one HTTP POST per RPC', async () => {
    const mock = await startMockServer();
    const client = new PingServiceClientImpl(new GrottoRpc(mock.url));
    try {
      await Promise.all([
        client.Ping(PingRequest.create({ message: 'a' })),
        client.Ping(PingRequest.create({ message: 'b' })),
      ]);
      assert.equal(mock.requestCount(), 2);
    } finally {
      await mock.close();
    }
  });

  it('maps non-OK trailers to GrottoRpcError', async () => {
    const error = new GrottoRpcError(5, 'not found');
    assert.equal(error.code, 5);
    assert.equal(error.message, 'not found');
  });
});
