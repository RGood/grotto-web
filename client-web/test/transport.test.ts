import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import { describe, it } from 'node:test';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { IncomingMessage } from 'node:http';
import { WebSocketServer } from 'ws';

import {
  PingRequest,
  PingServiceClientImpl,
} from '../../examples/basic/client/gen/ping/ping.js';
import {
  decodeFrame,
  encodeFrame,
  encodeJsonPayload,
  FrameType,
  GrottoRpc,
  GrottoRpcError,
  methodWebSocketUrl,
  SINGLE_CALL_ID,
  wrapGrpcMessage,
} from '../src/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PING_METHOD = '/ping.PingService/Ping';

function startMockServer(): Promise<{
  url: string;
  connectionCount: () => number;
  close: () => Promise<void>;
}> {
  const wss = new WebSocketServer({ port: 0 });
  let connections = 0;

  return new Promise((resolve) => {
    wss.on('listening', () => {
      const addr = wss.address();
      const port = typeof addr === 'object' && addr !== null ? addr.port : 0;
      const url = `ws://127.0.0.1:${port}`;

      wss.on('connection', (ws, req: IncomingMessage) => {
        connections += 1;
        if (req.url !== PING_METHOD) {
          ws.close();
          return;
        }

        ws.on('message', (raw) => {
          const data = new Uint8Array(raw as Buffer);
          const { header } = decodeFrame(data);
          if (header.type === FrameType.MESSAGE) {
            sendUnaryPing(ws);
          }
        });
      });

      resolve({
        url,
        connectionCount: () => connections,
        close: () =>
          new Promise((res, rej) => {
            for (const client of wss.clients) {
              client.close();
            }
            wss.close((err) => (err ? rej(err) : res()));
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
        url: `ws://127.0.0.1:${port}`,
        close: () =>
          new Promise((res, rej) => {
            server.close((err) => (err ? rej(err) : res()));
          }),
      });
    });
  });
}

function sendUnaryPing(ws: { send: (data: Uint8Array) => void }): void {
  const pong = Buffer.from([0x0a, 0x0b, ...Buffer.from('pong: hello', 'utf8')]);
  const response = wrapGrpcMessage(pong);
  const callId = SINGLE_CALL_ID;

  ws.send(encodeFrame(callId, FrameType.HEADERS, encodeJsonPayload({ metadata: {} })));
  ws.send(encodeFrame(callId, FrameType.MESSAGE, response));
  ws.send(
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
}

describe('Grotto WebSocket transport', { concurrency: 1 }, () => {
  it('frames round-trip', () => {
    const payload = new TextEncoder().encode('test');
    const frame = encodeFrame(SINGLE_CALL_ID, FrameType.MESSAGE, payload);
    const decoded = decodeFrame(frame);
    assert.equal(decoded.header.callId, SINGLE_CALL_ID);
    assert.equal(decoded.header.type, FrameType.MESSAGE);
    assert.deepEqual(decoded.payload, payload);
  });

  it('puts the method on the WebSocket URL', () => {
    assert.equal(
      methodWebSocketUrl('ws://localhost:50051', PING_METHOD),
      'ws://localhost:50051/ping.PingService/Ping'
    );
  });

  it('drives a ts-proto client via GrottoRpc', async () => {
    const mock = await startMockServer();
    const client = new PingServiceClientImpl(new GrottoRpc(mock.url));
    try {
      const response = await client.Ping(PingRequest.create({ message: 'hello' }));
      assert.equal(response.message, 'pong: hello');
      assert.equal(mock.connectionCount(), 1);
    } finally {
      await mock.close();
    }
  });

  it('fails unary RPC when the server does not upgrade the WebSocket', async () => {
    const mock = await startHttpRejectServer(501);
    const client = new PingServiceClientImpl(new GrottoRpc(mock.url));
    try {
      await assert.rejects(
        () => client.Ping(PingRequest.create({ message: 'hello' })),
        (err: Error) => {
          assert.match(err.message, /upgrade failed: HTTP 501/);
          return true;
        }
      );
    } finally {
      await mock.close();
    }
  });

  it('opens one WebSocket per RPC', async () => {
    const mock = await startMockServer();
    const client = new PingServiceClientImpl(new GrottoRpc(mock.url));
    try {
      await Promise.all([
        client.Ping(PingRequest.create({ message: 'a' })),
        client.Ping(PingRequest.create({ message: 'b' })),
      ]);
      assert.equal(mock.connectionCount(), 2);
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
