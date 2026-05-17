/**
 * Example: ts-proto PingService client over Grotto WebSocket transport.
 *
 * Targets the example server via Grotto WebSocket (ws://server:50051 in compose).
 * Requires a Grotto server (e.g. examples/basic/server) speaking the WebSocket RPC protocol.
 *
 *   yarn start ws://localhost:50051
 */
import { GrottoRpc } from '@grotto/client-web';

import { PingRequest, PingServiceClientImpl } from './gen/ping/ping.js';

const target = process.argv[2] ?? process.env.GROTTO_WS_URL ?? 'ws://localhost:50051';

async function main(): Promise<void> {
  const client = new PingServiceClientImpl(new GrottoRpc(target));

  const ping = await client.Ping(PingRequest.create({ message: 'hello' }));
  console.log('Ping:', ping);

  const serverMessages = [];
  for await (const response of client.PingServerStream(
    PingRequest.create({ message: 'stream', count: 3 })
  )) {
    serverMessages.push(response);
  }
  console.log('PingServerStream:', serverMessages);

  const clientReply = await client.PingClientStream(
    (async function* () {
      for (const message of ['one', 'two', 'three']) {
        yield PingRequest.create({ message });
      }
    })()
  );
  console.log('PingClientStream:', clientReply);

  const bidiMessages = [];
  for await (const response of client.PingBidiStream(
    (async function* () {
      yield PingRequest.create({ message: 'alpha', count: 1 });
      yield PingRequest.create({ message: 'beta', count: 2 });
    })()
  )) {
    bidiMessages.push(response);
  }
  console.log('PingBidiStream:', bidiMessages);
}

try {
  await main();
} catch (err) {
  console.error(err);
  process.exit(1);
}
