# @grotto-web/client-web

gRPC-over-WebSocket client transport for Node and browsers. Pair with [ts-proto](https://github.com/stephenh/ts-proto) generated clients (`outputServices=default`, `useAsyncIterable=true`).

## Install

```bash
npm install @grotto-web/client-web
# Node.js also needs the optional peer:
npm install ws
```

## Usage

```ts
import { GrottoRpc } from '@grotto-web/client-web';
import { PingServiceClientImpl, PingRequest } from './gen/ping/ping.js';

const client = new PingServiceClientImpl(new GrottoRpc('ws://localhost:50051'));

const response = await client.Ping(PingRequest.create({ message: 'hello' }));
```

Each RPC opens its own WebSocket. The gRPC method path is the URL path (e.g. `ws://host/ping.PingService/Ping`).

## API

- `GrottoRpc` — ts-proto `Rpc` implementation (unary, client/server/bidi streams)
- `GrottoConnection` — low-level frame I/O over one WebSocket
- Framing helpers — `encodeFrame`, `decodeFrame`, `wrapGrpcMessage`, etc.

## License

MIT
