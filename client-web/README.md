# @grotto-web/client-web

gRPC-over-HTTP streaming client transport for Node and browsers. Pair with [ts-proto](https://github.com/stephenh/ts-proto) generated clients (`outputServices=default`, `useAsyncIterable=true`).

## Install

```bash
npm install @grotto-web/client-web
```

## Usage

```ts
import { GrottoRpc } from '@grotto-web/client-web';
import { PingServiceClientImpl, PingRequest } from './gen/ping/ping.js';

const client = new PingServiceClientImpl(new GrottoRpc('http://localhost:50051'));

const response = await client.Ping(PingRequest.create({ message: 'hello' }));
```

Each RPC is one `POST`. The gRPC method path is the URL path (e.g. `http://host/ping.PingService/Ping`). Request and response bodies are streams of GRTC binary frames (`application/vnd.grotto+grtc`).

Unary, client-streaming, and server-streaming RPCs use a single HTTP POST. Bidirectional streaming uses two POSTs: a server leg (response includes `Grotto-Session-Id`) and a client leg (request includes that id), joined within 10 seconds.

## API

- `GrottoRpc` — ts-proto `Rpc` implementation
- `GrottoConnection` — low-level frame I/O over HTTP POST
- `methodRpcUrl`, `FrameStreamReader`, framing helpers

## License

MIT
