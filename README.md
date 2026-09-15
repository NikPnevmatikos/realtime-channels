# realtime-channels

[![npm](https://img.shields.io/npm/v/realtime-channels.svg)](https://www.npmjs.com/package/realtime-channels)
[![CI](https://github.com/NikPnevmatikos/realtime-channels/actions/workflows/ci.yml/badge.svg)](https://github.com/NikPnevmatikos/realtime-channels/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/realtime-channels.svg)](./LICENSE)
![zero dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)

Tiny, dependency-free realtime channel client with pluggable transports.
One API for subscribing to channels; adapters do the protocol work.

Ships with an **AWS AppSync Events** adapter that runs unchanged in browsers,
React Native (including Expo Go) and Node.

```ts
import { createClient } from 'realtime-channels';
import { appSyncEvents, cognitoUserPool } from 'realtime-channels/appsync-events';

const client = createClient(
  appSyncEvents({
    httpDomain: 'abc123.appsync-api.eu-west-1.amazonaws.com',
    auth: cognitoUserPool(() => getIdToken()),
  }),
);

client.subscribe(`users/${currentUserId}`, (event) => console.log(event));
```

## Why

- **Zero runtime dependencies.** A few KB minified. Nothing to audit but this repo.
- **One code path everywhere.** Browser, React Native and Node use the same client on the platform's own `WebSocket`. No native modules, so it works in Expo Go.
- **Bring your own auth.** Pass a function that returns the current token. It is called for every connection and every subscription, so refreshed credentials propagate on their own. No opinion about where you keep tokens.
- **Reconnects properly.** Jittered exponential backoff, every channel re-subscribed, keep-alive watchdog for silent connections, channels the server refused are not retried in a loop.
- **Transport-agnostic core.** The AppSync adapter is ~300 lines against a small `Adapter` interface. Other transports plug into the same client, hooks and tests. See [Adapters](#adapters).

If you use Amplify already and keep your tokens in Amplify Auth, the official `events` client is a fine choice. This library exists for everyone else.

## Install

```bash
npm install realtime-channels
# pnpm add realtime-channels · yarn add realtime-channels
```

## Usage

### Subscribe

```ts
const sub = client.subscribe('users/42', (event, meta) => {
  console.log(event, meta.channel, meta.receivedAt);
});

await sub.ready;      // optional: resolves on the server's ack, rejects if refused
sub.unsubscribe();
```

The first `subscribe()` opens the connection (`autoConnect: true`). Call `client.connect()` yourself to control timing, `client.close()` to stop; subscriptions survive a `close()` and resume on the next `connect()`.

### Status and errors

```ts
client.onStatus((status) => …);  // 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed'
client.onError((error) => …);    // RealtimeError with a stable `code`
```

Error codes you will branch on: `SUBSCRIBE_REJECTED` (authorization rule said no),
`KEEPALIVE_TIMEOUT`, `CONNECT_TIMEOUT`, `AUTH_ERROR` (your token provider threw),
`RECONNECT_GIVE_UP` (only with `maxReconnectAttempts`), `HANDLER_ERROR` (your handler threw; the subscription stays alive).

### React

```tsx
import { RealtimeProvider, useChannel, useConnectionStatus } from 'realtime-channels/react';

<RealtimeProvider client={client}>
  <App />
</RealtimeProvider>;

function Bell({ userId }: { userId: string }) {
  const status = useConnectionStatus();
  useChannel(`users/${userId}`, (event) => {
    queryClient.invalidateQueries({ queryKey: ['notifications'] });
  });
  return <span data-status={status} />;
}
```

`useChannel(null, …)` pauses the subscription while an id is loading. The latest handler is always used; changing it does not resubscribe.

### React Native / Expo

Nothing extra. See [`examples/expo`](./examples/expo) for a complete screen that reconnects on `AppState`.

### Node

Node 22+ has a global `WebSocket`. On older versions pass one in:

```ts
import WebSocket from 'ws';
appSyncEvents({ httpDomain, auth, WebSocket });
```

### Publishing

Supported when the transport and the API's authorization allow the client to publish:

```ts
await client.publish('default/chat', [{ text: 'hello' }]);
```

Most production APIs restrict publishing to the backend; keep clients subscribe-only and publish server-side.

## Adapters

| Transport | Import | Status |
| --- | --- | --- |
| AWS AppSync Events | `realtime-channels/appsync-events` | ✅ shipped |
| Plain WebSocket (JSON envelope, your own server) | `realtime-channels/websocket` | 🧭 planned, [help wanted](https://github.com/NikPnevmatikos/realtime-channels/issues) |
| Server-Sent Events (`EventSource`) | `realtime-channels/sse` | 🧭 planned |
| Azure Web PubSub (`json.webpubsub.azure.v1`) | `realtime-channels/azure-webpubsub` | 🧭 planned |
| AWS API Gateway WebSocket API | `realtime-channels/apigateway-websocket` | 💡 idea |
| Centrifugo, Ably, Pusher, Socket.IO, MQTT | wrappers around the official clients | 💡 idea |

Adapters are small: implement `connect()` returning a connection with `subscribe()`, optional `publish()` and `close()`. The core handles reconnection, resubscription, status, errors and React. **[Writing an adapter →](./docs/writing-an-adapter.md)**

### AppSync Events adapter

| Option | Default | Meaning |
| --- | --- | --- |
| `httpDomain` | required | Event API HTTP host or full URL |
| `realtimeDomain` | derived | override for custom domains |
| `auth` | required | see below |
| `WebSocket` | `globalThis.WebSocket` | implementation to use |
| `connectTimeoutMs` | `10000` | wait for `connection_ack` |
| `subscribeTimeoutMs` | `10000` | wait for `subscribe_success` |
| `publishTimeoutMs` | `10000` | wait for `publish_success` |
| `keepAliveTimeoutMs` | from server (5 min) | close when silent longer than this |

Authorization helpers, all sending the header the AppSync docs specify:

| Helper | AppSync auth mode |
| --- | --- |
| `cognitoUserPool(() => idToken)` | Amazon Cognito user pools |
| `oidc(() => jwt)` | OpenID Connect |
| `lambdaAuthorizer(() => token)` | AWS Lambda authorizer |
| `apiKey('da2-…')` | API key (public, read-only channels only) |
| `customHeaders(async () => headers)` | anything else, e.g. IAM SigV4 signed by your own code |

The adapter speaks the documented `aws-appsync-event-ws` protocol: authorization base64url-encoded into a `header-…` subprotocol on connect, `connection_init` → `connection_ack` (carries the keep-alive timeout), periodic `ka`, `subscribe`/`subscribe_success`/`subscribe_error`, `data`, `unsubscribe`, `publish`.

## Client options

| Option | Default | Meaning |
| --- | --- | --- |
| `autoConnect` | `true` | first `subscribe()` connects |
| `backoff.initialMs` | `500` | first retry delay |
| `backoff.maxMs` | `30000` | cap per delay |
| `backoff.factor` | `2` | growth per attempt |
| `backoff.jitter` | `0.5` | ±25 % randomisation |
| `maxReconnectAttempts` | `Infinity` | give up → status `closed`, error `RECONNECT_GIVE_UP` |
| `logger` | none | `(level, message, data) => void` |

## Examples

- [`examples/browser`](./examples/browser): single HTML file against a real Event API, no build tooling in the page.
- [`examples/expo`](./examples/expo): the same screen as an Expo app that runs in Expo Go.

## Development

```bash
pnpm install        # esbuild's install script is allow-listed in pnpm-workspace.yaml
pnpm typecheck
pnpm test           # vitest, runs against an in-memory protocol server; no cloud account needed
pnpm build          # ESM + CJS + d.ts into dist/, plus a browser bundle for examples/
```

See [CONTRIBUTING.md](./CONTRIBUTING.md). Changes are recorded in [CHANGELOG.md](./CHANGELOG.md).

## License

[MIT](./LICENSE)
