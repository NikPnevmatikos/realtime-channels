# Writing an adapter

An adapter teaches `realtime-channels` one transport. The core owns everything
that is the same for every transport: reconnection with backoff, re-subscribing
after a reconnect, status and error fan-out, the React hooks, handler isolation.
The adapter owns one live connection at a time and the wire protocol on it.

This page is the contract. If you follow it, your adapter gets the whole test
suite's guarantees for free and users switch transports without touching their code.

## The interface

```ts
import type { Adapter, AdapterConnection, CloseInfo } from 'realtime-channels';
import { RealtimeError } from 'realtime-channels';

export function myTransport(options: MyOptions): Adapter {
  return {
    name: 'my-transport',
    async connect(handlers): Promise<AdapterConnection> {
      // 1. open a fresh connection (socket, EventSource, SDK client, …)
      // 2. wait until it can accept subscriptions
      // 3. return the connection object below
      // 4. later, when the connection ends for ANY reason, call handlers.onClose(info) exactly once
    },
  };
}
```

```ts
interface AdapterConnection {
  subscribe(channel, onEvent, onError?): Promise<{ unsubscribe(): void }>;
  publish?(channel, events): Promise<void>;   // optional
  close(): void;
}
```

## Rules the core relies on

1. **`connect()` resolves only when subscriptions can be accepted.** If the handshake fails, reject with a `RealtimeError` (`CONNECT_FAILED`, `CONNECT_TIMEOUT`, `CONNECTION_ERROR`, `AUTH_ERROR`). The core schedules the retry.
2. **`onClose` fires exactly once per established connection, and never for a connection whose `connect()` rejected.** Otherwise the core schedules two reconnects for one failure. Keep an `established` flag: set it when you resolve `connect()`, and only call `onClose` when it is true.
3. **`onClose({ intentional })`**: `true` when the close came from `connection.close()`, `false` for anything else (network, server, watchdog). The core reconnects only when `intentional` is `false`.
4. **`subscribe()` resolves on the server's acknowledgement** (or immediately, if the transport has no ack). Reject with:
   - `SUBSCRIBE_REJECTED` when the server refused (authorization, unknown channel). The core will **not** retry that channel on reconnect and will reject `Subscription.ready`.
   - `SUBSCRIBE_TIMEOUT` / `CONNECTION_CLOSED` for transient failures. The core keeps the channel and retries after reconnect.
5. **Deliver parsed events.** If your wire format is JSON strings, parse them and pass the value to `onEvent`. Pass what you receive if it is not JSON.
6. **`unsubscribe()` must be safe** to call after the connection died. Check the socket state before sending.
7. **Auth is fetched per operation.** If the transport authenticates on connect and again per subscribe (as AppSync does), call the user's token provider each time; that is how refreshed tokens propagate. Wrap provider failures in `RealtimeError('…', 'AUTH_ERROR', cause)`.
8. **No timers left behind.** Clear every timeout in your close path. The test suite runs with fake timers and will hang on leaks.
9. **No dependencies.** The package ships with zero runtime dependencies. Wrapping an official SDK (Ably, Centrifugo, Socket.IO) is welcome, but the SDK must be an **optional peer dependency** imported only inside that adapter's entry point.
10. **Structural types for platform objects.** Do not import DOM or Node types into the public surface; accept a `WebSocket`/`EventSource` constructor via options with a structural type, like the AppSync adapter's `WebSocketLike`.

## Skeleton: plain WebSocket with a JSON envelope

A minimal adapter for a server that speaks `{"type":"subscribe","channel":…}` /
`{"type":"event","channel":…,"data":…}`:

```ts
import { RealtimeError, type Adapter, type AdapterConnection } from 'realtime-channels';

export interface JsonWebSocketOptions {
  url: string;                                   // wss://…
  getToken?: () => Promise<string> | string;     // appended as ?token=… or sent in a hello message
  WebSocket?: new (url: string) => any;
}

export function jsonWebSocket(opts: JsonWebSocketOptions): Adapter {
  const WS = opts.WebSocket ?? (globalThis as any).WebSocket;
  return {
    name: 'json-websocket',
    connect(handlers) {
      return new Promise<AdapterConnection>((resolve, reject) => {
        const ws = new WS(opts.url);
        const handlersByChannel = new Map<string, Set<(e: unknown) => void>>();
        let established = false;
        let intentional = false;

        ws.onopen = async () => {
          if (opts.getToken) ws.send(JSON.stringify({ type: 'hello', token: await opts.getToken() }));
          established = true;
          resolve({
            async subscribe(channel, onEvent) {
              ws.send(JSON.stringify({ type: 'subscribe', channel }));
              const set = handlersByChannel.get(channel) ?? new Set();
              set.add(onEvent);
              handlersByChannel.set(channel, set);
              return {
                unsubscribe() {
                  set.delete(onEvent);
                  if (set.size === 0 && ws.readyState === 1) ws.send(JSON.stringify({ type: 'unsubscribe', channel }));
                },
              };
            },
            close() {
              intentional = true;
              ws.close(1000);
            },
          });
        };
        ws.onmessage = (ev: { data: string }) => {
          const msg = JSON.parse(ev.data);
          if (msg.type === 'event') for (const h of handlersByChannel.get(msg.channel) ?? []) h(msg.data);
        };
        ws.onclose = (ev: { code?: number; reason?: string }) => {
          if (!established) return reject(new RealtimeError('closed before open', 'CONNECT_FAILED', ev));
          handlers.onClose({ code: ev.code, reason: ev.reason, intentional });
        };
      });
    },
  };
}
```

The real adapter would add: an ack for subscribe (so `SUBSCRIBE_REJECTED` is possible), a keep-alive watchdog, timeouts, and clearing of timers on close.

## Where files go

```
src/<transport>/index.ts        public entry: the factory + option types
src/<transport>/connection.ts   protocol implementation (optional split)
test/<transport>.test.ts        tests against an in-memory fake, no network
test/fake-<transport>.ts        the fake server
docs/adapters/<transport>.md    anything a user must know beyond the README table
```

Then:

1. Add the entry to `tsup.config.ts` (`'<transport>': 'src/<transport>/index.ts'`).
2. Add the subpath to `package.json` → `exports` (`"./<transport>"`), mirroring the existing entries.
3. Add a row to the **Adapters** table in `README.md`.
4. Add a line under **Unreleased** in `CHANGELOG.md`.

## Testing pattern

Every adapter is tested against an in-memory fake of its server, never the real
service. Look at `test/fake-appsync.ts`: a class that implements the
`WebSocketLike` shape, records what the client sent, and answers the way the
server would. Tests then drive it with `vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] })`
and `await vi.advanceTimersByTimeAsync(ms)`.

The behaviours every adapter test file should cover:

- connect resolves after the handshake, rejects on timeout
- subscribe resolves on ack, delivers parsed events, rejects refused channels with `SUBSCRIBE_REJECTED`
- unsubscribe sends the right message and stops delivery
- unexpected close → `onClose({ intentional: false })`, and the core reconnects and re-subscribes
- `close()` → `onClose({ intentional: true })`, no reconnect
- keep-alive or liveness timeout (if the protocol has one)
- token provider is called for every connection (and every subscribe, if applicable)

A real-service smoke test (like `examples/browser`) is welcome as an example, not as a test.
