import { LogLevel, type IHttpConnectionOptions, type ILogger } from '@microsoft/signalr';
import { RealtimeError } from '../core/errors';
import type { Adapter } from '../core/types';
import { openSignalRConnection, type AccessTokenProvider, type ResolvedConfig, type SignalRMethods } from './connection';

export type { AccessTokenProvider, SignalRMethods } from './connection';

/**
 * Everything `@microsoft/signalr` takes in `withUrl()` (`transport`, `skipNegotiation`, `withCredentials`,
 * `headers`, `httpClient`, `timeout`, `logger`, …) plus the channel convention and the timings.
 */
export interface SignalROptions extends Omit<IHttpConnectionOptions, 'accessTokenFactory'> {
  /**
   * The hub's URL, e.g. `https://api.example.com/hubs/events`. A path (`/hubs/events`) works in browsers
   * and is resolved against the page's address.
   */
  url: string;
  /**
   * Returns the token for each new connection. The SDK sends it as `Authorization: Bearer …`, or as
   * `access_token` in the URL where a browser can't set headers (WebSockets, Server-Sent Events).
   */
  accessToken?: AccessTokenProvider;
  /**
   * The hub's method names, when they differ from the defaults: `Subscribe(channel)`,
   * `Unsubscribe(channel)`, `Publish(channel, events)` on the hub, and `Event(channel, event)` on the client.
   */
  methods?: Partial<SignalRMethods>;
  /** The SDK's logger or minimum level. Default `LogLevel.Warning`; `LogLevel.None` silences it. */
  logger?: ILogger | LogLevel;
  /** How long to wait for the token, negotiate, the transport and the handshake together. Default 30 000 ms. */
  connectTimeoutMs?: number;
  /** How long to wait for the hub to accept a subscribe. Default 10 000 ms. */
  subscribeTimeoutMs?: number;
  /** How long to wait for the hub to accept a publish. Default 10 000 ms. */
  publishTimeoutMs?: number;
  /** Send a ping after this long without sending anything; the server drops silent clients. Default 15 000 ms. */
  keepAliveIntervalMs?: number;
  /** Close the connection when nothing arrives from the server for this long. Default 30 000 ms. */
  serverTimeoutMs?: number;
}

export const defaultSignalRMethods: SignalRMethods = {
  subscribe: 'Subscribe',
  unsubscribe: 'Unsubscribe',
  publish: 'Publish',
  event: 'Event',
};

/**
 * Adapter for an ASP.NET Core SignalR hub (or Azure SignalR Service) that follows the channel convention in
 * docs/adapters/signalr.md: the client joins and leaves channels through hub methods, and the hub delivers
 * `(channel, event)` to one client method. Built on the official `@microsoft/signalr` client, so negotiate,
 * transport fallback (WebSockets, Server-Sent Events, long polling) and Azure redirects come from it.
 */
export function signalR(options: SignalROptions): Adapter {
  const {
    url,
    accessToken,
    methods,
    logger,
    connectTimeoutMs,
    subscribeTimeoutMs,
    publishTimeoutMs,
    keepAliveIntervalMs,
    serverTimeoutMs,
    ...http
  } = options;

  // The SDK resolves a path against the page; anywhere else it would fail on every reconnect, so fail now.
  if (!/^https?:\/\//i.test(url) && typeof document === 'undefined') {
    throw new RealtimeError(
      `SignalR: "${url}" is not an absolute http(s) URL, and there is no page address to resolve it against`,
      'CONNECT_FAILED',
    );
  }

  const cfg: ResolvedConfig = {
    url,
    http,
    accessToken,
    logger: logger ?? LogLevel.Warning,
    methods: { ...defaultSignalRMethods, ...methods },
    connectTimeoutMs: connectTimeoutMs ?? 30_000,
    subscribeTimeoutMs: subscribeTimeoutMs ?? 10_000,
    publishTimeoutMs: publishTimeoutMs ?? 10_000,
    keepAliveIntervalMs: keepAliveIntervalMs ?? 15_000,
    serverTimeoutMs: serverTimeoutMs ?? 30_000,
  };

  return {
    name: 'signalr',
    connect: (handlers) => openSignalRConnection(cfg, handlers),
  };
}
