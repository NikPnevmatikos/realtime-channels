import { RealtimeError } from '../core/errors';
import type { Adapter } from '../core/types';
import type { AppSyncAuth } from './auth';
import { openAppSyncConnection, type ResolvedConfig, type WebSocketConstructor } from './connection';

export type { AppSyncAuth } from './auth';
export { apiKey, cognitoUserPool, customHeaders, lambdaAuthorizer, oidc } from './auth';
export type { WebSocketConstructor, WebSocketLike } from './connection';

export interface AppSyncEventsOptions {
  /**
   * The Event API HTTP endpoint host, e.g. `abc123.appsync-api.eu-west-1.amazonaws.com`
   * or your custom domain. A full URL (`https://…/event`) is accepted and reduced to the host.
   */
  httpDomain: string;
  /**
   * The realtime host. Defaults to the HTTP host with `appsync-api` replaced by
   * `appsync-realtime-api`, which is correct for AWS-provided domains. With a
   * custom domain both endpoints share the host, so pass the same value.
   */
  realtimeDomain?: string;
  auth: AppSyncAuth;
  /** WebSocket implementation. Defaults to `globalThis.WebSocket` (browsers, React Native, Node ≥ 22). */
  WebSocket?: WebSocketConstructor;
  /** How long to wait for `connection_ack`. Default 10 000 ms. */
  connectTimeoutMs?: number;
  /** How long to wait for `subscribe_success`. Default 10 000 ms. */
  subscribeTimeoutMs?: number;
  /** How long to wait for `publish_success`. Default 10 000 ms. */
  publishTimeoutMs?: number;
  /**
   * Close the connection when no server message arrives within this window.
   * Defaults to the `connectionTimeoutMs` the server sends in `connection_ack`
   * (5 minutes at the time of writing). Set explicitly to override.
   */
  keepAliveTimeoutMs?: number;
}

function toHost(value: string): string {
  let host = value.trim();
  host = host.replace(/^[a-z]+:\/\//i, '');
  const slash = host.indexOf('/');
  if (slash !== -1) host = host.slice(0, slash);
  if (host.length === 0) throw new RealtimeError('AppSync Events: httpDomain is empty', 'CONNECT_FAILED');
  return host;
}

/**
 * Adapter for AWS AppSync Events. One WebSocket per client, any number of
 * channel subscriptions on it. Publishing over the socket is supported when
 * the API's publish auth mode allows the client's credentials.
 */
export function appSyncEvents(options: AppSyncEventsOptions): Adapter {
  const httpDomain = toHost(options.httpDomain);
  const realtimeDomain = options.realtimeDomain
    ? toHost(options.realtimeDomain)
    : httpDomain.replace('appsync-api.', 'appsync-realtime-api.');

  const WebSocketImpl =
    options.WebSocket ?? (globalThis as unknown as { WebSocket?: WebSocketConstructor }).WebSocket;
  if (typeof WebSocketImpl !== 'function') {
    throw new RealtimeError(
      'No WebSocket implementation found. Pass one via the `WebSocket` option (e.g. from the "ws" package on Node < 22).',
      'NO_WEBSOCKET',
    );
  }

  const cfg: ResolvedConfig = {
    httpDomain,
    realtimeDomain,
    auth: options.auth,
    WebSocket: WebSocketImpl,
    connectTimeoutMs: options.connectTimeoutMs ?? 10_000,
    subscribeTimeoutMs: options.subscribeTimeoutMs ?? 10_000,
    publishTimeoutMs: options.publishTimeoutMs ?? 10_000,
    keepAliveTimeoutMs: options.keepAliveTimeoutMs,
  };

  return {
    name: 'appsync-events',
    connect: (handlers) => openAppSyncConnection(cfg, handlers),
  };
}
