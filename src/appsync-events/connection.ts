import { RealtimeError } from '../core/errors';
import type { AdapterConnectHandlers, AdapterConnection, AdapterSubscription } from '../core/types';
import type { AppSyncAuth } from './auth';
import { base64UrlEncode, randomId } from './encoding';

/**
 * Minimal structural WebSocket type so any implementation (browser, React Native,
 * Node's global WebSocket, the `ws` package) fits without casts. Handler
 * parameters are intentionally loose: DOM event types differ per runtime.
 */
export interface WebSocketLike {
  readonly readyState: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onopen: ((event: any) => void) | null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onmessage: ((event: any) => void) | null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onerror: ((event: any) => void) | null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onclose: ((event: any) => void) | null;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

interface MessageEventLike {
  data: unknown;
}

interface CloseEventLike {
  code?: number;
  reason?: string;
}

export type WebSocketConstructor = new (url: string, protocols?: string | string[]) => WebSocketLike;

export interface ResolvedConfig {
  httpDomain: string;
  realtimeDomain: string;
  auth: AppSyncAuth;
  WebSocket: WebSocketConstructor;
  connectTimeoutMs: number;
  subscribeTimeoutMs: number;
  publishTimeoutMs: number;
  keepAliveTimeoutMs: number | undefined;
}

const WS_OPEN = 1;
/** Application-defined close code (4000–4999 are free for applications). */
const CLOSE_KEEPALIVE_TIMEOUT = 4000;

interface Pending {
  timer: ReturnType<typeof setTimeout>;
  resolve: (message: ServerMessage) => void;
  reject: (error: RealtimeError) => void;
}

interface ActiveSubscription {
  channel: string;
  onEvent: (event: unknown) => void;
  onError: ((error: RealtimeError) => void) | undefined;
}

interface ServerMessage {
  type?: string;
  id?: string;
  connectionTimeoutMs?: number;
  event?: unknown;
  errors?: unknown;
  failed?: unknown[];
  successful?: unknown[];
}

function describeErrors(errors: unknown, fallback: string): string {
  if (Array.isArray(errors) && errors.length > 0) {
    return errors
      .map((e) => {
        if (e && typeof e === 'object') {
          const { errorType, message } = e as { errorType?: string; message?: string };
          return [errorType, message].filter(Boolean).join(': ');
        }
        return String(e);
      })
      .join('; ');
  }
  return fallback;
}

function parseEvent(raw: unknown): unknown {
  if (typeof raw !== 'string') return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

async function authorization(cfg: ResolvedConfig): Promise<Record<string, string>> {
  let headers: Record<string, string>;
  try {
    headers = await cfg.auth.headers();
  } catch (err) {
    throw new RealtimeError('AppSync auth provider failed', 'AUTH_ERROR', err);
  }
  return { host: cfg.httpDomain, ...headers };
}

/**
 * Opens one WebSocket to the AppSync Events realtime endpoint and speaks the
 * `aws-appsync-event-ws` protocol on it. Resolves after `connection_ack`.
 */
export async function openAppSyncConnection(
  cfg: ResolvedConfig,
  handlers: AdapterConnectHandlers,
): Promise<AdapterConnection> {
  const authObject = await authorization(cfg);
  const protocols = ['aws-appsync-event-ws', `header-${base64UrlEncode(JSON.stringify(authObject))}`];
  const url = `wss://${cfg.realtimeDomain}/event/realtime`;

  let ws: WebSocketLike;
  try {
    ws = new cfg.WebSocket(url, protocols);
  } catch (err) {
    throw new RealtimeError(`Could not open WebSocket to ${url}`, 'CONNECT_FAILED', err);
  }

  return new Promise<AdapterConnection>((resolveConnect, rejectConnect) => {
    /** connect() promise has been resolved or rejected. */
    let settled = false;
    /** connection_ack was received; from here on closes are reported through handlers.onClose. */
    let established = false;
    let intentionalClose = false;
    let lastError: unknown;
    let keepAliveTimeoutMs = cfg.keepAliveTimeoutMs ?? 300_000;
    let keepAliveTimer: ReturnType<typeof setTimeout> | null = null;
    const pending = new Map<string, Pending>();
    const subscriptions = new Map<string, ActiveSubscription>();

    const connectTimer = setTimeout(() => {
      if (settled) return;
      settled = true;
      lastError = new RealtimeError('Timed out waiting for connection_ack', 'CONNECT_TIMEOUT');
      safeClose(1000, 'connect timeout');
      rejectConnect(lastError as RealtimeError);
    }, cfg.connectTimeoutMs);

    function safeClose(code?: number, reason?: string): void {
      try {
        ws.close(code, reason);
      } catch {
        /* already closed */
      }
    }

    function send(message: unknown): void {
      ws.send(JSON.stringify(message));
    }

    function armKeepAlive(): void {
      if (keepAliveTimer !== null) clearTimeout(keepAliveTimer);
      keepAliveTimer = setTimeout(() => {
        lastError = new RealtimeError(
          `No keep-alive from server for ${keepAliveTimeoutMs} ms`,
          'KEEPALIVE_TIMEOUT',
        );
        safeClose(CLOSE_KEEPALIVE_TIMEOUT, 'keep-alive timeout');
      }, keepAliveTimeoutMs);
    }

    function cleanupTimers(): void {
      clearTimeout(connectTimer);
      if (keepAliveTimer !== null) {
        clearTimeout(keepAliveTimer);
        keepAliveTimer = null;
      }
      for (const p of pending.values()) clearTimeout(p.timer);
    }

    function awaitAck(id: string, timeoutMs: number, timeoutCode: 'SUBSCRIBE_TIMEOUT' | 'PUBLISH_TIMEOUT', what: string) {
      return new Promise<ServerMessage>((resolve, reject) => {
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(new RealtimeError(`Timed out waiting for ${what}`, timeoutCode));
        }, timeoutMs);
        pending.set(id, { timer, resolve, reject });
      });
    }

    function settlePending(id: string | undefined, fn: (p: Pending) => void): void {
      if (!id) return;
      const p = pending.get(id);
      if (!p) return;
      pending.delete(id);
      clearTimeout(p.timer);
      fn(p);
    }

    const connection: AdapterConnection = {
      async subscribe(channel, onEvent, onError): Promise<AdapterSubscription> {
        if (ws.readyState !== WS_OPEN) {
          throw new RealtimeError('Cannot subscribe: connection is not open', 'CONNECTION_CLOSED');
        }
        const id = randomId();
        const auth = await authorization(cfg);
        const ack = awaitAck(id, cfg.subscribeTimeoutMs, 'SUBSCRIBE_TIMEOUT', `subscribe_success on "${channel}"`);
        send({ type: 'subscribe', id, channel, authorization: auth });
        await ack;
        subscriptions.set(id, { channel, onEvent, onError });
        return {
          unsubscribe(): void {
            if (!subscriptions.delete(id)) return;
            if (ws.readyState === WS_OPEN) {
              try {
                send({ type: 'unsubscribe', id });
              } catch {
                /* socket went away; nothing to undo */
              }
            }
          },
        };
      },

      async publish(channel, events): Promise<void> {
        if (ws.readyState !== WS_OPEN) {
          throw new RealtimeError('Cannot publish: connection is not open', 'CONNECTION_CLOSED');
        }
        if (events.length === 0 || events.length > 5) {
          throw new RealtimeError('AppSync Events accepts 1 to 5 events per publish', 'PUBLISH_FAILED');
        }
        const id = randomId();
        const auth = await authorization(cfg);
        const ack = awaitAck(id, cfg.publishTimeoutMs, 'PUBLISH_TIMEOUT', `publish_success on "${channel}"`);
        send({
          type: 'publish',
          id,
          channel,
          events: events.map((e) => (typeof e === 'string' ? e : JSON.stringify(e))),
          authorization: auth,
        });
        const result = await ack;
        if (Array.isArray(result.failed) && result.failed.length > 0) {
          throw new RealtimeError(
            `${result.failed.length} of ${events.length} events were rejected by the publish handler`,
            'PUBLISH_REJECTED',
            result.failed,
          );
        }
      },

      close(): void {
        intentionalClose = true;
        safeClose(1000, 'client closed');
      },
    };

    ws.onopen = () => {
      try {
        send({ type: 'connection_init' });
      } catch (err) {
        lastError = err;
        safeClose(1000, 'init failed');
      }
    };

    ws.onerror = (event: unknown) => {
      lastError = event;
    };

    ws.onmessage = (event: MessageEventLike) => {
      let message: ServerMessage;
      try {
        message = JSON.parse(typeof event.data === 'string' ? event.data : String(event.data)) as ServerMessage;
      } catch {
        return;
      }
      armKeepAlive(); // any traffic proves the connection is alive

      switch (message.type) {
        case 'connection_ack': {
          if (cfg.keepAliveTimeoutMs === undefined && typeof message.connectionTimeoutMs === 'number') {
            keepAliveTimeoutMs = message.connectionTimeoutMs;
            armKeepAlive();
          }
          if (!settled) {
            settled = true;
            established = true;
            clearTimeout(connectTimer);
            resolveConnect(connection);
          }
          break;
        }
        case 'ka':
          break;
        case 'subscribe_success':
        case 'publish_success':
        case 'unsubscribe_success':
          settlePending(message.id, (p) => p.resolve(message));
          break;
        case 'subscribe_error':
          settlePending(message.id, (p) =>
            p.reject(
              new RealtimeError(describeErrors(message.errors, 'Subscription refused'), 'SUBSCRIBE_REJECTED', message.errors),
            ),
          );
          break;
        case 'publish_error':
          settlePending(message.id, (p) =>
            p.reject(new RealtimeError(describeErrors(message.errors, 'Publish refused'), 'PUBLISH_REJECTED', message.errors)),
          );
          break;
        case 'unsubscribe_error':
          settlePending(message.id, (p) => p.resolve(message)); // nothing useful to do; the local state is already gone
          break;
        case 'data': {
          const sub = message.id ? subscriptions.get(message.id) : undefined;
          if (!sub) break;
          const items = Array.isArray(message.event) ? message.event : [message.event];
          for (const raw of items) sub.onEvent(parseEvent(raw));
          break;
        }
        case 'broadcast_error': {
          const sub = message.id ? subscriptions.get(message.id) : undefined;
          sub?.onError?.(
            new RealtimeError(describeErrors(message.errors, 'Broadcast error'), 'BROADCAST_ERROR', message.errors),
          );
          break;
        }
        case 'connection_error':
        case 'error': {
          const error = new RealtimeError(
            describeErrors(message.errors, 'Connection error'),
            'CONNECTION_ERROR',
            message.errors,
          );
          lastError = error;
          if (!settled) {
            settled = true;
            clearTimeout(connectTimer);
            safeClose(1000, 'connection error');
            rejectConnect(error);
          }
          break;
        }
        default:
          break;
      }
    };

    ws.onclose = (event: CloseEventLike) => {
      cleanupTimers();
      const closeError = new RealtimeError(
        `Connection closed${event.code !== undefined ? ` (code ${event.code})` : ''}`,
        'CONNECTION_CLOSED',
        lastError,
      );
      for (const p of pending.values()) p.reject(closeError);
      pending.clear();
      subscriptions.clear();

      if (!established) {
        // The connection never became usable: report through the connect() promise only,
        // never through onClose, so the core does not schedule a second reconnect.
        if (!settled) {
          settled = true;
          rejectConnect(
            lastError instanceof RealtimeError
              ? lastError
              : new RealtimeError('Socket closed before connection_ack', 'CONNECT_FAILED', lastError ?? event),
          );
        }
        return;
      }
      const info: { code?: number; reason?: string; error?: unknown; intentional: boolean } = { intentional: intentionalClose };
      if (event.code !== undefined) info.code = event.code;
      if (event.reason !== undefined) info.reason = event.reason;
      if (lastError !== undefined) info.error = lastError;
      handlers.onClose(info);
    };
  });
}
