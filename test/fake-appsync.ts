/**
 * In-memory stand-in for the AppSync Events realtime endpoint. Implements just
 * enough of the `aws-appsync-event-ws` protocol for the tests: connection_init
 * → connection_ack, subscribe → subscribe_success | subscribe_error,
 * unsubscribe, publish, data delivery, keep-alive and server-side closes.
 */
import type { WebSocketLike } from '../src/appsync-events/connection';

type Listener<T> = ((event: T) => void) | null;

export interface Sent {
  type: string;
  id?: string;
  channel?: string;
  authorization?: Record<string, string>;
  events?: string[];
}

export class FakeSocket implements WebSocketLike {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState = FakeSocket.CONNECTING;
  onopen: Listener<unknown> = null;
  onmessage: Listener<{ data: unknown }> = null;
  onerror: Listener<unknown> = null;
  onclose: Listener<{ code?: number; reason?: string }> = null;

  readonly sent: Sent[] = [];
  readonly subscriptions = new Map<string, string>(); // id → channel
  closedWith: { code?: number; reason?: string } | null = null;

  constructor(
    readonly url: string,
    readonly protocols: string | string[] | undefined,
    readonly server: FakeAppSyncServer,
  ) {
    server.sockets.push(this);
    queueMicrotask(() => {
      if (this.readyState !== FakeSocket.CONNECTING) return;
      this.readyState = FakeSocket.OPEN;
      this.onopen?.({});
    });
  }

  send(data: string): void {
    if (this.readyState !== FakeSocket.OPEN) throw new Error('FakeSocket: send on non-open socket');
    const message = JSON.parse(data) as Sent;
    this.sent.push(message);
    this.server.handle(this, message);
  }

  close(code = 1000, reason = ''): void {
    if (this.readyState >= FakeSocket.CLOSING) return;
    this.readyState = FakeSocket.CLOSED;
    this.closedWith = { code, reason };
    queueMicrotask(() => this.onclose?.({ code, reason }));
  }

  /* ---- server side helpers ---- */

  serverSend(message: unknown): void {
    if (this.readyState !== FakeSocket.OPEN) return;
    this.onmessage?.({ data: JSON.stringify(message) });
  }

  /** Simulate the server dropping the connection. */
  serverClose(code = 1006, reason = 'abnormal'): void {
    if (this.readyState >= FakeSocket.CLOSING) return;
    this.readyState = FakeSocket.CLOSED;
    this.closedWith = { code, reason };
    this.onclose?.({ code, reason });
  }

  keepAlive(): void {
    this.serverSend({ type: 'ka' });
  }

  /** Deliver an event to every subscription on `channel`. */
  emit(channel: string, payload: unknown, asArray = false): void {
    const encoded = JSON.stringify(payload);
    for (const [id, ch] of this.subscriptions) {
      if (ch !== channel) continue;
      this.serverSend({ type: 'data', id, event: asArray ? [encoded] : encoded });
    }
  }

  sentOfType(type: string): Sent[] {
    return this.sent.filter((m) => m.type === type);
  }
}

export class FakeAppSyncServer {
  readonly sockets: FakeSocket[] = [];
  /** Channels for which subscribe is answered with subscribe_error. */
  readonly denyChannels = new Set<string>();
  connectionTimeoutMs = 300_000;
  /** When false the server never sends connection_ack (to test connect timeouts). */
  acknowledgeConnections = true;
  /** When false the server never answers subscribe messages (to test subscribe timeouts). */
  acknowledgeSubscriptions = true;

  /** A WebSocket constructor bound to this server instance. */
  readonly WebSocket = ((server: FakeAppSyncServer) =>
    class BoundSocket extends FakeSocket {
      constructor(url: string, protocols?: string | string[]) {
        super(url, protocols, server);
      }
    })(this);

  get last(): FakeSocket {
    const s = this.sockets[this.sockets.length - 1];
    if (!s) throw new Error('no socket opened yet');
    return s;
  }

  handle(socket: FakeSocket, message: Sent): void {
    switch (message.type) {
      case 'connection_init':
        if (this.acknowledgeConnections) {
          socket.serverSend({ type: 'connection_ack', connectionTimeoutMs: this.connectionTimeoutMs });
        }
        break;
      case 'subscribe': {
        if (!this.acknowledgeSubscriptions) break;
        const channel = message.channel ?? '';
        if (this.denyChannels.has(channel)) {
          socket.serverSend({
            type: 'subscribe_error',
            id: message.id,
            errors: [{ errorType: 'UnauthorizedException', message: 'You are not authorized to make this call.' }],
          });
        } else {
          if (message.id) socket.subscriptions.set(message.id, channel);
          socket.serverSend({ type: 'subscribe_success', id: message.id });
        }
        break;
      }
      case 'unsubscribe':
        if (message.id) socket.subscriptions.delete(message.id);
        socket.serverSend({ type: 'unsubscribe_success', id: message.id });
        break;
      case 'publish':
        socket.serverSend({
          type: 'publish_success',
          id: message.id,
          successful: (message.events ?? []).map((_, index) => ({ identifier: `evt-${index}`, index })),
          failed: [],
        });
        break;
      default:
        break;
    }
  }
}

/** Flush pending microtasks (the fake socket answers asynchronously). */
export async function flush(times = 5): Promise<void> {
  for (let i = 0; i < times; i++) await Promise.resolve();
}

export function decodeHeaderProtocol(protocols: string | string[] | undefined): Record<string, string> {
  const list = Array.isArray(protocols) ? protocols : protocols ? [protocols] : [];
  const header = list.find((p) => p.startsWith('header-'));
  if (!header) throw new Error('no header- subprotocol');
  const b64 = header.slice('header-'.length).replace(/-/g, '+').replace(/_/g, '/');
  const padded = b64 + '='.repeat((4 - (b64.length % 4)) % 4);
  return JSON.parse(Buffer.from(padded, 'base64').toString('utf8')) as Record<string, string>;
}
