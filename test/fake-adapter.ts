/**
 * A transport-free adapter for testing the core. Tests decide when connections open,
 * how subscribes are answered, and when the "server" drops the connection.
 */
import {
  RealtimeError,
  type Adapter,
  type AdapterConnectHandlers,
  type AdapterConnection,
  type AdapterSubscription,
  type CloseInfo,
} from '../src/core';

/** How subscribe() answers: acknowledge now, wait for the test, or fail with this error. */
export type SubscribeReply = 'ack' | 'hold' | RealtimeError;

export interface HeldSubscribe {
  channel: string;
  ack(): void;
  fail(error: RealtimeError): void;
}

interface Live {
  channel: string;
  onEvent: (event: unknown) => void;
  onError: ((error: RealtimeError) => void) | undefined;
}

export class FakeConnection implements AdapterConnection {
  readonly subscribeCalls: string[] = [];
  readonly unsubscribeCalls: string[] = [];
  readonly held: HeldSubscribe[] = [];
  closed = false;
  private readonly live = new Map<number, Live>();
  private nextId = 1;

  constructor(
    private readonly adapter: FakeAdapter,
    private readonly handlers: AdapterConnectHandlers,
  ) {}

  subscribe(
    channel: string,
    onEvent: (event: unknown) => void,
    onError?: (error: RealtimeError) => void,
  ): Promise<AdapterSubscription> {
    this.subscribeCalls.push(channel);
    if (this.closed) return Promise.reject(new RealtimeError('closed', 'CONNECTION_CLOSED'));
    const reply = this.adapter.subscribeReply(channel);
    return new Promise((resolve, reject) => {
      const ack = (): void => {
        if (this.closed) {
          reject(new RealtimeError('closed', 'CONNECTION_CLOSED'));
          return;
        }
        const id = this.nextId++;
        this.live.set(id, { channel, onEvent, onError });
        resolve({
          unsubscribe: () => {
            this.unsubscribeCalls.push(channel);
            this.live.delete(id);
          },
        });
      };
      if (reply === 'ack') ack();
      else if (reply === 'hold') this.held.push({ channel, ack, fail: reject });
      else reject(reply);
    });
  }

  close(): void {
    this.end({ intentional: true });
  }

  /** Deliver an event to every live subscription on `channel`. */
  emit(channel: string, event: unknown): void {
    for (const s of [...this.live.values()]) if (s.channel === channel) s.onEvent(event);
  }

  /** Report an error on every live subscription on `channel` (like a broadcast error). */
  reportError(channel: string, error: RealtimeError): void {
    for (const s of [...this.live.values()]) if (s.channel === channel) s.onError?.(error);
  }

  /** Server-side subscriptions currently open, optionally for one channel. */
  liveCount(channel?: string): number {
    return [...this.live.values()].filter((s) => channel === undefined || s.channel === channel).length;
  }

  /** The server or the network drops the connection. */
  drop(info: Partial<CloseInfo> = {}): void {
    this.end({ intentional: false, ...info });
  }

  private end(info: CloseInfo): void {
    if (this.closed) return;
    this.closed = true;
    this.live.clear();
    for (const h of this.held.splice(0)) h.fail(new RealtimeError('closed', 'CONNECTION_CLOSED'));
    this.handlers.onClose(info);
  }
}

export class FakeAdapter implements Adapter {
  readonly name = 'fake';
  readonly connections: FakeConnection[] = [];
  connectCalls = 0;
  /** Errors returned by the next connect() calls, in order. */
  readonly connectErrors: RealtimeError[] = [];
  /** While true every connect() fails with CONNECT_FAILED. */
  down = false;
  /** While true connect() waits for releaseConnect(). */
  holdConnects = false;
  /** Decides how each subscribe() is answered. */
  subscribeReply: (channel: string) => SubscribeReply = () => 'ack';
  private readonly heldConnects: Array<() => void> = [];

  connect(handlers: AdapterConnectHandlers): Promise<AdapterConnection> {
    this.connectCalls += 1;
    const error = this.connectErrors.shift() ?? (this.down ? new RealtimeError('down', 'CONNECT_FAILED') : undefined);
    if (error) return Promise.reject(error);
    const open = (): FakeConnection => {
      const conn = new FakeConnection(this, handlers);
      this.connections.push(conn);
      return conn;
    };
    if (!this.holdConnects) return Promise.resolve(open());
    return new Promise((resolve) => this.heldConnects.push(() => resolve(open())));
  }

  /** Let the oldest held connect() finish. */
  releaseConnect(): void {
    const next = this.heldConnects.shift();
    if (!next) throw new Error('no held connect');
    next();
  }

  get last(): FakeConnection {
    const conn = this.connections[this.connections.length - 1];
    if (!conn) throw new Error('no connection opened yet');
    return conn;
  }
}

/** Let pending promise callbacks run. */
export async function settle(times = 10): Promise<void> {
  for (let i = 0; i < times; i++) await Promise.resolve();
}
