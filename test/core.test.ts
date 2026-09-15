import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { computeBackoffDelay, createClient, RealtimeError, type Adapter, type AdapterConnection } from '../src/core';

interface TestConnection {
  conn: AdapterConnection;
  close: (intentional?: boolean) => void;
  subscribed: string[];
  emit: (channel: string, event: unknown) => void;
}

/** Adapter whose connections are controlled by the test. */
function controllableAdapter() {
  const connections: TestConnection[] = [];
  let failNext = 0;
  const adapter: Adapter = {
    name: 'test',
    async connect(handlers) {
      if (failNext > 0) {
        failNext -= 1;
        throw new RealtimeError('boom', 'CONNECT_FAILED');
      }
      const subscribed: string[] = [];
      const handlersByChannel = new Map<string, Array<(event: unknown) => void>>();
      let closed = false;
      const conn: AdapterConnection = {
        async subscribe(channel, onEvent) {
          subscribed.push(channel);
          const list = handlersByChannel.get(channel) ?? [];
          list.push(onEvent);
          handlersByChannel.set(channel, list);
          return {
            unsubscribe: () => {
              const current = handlersByChannel.get(channel) ?? [];
              handlersByChannel.set(
                channel,
                current.filter((h) => h !== onEvent),
              );
            },
          };
        },
        close() {
          if (closed) return;
          closed = true;
          handlers.onClose({ intentional: true });
        },
      };
      connections.push({
        conn,
        subscribed,
        emit: (channel, event) => {
          for (const h of handlersByChannel.get(channel) ?? []) h(event);
        },
        close: (intentional = false) => {
          if (closed) return;
          closed = true;
          handlers.onClose({ intentional, code: 1006 });
        },
      });
      return conn;
    },
  };
  return { adapter, connections, failNext: (n: number) => (failNext = n) };
}

describe('createClient (core)', () => {
  beforeEach(() => vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] }));
  afterEach(() => vi.useRealTimers());

  it('gives up after maxReconnectAttempts and reports RECONNECT_GIVE_UP', async () => {
    const { adapter, connections, failNext } = controllableAdapter();
    const client = createClient(adapter, { maxReconnectAttempts: 2, backoff: { initialMs: 10, jitter: 0 } });
    const errors: string[] = [];
    client.onError((e) => errors.push(e.code));
    await client.connect();
    expect(client.status).toBe('open');

    failNext(10);
    connections[0]!.close();
    expect(client.status).toBe('reconnecting');
    await vi.advanceTimersByTimeAsync(10); // attempt 1 fails
    await vi.advanceTimersByTimeAsync(20); // attempt 2 fails
    await vi.advanceTimersByTimeAsync(40);
    expect(client.status).toBe('closed');
    expect(errors).toContain('RECONNECT_GIVE_UP');
  });

  it('publish() throws NOT_SUPPORTED when the adapter has no publish', async () => {
    const { adapter } = controllableAdapter();
    const client = createClient(adapter);
    await expect(client.publish('x', [1])).rejects.toMatchObject({ code: 'NOT_CONNECTED' });
    await client.connect();
    await expect(client.publish('x', [1])).rejects.toMatchObject({ code: 'NOT_SUPPORTED' });
  });

  it('isolates handler exceptions as HANDLER_ERROR and keeps the subscription alive', async () => {
    const { adapter, connections } = controllableAdapter();
    const client = createClient(adapter);
    const errors: RealtimeError[] = [];
    client.onError((e) => errors.push(e));
    const seen: unknown[] = [];
    const sub = client.subscribe('c', (e) => {
      seen.push(e);
      if (e === 'bad') throw new Error('handler bug');
    });
    await sub.ready;
    expect(client.status).toBe('open');

    connections[0]!.emit('c', 'bad');
    connections[0]!.emit('c', 'good');
    expect(seen).toEqual(['bad', 'good']);
    expect(errors.map((e) => e.code)).toEqual(['HANDLER_ERROR']);
    expect(errors[0]?.message).toContain('"c"');
  });

  it('stops delivering to a handler after unsubscribe even if the adapter still emits', async () => {
    const { adapter, connections } = controllableAdapter();
    const client = createClient(adapter);
    const seen: unknown[] = [];
    const sub = client.subscribe('c', (e) => seen.push(e));
    await sub.ready;
    sub.unsubscribe();
    connections[0]!.emit('c', 'late');
    expect(seen).toEqual([]);
  });

  it('rejects ready with UNSUBSCRIBED when unsubscribing before the ack', async () => {
    const { adapter } = controllableAdapter();
    const client = createClient(adapter, { autoConnect: false });
    const sub = client.subscribe('c', () => {});
    sub.unsubscribe();
    await expect(sub.ready).rejects.toMatchObject({ code: 'UNSUBSCRIBED' });
    expect(client.status).toBe('idle');
  });

  it('validates the channel argument', () => {
    const { adapter } = controllableAdapter();
    const client = createClient(adapter, { autoConnect: false });
    expect(() => client.subscribe('', () => {})).toThrow(TypeError);
  });
});

describe('computeBackoffDelay', () => {
  it('grows exponentially and caps at maxMs', () => {
    const opts = { initialMs: 100, maxMs: 1_000, factor: 2, jitter: 0 };
    expect([1, 2, 3, 4, 5, 6].map((a) => computeBackoffDelay(a, opts))).toEqual([100, 200, 400, 800, 1_000, 1_000]);
  });

  it('applies symmetric jitter within bounds', () => {
    const low = computeBackoffDelay(1, { initialMs: 1_000, jitter: 0.5, random: () => 0 });
    const high = computeBackoffDelay(1, { initialMs: 1_000, jitter: 0.5, random: () => 1 });
    expect(low).toBe(750);
    expect(high).toBe(1_250);
  });
});
