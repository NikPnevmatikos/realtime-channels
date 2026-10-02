import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  computeBackoffDelay,
  createClient,
  RealtimeError,
  type ClientOptions,
  type CloseInfo,
  type ConnectionStatus,
  type Subscription,
  type SubscriptionStatus,
} from '../src/core';
import { FakeAdapter, settle, type SubscribeReply } from './fake-adapter';

const FAST: ClientOptions = { backoff: { initialMs: 100, maxMs: 100, jitter: 0 } };

function setup(options: ClientOptions = FAST) {
  const adapter = new FakeAdapter();
  const client = createClient(adapter, options);
  const errors: string[] = [];
  client.onError((e) => errors.push(e.code));
  return { adapter, client, errors };
}

describe('createClient (core)', () => {
  beforeEach(() => vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] }));
  afterEach(() => vi.useRealTimers());

  describe('connection', () => {
    it('gives up after maxReconnectAttempts and reports RECONNECT_GIVE_UP', async () => {
      const { adapter, client, errors } = setup({ maxReconnectAttempts: 2, backoff: { initialMs: 10, jitter: 0 } });
      await client.connect();
      expect(client.status).toBe('open');

      adapter.down = true;
      adapter.last.drop();
      expect(client.status).toBe('reconnecting');
      await vi.advanceTimersByTimeAsync(10); // attempt 1 fails
      await vi.advanceTimersByTimeAsync(20); // attempt 2 fails
      await vi.advanceTimersByTimeAsync(40);
      expect(client.status).toBe('closed');
      expect(errors).toContain('RECONNECT_GIVE_UP');
    });

    it('connect() after giving up starts a fresh retry budget', async () => {
      const { adapter, client } = setup({ maxReconnectAttempts: 3, backoff: { initialMs: 10, maxMs: 10, jitter: 0 } });
      adapter.down = true;
      await client.connect().catch(() => {});
      await vi.advanceTimersByTimeAsync(1_000);
      expect(client.status).toBe('closed');
      expect(adapter.connectCalls).toBe(4); // first try + 3 reconnects

      await client.connect().catch(() => {});
      await vi.advanceTimersByTimeAsync(1_000);
      expect(adapter.connectCalls).toBe(8);

      adapter.down = false;
      await client.connect();
      expect(client.status).toBe('open');
    });

    it('connect() while waiting to reconnect skips the rest of the backoff', async () => {
      const { adapter, client } = setup({ backoff: { initialMs: 30_000, jitter: 0 } });
      await client.connect();
      adapter.last.drop();
      expect(client.status).toBe('reconnecting');
      await client.connect();
      expect(client.status).toBe('open');
      expect(adapter.connections).toHaveLength(2);
    });

    it('close() from a status listener while reconnecting stops the reconnect', async () => {
      const { adapter, client } = setup();
      await client.connect();
      client.onStatus((s) => {
        if (s === 'reconnecting') client.close();
      });
      adapter.last.drop();
      expect(client.status).toBe('closed');
      await vi.advanceTimersByTimeAsync(1_000);
      expect(adapter.connections).toHaveLength(1);
      expect(client.status).toBe('closed');
    });

    it('connect() resolves once open, without waiting for subscribe acks', async () => {
      const { adapter, client } = setup({ ...FAST, autoConnect: false });
      adapter.subscribeReply = () => 'hold';
      const sub = client.subscribe('room', () => {});
      await client.connect();
      expect(client.status).toBe('open');
      expect(sub.status).toBe('pending');
      adapter.last.held[0]!.ack();
      await sub.ready;
      expect(sub.status).toBe('active');
    });

    it('a subscribe that never settles does not hold up reconnecting', async () => {
      const { adapter, client } = setup();
      adapter.subscribeReply = () => 'hold';
      client.subscribe('room', () => {});
      await settle();
      adapter.last.held.splice(0); // the ack never comes and the adapter never rejects it

      adapter.subscribeReply = () => 'ack';
      adapter.last.drop();
      await vi.advanceTimersByTimeAsync(100);
      await settle();
      expect(adapter.connections).toHaveLength(2);
      expect(client.status).toBe('open');
      expect(adapter.last.subscribeCalls).toEqual(['room']);
    });

    it('close() during a connect, then connect(), opens one connection', async () => {
      const { adapter, client } = setup();
      adapter.holdConnects = true;
      void client.connect(); // superseded by close()
      client.close();
      const current = client.connect();
      adapter.releaseConnect(); // the stale attempt opens and is closed again
      await settle();
      expect(client.connect()).toBe(current); // joins the live attempt instead of starting a third
      adapter.releaseConnect();
      await current;
      expect(client.status).toBe('open');
      expect(adapter.connectCalls).toBe(2);
      expect(adapter.connections.map((c) => c.closed)).toEqual([true, false]);
    });

    it('stops when connect() is rejected with CONNECTION_REJECTED', async () => {
      const { adapter, client, errors } = setup();
      adapter.connectErrors.push(new RealtimeError('banned', 'CONNECTION_REJECTED'));
      await expect(client.connect()).rejects.toMatchObject({ code: 'CONNECTION_REJECTED' });
      expect(client.status).toBe('closed');
      await vi.advanceTimersByTimeAsync(60_000);
      expect(adapter.connectCalls).toBe(1);
      expect(errors).toEqual(['CONNECTION_REJECTED']);
    });

    it('does not reconnect when an open connection closes with CONNECTION_REJECTED', async () => {
      const { adapter, client, errors } = setup();
      let closeInfo: CloseInfo | undefined;
      client.onStatus((s, info) => {
        if (s === 'closed') closeInfo = info;
      });
      await client.connect();
      const rejection = new RealtimeError('kicked', 'CONNECTION_REJECTED');
      adapter.last.drop({ error: rejection });
      expect(client.status).toBe('closed');
      expect(closeInfo).toMatchObject({ intentional: false, error: rejection });
      await vi.advanceTimersByTimeAsync(60_000);
      expect(adapter.connections).toHaveLength(1);
      expect(errors).toEqual(['CONNECTION_REJECTED']);
    });

    it('publish() throws NOT_CONNECTED, then NOT_SUPPORTED when the adapter has no publish', async () => {
      const { client } = setup();
      await expect(client.publish('x', [1])).rejects.toMatchObject({ code: 'NOT_CONNECTED' });
      await client.connect();
      await expect(client.publish('x', [1])).rejects.toMatchObject({ code: 'NOT_SUPPORTED' });
    });
  });

  describe('subscriptions', () => {
    it('validates the channel argument', () => {
      const { client } = setup({ autoConnect: false });
      expect(() => client.subscribe('', () => {})).toThrow(TypeError);
    });

    it('isolates handler exceptions as HANDLER_ERROR and keeps the subscription alive', async () => {
      const { adapter, client } = setup();
      const errors: RealtimeError[] = [];
      client.onError((e) => errors.push(e));
      const seen: unknown[] = [];
      const sub = client.subscribe('c', (e) => {
        seen.push(e);
        if (e === 'bad') throw new Error('handler bug');
      });
      await sub.ready;

      adapter.last.emit('c', 'bad');
      adapter.last.emit('c', 'good');
      expect(seen).toEqual(['bad', 'good']);
      expect(errors.map((e) => e.code)).toEqual(['HANDLER_ERROR']);
      expect(errors[0]?.message).toContain('"c"');
    });

    it('stops delivering to a handler after unsubscribe', async () => {
      const { adapter, client } = setup();
      const seen: unknown[] = [];
      const sub = client.subscribe('c', (e) => seen.push(e));
      const other = client.subscribe('c', () => {}); // keeps the channel open on the adapter
      await sub.ready;
      sub.unsubscribe();
      adapter.last.emit('c', 'late');
      expect(seen).toEqual([]);
      expect(sub.status).toBe('unsubscribed');
      other.unsubscribe();
    });

    it('rejects ready with UNSUBSCRIBED when unsubscribing before the ack', async () => {
      const { client } = setup({ autoConnect: false });
      const sub = client.subscribe('c', () => {});
      sub.unsubscribe();
      await expect(sub.ready).rejects.toMatchObject({ code: 'UNSUBSCRIBED' });
      expect(client.status).toBe('idle');
    });

    it('shares one adapter subscription between the subscribers of a channel', async () => {
      const { adapter, client } = setup();
      const a: unknown[] = [];
      const b: unknown[] = [];
      const subA = client.subscribe('room', (e) => a.push(e));
      const subB = client.subscribe('room', (e) => b.push(e));
      await Promise.all([subA.ready, subB.ready]);
      expect(adapter.last.subscribeCalls).toEqual(['room']);

      adapter.last.emit('room', 1);
      expect(a).toEqual([1]);
      expect(b).toEqual([1]);

      subA.unsubscribe();
      expect(adapter.last.unsubscribeCalls).toEqual([]);
      adapter.last.emit('room', 2);
      expect(a).toEqual([1]);
      expect(b).toEqual([1, 2]);

      subB.unsubscribe();
      expect(adapter.last.unsubscribeCalls).toEqual(['room']);
      expect(adapter.last.liveCount()).toBe(0);
    });

    it('a subscriber joining a live channel is active at once', async () => {
      const { adapter, client } = setup();
      await client.subscribe('room', () => {}).ready;
      const onActive = vi.fn();
      const second = client.subscribe('room', () => {}, { onActive });
      expect(second.status).toBe('active');
      expect(onActive).not.toHaveBeenCalled(); // never from inside subscribe()
      await settle();
      expect(onActive).toHaveBeenCalledWith({ resumed: false });
      await second.ready;
      expect(adapter.last.subscribeCalls).toEqual(['room']);
    });

    it('re-subscribing while the last unsubscribe is waiting for its ack reuses that request', async () => {
      const { adapter, client } = setup();
      adapter.subscribeReply = () => 'hold';
      const first = client.subscribe('room', () => {});
      await settle();
      first.unsubscribe(); // ack still in flight
      const second = client.subscribe('room', () => {});
      await settle();
      expect(adapter.last.subscribeCalls).toEqual(['room']); // no second request racing the first

      adapter.last.held[0]!.ack();
      await second.ready;
      expect(second.status).toBe('active');
      expect(adapter.last.unsubscribeCalls).toEqual([]);
      expect(adapter.last.liveCount('room')).toBe(1);
    });

    it('unsubscribes a request acknowledged after everyone left', async () => {
      const { adapter, client } = setup();
      adapter.subscribeReply = () => 'hold';
      const sub = client.subscribe('room', () => {});
      await settle();
      sub.unsubscribe();
      adapter.last.held[0]!.ack();
      await settle();
      expect(adapter.last.unsubscribeCalls).toEqual(['room']);
      expect(adapter.last.liveCount()).toBe(0);

      adapter.subscribeReply = () => 'ack';
      await client.subscribe('room', () => {}).ready; // a later subscribe starts a fresh request
      expect(adapter.last.subscribeCalls).toEqual(['room', 'room']);
    });

    it('subscribing from a status listener when the connection opens subscribes once', async () => {
      const { adapter, client } = setup();
      const seen: unknown[] = [];
      let sub: Subscription | undefined;
      client.onStatus((s) => {
        if (s === 'open' && !sub) sub = client.subscribe('room', (e) => seen.push(e));
      });
      await client.connect();
      await settle();
      expect(adapter.last.subscribeCalls).toEqual(['room']);
      adapter.last.emit('room', 1);
      expect(seen).toEqual([1]);
      sub!.unsubscribe();
      expect(adapter.last.liveCount()).toBe(0);
    });
  });

  describe('subscription lifecycle', () => {
    it('goes pending → active → pending → active across a reconnect, with onActive flags', async () => {
      const { adapter, client } = setup();
      const onActive = vi.fn();
      const sub = client.subscribe('room', () => {}, { onActive });
      const statuses: SubscriptionStatus[] = [];
      sub.onStatus((s) => statuses.push(s));
      expect(sub.status).toBe('pending');
      await sub.ready;
      expect(sub.status).toBe('active');
      expect(onActive).toHaveBeenLastCalledWith({ resumed: false });

      adapter.last.drop();
      expect(sub.status).toBe('pending');
      await vi.advanceTimersByTimeAsync(100);
      await settle();
      expect(sub.status).toBe('active');
      expect(onActive).toHaveBeenCalledTimes(2);
      expect(onActive).toHaveBeenLastCalledWith({ resumed: true });

      sub.unsubscribe();
      expect(statuses).toEqual(['active', 'pending', 'active', 'unsubscribed']);
    });

    it('tells subscription listeners about a drop after the client status changed', async () => {
      const { adapter, client } = setup();
      const sub = client.subscribe('room', () => {});
      await sub.ready;
      let clientStatus: ConnectionStatus | undefined;
      sub.onStatus(() => (clientStatus = client.status));
      adapter.last.drop();
      expect(clientStatus).toBe('reconnecting');
    });

    it('retries a subscribe that failed on a live connection, with backoff', async () => {
      const { adapter, client, errors } = setup();
      const replies: SubscribeReply[] = [
        new RealtimeError('slow', 'SUBSCRIBE_TIMEOUT'),
        new RealtimeError('slow', 'SUBSCRIBE_TIMEOUT'),
      ];
      adapter.subscribeReply = () => replies.shift() ?? 'ack';
      const onError = vi.fn();
      const sub = client.subscribe('room', () => {}, { onError });
      await settle();
      expect(sub.status).toBe('pending');
      expect(errors).toEqual(['SUBSCRIBE_TIMEOUT']);
      expect(onError).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(100); // retry 1 fails
      await vi.advanceTimersByTimeAsync(100); // retry 2 is acknowledged
      await sub.ready;
      expect(sub.status).toBe('active');
      expect(adapter.last.subscribeCalls).toEqual(['room', 'room', 'room']);
      expect(adapter.connections).toHaveLength(1);
      expect(errors).toEqual(['SUBSCRIBE_TIMEOUT', 'SUBSCRIBE_TIMEOUT']);
    });

    it('does not report failures caused by the connection dropping; ready waits for the re-subscribe', async () => {
      const { adapter, client, errors } = setup();
      adapter.subscribeReply = () => 'hold';
      const onError = vi.fn();
      const sub = client.subscribe('room', () => {}, { onError });
      await settle();

      adapter.subscribeReply = () => 'ack';
      adapter.last.drop(); // fails the held subscribe with CONNECTION_CLOSED
      await settle();
      expect(errors).toEqual([]);
      expect(onError).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(100);
      await expect(sub.ready).resolves.toBeUndefined();
      expect(sub.status).toBe('active');
    });

    it('rejects every subscriber of a refused channel and does not retry it', async () => {
      const { adapter, client, errors } = setup();
      adapter.subscribeReply = (ch) => (ch === 'secret' ? new RealtimeError('no', 'SUBSCRIBE_REJECTED') : 'ack');
      const onErrorA = vi.fn();
      const a = client.subscribe('secret', () => {}, { onError: onErrorA });
      const b = client.subscribe('secret', () => {});
      const seen: unknown[] = [];
      b.onStatus((s, e) => seen.push([s, e?.code]));

      await expect(a.ready).rejects.toMatchObject({ code: 'SUBSCRIBE_REJECTED' });
      await expect(b.ready).rejects.toMatchObject({ code: 'SUBSCRIBE_REJECTED' });
      expect(a.status).toBe('rejected');
      expect(seen).toEqual([['rejected', 'SUBSCRIBE_REJECTED']]);
      expect(onErrorA).toHaveBeenCalledTimes(1);
      expect(errors).toEqual(['SUBSCRIBE_REJECTED']);
      expect(adapter.last.subscribeCalls).toEqual(['secret']);
      a.unsubscribe(); // no-op after a rejection

      adapter.last.drop();
      await vi.advanceTimersByTimeAsync(100);
      expect(adapter.connections).toHaveLength(2);
      expect(adapter.last.subscribeCalls).toEqual([]); // not retried after the reconnect

      const again = client.subscribe('secret', () => {}); // asking again does try again
      await expect(again.ready).rejects.toMatchObject({ code: 'SUBSCRIBE_REJECTED' });
      expect(adapter.last.subscribeCalls).toEqual(['secret']);
    });

    it('passes transport errors on a live subscription to every subscriber', async () => {
      const { adapter, client, errors } = setup();
      const onErrorA = vi.fn();
      const onErrorB = vi.fn();
      const a = client.subscribe('room', () => {}, { onError: onErrorA });
      client.subscribe('room', () => {}, { onError: onErrorB });
      await a.ready;
      adapter.last.reportError('room', new RealtimeError('bad payload', 'BROADCAST_ERROR'));
      expect(onErrorA).toHaveBeenCalledTimes(1);
      expect(onErrorB).toHaveBeenCalledTimes(1);
      expect(errors).toEqual(['BROADCAST_ERROR']);
      expect(a.status).toBe('active');
    });

    it('reports an onActive that throws as HANDLER_ERROR and stays active', async () => {
      const { client, errors } = setup();
      const sub = client.subscribe('room', () => {}, {
        onActive: () => {
          throw new Error('refetch failed');
        },
      });
      await sub.ready;
      expect(errors).toEqual(['HANDLER_ERROR']);
      expect(sub.status).toBe('active');
    });

    it('keeps subscriptions across close() and resumes them on connect()', async () => {
      const { adapter, client } = setup();
      const onActive = vi.fn();
      const sub = client.subscribe('room', () => {}, { onActive });
      await sub.ready;
      client.close();
      expect(sub.status).toBe('pending');
      await vi.advanceTimersByTimeAsync(10_000);
      expect(adapter.connections).toHaveLength(1);

      await client.connect();
      await settle();
      expect(sub.status).toBe('active');
      expect(onActive).toHaveBeenLastCalledWith({ resumed: true });
    });
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
