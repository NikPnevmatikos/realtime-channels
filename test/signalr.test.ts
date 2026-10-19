import { HttpTransportType } from '@microsoft/signalr';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createClient, RealtimeError, type ConnectionStatus } from '../src/core';
import { signalR, type SignalROptions } from '../src/signalr';
import { FakeSignalRServer, flush } from './fake-signalr';

const HUB = 'https://api.example.com/hubs/events';

function setup(overrides: Partial<SignalROptions> = {}) {
  const server = new FakeSignalRServer();
  let tokenCounter = 0;
  const accessToken = vi.fn(async () => {
    tokenCounter += 1;
    return `jwt-${tokenCounter}`;
  });
  const adapter = signalR({ url: HUB, accessToken, ...server.options(), ...overrides });
  const statuses: ConnectionStatus[] = [];
  const errors: RealtimeError[] = [];
  const client = createClient(adapter, { backoff: { initialMs: 100, maxMs: 100, jitter: 0 } });
  client.onStatus((s) => statuses.push(s));
  client.onError((e) => errors.push(e));
  return { server, client, statuses, errors, accessToken };
}

/** The adapter on its own, without the core, to check exactly what it reports. */
async function connectAdapter(overrides: Partial<SignalROptions> = {}) {
  const server = new FakeSignalRServer();
  const adapter = signalR({ url: HUB, ...server.options(), ...overrides });
  const onClose = vi.fn();
  const connection = await adapter.connect({ onClose });
  return { server, connection, onClose };
}

describe('signalR adapter', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('negotiates with the token, then opens the transport with the connection token and does the JSON handshake', async () => {
    const { server, client } = setup();
    await client.connect();

    expect(server.negotiations).toHaveLength(1);
    expect(server.negotiations[0]).toMatchObject({
      url: `${HUB}/negotiate?negotiateVersion=1`,
      headers: { Authorization: 'Bearer jwt-1' },
    });
    expect(server.last.url).toBe('wss://api.example.com/hubs/events?id=token-1');
    expect(server.last.headers['Authorization']).toBe('Bearer jwt-2'); // asked again for the transport
    expect(server.last.handshake).toEqual({ protocol: 'json', version: 1 });
    expect(client.status).toBe('open');
  });

  it('follows a negotiate redirect with its token, as Azure SignalR Service sends', async () => {
    const { server, client } = setup();
    server.redirect = { url: 'https://example.service.signalr.net/client/?hub=events', accessToken: 'azure-token' };
    await client.connect();

    expect(server.negotiations.map((n) => n.url)).toEqual([
      `${HUB}/negotiate?negotiateVersion=1`,
      'https://example.service.signalr.net/client/negotiate?hub=events&negotiateVersion=1',
    ]);
    expect(server.negotiations[1]?.headers['Authorization']).toBe('Bearer azure-token');
    expect(server.last.url).toBe('wss://example.service.signalr.net/client/?hub=events&id=token-1');
    expect(server.last.headers['Authorization']).toBe('Bearer azure-token');
  });

  it('skips negotiate when asked, and connects without a token when there is none', async () => {
    const { server, client } = setup({
      skipNegotiation: true,
      transport: HttpTransportType.WebSockets,
      accessToken: () => undefined,
    });
    await client.connect();

    expect(server.negotiations).toHaveLength(0);
    expect(server.last.url).toBe('wss://api.example.com/hubs/events');
    expect(server.last.headers['Authorization']).toBeUndefined();
  });

  it('subscribes through the hub, resolves ready on the completion and delivers events by channel', async () => {
    const { server, client } = setup();
    const received: unknown[] = [];
    const sub = client.subscribe('events/concert', (event, meta) => received.push({ event, channel: meta.channel }));
    await flush(20);
    await sub.ready;

    expect(server.last.sentOfTarget('Subscribe')[0]).toMatchObject({ arguments: ['events/concert'] });
    expect(server.last.sentOfTarget('Subscribe')[0]?.invocationId).toBeDefined();
    server.last.emit('events/concert', { toVersion: 7 });
    server.last.serverSend({ type: 1, target: 'Event', arguments: ['events/other', { notMine: true }] });
    expect(received).toEqual([{ event: { toVersion: 7 }, channel: 'events/concert' }]);
  });

  it('reads several messages in one frame, whatever the case of the client method', async () => {
    const { server, client } = setup();
    const received: unknown[] = [];
    const sub = client.subscribe('a', (event) => received.push(event));
    await flush(20);
    await sub.ready;

    server.last.serverSend(
      { type: 1, target: 'Event', arguments: ['a', 1] },
      { type: 1, target: 'event', arguments: ['a', 2] },
    );
    expect(received).toEqual([1, 2]);
  });

  it('rejects ready with SUBSCRIBE_REJECTED when the hub refuses, and does not retry that channel', async () => {
    const { server, client, errors } = setup();
    server.denyChannels.add('events/secret');
    const sub = client.subscribe('events/secret', () => {});
    await flush(20);

    await expect(sub.ready).rejects.toMatchObject({ code: 'SUBSCRIBE_REJECTED', message: 'Not allowed to join this channel.' });
    expect(errors[0]?.code).toBe('SUBSCRIBE_REJECTED');

    server.last.serverClose();
    await vi.advanceTimersByTimeAsync(150);
    await flush(20);
    expect(server.sockets).toHaveLength(2);
    expect(server.last.sentOfTarget('Subscribe')).toHaveLength(0);
  });

  it('sends Unsubscribe without waiting and stops delivering after unsubscribe()', async () => {
    const { server, client } = setup();
    const received: unknown[] = [];
    const sub = client.subscribe('a', (e) => received.push(e));
    await flush(20);
    await sub.ready;

    sub.unsubscribe();
    await flush(5);
    const unsubscribe = server.last.sentOfTarget('Unsubscribe')[0];
    expect(unsubscribe?.arguments).toEqual(['a']);
    expect(unsubscribe?.invocationId).toBeUndefined();
    server.last.serverSend({ type: 1, target: 'Event', arguments: ['a', 'late'] });
    expect(received).toEqual([]);
  });

  it('reconnects after an unexpected close with a fresh token, and joins its channels again', async () => {
    const { server, client, statuses } = setup();
    const received: unknown[] = [];
    const sub = client.subscribe('a', (e) => received.push(e));
    await flush(20);
    await sub.ready;

    server.last.serverClose(1006, 'network');
    await flush(5);
    expect(client.status).toBe('reconnecting');
    await vi.advanceTimersByTimeAsync(100);
    await flush(20);

    expect(server.sockets).toHaveLength(2);
    expect(server.negotiations[1]?.headers['Authorization']).toBe('Bearer jwt-3');
    expect(server.last.sentOfTarget('Subscribe')[0]?.arguments).toEqual(['a']);
    server.last.emit('a', 'after');
    expect(received).toEqual(['after']);
    expect(statuses).toEqual(['connecting', 'open', 'reconnecting', 'open']);
  });

  it('does not reconnect after close(), and the SDK does not either', async () => {
    const { server, client } = setup();
    await client.connect();

    client.close();
    await flush(20);
    expect(client.status).toBe('closed');
    expect(server.last.closedWith?.code).toBe(1000);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(server.sockets).toHaveLength(1);
  });

  it('stops for good when the hub closes with an error and does not allow a reconnect', async () => {
    const { server, client, errors } = setup();
    await client.connect();

    server.last.serverSend({ type: 7, error: 'Connection closed with an error. Unauthorized' });
    await flush(20);
    await vi.advanceTimersByTimeAsync(5_000);

    expect(client.status).toBe('closed');
    expect(server.sockets).toHaveLength(1);
    expect(errors.some((e) => e.code === 'CONNECTION_REJECTED')).toBe(true);
  });

  it('reconnects when the hub closes and allows it, or closes without an error', async () => {
    const { server, client } = setup();
    await client.connect();

    server.last.serverSend({ type: 7, error: 'Server is shutting down.', allowReconnect: true });
    await flush(20);
    await vi.advanceTimersByTimeAsync(100);
    await flush(20);
    expect(server.sockets).toHaveLength(2);

    server.last.serverSend({ type: 7 });
    await flush(20);
    await vi.advanceTimersByTimeAsync(100);
    await flush(20);
    expect(server.sockets).toHaveLength(3);
    expect(client.status).toBe('open');
  });

  it('pings the hub once keepAliveIntervalMs has passed without sending anything', async () => {
    // The SDK checks when traffic arrives; a real hub pings every 15 s, so that is never long.
    const { server, client } = setup({ keepAliveIntervalMs: 1_000 });
    await client.connect();
    const before = server.last.sentOfType(6).length;

    await vi.advanceTimersByTimeAsync(500);
    server.last.ping();
    await flush(20);
    expect(server.last.sentOfType(6)).toHaveLength(before);

    await vi.advanceTimersByTimeAsync(600);
    server.last.ping();
    await flush(20);
    expect(server.last.sentOfType(6)).toHaveLength(before + 1);
  });

  it('closes and reconnects when the hub is silent for serverTimeoutMs', async () => {
    const { server, client, statuses } = setup({ serverTimeoutMs: 1_000, keepAliveIntervalMs: 60_000 });
    await client.connect();
    const first = server.last;

    await vi.advanceTimersByTimeAsync(900);
    first.ping(); // resets the watchdog
    await vi.advanceTimersByTimeAsync(900);
    expect(first.closedWith).toBeNull();

    await vi.advanceTimersByTimeAsync(150);
    expect(first.closedWith).not.toBeNull();
    await flush(20);
    expect(client.status).toBe('reconnecting');
    await vi.advanceTimersByTimeAsync(100);
    await flush(20);
    expect(server.sockets).toHaveLength(2);
    expect(statuses).toEqual(['connecting', 'open', 'reconnecting', 'open']);
  });

  it('fails connect with CONNECT_TIMEOUT when the handshake is never answered, then keeps retrying', async () => {
    const { server, client } = setup({ connectTimeoutMs: 500 });
    server.answerHandshakes = false;
    const attempt = client.connect();
    attempt.catch(() => {});
    await flush(5);
    await vi.advanceTimersByTimeAsync(500);

    await expect(attempt).rejects.toMatchObject({ code: 'CONNECT_TIMEOUT' });
    await flush(20);
    expect(server.sockets[0]?.closedWith).not.toBeNull();
    expect(client.status).toBe('reconnecting');
    server.answerHandshakes = true;
    await vi.advanceTimersByTimeAsync(100);
    await flush(20);
    expect(client.status).toBe('open');
  });

  it('stops for good when the handshake is refused', async () => {
    const { server, client } = setup();
    server.handshakeError = "The protocol 'json' is not supported.";
    const attempt = client.connect();
    attempt.catch(() => {});
    await flush(20);

    await expect(attempt).rejects.toMatchObject({ code: 'CONNECTION_REJECTED' });
    await vi.advanceTimersByTimeAsync(5_000);
    expect(server.sockets).toHaveLength(1);
    expect(client.status).toBe('closed');
  });

  it('reports a refused negotiate as AUTH_ERROR and tries again with a new token', async () => {
    const { server, client } = setup();
    server.negotiateStatus = 401;
    const attempt = client.connect();
    attempt.catch(() => {});
    await flush(20);

    await expect(attempt).rejects.toMatchObject({ code: 'AUTH_ERROR' });
    expect(server.sockets).toHaveLength(0);
    server.negotiateStatus = 200;
    await vi.advanceTimersByTimeAsync(100);
    await flush(20);
    expect(client.status).toBe('open');
    expect(server.negotiations[1]?.headers['Authorization']).toBe('Bearer jwt-2');
  });

  it('reports a failing token provider as AUTH_ERROR', async () => {
    const { server, client } = setup({
      accessToken: () => {
        throw new Error('signed out');
      },
    });
    const attempt = client.connect();
    attempt.catch(() => {});
    await flush(20);

    await expect(attempt).rejects.toMatchObject({ code: 'AUTH_ERROR' });
    expect(server.negotiations).toHaveLength(0);
  });

  it('settles connect within the timeout when the token provider never answers', async () => {
    const { client, server } = setup({ accessToken: () => new Promise<string>(() => {}), connectTimeoutMs: 500 });
    const attempt = client.connect();
    attempt.catch(() => {});
    await vi.advanceTimersByTimeAsync(500);

    await expect(attempt).rejects.toMatchObject({ code: 'AUTH_ERROR' });
    expect(server.negotiations).toHaveLength(0);
  });

  it('rejects a waiting subscribe at once when the connection drops, and leaves no timer behind', async () => {
    const server = new FakeSignalRServer();
    server.answerSubscribes = false;
    const adapter = signalR({ url: HUB, ...server.options() });
    const onClose = vi.fn();
    const connection = await adapter.connect({ onClose });
    const subscribing = connection.subscribe('a', () => {});
    subscribing.catch(() => {});
    await flush(5);

    server.last.serverClose();

    await expect(subscribing).rejects.toMatchObject({ code: 'CONNECTION_CLOSED' });
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(onClose).toHaveBeenCalledWith(
      expect.objectContaining({ intentional: false, error: expect.objectContaining({ code: 'CONNECTION_ERROR' }) }),
    );
    expect(vi.getTimerCount()).toBe(0);
  });

  it('reports its own close() as intentional, once', async () => {
    const { connection, onClose } = await connectAdapter();

    connection.close();
    await flush(10);

    expect(onClose).toHaveBeenCalledTimes(1);
    expect(onClose).toHaveBeenCalledWith({ intentional: true });
    expect(vi.getTimerCount()).toBe(0);
  });

  it('undoes a subscribe that the hub accepts after the client gave up on it', async () => {
    const { server, connection } = await connectAdapter({ subscribeTimeoutMs: 500 });
    server.subscribeAckDelayMs = 800;
    const subscribing = connection.subscribe('a', () => {});
    subscribing.catch(() => {});
    await flush(5);

    await vi.advanceTimersByTimeAsync(500);
    await expect(subscribing).rejects.toMatchObject({ code: 'SUBSCRIBE_TIMEOUT' });
    await vi.advanceTimersByTimeAsync(300); // the late acceptance arrives
    await flush(5);

    expect(server.last.sentOfTarget('Unsubscribe').map((m) => m.arguments)).toEqual([['a']]);
  });

  it('leaves a late acceptance alone while another subscribe to the channel is on its way', async () => {
    const { server, connection } = await connectAdapter({ subscribeTimeoutMs: 500 });
    server.subscribeAckDelayMs = 800;
    const first = connection.subscribe('a', () => {});
    first.catch(() => {});
    await flush(5);
    await vi.advanceTimersByTimeAsync(500); // the first try times out, the core would try again
    server.subscribeAckDelayMs = 400;
    const second = connection.subscribe('a', () => {});
    await flush(5);

    await vi.advanceTimersByTimeAsync(300); // t = 800: the first acceptance arrives late
    await flush(5);
    expect(server.last.sentOfTarget('Unsubscribe')).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(100); // t = 900: the second one arrives in time
    await expect(second).resolves.toBeDefined();
    expect(server.last.channels.has('a')).toBe(true);
  });

  it('publishes through the hub and reports a refusal as PUBLISH_REJECTED', async () => {
    const { server, client } = setup();
    await client.connect();

    await client.publish('chat', [{ text: 'hi' }]);
    expect(server.last.sentOfTarget('Publish')[0]?.arguments).toEqual(['chat', [{ text: 'hi' }]]);
    await expect(client.publish('read-only', [1])).rejects.toMatchObject({ code: 'PUBLISH_REJECTED' });
  });

  it('answers a hub call that waits for a client result, so the hub does not hang', async () => {
    const { server, client } = setup();
    await client.connect();

    server.last.serverSend({ type: 1, invocationId: '42', target: 'GetSomething', arguments: [] });
    await flush(10);
    const completion = server.last.sentOfType(3)[0];
    expect(completion).toMatchObject({ invocationId: '42' });
    expect(completion?.error).toBeDefined();
  });

  it('uses the method names it is given', async () => {
    const { server, client } = setup({ methods: { subscribe: 'Join', event: 'Changed' } });
    const received: unknown[] = [];
    server.handle = (socket, message) => {
      if (message.target === 'Join') {
        socket.channels.add(String(message.arguments?.[0]));
        socket.serverSend({ type: 3, invocationId: message.invocationId ?? '' });
      }
    };
    const sub = client.subscribe('a', (e) => received.push(e));
    await flush(20);
    await sub.ready;

    server.last.serverSend({ type: 1, target: 'Changed', arguments: ['a', 'x'] });
    expect(server.last.sentOfTarget('Join')).toHaveLength(1);
    expect(received).toEqual(['x']);
  });

  it('refuses a relative URL when there is no page to resolve it against', () => {
    expect(() => signalR({ url: '/hubs/events' })).toThrow(RealtimeError);
  });
});
