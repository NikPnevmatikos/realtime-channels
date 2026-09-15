import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createClient, RealtimeError, type ConnectionStatus } from '../src/core';
import { appSyncEvents, cognitoUserPool, apiKey } from '../src/appsync-events';
import { FakeAppSyncServer, decodeHeaderProtocol, flush } from './fake-appsync';

const HTTP_DOMAIN = 'abc123.appsync-api.eu-west-1.amazonaws.com';

function setup(overrides: Partial<Parameters<typeof appSyncEvents>[0]> = {}) {
  const server = new FakeAppSyncServer();
  const tokens: string[] = [];
  let tokenCounter = 0;
  const getToken = vi.fn(async () => {
    tokenCounter += 1;
    const t = `token-${tokenCounter}`;
    tokens.push(t);
    return t;
  });
  const adapter = appSyncEvents({
    httpDomain: HTTP_DOMAIN,
    auth: cognitoUserPool(getToken),
    WebSocket: server.WebSocket,
    ...overrides,
  });
  const statuses: ConnectionStatus[] = [];
  const errors: RealtimeError[] = [];
  const client = createClient(adapter, { backoff: { initialMs: 100, maxMs: 100, jitter: 0 } });
  client.onStatus((s) => statuses.push(s));
  client.onError((e) => errors.push(e));
  return { server, client, statuses, errors, getToken, tokens };
}

describe('appSyncEvents adapter', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('connects to the realtime endpoint with the auth header encoded in the subprotocol', async () => {
    const { server, client } = setup();
    await client.connect();

    const socket = server.last;
    expect(socket.url).toBe('wss://abc123.appsync-realtime-api.eu-west-1.amazonaws.com/event/realtime');
    expect(socket.protocols).toContain('aws-appsync-event-ws');
    expect(decodeHeaderProtocol(socket.protocols)).toEqual({ host: HTTP_DOMAIN, Authorization: 'token-1' });
    expect(socket.sentOfType('connection_init')).toHaveLength(1);
    expect(client.status).toBe('open');
  });

  it('accepts a full https URL and a custom realtime domain', async () => {
    const { server, client } = setup({
      httpDomain: 'https://realtime.example.com/event',
      realtimeDomain: 'realtime.example.com',
    });
    await client.connect();
    expect(server.last.url).toBe('wss://realtime.example.com/event/realtime');
    expect(decodeHeaderProtocol(server.last.protocols).host).toBe('realtime.example.com');
  });

  it('subscribes, resolves ready on subscribe_success and delivers parsed events', async () => {
    const { server, client } = setup();
    const received: unknown[] = [];
    const sub = client.subscribe('users/abc', (event, meta) => received.push({ event, channel: meta.channel }));
    await flush(20);
    await sub.ready;

    const subscribeMsg = server.last.sentOfType('subscribe')[0];
    expect(subscribeMsg?.channel).toBe('users/abc');
    expect(subscribeMsg?.authorization).toEqual({ host: HTTP_DOMAIN, Authorization: 'token-2' });

    server.last.emit('users/abc', { type: 'notification.changed', id: 4711 });
    server.last.emit('users/abc', { type: 'second' }, true); // array form
    expect(received).toEqual([
      { event: { type: 'notification.changed', id: 4711 }, channel: 'users/abc' },
      { event: { type: 'second' }, channel: 'users/abc' },
    ]);
  });

  it('rejects ready with SUBSCRIBE_REJECTED when the server refuses, and does not retry that channel', async () => {
    const { server, client, errors } = setup();
    server.denyChannels.add('users/someone-else');
    const onError = vi.fn();
    const sub = client.subscribe('users/someone-else', () => {}, { onError });
    await flush(20);

    await expect(sub.ready).rejects.toMatchObject({ code: 'SUBSCRIBE_REJECTED' });
    expect(onError).toHaveBeenCalledTimes(1);
    expect(errors[0]?.code).toBe('SUBSCRIBE_REJECTED');
    expect(errors[0]?.message).toContain('UnauthorizedException');

    // Force a reconnect: the refused channel must not be re-subscribed.
    server.last.serverClose();
    await vi.advanceTimersByTimeAsync(150);
    await flush(20);
    expect(server.sockets).toHaveLength(2);
    expect(server.last.sentOfType('subscribe')).toHaveLength(0);
  });

  it('sends unsubscribe and stops delivering after unsubscribe()', async () => {
    const { server, client } = setup();
    const received: unknown[] = [];
    const sub = client.subscribe('users/abc', (e) => received.push(e));
    await flush(20);
    await sub.ready;

    sub.unsubscribe();
    expect(server.last.sentOfType('unsubscribe')).toHaveLength(1);
    server.last.emit('users/abc', { late: true });
    expect(received).toEqual([]);
  });

  it('reconnects with backoff after an unexpected close, re-subscribes and fetches a fresh token', async () => {
    const { server, client, statuses, getToken } = setup();
    const received: unknown[] = [];
    const sub = client.subscribe('users/abc', (e) => received.push(e));
    await flush(20);
    await sub.ready;
    const callsAfterFirstConnect = getToken.mock.calls.length; // connect + subscribe

    server.last.serverClose(1006, 'network');
    expect(client.status).toBe('reconnecting');

    await vi.advanceTimersByTimeAsync(100);
    await flush(20);

    expect(server.sockets).toHaveLength(2);
    expect(client.status).toBe('open');
    expect(server.last.sentOfType('subscribe')[0]?.channel).toBe('users/abc');
    expect(getToken.mock.calls.length).toBe(callsAfterFirstConnect + 2); // fresh token for connect and for subscribe
    expect(decodeHeaderProtocol(server.last.protocols).Authorization).toBe('token-3');

    server.last.emit('users/abc', { after: 'reconnect' });
    expect(received).toEqual([{ after: 'reconnect' }]);
    expect(statuses).toEqual(['connecting', 'open', 'reconnecting', 'open']);
  });

  it('closes and reconnects when no keep-alive arrives within connectionTimeoutMs', async () => {
    const { server, client } = setup();
    server.connectionTimeoutMs = 1_000;
    await client.connect();
    const first = server.last;

    await vi.advanceTimersByTimeAsync(900);
    first.keepAlive(); // resets the watchdog
    await vi.advanceTimersByTimeAsync(900);
    expect(first.closedWith).toBeNull();

    await vi.advanceTimersByTimeAsync(150); // 1 050 ms since last ka: watchdog fired, backoff (100 ms) not yet
    expect(first.closedWith).toEqual({ code: 4000, reason: 'keep-alive timeout' });
    await flush(20);
    expect(client.status).toBe('reconnecting');
    expect(server.sockets).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(100);
    await flush(20);
    expect(server.sockets).toHaveLength(2);
    expect(client.status).toBe('open');
  });

  it('does not reconnect after close(), and connect() works again afterwards', async () => {
    const { server, client } = setup();
    const sub = client.subscribe('users/abc', () => {});
    await flush(20);
    await sub.ready;

    client.close();
    await flush(20);
    expect(client.status).toBe('closed');
    expect(server.last.closedWith?.code).toBe(1000);

    await vi.advanceTimersByTimeAsync(5_000);
    expect(server.sockets).toHaveLength(1);

    await client.connect();
    await flush(20);
    expect(server.sockets).toHaveLength(2);
    expect(server.last.sentOfType('subscribe')[0]?.channel).toBe('users/abc'); // kept subscriptions resume
  });

  it('fails connect with CONNECT_TIMEOUT when connection_ack never arrives, then keeps retrying', async () => {
    const { server, client, errors } = setup({ connectTimeoutMs: 500 });
    server.acknowledgeConnections = false;
    const attempt = client.connect();
    attempt.catch(() => {});
    await flush(5);
    await vi.advanceTimersByTimeAsync(500);
    await expect(attempt).rejects.toMatchObject({ code: 'CONNECT_TIMEOUT' });
    expect(errors[0]?.code).toBe('CONNECT_TIMEOUT');
    expect(client.status).toBe('reconnecting');

    server.acknowledgeConnections = true;
    await vi.advanceTimersByTimeAsync(100);
    await flush(20);
    expect(client.status).toBe('open');
  });

  it('publishes over the socket and reports rejected events', async () => {
    const { server, client } = setup({ auth: apiKey('da2-key') });
    await client.connect();
    await client.publish('default/chat', [{ msg: 'hi' }, 'already-a-string']);
    const publishMsg = server.last.sentOfType('publish')[0];
    expect(publishMsg?.events).toEqual(['{"msg":"hi"}', 'already-a-string']);
    expect(publishMsg?.authorization).toEqual({ host: HTTP_DOMAIN, 'x-api-key': 'da2-key' });

    await expect(client.publish('default/chat', [])).rejects.toMatchObject({ code: 'PUBLISH_FAILED' });
  });

  it('surfaces auth provider failures as AUTH_ERROR without opening a socket', async () => {
    const server = new FakeAppSyncServer();
    const adapter = appSyncEvents({
      httpDomain: HTTP_DOMAIN,
      auth: cognitoUserPool(() => {
        throw new Error('no session');
      }),
      WebSocket: server.WebSocket,
    });
    const client = createClient(adapter, { maxReconnectAttempts: 0 });
    await expect(client.connect()).rejects.toMatchObject({ code: 'AUTH_ERROR' });
    expect(server.sockets).toHaveLength(0);
    expect(client.status).toBe('closed');
  });

  it('throws NO_WEBSOCKET when no implementation is available', () => {
    const original = (globalThis as { WebSocket?: unknown }).WebSocket;
    (globalThis as { WebSocket?: unknown }).WebSocket = undefined;
    try {
      expect(() => appSyncEvents({ httpDomain: HTTP_DOMAIN, auth: apiKey('k') })).toThrow(RealtimeError);
    } finally {
      (globalThis as { WebSocket?: unknown }).WebSocket = original;
    }
  });
});
