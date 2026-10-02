// @vitest-environment happy-dom
import { act, type ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createClient, RealtimeError, type RealtimeClient } from '../src/core';
import { RealtimeProvider, useChannel, useConnectionStatus } from '../src/react';
import { FakeAdapter } from './fake-adapter';

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const clients: RealtimeClient[] = [];
const roots: Array<() => void> = [];

function setup() {
  const adapter = new FakeAdapter();
  const client = createClient(adapter, { backoff: { initialMs: 5, maxMs: 5, jitter: 0 } });
  clients.push(client);
  return { adapter, client };
}

/** Renders `hook` inside a component and exposes its latest return value. */
function renderHook<P, R>(hook: (props: P) => R, initialProps: P, wrapper?: (children: ReactNode) => ReactNode) {
  const result = { current: undefined as R };
  function Probe({ props }: { props: P }) {
    result.current = hook(props);
    return null;
  }
  const root = createRoot(document.createElement('div'));
  const render = (props: P) => {
    const element = <Probe props={props} />;
    act(() => root.render(wrapper ? wrapper(element) : element));
  };
  render(initialProps);
  let mounted = true;
  const unmount = () => {
    if (!mounted) return;
    mounted = false;
    act(() => root.unmount());
  };
  roots.push(unmount);
  return { result, rerender: render, unmount };
}

/** Let promise callbacks (connects, acks) run inside act(). */
const flushAsync = () => act(async () => {});

afterEach(() => {
  for (const unmount of roots.splice(0)) unmount();
  for (const client of clients.splice(0)) client.close();
});

describe('useConnectionStatus', () => {
  it('follows the client status', async () => {
    const { adapter, client } = setup();
    const { result } = renderHook(() => useConnectionStatus(client), undefined);
    expect(result.current).toBe('idle');

    await act(() => client.connect());
    expect(result.current).toBe('open');

    act(() => adapter.last.drop());
    expect(result.current).toBe('reconnecting');

    act(() => client.close());
    expect(result.current).toBe('closed');
  });
});

describe('useChannel', () => {
  it('subscribes on mount, reports its status and unsubscribes on unmount', async () => {
    const { adapter, client } = setup();
    adapter.subscribeReply = () => 'hold';
    const events: unknown[] = [];
    const { result, unmount } = renderHook(() => useChannel('room', (e) => events.push(e), { client }), undefined);
    expect(result.current).toEqual({ status: 'pending', error: undefined });

    await flushAsync();
    expect(adapter.last.subscribeCalls).toEqual(['room']);
    expect(result.current.status).toBe('pending');

    await act(async () => adapter.last.held[0]!.ack());
    expect(result.current).toEqual({ status: 'active', error: undefined });

    act(() => adapter.last.emit('room', { n: 1 }));
    expect(events).toEqual([{ n: 1 }]);

    unmount();
    expect(adapter.last.unsubscribeCalls).toEqual(['room']);
  });

  it('uses the latest handler without resubscribing', async () => {
    const { adapter, client } = setup();
    const first = vi.fn();
    const second = vi.fn();
    const { rerender } = renderHook((handler: (e: unknown) => void) => useChannel('room', handler, { client }), first);
    await flushAsync();

    rerender(second);
    act(() => adapter.last.emit('room', 1));
    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalledWith(1, expect.objectContaining({ channel: 'room' }));
    expect(adapter.last.subscribeCalls).toEqual(['room']);
  });

  it('is idle for null and moves the subscription when the channel changes', async () => {
    const { adapter, client } = setup();
    const { result, rerender } = renderHook(
      (channel: string | null) => useChannel(channel, () => {}, { client }),
      null as string | null,
    );
    await flushAsync();
    expect(result.current.status).toBe('idle');
    expect(adapter.connections).toHaveLength(0);

    rerender('a');
    expect(result.current.status).toBe('pending'); // never the previous channel's status
    await flushAsync();
    expect(result.current.status).toBe('active');

    rerender('b');
    expect(result.current.status).toBe('pending');
    await flushAsync();
    expect(result.current.status).toBe('active');
    expect(adapter.last.subscribeCalls).toEqual(['a', 'b']);
    expect(adapter.last.unsubscribeCalls).toEqual(['a']);

    rerender(null);
    expect(result.current.status).toBe('idle');
    expect(adapter.last.unsubscribeCalls).toEqual(['a', 'b']);
  });

  it('reports a refused channel as rejected, with the error', async () => {
    const { adapter, client } = setup();
    adapter.subscribeReply = () => new RealtimeError('not yours', 'SUBSCRIBE_REJECTED');
    const onError = vi.fn();
    const { result } = renderHook(() => useChannel('secret', () => {}, { client, onError }), undefined);
    await flushAsync();
    expect(result.current.status).toBe('rejected');
    expect(result.current.error?.code).toBe('SUBSCRIBE_REJECTED');
    expect(onError).toHaveBeenCalledTimes(1);
  });

  it('calls onActive when active, then with resumed: true after a reconnect', async () => {
    const { adapter, client } = setup();
    const onActive = vi.fn();
    const { result } = renderHook(() => useChannel('room', () => {}, { client, onActive }), undefined);
    await flushAsync();
    expect(onActive).toHaveBeenLastCalledWith({ resumed: false });

    act(() => adapter.last.drop());
    expect(result.current.status).toBe('pending');

    await act(() => new Promise((resolve) => setTimeout(resolve, 20))); // backoff is 5 ms
    expect(result.current.status).toBe('active');
    expect(onActive).toHaveBeenCalledTimes(2);
    expect(onActive).toHaveBeenLastCalledWith({ resumed: true });
  });

  it('takes the client from RealtimeProvider', async () => {
    const { adapter, client } = setup();
    const { result } = renderHook(
      () => useChannel('room', () => {}),
      undefined,
      (children) => <RealtimeProvider client={client}>{children}</RealtimeProvider>,
    );
    await flushAsync();
    expect(result.current.status).toBe('active');
    expect(adapter.last.subscribeCalls).toEqual(['room']);
  });
});
