import { createContext, useContext, useEffect, useRef, useState, useSyncExternalStore, type ReactNode } from 'react';
import type { RealtimeClient } from '../core/client';
import type { RealtimeError } from '../core/errors';
import type { ActiveInfo, ConnectionStatus, EventHandler, SubscriptionStatus } from '../core/types';

const RealtimeContext = createContext<RealtimeClient | null>(null);

export interface RealtimeProviderProps {
  client: RealtimeClient;
  children?: ReactNode;
}

/** Makes a client available to the hooks below. Create the client once, outside render. */
export function RealtimeProvider({ client, children }: RealtimeProviderProps) {
  return <RealtimeContext.Provider value={client}>{children}</RealtimeContext.Provider>;
}

/** Returns the client passed explicitly, or the one from the nearest RealtimeProvider. */
export function useRealtimeClient(client?: RealtimeClient): RealtimeClient {
  const fromContext = useContext(RealtimeContext);
  const resolved = client ?? fromContext;
  if (!resolved) {
    throw new Error('realtime-channels: no client. Wrap your tree in <RealtimeProvider> or pass a client explicitly.');
  }
  return resolved;
}

/** Re-renders on every connection status change. */
export function useConnectionStatus(client?: RealtimeClient): ConnectionStatus {
  const c = useRealtimeClient(client);
  return useSyncExternalStore(
    (onChange) => c.onStatus(() => onChange()),
    () => c.status,
    () => c.status,
  );
}

export interface UseChannelOptions {
  client?: RealtimeClient;
  onError?: (error: RealtimeError) => void;
  /**
   * Called every time the subscription becomes active. `resumed: true` follows a gap
   * (a reconnect, for example) in which events were missed: the cue to refetch.
   */
  onActive?: (info: ActiveInfo) => void;
}

export interface UseChannelResult {
  /** `idle` while `channel` is null or empty, otherwise the subscription's status. */
  status: 'idle' | Exclude<SubscriptionStatus, 'unsubscribed'>;
  /** Why the server refused the subscription, when `status` is `rejected`. */
  error: RealtimeError | undefined;
}

interface ChannelState {
  client: RealtimeClient;
  channel: string;
  status: SubscriptionStatus;
  error: RealtimeError | undefined;
}

/**
 * Subscribe to a channel for the lifetime of the component. Pass `null` to
 * pause (for example while an id is still loading). The latest `handler` is
 * always used; changing it does not resubscribe. Re-renders when the
 * subscription's status changes.
 */
export function useChannel<T = unknown>(
  channel: string | null | undefined,
  handler: EventHandler<T>,
  options: UseChannelOptions = {},
): UseChannelResult {
  const c = useRealtimeClient(options.client);
  const handlerRef = useRef(handler);
  handlerRef.current = handler;
  const onErrorRef = useRef(options.onError);
  onErrorRef.current = options.onError;
  const onActiveRef = useRef(options.onActive);
  onActiveRef.current = options.onActive;
  const [state, setState] = useState<ChannelState | null>(null);

  useEffect(() => {
    if (!channel) return;
    const subscription = c.subscribe<T>(channel, (event, meta) => handlerRef.current(event, meta), {
      onError: (error) => onErrorRef.current?.(error),
      onActive: (info) => onActiveRef.current?.(info),
    });
    const update = (status: SubscriptionStatus, error?: RealtimeError): void =>
      setState((prev) =>
        prev !== null && prev.client === c && prev.channel === channel && prev.status === status && prev.error === error
          ? prev
          : { client: c, channel, status, error },
      );
    update(subscription.status);
    const stop = subscription.onStatus(update);
    return () => {
      stop();
      subscription.unsubscribe();
    };
  }, [c, channel]);

  if (!channel) return { status: 'idle', error: undefined };
  // Until this client and channel's effect has run, the stored state belongs to the previous subscription.
  if (state === null || state.client !== c || state.channel !== channel || state.status === 'unsubscribed') {
    return { status: 'pending', error: undefined };
  }
  return { status: state.status, error: state.error };
}
