import { createContext, useContext, useEffect, useRef, useSyncExternalStore, type ReactNode } from 'react';
import type { RealtimeClient } from '../core/client';
import type { RealtimeError } from '../core/errors';
import type { ConnectionStatus, EventHandler } from '../core/types';

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
}

/**
 * Subscribe to a channel for the lifetime of the component. Pass `null` to
 * pause (for example while an id is still loading). The latest `handler` is
 * always used; changing it does not resubscribe.
 */
export function useChannel<T = unknown>(
  channel: string | null | undefined,
  handler: EventHandler<T>,
  options: UseChannelOptions = {},
): void {
  const c = useRealtimeClient(options.client);
  const handlerRef = useRef(handler);
  handlerRef.current = handler;
  const onErrorRef = useRef(options.onError);
  onErrorRef.current = options.onError;

  useEffect(() => {
    if (!channel) return;
    const subscription = c.subscribe<T>(channel, (event, meta) => handlerRef.current(event, meta), {
      onError: (error) => onErrorRef.current?.(error),
    });
    return () => subscription.unsubscribe();
  }, [c, channel]);
}
