import { computeBackoffDelay, type BackoffOptions } from './backoff';
import { RealtimeError, toRealtimeError } from './errors';
import type {
  Adapter,
  AdapterConnection,
  AdapterSubscription,
  CloseInfo,
  ConnectionStatus,
  EventHandler,
  Subscription,
} from './types';

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export type Logger = (level: LogLevel, message: string, data?: unknown) => void;

export interface ClientOptions {
  backoff?: BackoffOptions;
  /** Give up and move to `closed` after this many consecutive failed reconnects. Default: never give up. */
  maxReconnectAttempts?: number;
  /** When true (default) the first subscribe() opens the connection for you. */
  autoConnect?: boolean;
  logger?: Logger;
}

export interface SubscribeOptions {
  /** Called when the server refuses the subscription or reports an error on it. */
  onError?: (error: RealtimeError) => void;
}

export interface RealtimeClient {
  readonly status: ConnectionStatus;
  /** Open the connection. Safe to call repeatedly. Resolves once the connection is open. */
  connect(): Promise<void>;
  /** Close the connection and stop reconnecting. Subscriptions are kept and resume on the next connect(). */
  close(): void;
  /** Register a channel handler. Survives reconnects until unsubscribe() is called. */
  subscribe<T = unknown>(channel: string, handler: EventHandler<T>, options?: SubscribeOptions): Subscription;
  /** Publish over the connection when the adapter supports it. */
  publish(channel: string, events: unknown[]): Promise<void>;
  onStatus(listener: (status: ConnectionStatus, info?: CloseInfo) => void): () => void;
  onError(listener: (error: RealtimeError) => void): () => void;
}

interface Entry {
  id: number;
  channel: string;
  handler: EventHandler<unknown>;
  onError: ((error: RealtimeError) => void) | undefined;
  adapterSub: AdapterSubscription | undefined;
  readySettled: boolean;
  resolveReady: () => void;
  rejectReady: (error: unknown) => void;
}

const noopLogger: Logger = () => {};

export function createClient(adapter: Adapter, options: ClientOptions = {}): RealtimeClient {
  const maxReconnectAttempts = options.maxReconnectAttempts ?? Number.POSITIVE_INFINITY;
  const autoConnect = options.autoConnect ?? true;
  const log = options.logger ?? noopLogger;

  let status: ConnectionStatus = 'idle';
  let connection: AdapterConnection | null = null;
  let connectPromise: Promise<void> | null = null;
  let closedByUser = false;
  /** Bumped on every connect() and close(); callbacks from older generations are ignored. */
  let generation = 0;
  let reconnectAttempts = 0;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  const entries = new Map<number, Entry>();
  let nextEntryId = 1;
  const statusListeners = new Set<(status: ConnectionStatus, info?: CloseInfo) => void>();
  const errorListeners = new Set<(error: RealtimeError) => void>();

  function setStatus(next: ConnectionStatus, info?: CloseInfo): void {
    if (status === next) return;
    status = next;
    log('debug', `status → ${next}`, info);
    for (const listener of statusListeners) {
      try {
        listener(next, info);
      } catch (err) {
        log('error', 'status listener threw', err);
      }
    }
  }

  function emitError(error: RealtimeError): void {
    log('error', error.message, error);
    for (const listener of errorListeners) {
      try {
        listener(error);
      } catch (err) {
        log('error', 'error listener threw', err);
      }
    }
  }

  async function attach(entry: Entry, conn: AdapterConnection, gen: number): Promise<void> {
    try {
      const sub = await conn.subscribe(
        entry.channel,
        (event) => {
          if (!entries.has(entry.id)) return;
          try {
            entry.handler(event, { channel: entry.channel, receivedAt: Date.now() });
          } catch (err) {
            emitError(new RealtimeError(`Handler for "${entry.channel}" threw`, 'HANDLER_ERROR', err));
          }
        },
        (error) => {
          entry.onError?.(error);
          emitError(error);
        },
      );
      if (gen !== generation || !entries.has(entry.id)) {
        // Unsubscribed (or reconnected) while the ack was in flight.
        sub.unsubscribe();
        return;
      }
      entry.adapterSub = sub;
      if (!entry.readySettled) {
        entry.readySettled = true;
        entry.resolveReady();
      }
    } catch (err) {
      const error = toRealtimeError(err, 'SUBSCRIBE_FAILED', `Subscribe to "${entry.channel}" failed`);
      if (error.code === 'SUBSCRIBE_REJECTED') {
        // The server said no. Retrying on every reconnect would only spam it.
        entries.delete(entry.id);
      }
      if (!entry.readySettled) {
        entry.readySettled = true;
        entry.rejectReady(error);
      }
      entry.onError?.(error);
      emitError(error);
    }
  }

  function clearReconnectTimer(): void {
    if (reconnectTimer !== null) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  }

  function handleClose(gen: number, info: CloseInfo): void {
    if (gen !== generation) return; // stale connection
    connection = null;
    for (const entry of entries.values()) entry.adapterSub = undefined;

    if (closedByUser || info.intentional) {
      setStatus('closed', info);
      return;
    }
    log('warn', 'connection lost', info);
    scheduleReconnect(info);
  }

  function scheduleReconnect(info: CloseInfo): void {
    if (closedByUser) return;
    reconnectAttempts += 1;
    if (reconnectAttempts > maxReconnectAttempts) {
      setStatus('closed', info);
      emitError(
        new RealtimeError(`Gave up reconnecting after ${maxReconnectAttempts} attempts`, 'RECONNECT_GIVE_UP', info.error),
      );
      return;
    }
    const delay = computeBackoffDelay(reconnectAttempts, options.backoff);
    log('info', `reconnecting in ${delay} ms (attempt ${reconnectAttempts})`);
    setStatus('reconnecting', info);
    clearReconnectTimer();
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      void connect().catch(() => {
        /* failure already reported through onError and status */
      });
    }, delay);
  }

  function connect(): Promise<void> {
    if (connection !== null && status === 'open') return Promise.resolve();
    if (connectPromise !== null) return connectPromise;

    closedByUser = false;
    clearReconnectTimer();
    if (status !== 'reconnecting') setStatus('connecting');
    const gen = ++generation;

    connectPromise = (async () => {
      try {
        const conn = await adapter.connect({ onClose: (info) => handleClose(gen, info) });
        if (gen !== generation || closedByUser) {
          conn.close();
          return;
        }
        connection = conn;
        reconnectAttempts = 0;
        setStatus('open');
        await Promise.all([...entries.values()].map((entry) => attach(entry, conn, gen)));
      } catch (err) {
        if (closedByUser || gen !== generation) return;
        const error = toRealtimeError(err, 'CONNECT_FAILED', 'Connection failed');
        emitError(error);
        scheduleReconnect({ intentional: false, error: err });
        throw error;
      } finally {
        connectPromise = null;
      }
    })();
    return connectPromise;
  }

  function close(): void {
    closedByUser = true;
    generation += 1;
    clearReconnectTimer();
    connectPromise = null;
    const conn = connection;
    connection = null;
    for (const entry of entries.values()) entry.adapterSub = undefined;
    if (conn !== null) {
      try {
        conn.close();
      } catch (err) {
        log('warn', 'adapter close threw', err);
      }
    }
    setStatus('closed', { intentional: true });
  }

  function subscribe<T>(channel: string, handler: EventHandler<T>, subscribeOptions: SubscribeOptions = {}): Subscription {
    if (typeof channel !== 'string' || channel.length === 0) {
      throw new TypeError('subscribe(channel): channel must be a non-empty string');
    }
    let resolveReady!: () => void;
    let rejectReady!: (error: unknown) => void;
    const ready = new Promise<void>((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });
    // Awaiting `ready` is optional; make sure an un-awaited rejection is not an unhandled rejection.
    ready.catch(() => {});

    const entry: Entry = {
      id: nextEntryId++,
      channel,
      handler: handler as EventHandler<unknown>,
      onError: subscribeOptions.onError,
      adapterSub: undefined,
      readySettled: false,
      resolveReady,
      rejectReady,
    };
    entries.set(entry.id, entry);

    if (connection !== null && status === 'open') {
      void attach(entry, connection, generation);
    } else if (autoConnect && (status === 'idle' || status === 'closed')) {
      void connect().catch(() => {});
    }

    return {
      channel,
      ready,
      unsubscribe(): void {
        if (!entries.delete(entry.id)) return;
        const sub = entry.adapterSub;
        entry.adapterSub = undefined;
        try {
          sub?.unsubscribe();
        } catch (err) {
          log('warn', 'adapter unsubscribe threw', err);
        }
        if (!entry.readySettled) {
          entry.readySettled = true;
          entry.rejectReady(new RealtimeError(`Unsubscribed from "${channel}" before it was acknowledged`, 'UNSUBSCRIBED'));
        }
      },
    };
  }

  async function publish(channel: string, events: unknown[]): Promise<void> {
    const conn = connection;
    if (conn === null || status !== 'open') {
      throw new RealtimeError('Cannot publish: not connected', 'NOT_CONNECTED');
    }
    if (typeof conn.publish !== 'function') {
      throw new RealtimeError(`Adapter "${adapter.name}" does not support publishing`, 'NOT_SUPPORTED');
    }
    await conn.publish(channel, events);
  }

  return {
    get status() {
      return status;
    },
    connect,
    close,
    subscribe,
    publish,
    onStatus(listener) {
      statusListeners.add(listener);
      return () => {
        statusListeners.delete(listener);
      };
    },
    onError(listener) {
      errorListeners.add(listener);
      return () => {
        errorListeners.delete(listener);
      };
    },
  };
}
