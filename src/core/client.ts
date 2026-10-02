import { computeBackoffDelay, type BackoffOptions } from './backoff';
import { RealtimeError, toRealtimeError } from './errors';
import type {
  ActiveInfo,
  Adapter,
  AdapterConnection,
  AdapterSubscription,
  CloseInfo,
  ConnectionStatus,
  EventHandler,
  Subscription,
  SubscriptionStatus,
} from './types';

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export type Logger = (level: LogLevel, message: string, data?: unknown) => void;

export interface ClientOptions {
  /** Delays between reconnects, and between retries of a subscribe that failed on a live connection. */
  backoff?: BackoffOptions;
  /** Give up and move to `closed` after this many consecutive failed reconnects. Default: never give up. */
  maxReconnectAttempts?: number;
  /** When true (default) the first subscribe() opens the connection for you. */
  autoConnect?: boolean;
  logger?: Logger;
}

export interface SubscribeOptions {
  /**
   * Called when the server refuses the subscription (status `rejected`), when subscribing
   * fails on a live connection (it is retried with backoff), or when the transport reports
   * an error on it.
   */
  onError?: (error: RealtimeError) => void;
  /**
   * Called every time the subscription becomes active: after the first acknowledgement
   * (`resumed: false`) and after every re-subscribe that follows a disconnect (`resumed: true`).
   */
  onActive?: (info: ActiveInfo) => void;
}

export interface RealtimeClient {
  readonly status: ConnectionStatus;
  /**
   * Open the connection. Safe to call repeatedly. Resolves once the connection is open,
   * without waiting for subscriptions (await `Subscription.ready` for those).
   */
  connect(): Promise<void>;
  /** Close the connection and stop reconnecting. Subscriptions are kept and resume on the next connect(). */
  close(): void;
  /**
   * Register a channel handler. Survives reconnects until unsubscribe() is called.
   * Subscribers of the same channel share one server subscription.
   */
  subscribe<T = unknown>(channel: string, handler: EventHandler<T>, options?: SubscribeOptions): Subscription;
  /** Publish over the connection when the adapter supports it. */
  publish(channel: string, events: unknown[]): Promise<void>;
  onStatus(listener: (status: ConnectionStatus, info?: CloseInfo) => void): () => void;
  onError(listener: (error: RealtimeError) => void): () => void;
}

/** One subscribe() call. */
interface Listener {
  readonly handler: EventHandler<unknown>;
  readonly onError: ((error: RealtimeError) => void) | undefined;
  readonly onActive: ((info: ActiveInfo) => void) | undefined;
  readonly statusListeners: Set<(status: SubscriptionStatus, error?: RealtimeError) => void>;
  status: SubscriptionStatus;
  /** Has been active before, so the next activation is a resume. */
  wasActive: boolean;
  readySettled: boolean;
  resolveReady: () => void;
  rejectReady: (error: unknown) => void;
}

/** One adapter subscribe on one connection. `sub` is set once the server acknowledged it. */
interface Attempt {
  readonly conn: AdapterConnection;
  sub: AdapterSubscription | undefined;
}

/** The listeners of one channel. They share a single adapter subscription. */
interface Channel {
  readonly name: string;
  readonly listeners: Set<Listener>;
  /** The attempt on the current connection, in flight or live. Any other attempt is stale. */
  attempt: Attempt | undefined;
  retryAttempts: number;
  retryTimer: ReturnType<typeof setTimeout> | null;
}

const noopLogger: Logger = () => {};

/** A function, so TypeScript does not narrow the status across callbacks that can change it. */
function isActive(listener: Listener): boolean {
  return listener.status === 'active';
}

function isConnectionRejected(error: unknown): error is RealtimeError {
  return error instanceof RealtimeError && error.code === 'CONNECTION_REJECTED';
}

export function createClient(adapter: Adapter, options: ClientOptions = {}): RealtimeClient {
  const maxReconnectAttempts = options.maxReconnectAttempts ?? Number.POSITIVE_INFINITY;
  const autoConnect = options.autoConnect ?? true;
  const log = options.logger ?? noopLogger;

  let status: ConnectionStatus = 'idle';
  let connection: AdapterConnection | null = null;
  let connectPromise: Promise<void> | null = null;
  let closedByUser = false;
  /** Bumped on every connection attempt and close(); callbacks from older generations are ignored. */
  let generation = 0;
  let reconnectAttempts = 0;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  const channels = new Map<string, Channel>();
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

  /* ---- subscriber callbacks ---- */

  function notifyStatus(listener: Listener, error?: RealtimeError): void {
    for (const fn of [...listener.statusListeners]) {
      try {
        fn(listener.status, error);
      } catch (err) {
        log('error', 'subscription status listener threw', err);
      }
    }
  }

  function setListenerStatus(listener: Listener, next: SubscriptionStatus, error?: RealtimeError): void {
    if (listener.status === next) return;
    listener.status = next;
    notifyStatus(listener, error);
  }

  function callOnError(listener: Listener, error: RealtimeError): void {
    if (!listener.onError) return;
    try {
      listener.onError(error);
    } catch (err) {
      log('error', 'subscription onError threw', err);
    }
  }

  function callOnActive(listener: Listener, channel: string): void {
    const resumed = listener.wasActive;
    listener.wasActive = true;
    if (!listener.onActive) return;
    try {
      listener.onActive({ resumed });
    } catch (err) {
      emitError(new RealtimeError(`onActive for "${channel}" threw`, 'HANDLER_ERROR', err));
    }
  }

  function activate(listener: Listener, channel: string): void {
    if (listener.status === 'active') return;
    if (!listener.readySettled) {
      listener.readySettled = true;
      listener.resolveReady();
    }
    setListenerStatus(listener, 'active');
    // A status listener may have unsubscribed or closed the client in the meantime.
    if (isActive(listener)) callOnActive(listener, channel);
  }

  function deliver(channel: Channel, event: unknown): void {
    const receivedAt = Date.now();
    for (const listener of [...channel.listeners]) {
      if (!channel.listeners.has(listener)) continue; // unsubscribed by an earlier handler
      try {
        listener.handler(event, { channel: channel.name, receivedAt });
      } catch (err) {
        emitError(new RealtimeError(`Handler for "${channel.name}" threw`, 'HANDLER_ERROR', err));
      }
    }
  }

  /* ---- channels ---- */

  function clearRetry(channel: Channel): void {
    if (channel.retryTimer !== null) {
      clearTimeout(channel.retryTimer);
      channel.retryTimer = null;
    }
  }

  function dropChannel(channel: Channel): void {
    clearRetry(channel);
    channel.attempt = undefined;
    if (channels.get(channel.name) === channel) channels.delete(channel.name);
  }

  function safeUnsubscribe(sub: AdapterSubscription): void {
    try {
      sub.unsubscribe();
    } catch (err) {
      log('warn', 'adapter unsubscribe threw', err);
    }
  }

  function attach(channel: Channel, conn: AdapterConnection): void {
    if (conn !== connection || status !== 'open') return;
    if (channel.listeners.size === 0 || channel.retryTimer !== null) return;
    if (channel.attempt !== undefined && channel.attempt.conn === conn) return; // in flight or live already

    const attempt: Attempt = { conn, sub: undefined };
    channel.attempt = attempt;
    let acknowledged: Promise<AdapterSubscription>;
    try {
      acknowledged = Promise.resolve(
        conn.subscribe(
          channel.name,
          (event) => {
            if (channel.attempt === attempt) deliver(channel, event);
          },
          (error) => {
            if (channel.attempt !== attempt) return;
            for (const listener of [...channel.listeners]) callOnError(listener, error);
            emitError(error);
          },
        ),
      );
    } catch (err) {
      acknowledged = Promise.reject(err);
    }
    acknowledged.then(
      (sub) => subscribed(channel, attempt, sub),
      (err: unknown) => subscribeFailed(channel, attempt, err),
    );
  }

  function subscribed(channel: Channel, attempt: Attempt, sub: AdapterSubscription): void {
    if (channel.attempt !== attempt) {
      safeUnsubscribe(sub); // the connection went away while the ack was in flight
      return;
    }
    if (channel.listeners.size === 0) {
      dropChannel(channel); // everyone unsubscribed while the ack was in flight
      safeUnsubscribe(sub);
      return;
    }
    attempt.sub = sub;
    channel.retryAttempts = 0;
    for (const listener of [...channel.listeners]) {
      if (channel.attempt !== attempt) break; // a callback closed the client or unsubscribed everyone
      if (channel.listeners.has(listener)) activate(listener, channel.name);
    }
  }

  function subscribeFailed(channel: Channel, attempt: Attempt, err: unknown): void {
    if (channel.attempt !== attempt) return; // stale: the re-subscribe after the reconnect takes over
    channel.attempt = undefined;
    if (channel.listeners.size === 0) {
      dropChannel(channel);
      return;
    }
    const error = toRealtimeError(err, 'SUBSCRIBE_FAILED', `Subscribe to "${channel.name}" failed`);
    if (error.code === 'SUBSCRIBE_REJECTED') {
      // The server said no. Retrying would only spam it.
      rejectChannel(channel, error);
      return;
    }
    if (attempt.conn !== connection) return; // going away; the reconnect re-subscribes
    for (const listener of [...channel.listeners]) callOnError(listener, error);
    emitError(error);
    // The callbacks above may have closed the client, unsubscribed everyone or subscribed again.
    if (channels.get(channel.name) === channel && channel.attempt === undefined && attempt.conn === connection) {
      scheduleRetry(channel);
    }
  }

  function scheduleRetry(channel: Channel): void {
    channel.retryAttempts += 1;
    const delay = computeBackoffDelay(channel.retryAttempts, options.backoff);
    log('info', `retrying subscribe to "${channel.name}" in ${delay} ms (attempt ${channel.retryAttempts})`);
    channel.retryTimer = setTimeout(() => {
      channel.retryTimer = null;
      if (connection !== null) attach(channel, connection);
    }, delay);
  }

  function rejectChannel(channel: Channel, error: RealtimeError): void {
    dropChannel(channel);
    const listeners = [...channel.listeners];
    channel.listeners.clear();
    for (const listener of listeners) {
      if (!listener.readySettled) {
        listener.readySettled = true;
        listener.rejectReady(error);
      }
      setListenerStatus(listener, 'rejected', error);
      callOnError(listener, error);
    }
    emitError(error);
  }

  /** The last listener left. */
  function releaseChannel(channel: Channel): void {
    clearRetry(channel);
    const attempt = channel.attempt;
    // Ack still in flight: keep the record so subscribed() cleans up, and so a quick
    // re-subscribe reuses this request instead of racing a second one against it.
    if (attempt !== undefined && attempt.sub === undefined) return;
    dropChannel(channel);
    if (attempt?.sub !== undefined) safeUnsubscribe(attempt.sub);
  }

  /**
   * The connection is gone: forget every attempt and mark active listeners pending.
   * Listeners are notified by notifySuspended() once the client status is updated too.
   */
  function suspendChannels(): Listener[] {
    const suspended: Listener[] = [];
    for (const channel of [...channels.values()]) {
      clearRetry(channel);
      channel.attempt = undefined;
      channel.retryAttempts = 0;
      if (channel.listeners.size === 0) {
        channels.delete(channel.name);
        continue;
      }
      for (const listener of channel.listeners) {
        if (listener.status === 'active') {
          listener.status = 'pending';
          suspended.push(listener);
        }
      }
    }
    return suspended;
  }

  function notifySuspended(listeners: Listener[]): void {
    for (const listener of listeners) {
      if (listener.status === 'pending') notifyStatus(listener);
    }
  }

  /* ---- connection ---- */

  function clearReconnectTimer(): void {
    if (reconnectTimer !== null) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  }

  function safeClose(conn: AdapterConnection): void {
    try {
      conn.close();
    } catch (err) {
      log('warn', 'adapter close threw', err);
    }
  }

  function handleClose(gen: number, info: CloseInfo): void {
    if (gen !== generation) return; // stale connection
    connection = null;
    const suspended = suspendChannels();
    if (closedByUser || info.intentional) {
      setStatus('closed', info);
    } else if (isConnectionRejected(info.error)) {
      log('warn', 'connection rejected by the server', info);
      setStatus('closed', info);
      emitError(info.error);
    } else {
      log('warn', 'connection lost', info);
      scheduleReconnect(info);
    }
    notifySuspended(suspended);
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
    clearReconnectTimer();
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (connectPromise !== null) return;
      void openConnection().catch(() => {
        /* failure already reported through onError and status */
      });
    }, delay);
    // After the timer is armed, so a status listener calling close() cancels it.
    setStatus('reconnecting', info);
  }

  function connect(): Promise<void> {
    if (connection !== null && status === 'open') return Promise.resolve();
    if (connectPromise !== null) return connectPromise;
    reconnectAttempts = 0; // an explicit connect() gets a fresh retry budget, also after giving up
    return openConnection();
  }

  function openConnection(): Promise<void> {
    closedByUser = false;
    clearReconnectTimer();
    const gen = ++generation;
    const attempt = runConnect(gen);
    connectPromise = attempt;
    // After connectPromise is set, so a status listener calling connect() joins this attempt.
    if (status !== 'reconnecting') setStatus('connecting');
    return attempt;
  }

  async function runConnect(gen: number): Promise<void> {
    try {
      const conn = await callAdapterConnect(gen);
      if (gen !== generation || closedByUser) {
        safeClose(conn);
        return;
      }
      connection = conn;
      reconnectAttempts = 0;
      setStatus('open');
      // Not awaited: acks arrive on their own, and a slow one must never hold up the next reconnect.
      for (const channel of [...channels.values()]) attach(channel, conn);
    } catch (err) {
      if (closedByUser || gen !== generation) return;
      const error = toRealtimeError(err, 'CONNECT_FAILED', 'Connection failed');
      emitError(error);
      if (error.code === 'CONNECTION_REJECTED') setStatus('closed', { intentional: false, error });
      else scheduleReconnect({ intentional: false, error: err });
      throw error;
    } finally {
      if (gen === generation) connectPromise = null;
    }
  }

  /** Always asynchronous, even when the adapter throws synchronously. */
  function callAdapterConnect(gen: number): Promise<AdapterConnection> {
    try {
      return Promise.resolve(adapter.connect({ onClose: (info) => handleClose(gen, info) }));
    } catch (err) {
      return Promise.reject(err);
    }
  }

  function close(): void {
    closedByUser = true;
    generation += 1;
    clearReconnectTimer();
    connectPromise = null;
    const conn = connection;
    connection = null;
    const suspended = suspendChannels();
    if (conn !== null) safeClose(conn);
    setStatus('closed', { intentional: true });
    notifySuspended(suspended);
  }

  function subscribe<T>(name: string, handler: EventHandler<T>, subscribeOptions: SubscribeOptions = {}): Subscription {
    if (typeof name !== 'string' || name.length === 0) {
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

    const listener: Listener = {
      handler: handler as EventHandler<unknown>,
      onError: subscribeOptions.onError,
      onActive: subscribeOptions.onActive,
      statusListeners: new Set(),
      status: 'pending',
      wasActive: false,
      readySettled: false,
      resolveReady,
      rejectReady,
    };

    let found = channels.get(name);
    if (found === undefined) {
      found = { name, listeners: new Set(), attempt: undefined, retryAttempts: 0, retryTimer: null };
      channels.set(name, found);
    }
    const channel = found;
    channel.listeners.add(listener);

    if (channel.attempt?.sub !== undefined) {
      // Already live: share it. onActive runs later, never from inside subscribe().
      listener.status = 'active';
      listener.readySettled = true;
      resolveReady();
      void Promise.resolve().then(() => {
        if (listener.status === 'active') callOnActive(listener, name);
      });
    } else if (connection !== null && status === 'open') {
      attach(channel, connection);
    } else if (autoConnect && (status === 'idle' || status === 'closed')) {
      void connect().catch(() => {});
    }

    return {
      channel: name,
      ready,
      get status() {
        return listener.status;
      },
      onStatus(fn) {
        listener.statusListeners.add(fn);
        return () => {
          listener.statusListeners.delete(fn);
        };
      },
      unsubscribe(): void {
        if (!channel.listeners.delete(listener)) return;
        if (!listener.readySettled) {
          listener.readySettled = true;
          rejectReady(new RealtimeError(`Unsubscribed from "${name}" before it was acknowledged`, 'UNSUBSCRIBED'));
        }
        setListenerStatus(listener, 'unsubscribed');
        if (channel.listeners.size === 0) releaseChannel(channel);
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
