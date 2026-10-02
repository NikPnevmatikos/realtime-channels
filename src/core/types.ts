import type { RealtimeError } from './errors';

/** Lifecycle of a client. `idle` until the first connect, `closed` after close() or when reconnecting gave up. */
export type ConnectionStatus = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed';

/**
 * Lifecycle of one subscribe() call. `pending` until the server acknowledges it and
 * again while it is re-subscribed after a disconnect, `active` while events flow,
 * `rejected` when the server refused it, `unsubscribed` after unsubscribe().
 */
export type SubscriptionStatus = 'pending' | 'active' | 'rejected' | 'unsubscribed';

export interface EventMeta {
  /** Channel the event was delivered on. */
  channel: string;
  /** Local timestamp (ms since epoch) when the event was received. */
  receivedAt: number;
}

export type EventHandler<T = unknown> = (event: T, meta: EventMeta) => void;

export interface ActiveInfo {
  /**
   * False the first time the subscription becomes active, true every time after that.
   * Events sent while it was not active were not delivered, so refetch what you show.
   */
  resumed: boolean;
}

export interface Subscription {
  readonly channel: string;
  /**
   * Resolves the first time the server acknowledges the subscription.
   * Rejects if the server refuses it (for example an authorization rule) or if
   * you unsubscribe before it was acknowledged. Awaiting it is optional.
   */
  readonly ready: Promise<void>;
  readonly status: SubscriptionStatus;
  /** Called on every status change; `error` is set when the status becomes `rejected`. Returns a remover. */
  onStatus(listener: (status: SubscriptionStatus, error?: RealtimeError) => void): () => void;
  unsubscribe(): void;
}

export interface CloseInfo {
  /** Transport close code when one exists (WebSocket close code, HTTP status, ...). */
  code?: number;
  reason?: string;
  /**
   * The last error observed on the connection, if any. A RealtimeError with code
   * CONNECTION_REJECTED means the server refused the connection for good: the client does not reconnect.
   */
  error?: unknown;
  /** True when the close was requested locally via close(). */
  intentional: boolean;
}

/* ------------------------------------------------------------------------ */
/* Adapter contract. Implement this to plug a new transport into the core.   */
/* ------------------------------------------------------------------------ */

export interface AdapterSubscription {
  unsubscribe(): void;
}

export interface AdapterConnection {
  /**
   * Subscribe to a channel on this live connection. The core calls this at most
   * once per channel per connection and shares the result between its subscribers.
   * Resolve once the server acknowledged the subscription; reject with a
   * RealtimeError (code SUBSCRIBE_REJECTED when the server refused, which the core
   * does not retry; any other code is retried with backoff).
   */
  subscribe(
    channel: string,
    onEvent: (event: unknown) => void,
    onError?: (error: RealtimeError) => void,
  ): Promise<AdapterSubscription>;
  /** Optional: publish events over the same connection when the transport supports it. */
  publish?(channel: string, events: unknown[]): Promise<void>;
  /** Close the connection. Must eventually trigger the onClose handler passed to connect(). */
  close(): void;
}

export interface AdapterConnectHandlers {
  /** Called exactly once when the connection ends, for any reason. */
  onClose(info: CloseInfo): void;
}

export interface Adapter {
  readonly name: string;
  /**
   * Open a fresh connection. Resolve when it is ready to accept subscriptions.
   * Reject with a RealtimeError when the connection could not be established; the
   * core retries with backoff unless the code is CONNECTION_REJECTED.
   */
  connect(handlers: AdapterConnectHandlers): Promise<AdapterConnection>;
}
