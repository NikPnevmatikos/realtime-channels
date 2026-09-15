import type { RealtimeError } from './errors';

/** Lifecycle of a client. `idle` until the first connect, `closed` after close() or when reconnecting gave up. */
export type ConnectionStatus = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed';

export interface EventMeta {
  /** Channel the event was delivered on. */
  channel: string;
  /** Local timestamp (ms since epoch) when the event was received. */
  receivedAt: number;
}

export type EventHandler<T = unknown> = (event: T, meta: EventMeta) => void;

export interface Subscription {
  readonly channel: string;
  /**
   * Resolves the first time the server acknowledges the subscription.
   * Rejects if the server refuses it (for example an authorization rule) or if
   * you unsubscribe before it was acknowledged. Awaiting it is optional.
   */
  readonly ready: Promise<void>;
  unsubscribe(): void;
}

export interface CloseInfo {
  /** Transport close code when one exists (WebSocket close code, HTTP status, ...). */
  code?: number;
  reason?: string;
  /** The last error observed on the connection, if any. */
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
   * Subscribe to a channel on this live connection.
   * Resolve once the server acknowledged the subscription; reject with a
   * RealtimeError (code SUBSCRIBE_REJECTED when the server refused).
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
   * Reject with a RealtimeError when the connection could not be established.
   */
  connect(handlers: AdapterConnectHandlers): Promise<AdapterConnection>;
}
