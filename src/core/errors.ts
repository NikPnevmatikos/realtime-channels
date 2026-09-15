/**
 * Error codes are stable strings so callers can branch on them without
 * matching message text.
 */
export type RealtimeErrorCode =
  | 'NO_WEBSOCKET'
  | 'CONNECT_FAILED'
  | 'CONNECT_TIMEOUT'
  | 'CONNECTION_ERROR'
  | 'CONNECTION_CLOSED'
  | 'KEEPALIVE_TIMEOUT'
  | 'RECONNECT_GIVE_UP'
  | 'SUBSCRIBE_TIMEOUT'
  | 'SUBSCRIBE_REJECTED'
  | 'SUBSCRIBE_FAILED'
  | 'UNSUBSCRIBED'
  | 'BROADCAST_ERROR'
  | 'PUBLISH_TIMEOUT'
  | 'PUBLISH_REJECTED'
  | 'PUBLISH_FAILED'
  | 'NOT_CONNECTED'
  | 'NOT_SUPPORTED'
  | 'HANDLER_ERROR'
  | 'AUTH_ERROR';

export class RealtimeError extends Error {
  readonly code: RealtimeErrorCode;
  override readonly cause: unknown;

  constructor(message: string, code: RealtimeErrorCode, cause?: unknown) {
    super(message);
    this.name = 'RealtimeError';
    this.code = code;
    this.cause = cause;
  }
}

export function toRealtimeError(err: unknown, fallbackCode: RealtimeErrorCode, fallbackMessage: string): RealtimeError {
  if (err instanceof RealtimeError) return err;
  const message = err instanceof Error ? `${fallbackMessage}: ${err.message}` : fallbackMessage;
  return new RealtimeError(message, fallbackCode, err);
}
