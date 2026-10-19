import {
  HubConnectionBuilder,
  HubConnectionState,
  JsonHubProtocol,
  MessageType,
  type CloseMessage,
  type HubMessage,
  type IHttpConnectionOptions,
  type ILogger,
  type LogLevel,
} from '@microsoft/signalr';
import { RealtimeError, type RealtimeErrorCode } from '../core/errors';
import type { AdapterConnectHandlers, AdapterConnection, AdapterSubscription, CloseInfo } from '../core/types';
import { withTimeout } from '../shared/timeout';

export interface SignalRMethods {
  /** Hub method the client invokes to join a channel. The completion is the acknowledgement; an error refuses it. */
  subscribe: string;
  /** Hub method the client sends (without waiting) to leave a channel. */
  unsubscribe: string;
  /** Hub method the client invokes to publish `(channel, events)`. */
  publish: string;
  /** Client method the hub calls to deliver `(channel, event)`. */
  event: string;
}

export type AccessTokenProvider = () => string | null | undefined | Promise<string | null | undefined>;

export interface ResolvedConfig {
  url: string;
  /** Passed to the SDK as they are; the token and the logger are set separately. */
  http: Omit<IHttpConnectionOptions, 'accessTokenFactory' | 'logger'>;
  accessToken: AccessTokenProvider | undefined;
  logger: ILogger | LogLevel;
  methods: SignalRMethods;
  connectTimeoutMs: number;
  subscribeTimeoutMs: number;
  publishTimeoutMs: number;
  keepAliveIntervalMs: number;
  serverTimeoutMs: number;
}

/**
 * The JSON hub protocol, remembering the hub's last Close message. The SDK acts on its `allowReconnect` flag
 * but never hands it on, and the core needs it to tell a refusal from a restart.
 */
class ObservedJsonProtocol extends JsonHubProtocol {
  lastClose: CloseMessage | undefined;

  override parseMessages(input: string, logger: ILogger): HubMessage[] {
    const messages = super.parseMessages(input, logger);
    for (const message of messages) {
      if (message.type === MessageType.Close) this.lastClose = message;
    }
    return messages;
  }
}

function messageOf(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Why start() failed, in the core's terms. Only CONNECTION_REJECTED stops the core from trying again. */
function startError(err: unknown, tokenError: unknown): RealtimeError {
  if (err instanceof RealtimeError) return err;
  if (tokenError !== undefined) return new RealtimeError('SignalR access token provider failed', 'AUTH_ERROR', tokenError);
  const message = messageOf(err);
  // The SDK folds the status of a refused negotiate into its message ("…: Unauthorized: Status code '401'").
  if (/Status code '(401|403)'/.test(message)) {
    return new RealtimeError(`SignalR negotiate was refused: ${message}`, 'AUTH_ERROR', err);
  }
  // A hub protocol the server does not speak: trying again would fail the same way.
  if (message.startsWith('Server returned handshake error')) return new RealtimeError(message, 'CONNECTION_REJECTED', err);
  return new RealtimeError(`SignalR connection failed: ${message}`, 'CONNECT_FAILED', err);
}

/** Why an established connection ended, if not because the client asked. */
function closeReason(error: Error | undefined, close: CloseMessage | undefined): RealtimeError | undefined {
  // An error and no permission to come back: the hub refused this client for good, so the core stops.
  // Anything else (a restart, a scale-in, a dropped network) is worth reconnecting to.
  if (close?.error && close.allowReconnect !== true) {
    return new RealtimeError(`The hub closed the connection: ${close.error}`, 'CONNECTION_REJECTED', close);
  }
  if (error) return new RealtimeError(`SignalR connection lost: ${error.message}`, 'CONNECTION_ERROR', error);
  return undefined;
}

/**
 * Starts one HubConnection with the SDK's own reconnect off: reconnecting, backoff and joining the channels
 * again belong to the core. Resolves after the handshake.
 */
export async function openSignalRConnection(cfg: ResolvedConfig, handlers: AdapterConnectHandlers): Promise<AdapterConnection> {
  const protocol = new ObservedJsonProtocol();
  let tokenPending = false;
  let tokenError: unknown;

  const http: IHttpConnectionOptions = { ...cfg.http };
  const provider = cfg.accessToken;
  if (provider) {
    // Called for negotiate and again for the transport, on every connection, so refreshed tokens are picked up.
    http.accessTokenFactory = async () => {
      tokenPending = true;
      try {
        return (await provider()) ?? '';
      } catch (err) {
        tokenError = err ?? new Error('access token provider failed');
        throw err;
      } finally {
        tokenPending = false;
      }
    };
  }

  const hub = new HubConnectionBuilder()
    .withUrl(cfg.url, http)
    .withHubProtocol(protocol)
    .withKeepAliveInterval(cfg.keepAliveIntervalMs)
    .withServerTimeout(cfg.serverTimeoutMs)
    .configureLogging(cfg.logger)
    .build();

  /** One handler per channel: the core subscribes once per channel and fans out itself. */
  const channels = new Map<string, (event: unknown) => void>();
  /** Subscribes sent and not answered yet, per channel. */
  const joining = new Map<string, number>();
  /** start() succeeded; from here on closes are reported through handlers.onClose. */
  let established = false;
  let closed = false;
  let intentional = false;

  hub.on(cfg.methods.event, (channel: unknown, event: unknown) => {
    if (typeof channel === 'string') channels.get(channel)?.(event);
  });

  hub.onclose((error) => {
    // Before start() succeeded the SDK reports failures through start() only, and so do we: an onClose
    // there would make the core schedule a second reconnect.
    if (!established || closed) return;
    closed = true;
    channels.clear();
    const info: CloseInfo = { intentional };
    const reason = intentional ? undefined : closeReason(error, protocol.lastClose);
    if (reason) info.error = reason;
    handlers.onClose(info);
  });

  const started = hub.start();
  try {
    // The SDK gives up on its own eventually, but a token provider that never answers would stall it forever.
    await withTimeout(started, cfg.connectTimeoutMs, () =>
      tokenPending
        ? new RealtimeError(`SignalR access token provider did not answer within ${cfg.connectTimeoutMs} ms`, 'AUTH_ERROR')
        : new RealtimeError(`SignalR: no connection within ${cfg.connectTimeoutMs} ms`, 'CONNECT_TIMEOUT'),
    );
  } catch (err) {
    hub.stop().catch(() => {});
    throw startError(err, tokenError);
  }
  established = true;

  /** The SDK rejects every waiting call when the connection goes, just before it reports the close. */
  function invokeError(err: unknown, refused: RealtimeErrorCode): RealtimeError {
    if (err instanceof RealtimeError) return err;
    if (closed || hub.state !== HubConnectionState.Connected) {
      return new RealtimeError('Connection closed', 'CONNECTION_CLOSED', err);
    }
    return new RealtimeError(messageOf(err), refused, err);
  }

  /** Fire and forget: the hub sends no completion. */
  function sendUnsubscribe(channel: string): void {
    if (hub.state !== HubConnectionState.Connected) return;
    hub.send(cfg.methods.unsubscribe, channel).catch(() => {
      /* connection going away; nothing to undo */
    });
  }

  function joined(channel: string): void {
    const left = (joining.get(channel) ?? 1) - 1;
    if (left > 0) joining.set(channel, left);
    else joining.delete(channel);
  }

  return {
    async subscribe(channel, onEvent): Promise<AdapterSubscription> {
      let gaveUp = false;
      joining.set(channel, (joining.get(channel) ?? 0) + 1);
      const accepted = hub.invoke(cfg.methods.subscribe, channel);
      accepted.then(
        () => {
          joined(channel);
          // Accepted after we gave up on it: undo it, unless the channel is active again or another try is on its way.
          if (gaveUp && !channels.has(channel) && !joining.has(channel)) sendUnsubscribe(channel);
        },
        () => joined(channel),
      );

      try {
        await withTimeout(accepted, cfg.subscribeTimeoutMs, () => {
          gaveUp = true;
          return new RealtimeError(`Timed out waiting for the hub to accept "${channel}"`, 'SUBSCRIBE_TIMEOUT');
        });
      } catch (err) {
        throw invokeError(err, 'SUBSCRIBE_REJECTED');
      }
      if (closed) throw new RealtimeError('Connection closed', 'CONNECTION_CLOSED');

      channels.set(channel, onEvent);
      return {
        unsubscribe(): void {
          if (channels.get(channel) !== onEvent) return;
          channels.delete(channel);
          sendUnsubscribe(channel);
        },
      };
    },

    async publish(channel, events): Promise<void> {
      try {
        await withTimeout(
          hub.invoke(cfg.methods.publish, channel, events),
          cfg.publishTimeoutMs,
          () => new RealtimeError(`Timed out waiting for the hub to accept a publish on "${channel}"`, 'PUBLISH_TIMEOUT'),
        );
      } catch (err) {
        throw invokeError(err, 'PUBLISH_REJECTED');
      }
    },

    close(): void {
      intentional = true;
      hub.stop().catch(() => {});
    },
  };
}
