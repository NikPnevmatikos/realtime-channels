/**
 * In-memory stand-in for an ASP.NET Core SignalR hub that follows the channel convention, for the official
 * `@microsoft/signalr` client to talk to. Implements just enough for the tests: negotiate (with redirects and
 * refusals) through an SDK `HttpClient`, the JSON handshake over a fake WebSocket, Subscribe / Unsubscribe /
 * Publish invocations with completions, Event delivery, pings and server-side closes.
 */
import { HttpClient, HttpError, HttpResponse, LogLevel, type HttpRequest } from '@microsoft/signalr';
import type { SignalROptions } from '../src/signalr';

const RS = '\x1e';

export interface HubMessage {
  type: number;
  invocationId?: string;
  target?: string;
  arguments?: unknown[];
  result?: unknown;
  error?: string;
  allowReconnect?: boolean;
}

type Listener<T> = ((event: T) => void) | null;

export interface Negotiation {
  url: string;
  headers: Record<string, string>;
}

interface CloseEventLike {
  code: number;
  reason: string;
  wasClean: boolean;
}

export class FakeHubSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState = FakeHubSocket.CONNECTING;
  onopen: Listener<unknown> = null;
  onmessage: Listener<{ data: unknown }> = null;
  onerror: Listener<unknown> = null;
  onclose: Listener<CloseEventLike> = null;

  /** The handshake request, once received. */
  handshake: unknown;
  /** Every hub message the client sent after the handshake. */
  readonly sent: HubMessage[] = [];
  readonly channels = new Set<string>();
  closedWith: { code: number; reason: string } | null = null;

  constructor(
    readonly url: string,
    /** Outside browsers the SDK sends the token as a header instead of in the URL. */
    readonly headers: Record<string, string>,
    readonly server: FakeSignalRServer,
  ) {
    server.sockets.push(this);
    queueMicrotask(() => {
      if (this.readyState !== FakeHubSocket.CONNECTING) return;
      this.readyState = FakeHubSocket.OPEN;
      this.onopen?.({});
    });
  }

  send(data: string): void {
    if (this.readyState !== FakeHubSocket.OPEN) throw new Error('FakeHubSocket: send on non-open socket');
    for (const record of data.split(RS).filter(Boolean)) {
      const message = JSON.parse(record) as HubMessage;
      if (this.handshake === undefined) {
        this.handshake = message;
        this.server.answerHandshake(this);
      } else {
        this.sent.push(message);
        this.server.handle(this, message);
      }
    }
  }

  close(code = 1000, reason = ''): void {
    if (this.readyState >= FakeHubSocket.CLOSING) return;
    this.readyState = FakeHubSocket.CLOSED;
    this.closedWith = { code, reason };
    queueMicrotask(() => this.onclose?.({ code, reason, wasClean: code === 1000 }));
  }

  /* ---- server side helpers ---- */

  /** Sends raw text as one frame. */
  serverSendRaw(text: string): void {
    if (this.readyState !== FakeHubSocket.OPEN) return;
    this.onmessage?.({ data: text });
  }

  serverSend(...messages: HubMessage[]): void {
    this.serverSendRaw(messages.map((m) => JSON.stringify(m) + RS).join(''));
  }

  /** Simulates the server dropping the connection. */
  serverClose(code = 1006, reason = 'abnormal'): void {
    if (this.readyState >= FakeHubSocket.CLOSING) return;
    this.readyState = FakeHubSocket.CLOSED;
    this.closedWith = { code, reason };
    this.onclose?.({ code, reason, wasClean: false });
  }

  ping(): void {
    this.serverSend({ type: 6 });
  }

  /** Delivers an event to the client, if it joined the channel. */
  emit(channel: string, event: unknown): void {
    if (!this.channels.has(channel)) return;
    this.serverSend({ type: 1, target: 'Event', arguments: [channel, event] });
  }

  sentOfTarget(target: string): HubMessage[] {
    return this.sent.filter((m) => m.type === 1 && m.target === target);
  }

  sentOfType(type: number): HubMessage[] {
    return this.sent.filter((m) => m.type === type);
  }
}

/** The SDK's HTTP client, answered by the fake server instead of the network. */
class FakeHttpClient extends HttpClient {
  constructor(private readonly server: FakeSignalRServer) {
    super();
  }

  override send(request: HttpRequest): Promise<HttpResponse> {
    return Promise.resolve(this.server.negotiate(request));
  }
}

export class FakeSignalRServer {
  readonly sockets: FakeHubSocket[] = [];
  readonly negotiations: Negotiation[] = [];
  /** HTTP status of negotiate. */
  negotiateStatus = 200;
  /** When set, the next negotiate answers with this redirect (as Azure SignalR Service does). */
  redirect: { url: string; accessToken: string } | undefined;
  /** When false the hub never answers the handshake (to test connect timeouts). */
  answerHandshakes = true;
  /** When set, the handshake is answered with this error. */
  handshakeError: string | undefined;
  /** When false the hub never answers Subscribe (to test subscribe timeouts). */
  answerSubscribes = true;
  /** Answer Subscribe after this delay (to test acknowledgements that arrive after the timeout). */
  subscribeAckDelayMs = 0;
  /** Channels whose Subscribe is answered with an error. */
  readonly denyChannels = new Set<string>();
  private connectionCounter = 0;

  /** A WebSocket constructor bound to this server instance, with the SDK's Node signature. */
  readonly WebSocket = ((server: FakeSignalRServer) =>
    class BoundSocket extends FakeHubSocket {
      constructor(url: string, _protocols?: unknown, options?: { headers?: Record<string, string> }) {
        super(url, options?.headers ?? {}, server);
      }
    })(this);

  /**
   * Options that point the SDK at this server. `WebSocket` is an option the SDK still reads but no longer
   * declares, hence the cast.
   */
  options(): Partial<SignalROptions> {
    return { httpClient: new FakeHttpClient(this), WebSocket: this.WebSocket, logger: LogLevel.None } as Partial<SignalROptions>;
  }

  negotiate(request: HttpRequest): HttpResponse {
    this.negotiations.push({ url: request.url ?? '', headers: { ...request.headers } });
    if (this.negotiateStatus < 200 || this.negotiateStatus >= 300) {
      throw new HttpError(this.negotiateStatus === 401 ? 'Unauthorized' : 'Error', this.negotiateStatus);
    }
    let body: unknown;
    if (this.redirect) {
      body = this.redirect;
      this.redirect = undefined;
    } else {
      this.connectionCounter += 1;
      body = {
        negotiateVersion: 1,
        connectionId: `conn-${this.connectionCounter}`,
        connectionToken: `token-${this.connectionCounter}`,
        availableTransports: [{ transport: 'WebSockets', transferFormats: ['Text', 'Binary'] }],
      };
    }
    return new HttpResponse(200, 'OK', JSON.stringify(body));
  }

  get last(): FakeHubSocket {
    const s = this.sockets[this.sockets.length - 1];
    if (!s) throw new Error('no socket opened yet');
    return s;
  }

  answerHandshake(socket: FakeHubSocket): void {
    if (!this.answerHandshakes) return;
    socket.serverSendRaw(JSON.stringify(this.handshakeError ? { error: this.handshakeError } : {}) + RS);
  }

  handle(socket: FakeHubSocket, message: HubMessage): void {
    if (message.type !== 1) return;
    const [channel] = message.arguments ?? [];
    const invocationId = message.invocationId ?? '';
    switch (message.target) {
      case 'Subscribe': {
        if (!this.answerSubscribes) break;
        const answer = (): void => {
          if (this.denyChannels.has(String(channel))) {
            socket.serverSend({ type: 3, invocationId, error: 'Not allowed to join this channel.' });
          } else {
            socket.channels.add(String(channel));
            socket.serverSend({ type: 3, invocationId });
          }
        };
        if (this.subscribeAckDelayMs > 0) setTimeout(answer, this.subscribeAckDelayMs);
        else answer();
        break;
      }
      case 'Unsubscribe':
        socket.channels.delete(String(channel));
        break;
      case 'Publish':
        if (channel === 'read-only') {
          socket.serverSend({ type: 3, invocationId, error: 'Publishing here is not allowed.' });
        } else {
          socket.serverSend({ type: 3, invocationId, result: null });
        }
        break;
      default:
        socket.serverSend({ type: 3, invocationId, error: `Unknown hub method '${message.target}'.` });
        break;
    }
  }
}

/** Flush pending microtasks (the fake socket answers asynchronously). */
export async function flush(times = 5): Promise<void> {
  for (let i = 0; i < times; i++) await Promise.resolve();
}
