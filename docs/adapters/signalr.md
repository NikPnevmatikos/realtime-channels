# SignalR adapter

`realtime-channels/signalr` connects to an ASP.NET Core SignalR hub, directly or through Azure SignalR
Service. SignalR has hubs, methods and groups rather than channels, so the adapter relies on a small
convention that the hub follows.

The protocol work is done by the official client, `@microsoft/signalr` (8.0 or later), which you install
next to this package. The adapter adds the channels, and hands reconnecting to the core so that it behaves
like every other transport.

```sh
npm install realtime-channels @microsoft/signalr
```

## The convention

| Direction | Call | Meaning |
| --- | --- | --- |
| client → hub | `Subscribe(string channel)` | join the channel. Returning is the acknowledgement; throwing a `HubException` refuses the channel |
| client → hub | `Unsubscribe(string channel)` | leave the channel. Sent without waiting for an answer |
| client → hub | `Publish(string channel, JsonElement[] events)` | optional. Returning accepts, throwing refuses |
| hub → client | `Event(string channel, object payload)` | one event on a channel the client joined |

The names can be changed with the `methods` option. A channel is simply a SignalR group with the same name.

## A complete hub

```csharp
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;

[Authorize]
public sealed class ChannelsHub : Hub
{
    public async Task Subscribe(string channel)
    {
        // Refusing: the client's Subscription.ready rejects with SUBSCRIBE_REJECTED and the channel is not retried.
        if (!MayJoin(Context.User, channel))
        {
            throw new HubException("Not allowed to join this channel.");
        }

        await Groups.AddToGroupAsync(Context.ConnectionId, channel);
    }

    public Task Unsubscribe(string channel) =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, channel);

    private static bool MayJoin(System.Security.Claims.ClaimsPrincipal? user, string channel) => true;
}
```

```csharp
builder.Services.AddSignalR();
// ...
app.MapHub<ChannelsHub>("/hubs/channels");
```

Sending from anywhere in the app, e.g. a controller or a background service:

```csharp
public sealed class OrderNotifier(IHubContext<ChannelsHub> hub)
{
    public Task OrderChanged(Order order) =>
        hub.Clients.Group($"orders/{order.Id}").SendAsync("Event", $"orders/{order.Id}", new { order.Id, order.Status });
}
```

Only the message of a `HubException` reaches the client. Any other exception becomes a generic error, unless
`EnableDetailedErrors` is on.

## Authentication

`accessToken` is called for every new connection, for negotiate and again for the transport. The SDK sends the
token as `Authorization: Bearer …`, except where a browser can't set headers: on WebSockets and Server-Sent
Events it goes in the `access_token` query parameter. With JWT bearer authentication, read it from the query
string for the hub's path:

```csharp
options.Events = new JwtBearerEvents
{
    OnMessageReceived = context =>
    {
        var token = context.Request.Query["access_token"];
        if (!string.IsNullOrEmpty(token) && context.HttpContext.Request.Path.StartsWithSegments("/hubs"))
        {
            context.Token = token;
        }

        return Task.CompletedTask;
    },
};
```

A negotiate answered with 401 or 403, or a token provider that throws, is reported as `AUTH_ERROR` and retried
with a fresh token.

## From another origin

When the page and the hub are on different origins, the server's CORS policy has to allow:

- the `Authorization` header, and the two the SDK adds: `X-Requested-With` and `X-SignalR-User-Agent`;
- credentials, because the SDK sends cookies by default. If you authenticate with a token, pass
  `withCredentials: false` instead, and the policy needs no `AllowCredentials()`.

## Azure SignalR Service

Nothing to configure on the client. Negotiate answers with a redirect to the service and a token for it; the
SDK follows it. `skipNegotiation` must stay `false`.

## Reconnecting and closing

- The SDK's automatic reconnect is off. When a connection ends, the core reconnects with its backoff, joins
  each channel again and calls `onActive({ resumed: true })`. SignalR does not replay events sent in
  between, so that is the moment to refetch.
- The client sends a ping when it has sent nothing for `keepAliveIntervalMs` (15 s), as the server drops
  clients that stay silent for its `ClientTimeoutInterval` (30 s). The SDK checks this when something
  arrives, and the server pings every 15 s.
- When nothing arrives from the server for `serverTimeoutMs` (30 s), the connection is treated as dead and
  the core reconnects.
- A `Close` message **with an error and without `allowReconnect`** means the server refused the client for
  good: the client moves to `closed` with `CONNECTION_REJECTED` and does not reconnect. This is what
  ASP.NET Core sends when `OnConnectedAsync` throws. Any other close, a server restart included, is followed
  by a reconnect.
- A handshake the server refuses is final too (`CONNECTION_REJECTED`): a hub that doesn't speak JSON never will.

## What it does not do

- **JSON protocol only**, not MessagePack.
- **No client results.** If the hub calls a client method with `InvokeAsync` and waits for a result, the SDK
  answers at once with an error, so the hub doesn't wait for its timeout.
- **No streaming and no stateful reconnect.** The core's `onActive` covers what stateful reconnect would.

## When to use `@microsoft/signalr` on its own

Use the SDK directly when SignalR is your only transport and you want what lies outside channels: calling hub
methods with results, streaming, MessagePack, stateful reconnect. Use this adapter when you want channels
with the same client, React hooks, error codes and reconnect behaviour as your other transports, or a hub you
can swap for AppSync Events (or the other way round) without touching the code that subscribes.
