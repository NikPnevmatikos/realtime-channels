# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0] - 2026-10-02

### Fixed

- The client could stay in `reconnecting` forever when the connection dropped while a subscription was still being set up (for example while the token provider was refreshing). A slow or stuck subscribe can no longer hold up a reconnect.
- A subscribe that failed on a live connection (timeout, token provider error) was only retried after the next disconnect, leaving the channel silently dead. It is now retried with backoff.
- `Subscription.ready` and `onError` no longer report failures caused by the connection dropping mid-subscribe. The channel is re-subscribed after the reconnect and `ready` resolves then, as documented.
- Subscribing from a status listener as the connection opened sent two subscribe requests, delivered every event twice and leaked a server-side subscription.
- Calling `close()` from a status listener on `reconnecting` did not stop the reconnect.
- `connect()` after `RECONNECT_GIVE_UP` made a single attempt; it now starts a fresh retry budget.
- `close()` during an in-flight connect followed by `connect()` could open a redundant connection.
- AppSync Events: a subscribe or publish no longer goes out on a socket that closed while its token was being fetched (this left a stray timer and a late `SUBSCRIBE_TIMEOUT`); the connect, subscribe and publish timeouts now include the token provider, so a provider that never answers cannot stall the client; a subscription the server acknowledges after its timeout is unsubscribed instead of left on the server.

### Changed

- Subscribers of the same channel share one server subscription. Adapters are called once per channel per connection.
- `connect()` resolves as soon as the connection is open, without waiting for subscriptions. Await `Subscription.ready` for those.

### Added

- `Subscription.status` (`pending`, `active`, `rejected`, `unsubscribed`) and `Subscription.onStatus(listener)`.
- `SubscribeOptions.onActive({ resumed })`, called every time a subscription becomes active. `resumed: true` follows a gap in which events may have been missed: the cue to refetch.
- `useChannel` returns `{ status, error }` and accepts `onActive`.
- Error code `CONNECTION_REJECTED`, for adapters to stop the client from reconnecting when the server refused the connection for good.
- Tests for the React hooks.

## [0.1.0] - 2026-09-15

### Added

- Core client: `createClient(adapter, options)` with reconnection (jittered exponential backoff), automatic re-subscription, `Subscription.ready`, status and error listeners, handler isolation (`HANDLER_ERROR`), `maxReconnectAttempts`.
- AWS AppSync Events adapter (`realtime-channels/appsync-events`): `aws-appsync-event-ws` protocol, subprotocol-encoded authorization, `connection_ack`-driven keep-alive watchdog, per-subscribe authorization, publish over WebSocket (1–5 events).
- Auth helpers: `cognitoUserPool`, `oidc`, `lambdaAuthorizer`, `apiKey`, `customHeaders`.
- React bindings (`realtime-channels/react`): `RealtimeProvider`, `useRealtimeClient`, `useConnectionStatus`, `useChannel`.
- Browser bundle (`dist/realtime-channels.appsync.global.js`) and examples for the browser and Expo.
- Test suite against an in-memory AppSync protocol fake.

[Unreleased]: https://github.com/NikPnevmatikos/realtime-channels/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/NikPnevmatikos/realtime-channels/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/NikPnevmatikos/realtime-channels/releases/tag/v0.1.0
