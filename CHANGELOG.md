# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-09-15

### Added

- Core client: `createClient(adapter, options)` with reconnection (jittered exponential backoff), automatic re-subscription, `Subscription.ready`, status and error listeners, handler isolation (`HANDLER_ERROR`), `maxReconnectAttempts`.
- AWS AppSync Events adapter (`realtime-channels/appsync-events`): `aws-appsync-event-ws` protocol, subprotocol-encoded authorization, `connection_ack`-driven keep-alive watchdog, per-subscribe authorization, publish over WebSocket (1–5 events).
- Auth helpers: `cognitoUserPool`, `oidc`, `lambdaAuthorizer`, `apiKey`, `customHeaders`.
- React bindings (`realtime-channels/react`): `RealtimeProvider`, `useRealtimeClient`, `useConnectionStatus`, `useChannel`.
- Browser bundle (`dist/realtime-channels.appsync.global.js`) and examples for the browser and Expo.
- Test suite against an in-memory AppSync protocol fake.

[Unreleased]: https://github.com/NikPnevmatikos/realtime-channels/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/NikPnevmatikos/realtime-channels/releases/tag/v0.1.0
