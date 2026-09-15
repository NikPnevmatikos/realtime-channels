# Expo example

A subscribe-only React Native screen for AWS AppSync Events using `realtime-channels`.
Runs in **Expo Go**: the library uses React Native's built-in `WebSocket`, no native modules.

## Run

```bash
# 1. build the library once (this example links it from the repository root)
cd ../..
pnpm install && pnpm build

# 2. install and start the example
cd examples/expo
npm install
npx expo start
```

Scan the QR code with Expo Go, then:

1. Enter your Event API HTTP domain (`your-api-id.appsync-api.<region>.amazonaws.com`).
2. Pick the auth mode and paste a token. For Cognito the channel is prefilled with `users/<sub>` from the token.
3. **Connect & subscribe**, then publish to that channel from your backend. Events show up with their
   latency when the payload carries `sentAtUtc` (ISO 8601).
4. **Subscribe users/aaa** should be rejected by your `onSubscribe` handler → `SUBSCRIBE_REJECTED`.
5. Send the app to the background and back: the screen reconnects on `AppState` becoming `active`.

## What to look at

- `App.tsx` builds the client with `createClient(appSyncEvents({ httpDomain, auth }))` and hands it to
  `RealtimeProvider`; the `Connected` component uses `useChannel` and `useConnectionStatus`.
- `metro.config.js` is only there because the library is linked with `file:../..`. When you install
  `realtime-channels` from npm you do not need it.

If you change the library, run `pnpm build` at the root and restart Metro with `npx expo start --clear`.
