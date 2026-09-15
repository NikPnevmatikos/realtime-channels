<!-- Thanks for the PR. Keep one topic per PR; delete the sections that do not apply. -->

## What does this change?

<!-- One or two sentences. Link the issue if there is one: Closes #… -->

## Type of change

- [ ] Bug fix
- [ ] New transport adapter
- [ ] Change to the core / public API
- [ ] Docs, examples, tooling

## Checklist

- [ ] `pnpm typecheck && pnpm test && pnpm build` pass locally
- [ ] Tests added or updated, running against an in-memory fake (no real services)
- [ ] README updated if the public API or options changed
- [ ] `CHANGELOG.md` has a line under **Unreleased**
- [ ] No new runtime dependency (SDK wrappers: optional peer dependency, imported only in the adapter entry)

## Adding a transport adapter?

The contract is in [docs/writing-an-adapter.md](../docs/writing-an-adapter.md). Confirm each point:

- [ ] `connect()` resolves only when subscriptions can be accepted; failures reject with a `RealtimeError`
- [ ] `handlers.onClose()` is called **exactly once per established connection** and **never** for a connection whose `connect()` rejected
- [ ] `onClose({ intentional: true })` only when `connection.close()` was called
- [ ] `subscribe()` rejects refused channels with code `SUBSCRIBE_REJECTED`; transient failures use `SUBSCRIBE_TIMEOUT` / `CONNECTION_CLOSED`
- [ ] Events are delivered parsed (JSON strings decoded)
- [ ] The token provider is called for every connection (and per subscribe where the protocol re-authorizes)
- [ ] All timers are cleared on close; `unsubscribe()` is safe after the connection died
- [ ] Platform objects (`WebSocket`, `EventSource`, …) are injectable via options with a structural type
- [ ] Files: `src/<transport>/index.ts`, `test/fake-<transport>.ts`, `test/<transport>.test.ts`
- [ ] Wiring: entry in `tsup.config.ts`, subpath in `package.json` `exports`, row in the README **Adapters** table

Which real service did you smoke-test against, and how? (An `examples/` folder is welcome; it is not a test.)

## Notes for the reviewer

<!-- Anything non-obvious: trade-offs, follow-ups, things you are unsure about. -->
