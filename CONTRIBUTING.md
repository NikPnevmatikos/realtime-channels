# Contributing

Thanks for helping. This is a small library with a strict scope; the notes below keep it that way.

## Scope

- **In:** the channel client core, transport adapters, the React hooks, docs and examples.
- **Out:** presence, message history, offline queues, product-specific channel catalogues. These differ per transport and belong in the application or in a separate package built on top.
- **Zero runtime dependencies.** A wrapper adapter around an official SDK is welcome, but the SDK must be an optional peer dependency imported only inside that adapter's entry point.

## Setup

```bash
pnpm install        # esbuild's install script is allow-listed in pnpm-workspace.yaml
pnpm typecheck
pnpm test           # vitest against in-memory fakes; no cloud account needed
pnpm build          # dist/ (ESM, CJS, d.ts) + browser bundle used by examples/browser
```

Node 22+ for development (pnpm 11 requires it); CI runs 22 and 24. The published library runs on Node 20+ when you pass a `WebSocket` implementation, and on Node 22+ with the global one.

## Adding a transport adapter

Read [docs/writing-an-adapter.md](./docs/writing-an-adapter.md) first. It is the contract the core relies on
(one `onClose` per established connection, `SUBSCRIBE_REJECTED` semantics, per-operation auth, no leaked timers).
Then:

1. `src/<transport>/index.ts` exporting a factory that returns an `Adapter`.
2. `test/fake-<transport>.ts` + `test/<transport>.test.ts` covering the behaviours listed in the guide.
3. Entry in `tsup.config.ts`, subpath in `package.json` → `exports`.
4. Row in the README **Adapters** table, line in `CHANGELOG.md` under Unreleased.
5. Optional: `examples/…` against the real service.

Open an issue with the **Adapter proposal** template before starting a big one, so we can agree on the option names.

## Pull requests

- One topic per PR. Keep refactors separate from behaviour changes.
- `pnpm typecheck && pnpm test && pnpm build` must pass; CI runs the same.
- Public API changes need a README update and a CHANGELOG line.
- Tests use fake servers and fake timers; do not add tests that call real services.
- Fill in the PR template; it doubles as the review checklist.

## Style

- TypeScript strict, `exactOptionalPropertyTypes` on. Prefer structural types over DOM/Node types on the public surface.
- Stable error codes (`RealtimeErrorCode`) instead of message matching. Add new codes to `src/core/errors.ts`.
- No default exports. Small files named after what they contain.

## Releases

Semantic versioning. `CHANGELOG.md` is updated with every user-visible change under **Unreleased**;
a release moves that section under a version heading and tags `vX.Y.Z`. Publishing runs `typecheck`, `test`
and `build` first (`prepublishOnly`).

## Code of conduct

Be kind, assume good faith, keep feedback about the code. Unacceptable behaviour can be reported privately to the maintainer via the email on the npm package page.
