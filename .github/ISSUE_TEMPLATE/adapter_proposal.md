---
name: Adapter proposal
about: Propose a new transport adapter before building it
labels: adapter
---

## Transport

<!-- Name and link to its protocol documentation. -->

## Why it fits

<!-- One paragraph: who uses it, why the existing adapters do not cover it. -->

## Protocol summary

- Connection / handshake:
- How subscribe is acknowledged (or not):
- How the server refuses a subscription:
- Keep-alive / liveness:
- Publish over the same connection: yes / no
- Auth: where the token goes (URL, header, first message, per subscribe)

## Proposed options

```ts
myTransport({
  // …
})
```

## Dependencies

- [ ] None (implements the protocol directly)
- [ ] Wraps an official SDK: `<package>` as an optional peer dependency

## Will you implement it?

- [ ] Yes, I will open the PR
- [ ] No, looking for someone to pick it up
