# External product facts

> Verified: 2026-08-24
> Sources: https://github.com/xinjiyuan97/ChatUIComponent, https://registry.npmjs.org/

## Existence and status

- Repository exists; verified HEAD: `b6023c07c53aa669cd650a406d0bc5a42ed89c54` (2026-08-19).
- Packages declared by the repository: `@agent-chat/core`, `@agent-chat/a2ui`, and `@agent-chat/ui`, all at `0.1.0` in the verified checkout.
- The three packages were not available from the public npm registry when checked; Mina consumes the repository as a pinned Git submodule/workspace dependency.

## Compatibility and license

- React peer range: React 18.2 or React 19.
- The UI package ships a precompiled stylesheet and a Next.js App Router example.
- License: MIT.

## OpenAI-compatible protocol

> Verified: 2026-08-24
> Source: https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create

- Chat Completions creates a completion with `POST /chat/completions` from a list of conversation messages.
- Mina uses this protocol for the initial broadly compatible, non-streaming adapter and normalizes its response before returning it to the Agent.
