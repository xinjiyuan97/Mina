# Chat UI source snapshot

The two tarballs in this directory are reproducible builds of
[`xinjiyuan97/ChatUIComponent`](https://github.com/xinjiyuan97/ChatUIComponent)
at commit `46a2a02c70fc4d5da7eea0770f0ecb8d1b88718a`.

They are vendored temporarily because the repository contains the new UI while npm's
`latest` tag still points at `0.2.0`, which predates that commit. Replace the `file:`
dependencies in `apps/web/package.json` with the next published npm versions once the
upstream changesets release is available.
