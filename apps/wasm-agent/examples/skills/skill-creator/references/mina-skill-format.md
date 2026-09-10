# Mina browser Skill package format

Read this reference when creating or validating a Skill for the Mina browser
WASM runtime.

## Required layout

```text
skill-name/
├── skill.toml
├── SKILL.md
└── references/       # optional
```

`skill.toml` and `SKILL.md` must be UTF-8 and live at the package root. The
instruction file must not be empty. Resource paths are relative to that root
and may not escape it.

## Manifest

```toml
skill_id = "example-skill"
version = "1.0.0"
description = "What the Skill does and the requests that should activate it"
required_tools = ["read_skill_resource"]
optional_tools = []
required_capabilities = []
activation = { type = "routable", hints = ["specific routing phrase"] }
resources = [{ path = "references/guide.md", media_type = "text/markdown" }]
```

Supported activation types:

- `explicit_only`: selected only through an explicit host choice.
- `profile_default`: included in every new Run.
- `routable`: selected when the current user input contains one of its hints,
  using case-insensitive substring matching.

Use narrow routable hints that resemble realistic user language. Avoid generic
terms such as `file`, `code`, `write`, or `help` because they attract unrelated
requests. Use `profile_default` only for guidance that truly applies to every
Run.

## Runtime semantics

- A new Run resolves at most four Skills, compiles their instructions, and
  records exact package digests in `SkillLock`.
- A restored Run requires the same locked bytes and compiler version.
- Installed `skill_id` and `version` pairs are immutable. Changed content must
  use a new version.
- `read_skill_resource` can read only UTF-8 resources belonging to a Skill
  locked to the current Run. It cannot expose `skill.toml` or `SKILL.md`.
- Declaring a tool documents the dependency but does not create a new browser
  handler. Instructions must not assume unavailable tools.
- The package limit is 2 MiB; an individual resource read is limited to 1 MiB.

## Validation checklist

- The id uses a short lowercase, hyphenated name and the version is explicit.
- The description says both what the Skill does and when it applies.
- Instructions preserve user choices and do not broaden permission.
- Every declared resource exists and is discoverable from `SKILL.md`.
- Routing hints are specific and tested against representative prompts.
- The package contains no secrets, credentials, generated output, or unrelated
  files.
- The Agent can actually access every tool or capability claimed by the
  instructions.

Installation is performed by the user-facing Skills inspector. The Agent may
prepare package contents in its normal workspace but cannot install or mutate
the OPFS Skill store itself.
