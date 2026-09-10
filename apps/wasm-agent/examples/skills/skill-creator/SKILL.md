# Skill Creator

Create or revise Mina browser Skill packages that provide useful, specific
guidance without taking over unrelated work.

## Principles

- Preserve the user's requested capability, scope, provider, and authorization
  boundaries.
- Assume the Agent is already capable. Include only guidance that changes its
  decisions, avoids a real failure, or captures a non-obvious invariant.
- Make the description concise and discriminating so routing does not activate
  the Skill for unrelated requests.
- Keep `SKILL.md` focused. Put substantial conditional details in resources and
  explain when they should be read.
- Add resources only when they have a concrete reusable purpose. Do not create
  placeholder directories, duplicated documentation, or speculative scripts.
- Match prescriptiveness to risk: use fixed steps only where deviation can
  cause a concrete correctness, security, or compatibility failure.

## Browser workflow

1. Clarify only missing choices that materially change the requested Skill.
2. Read `references/mina-skill-format.md` with `read_skill_resource` before
   drafting or validating a Mina package.
3. Draft the smallest useful `skill.toml`, `SKILL.md`, and necessary resources.
4. Check that routing hints are specific, declared resources exist, paths are
   package-relative, and instructions do not claim unavailable tools.
5. Return the finished file contents. If the user provides existing writable
   workspace paths, the browser file tools may update them; do not overwrite
   unrelated files or claim that the package was installed.

The installed Skill store is management-only and read-only to the Agent. The
user completes installation through the Skills inspector's “导入目录” action.
Never attempt to bypass that boundary or represent a workspace draft as an
installed Skill.
