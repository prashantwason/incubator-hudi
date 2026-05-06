# AGENTS.md

Behavioral rules for AI coding agents (Claude Code, etc.) working in this repository. For build, test, Drogon, PR workflow, and version-bump details, **read [DEV.md](./DEV.md)**.

## Repository identity

This is **Uber's internal fork** of Apache Hudi, hosted at `gitolite@code.uber.internal:data/hoodie_oss` and mirrored on GitHub at https://github.com/uber-code/data-hoodie_oss/. It is **not** Apache Hudi open source — do not assume Apache `master` behavior, and do not push to any `apache/*` branch from this checkout.

## Branch rules

- All work targets **`release-1.2`** — the production branch for the 1.2.x line that runs at Uber.
- The `master` and `oss_master` branches in this repo are **not** in use. Do not branch from them, commit to them, or target them with PRs.
- When asked to "create a branch for X", use `<username>/<branch_name>` (e.g. `pwason/foo-bar`) with `release-1.2` as the parent — unless the user explicitly says otherwise.

## Non-negotiable conventions

- **Java 11.** Don't switch versions to work around an error — fix the root cause.
- **Always pass `-Dspark3.3 -Dflink1.18`** to every Maven invocation.
- **Never hand-edit `pom.xml` versions** — use `./scripts/update_pom_version.sh` so the Drogon application files stay in sync. See DEV.md.
- **Every PR must be linked to a JIRA ticket.** If no ticket exists, ask the user for one before filing — do not file a PR without a JIRA link.

## Code review markers

When the user leaves inline review comments as `// REVIEW: <text>`, address them and rewrite the line to `// FIXED: <text>`. The `/review`, `/review-status`, and `/review-clear` skills find, summarize, and clean up these markers.

## Apache OSS sync

`apache/master` is periodically merged **directly** into `release-1.2` (or another active branch) — there is no `oss_master` / `master` staging step. Do not perform this sync without explicit instruction; it requires conflict-resolution judgment.

## Things to avoid

- Branching from or pushing to `master` / `oss_master`.
- Pushing to any `apache/*` branch from this clone.
- Running Maven without `-Dspark3.3 -Dflink1.18`.
- Editing `pom.xml` versions by hand.
- Using `gh pr create` instead of `arh publish`.
- Assuming Apache OSS docs describe current behavior — `[UBER]` commits frequently diverge.
