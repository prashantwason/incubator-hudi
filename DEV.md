# DEV.md

Developer guide for working on Uber's Hudi fork.

## Setup

- **Java 11** (Google Java Style for new code)
- **Maven 3.x**
- Active git remotes: `origin` (`gitolite@code.uber.internal:data/hoodie_oss`), `apache` (upstream OSS at `https://github.com/apache/hudi.git`)

## Branches

Production work happens on **`release-1.2`** — the 1.2.x line that runs in Uber prod. The `master` and `oss_master` branches in this repo are **not** used; ignore them.

For new work, create a branch named `<username>/<topic>` (e.g. `pwason/fix-foo`) off `release-1.2`.

## Required Maven profiles

**Always pass `-Dspark3.3 -Dflink1.18`** to every Maven invocation. Omitting these activates the wrong profiles and produces artifacts that don't match production.

## Build

```bash
# Full build (skip tests)
mvn clean install -Dspark3.3 -Dflink1.18 -DskipTests

# Single module
mvn -pl hudi-common -am install -Dspark3.3 -Dflink1.18 -DskipTests

# Spark bundle (used by Drogon applications)
mvn -pl packaging/hudi-spark-bundle -am install -Dspark3.3 -Dflink1.18 -DskipTests
```

Or via the helper, which uses the version baked into the Drogon application files:

```bash
python3 ./scripts/check_and_build.py packaging/hudi-spark-bundle <HUDI_VERSION> 2.12
```

Skip lint/checks too if you just want a fast build:

```bash
mvn install -Dspark3.3 -Dflink1.18 -DskipTests -Dcheckstyle.skip -Drat.skip -Dscalastyle.skip
```

## Running tests

Single test class:

```bash
mvn -pl hudi-common test -Dspark3.3 -Dflink1.18 -Dtest=TestHoodieTableMetadata
```

Single test method:

```bash
mvn -pl hudi-spark-datasource/hudi-spark test -Dspark3.3 -Dflink1.18 \
    -Dtest='TestHoodieSparkSqlWriter#testCreateExternalMorPartitionedRoRtViews'
```

Whole module:

```bash
mvn -pl hudi-client/hudi-spark-client test -Dspark3.3 -Dflink1.18
```

## Drogon

Drogon is a general-purpose Spark job launcher (analogous to `spark-submit`) used at Uber. It is not Hudi-specific and is used for many kinds of Spark workloads. Each application file declares the bundle JAR, classpath, Spark config, and the variables it accepts.

### Launching

Variables are supplied as **environment-style key=value pairs placed before `drogon launch`** on the command line (not as `--var` flags):

```bash
VAR1=VAL1 VAR2=VAL2 drogon launch <path/to/app.drogon.json>
```
Check the bundle to build within the .drogon.json itself as it may be different from the Spark bundle for some applications. 

Other common arguments to drogon are:
 -c phx2/dca1/phx_cloud/etc (the cluster to run on)
 -t required argument which specifies that jars should be deployed to terrablob platform
 -d deploy the jars (required only when the bundle jar has been updated)

When targeting `phx_cloud`, also pass `X_UBER_REGION_ROUTING=cloudlake-phx` so the launch is routed to the cloudlake region:

```bash
X_UBER_REGION_ROUTING=cloudlake-phx VAR1=VAL1 drogon launch -c phx_cloud <path/to/app.drogon.json>
```

After bumping the pom version, the `HUDI_VERSION` value inside each Drogon application file is rewritten by `./scripts/update_pom_version.sh` — keep that script as the single source of truth.

## Bumping the pom version

```bash
./scripts/update_pom_version.sh
```

Auto-increments the patch component (e.g. `1.2.0.1` → `1.2.0.2`), prompts for confirmation, then rewrites every `pom.xml` and every `HUDI_VERSION` in `drogon/*.json`. Pick `e` to enter a custom version. **Do not hand-edit `pom.xml` versions** — the Drogon applications will silently drift.

## Creating a PR

Internal PRs go through Uber's `arh` tool. `gh pr create` does **not** work against the internal mirror.

**Every PR must be linked to a JIRA ticket.** Do not file a PR without one — if no ticket exists for the work, create or request one first.

```bash
# from a working branch that already contains your commits
arh feature -p release-1.2 <feature_branch_name>
arh publish --base release-1.2
```

`arh feature` creates a clean feature branch off `release-1.2` carrying your commits; `arh publish` opens the PR. For stacked work, repeat with incrementing names (e.g. `my-feature.1`, `my-feature.2`, …) — one feature branch per PR.

PRs land at https://github.com/uber-code/data-hoodie_oss/pulls.

To enumerate open PRs from inside Claude Code:

```
mcp__code-mcp__get_review_items repositories=["uber-code/data-hoodie_oss"] lifecycle_phases=["in_review"]
```

For a specific PR:

```
mcp__code-mcp__get_github_pull_request_metadata org=uber-code repo=data-hoodie_oss number=<N>
```

## Apache OSS sync

The `apache` remote points at upstream OSS. Periodically `apache/master` is fetched and **merged directly into `release-1.2`** (or another active branch) — there is no intermediate `oss_master` / `master` staging step. This is a deliberate, manually driven activity; don't initiate it unprompted.

## Useful Claude Code skills

- `/uber-dev:pr-create`, `/uber-dev:pr-update` — wrap `arh` for PR ops.
- `/uber-dev:babysit-pr` — auto-fix CI on a PR until green.
- `/uber-reviewer:ureview` — code review of local changes or a PR/diff.
- `/review`, `/review-status`, `/review-clear` — work with `// REVIEW:` / `// FIXED:` markers.

## MCP servers

Project MCP servers are declared in `.mcp.json` at the repo root. Claude Code picks it up automatically.

- **`engwiki`**, **`code-mcp`** — local stdio servers, auto-installed and launched by `aifx mcp run` on first use. No setup required beyond having `aifx` on `$PATH`.
- **`spark_sense`** — served via the MCP Gateway (URL-based), not runnable through `aifx`. Before it works, start the Cerberus tunnel in another shell:

  ```bash
  cerberus -s mcp-gateway --no-status-page
  ```

  See the [Spark Sense engwiki page](https://engwiki.uberinternal.com/pages/viewpage.action?pageId=1067485354). For Cerberus tunnel setup, troubleshooting (port conflicts, stale task ID, slow init), and the `/cerberus:start-cerberus` / `/cerberus:stop-cerberus` skills, see the [Cerberus - Guide engwiki page](https://engwiki.uberinternal.com/pages/viewpage.action?pageId=1167428244).

## Repository layout

| Module | Purpose |
| --- | --- |
| `hudi-common`, `hudi-io`, `hudi-hadoop-common` | Core libraries |
| `hudi-client/*` | Write-path engines (Spark, Flink, Java) |
| `hudi-spark-datasource/*` | Spark connector + SQL extensions |
| `hudi-flink-datasource/*` | Flink connector |
| `hudi-utilities`, `hudi-cli`, `hudi-sync` | Tooling, CLI, sync to Hive/HMS/Glue/etc. |
| `hudi-uber` | **Uber-only patches** layered on top of OSS code |
| `packaging/*` | Shaded bundle JARs (Spark, Flink, Hive, Presto, Trino, …) |
| `drogon/` | Drogon application files for Spark jobs run at Uber |
| `scripts/` | Build, release, version-bump helpers |

## Style and conventions

- Java 11, Google Java Style.
- Uber-specific commits are prefixed `[UBER]`. Keep them minimal and self-contained so they replay cleanly across OSS merges.
- **Do not add `Co-Authored-By:` trailers to commit messages** (including AI co-author lines like `Co-Authored-By: Claude …`). Author attribution stays with the human committer.
- Address inline review comments by rewriting `// REVIEW:` to `// FIXED:` once handled.
