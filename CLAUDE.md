# CLAUDE.md — instructions for AI assistants working in this repo

This file is auto-loaded by Claude Code at the start of every session in this directory. It
carries the institutional knowledge that would otherwise only live in one person's chat history —
read it before making changes, and add to it when you learn something a future session would
otherwise have to re-discover the hard way.

## What this project is

A three-phase, offline reporting tool that scopes a Snowflake → Databricks migration for Guild's
data platform. All three phases are **built and working**, not a plan:

1. **`guild.snowflakeusage`** — pulls `SNOWFLAKE.ACCOUNT_USAGE`/`ORGANIZATION_USAGE`, generates
   `output/snowflake-usage-report.html`.
2. **`guild.databricksusage`** — pulls Databricks Unity Catalog System Tables, generates
   `output/databricks-usage-report.html`.
3. **`guild.comparison`** — reads the two reports' own small JSON summaries (no live connection of
   its own) and generates `output/comparison-report.html`: side-by-side stats, a capability-parity
   matrix, and a data-driven migration-load cost projection.

Everything is Scala 2.12 / Spark 3.5.1, strongly-typed `Dataset[T]` throughout (not
`DataFrame`/dynamic `Row` access), rendered as self-contained HTML/Plotly with no server and no
templating engine — open the file in a browser, or print it to PDF.

Shared rendering primitives (`HTMLUtil`/`PlotlyUtil`/`JSUtil`) live in `guild.reportkit.dashboard`
and are used verbatim by all three phases — don't fork them per-phase.

## Quick start

```bash
# Phase 1 — Snowflake (opens a browser window for SSO the first time)
sbt "runMain guild.snowflakeusage.UsageReportRunner --account guild-aa40032 --user you@guild.com --role SNOWFLAKE_ANALYSTS --warehouse PUBLIC_WAREHOUSE --lookback-days 180"

# Phase 2 — Databricks (reads host/token from ~/.databrickscfg automatically if already configured)
sbt "runMain guild.databricksusage.DatabricksUsageReportRunner --lookback-days 180"

# Phase 3 — Comparison (run AFTER both of the above; no Spark session, no live connection)
sbt "runMain guild.comparison.ComparisonReportRunner"

# Rebuild any report from its local cache without re-querying:
sbt "runMain guild.snowflakeusage.UsageReportRunner --skip-fetch"
sbt "runMain guild.databricksusage.DatabricksUsageReportRunner --skip-fetch"
```

Every CLI flag has a matching env var — see `Config.scala` (Snowflake) / `DatabricksConfig.scala`
(Databricks). `--lookback-days` should generally **match between the two platforms** before
running Phase 3 — the comparison runner warns (doesn't fail) if they differ by more than 2 days,
but every volume metric is apples-to-oranges until they match.

Both `data/` and `output/` are gitignored — nothing pulled or generated persists in git. A fresh
clone has no cached data and no reports; you have to run the fetches yourself.

## Credentials this needs (not committed — set these up yourself)

- **Snowflake**: `--account`/`--user`/`--role`/`--warehouse` (or `SNOWFLAKE_ACCOUNT`/`SNOWFLAKE_USER`/
  `SNOWFLAKE_ROLE`/`SNOWFLAKE_WAREHOUSE`). Default auth is `externalbrowser` (SSO) — a real browser
  window pops up and **must be completed within ~120 seconds** or the JDBC driver times out; if you
  see `External browser authentication failed within timeout`, just retry, and complete the login
  promptly this time.
- **Databricks**: reads `host`/`token` straight from `~/.databrickscfg` (the file `databricks auth
  login` writes) — no flags needed if that's already configured. `--warehouse-id` is optional
  (auto-discovers a `RUNNING` SQL warehouse otherwise) but **pin it explicitly**
  (`--warehouse-id a38f7ef4fd0521d0` on this workspace) if you hit `"Databricks Default Storage
  cannot be accessed using Classic Compute"` — some newer system tables (e.g.
  `system.data_quality_monitoring`) are serverless-only, and auto-discovery doesn't always land on
  a warehouse that can reach them.
- **Real (not list-price) Databricks cost, optional**: drop a CSV at
  `data/manual/databricks-billable-usage.csv` (columns: `timestamp,dbus,dollars`, one row/day) —
  export it yourself from the Databricks **Account Console → Usage** page
  (`https://accounts.cloud.databricks.com/usage/legacy?account_id=...`). This requires
  account-admin browser/SSO access, which is a **completely different auth boundary** from the
  workspace-scoped PAT this tool otherwise uses — there is no API for this data reachable with a
  workspace token (confirmed: `databricks account budgets list`, `budget-policy list`, and
  `/api/2.0/accounts/{id}/usage/download` all 404). If the file's missing, cost reporting silently
  falls back to the list-price estimate, exactly as before this feature existed — nothing breaks.

## Core conventions — follow these, don't relitigate them

- **Strongly-typed `Dataset[T]` everywhere.** Case classes used as a Spark `Dataset[T]` element
  type must NOT be `private` — Spark's codegen can't access private members and fails at
  **runtime** (`Private member cannot be accessed from type ...SpecificSafeProjection`), not at
  compile time. This has bitten every phase at least once.
- **Every `system.*`/`ACCOUNT_USAGE` pull that scans a large table is `GROUP BY`-aggregated in
  SQL, never pulled as raw rows into Spark.** `system.access.audit` runs ~46M rows/week on the
  Databricks workspace; `QUERY_HISTORY`/`ACCESS_HISTORY` are similarly huge on the Snowflake side.
  This was a real, expensive lesson learned mid-project (Phase 1 was originally raw-row pulls,
  rewritten to SQL-side aggregation) — don't regress it when adding new pulls.
- **Verify every new/changed SQL query against real data before wiring it into the typed
  pipeline.** For Databricks, a CLI probe works (`databricks api post /api/2.0/sql/statements
  --json '{...}'`). For Snowflake, there's no equivalent CLI — write a small throwaway probe
  object (direct JDBC `Statement`, no Spark session, prints raw rows) under `src/main/scala/...`,
  run it once, confirm the numbers, then **delete it** — don't leave one-off probes in the
  codebase. (One was added and removed for this exact reason on 2026-08-12; see git history /
  `snowflake_usage_report_tool` notes if you have access to them.)
- **Every cost/estimate figure is honestly labeled, never presented as fact.** Snowflake's costs
  come from `ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY` — real billed dollars. Databricks' costs
  are list-price estimates from `system.billing.list_prices` **unless** the manual CSV above is
  present, in which case the account-wide Total Cost becomes real while Compute-Only Cost (no
  per-SKU real breakdown exists) stays an estimate — these are two independent flags
  (`costIsEstimate` vs. `computeCostIsEstimate` on `PlatformSummary`), not one. Any new cost figure
  needs the same real-vs-estimate honesty, rendered inline next to the number itself (not in a
  separate table row someone could miss).
- **Resilience over completeness for optional/newer system tables.** Databricks' AI/ML pulls
  (MLflow, Model Serving, AI Gateway, Lakehouse Monitoring, Data Classification) are each wrapped
  in `tryOrDefault` — a real access constraint on one shouldn't sink the whole report. Follow this
  pattern for anything similarly new/edition-gated (Snowflake's Enterprise-only views —
  `ACCESS_HISTORY`, `QUERY_ATTRIBUTION_HISTORY`, `TASK_HISTORY` — already do the equivalent via
  `Try`).
- **Cache single-row/scalar values, not just `Dataset[T]`s.** A Parquet round-trip for one row is
  overhead; use the `cacheScalar`/`loadScalar` JSON-file pattern in `DatabricksUsageReportRunner`
  instead. Forgetting to cache a scalar value at all is a real bug that happened twice
  (`overviewStats`/`jobRunTotals` silently reset to zero under `--skip-fetch` until fixed) — any
  new single-row pull needs this from the start, not as an afterthought.
- **No defensive/speculative code for scenarios that can't happen; comments only where the WHY is
  non-obvious** (a hidden constraint, a workaround for a specific real bug) — this codebase follows
  that discipline throughout; keep doing so.

## Real, non-obvious gotchas (things that cost real debugging time)

- **Scala 2.12 `s"""..."""` interpolated triple-quoted strings process backslash escapes**
  (`\\` → `\`), unlike a plain non-interpolated `"""..."""` which is fully raw. A regex built this
  way silently lost a backslash at runtime. If you're building SQL/regex inside an `s"""..."""`,
  double-check the escaping with a length check (`s"""^\\[foo""".length` vs. `"""^\\[foo""".length`).
- **A literal `\"` immediately after an `s"..."` interpolator's opening quote can produce a bogus
  `not found: value ...` compile error** in this Scala/sbt setup — root cause not fully diagnosed;
  workaround is to rephrase the string to avoid embedded literal double-quotes.
- **Databricks CLI/Scallop flag names have no tool-name prefix** — `warehouseId` (the `val` name)
  becomes `--warehouse-id`, not `--databricks-warehouse-id`. Easy to guess wrong.
- **`SHOW STREAMS`/`SHOW SHARES` (Snowflake) return all-VARCHAR columns** — booleans like `stale`
  come back as the literal string `"true"`/`"false"`, not a JDBC boolean. Same pattern on the
  Databricks side for `is_success` in `LOGIN_HISTORY` (`'YES'`/`'NO'` strings).
- **Log4j "Unrecognized format specifier"/`StatusLogger` noise on every Databricks JDBC run** —
  this is real but harmless driver-internal logging noise, not a sign of failure. Don't chase it.
- **`--skip-fetch` doesn't validate that `--lookback-days` matches what the cache was actually
  fetched at.** `loadFromCache` trusts whatever `--lookback-days` this invocation passes/defaults
  to for labeling purposes (e.g. the Overview's "Day Lookback Window" stat) — it does NOT re-derive
  it from the cached data. If you rebuild from cache without repeating the original
  `--lookback-days`, that one label can be wrong even though the underlying numbers are correct.
  Known, not yet fixed — if you fix it, persist the fetch's actual lookback alongside the rest of
  the cache (same `cacheScalar` pattern already used for other scalars) rather than trusting the
  flag.
- **Background shell commands**: if you background a long-running fetch, never trail a bare `&`
  inside a command that's already being run in the background by your own tooling — the wrapper
  shell returns (and gets reported as "done") the instant it detaches the real process, not when
  the real work finishes. Pass the actual long-running command directly as the backgrounded
  command instead. And when you do need to poll a log for completion, the success/failure pattern
  must cover every real terminal state (a fast failure looks nothing like a slow success) or the
  wait hangs forever.

## Current status / what's open

- All three phases are built and verified against real data at multiple lookback windows (30/90/180
  days on both platforms; Snowflake alone has been run at 365 days too).
- The Databricks report's full storage inventory (`DESCRIBE DETAIL` across every real table, not a
  sample) takes ~7 minutes on its own; a full 180-day Databricks fetch end-to-end takes
  ~7.5 minutes total — cheaper than it looks, since the storage sweep dominates regardless of
  lookback length.
- **Not yet wired into the dashboard** (query/row/loader layer exists and is verified, but not
  connected to `Aggregates`/`Aggregations`/`UsageReportRunner`/`UsageDashboard` on the Snowflake
  side): `Queries.authFactorBreakdown` (real auth-method breakdown — found this account's logins
  are 97.8% key-pair auth, with **zero recorded MFA usage** over 180 days, worth flagging) and
  `Queries.replicationGroupUsage` (confirmed 0 rows — no cross-region DR replication configured).
  Wire these in next time a full refetch happens anyway; don't burn a fresh multi-hour refetch just
  to backfill these two fields in isolation.
- The Snowflake-side 365-day scale-up is the one deliberately deprioritized/parked item — not
  urgent, revisit if a full year of Snowflake history is actually needed for something.
- `output/snowflake-source-questionnaire.txt` is a filled-in answer sheet for a real external
  migration-scoping questionnaire, tagged `[REAL]`/`[PARTIAL]`/`[NOT COVERED]` per answer — useful
  both as a real deliverable and as a map of what this tool can/can't currently answer about the
  Snowflake environment (sources, tools connecting, transformation pattern, consumption, auth,
  network, DR). Regenerate/update it the same way if asked similar migration-scoping questions
  again — pull real signal from what's already fetched before answering from general knowledge.

## Porting to Python (this codebase is Scala/Spark by circumstance, not requirement)

Most people on this team work in Python — Scala/Spark here just matched the precedent of an
existing dashboard generator elsewhere in the org. If this gets ported, the single most important
thing to know: **Spark is not load-bearing.** Every heavy `system.*`/`ACCOUNT_USAGE` pull is
already `GROUP BY`-aggregated in SQL before it reaches Spark (see "Core conventions" above) — what
Spark actually processes afterward is small (bounded by days × warehouses/SKUs/users, not by raw
event volume). A Python port doesn't need PySpark or any distributed engine at all; `pandas`/
`polars`, or even plain lists of `dataclass` instances, are enough for every `groupByKey`/
`mapGroups`/`filter`/`map` in this codebase today.

**Ports over almost as-is (already language-agnostic):**
- Every SQL query in each `queries/Queries.scala` — plain strings, copy verbatim.
- The real findings/gotchas in this file and in code comments — data facts, not Scala facts.
- Hand-authored reference data (`KNOWN_NETWORK_POLICIES` IP-CIDR list, `CAPABILITY_MATRIX`) — plain
  data, not logic.
- The two `PlatformSummary` JSON files' schema (Phase 3's entire contract) — a Python `dataclass`/
  `TypedDict` + `json.dump`/`json.load` reproduces it directly.
- The self-contained HTML + embedded-Plotly-JS rendering (`HTMLUtil`/`PlotlyUtil`/`JSUtil`) —
  deliberately just string templating, no templating engine or server; f-strings do the same job.
  (Switching to real `plotly.graph_objects` + `fig.to_html()` is a reasonable design choice too,
  just not a requirement the current charts impose.)
- The parallelized Databricks storage sweep (`DatabricksJdbc.loadAllTableSizes`) — maps directly to
  `concurrent.futures.ThreadPoolExecutor` + one DB connection per worker (a single Python DB-API
  connection isn't thread-safe either, same constraint as the Scala `Connection`).

**Needs a real Python equivalent, not a transliteration:**

| Scala thing | Python equivalent |
|---|---|
| `Dataset[T]` (case class) | a `dataclass`/`pydantic` model, or a `pandas`/`polars` DataFrame with a known schema |
| JDBC (`net.snowflake.client.jdbc.SnowflakeDriver`, `com.databricks.client.jdbc.Driver`) | `snowflake-connector-python` and `databricks-sql-connector` — Snowflake's official connector supports `authenticator='externalbrowser'` natively |
| Scallop (`ScallopConf`) | `argparse`/`click` + `os.environ.get(...)` for the env-var fallbacks |
| Jackson (`ObjectMapper` + `DefaultScalaModule`) | `dataclasses` + `json.dumps(dataclasses.asdict(x))` |
| Parquet caching (`ds.write.parquet(...)`) | `df.to_parquet(...)` or `pyarrow.parquet.write_table` directly |
| `Future`/`Await.result` (storage sweep) | `concurrent.futures.ThreadPoolExecutor` + `as_completed()`/`.result()` |

**Might just disappear rather than need porting:** the JDBC-driver-specific gotchas above (log4j
`StatusLogger` noise, Arrow-buffer class-loading errors, the exact "browser response timeout"
wording) are Java/JDBC-driver artifacts. The native Python connectors don't share that plumbing —
don't assume every quirk in this file needs a Python-side equivalent; re-verify each one fresh
against the new connector rather than "translating" it defensively.

**Don't lose in translation:** the conventions are the actual value here, not the Scala syntax —
verify every new/changed query against real data before wiring it in, label every cost figure real
vs. estimate inline, and keep the `GROUP BY`-at-the-source discipline. Those matter exactly as much
in Python.

## Where things live

```
src/main/scala/guild/
  reportkit/                shared: PlatformSummary (+ IO), HTMLUtil/PlotlyUtil/JSUtil
  snowflakeusage/           Phase 1 — Config, SnowflakeJdbc, queries/{Queries,Rows},
                            aggregate/{Aggregates,Aggregations}, dashboard/UsageDashboard,
                            UsageReportRunner (entry point)
  databricksusage/          Phase 2 — same shape: DatabricksConfig, DatabricksJdbc,
                            ManualBillableUsage (real-cost CSV overlay), queries/, aggregate/,
                            dashboard/, DatabricksUsageReportRunner (entry point)
  comparison/               Phase 3 — ComparisonAggregations, ComparisonDashboard,
                            ComparisonReportRunner (entry point; no Spark session)
output/                     generated HTML reports + the two PlatformSummary JSON files
                            (gitignored — regenerate, don't expect these in a fresh clone)
data/raw/, data/raw-databricks/   Parquet/JSON cache per platform (gitignored)
data/manual/                hand-placed inputs — currently just the optional real-cost CSV
```

See `README.md` for the human-facing "what does this do and how do I run it" version;
this file is the "how to work on it without repeating past mistakes" version.
