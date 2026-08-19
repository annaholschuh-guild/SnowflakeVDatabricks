package guild.databricksusage.queries


/** Typed rows for each `system.*` pull. Every pull here is pre-aggregated (`GROUP BY` in
  * Databricks SQL), the same discipline the Snowflake tool adopted in its own Phase 1 rewrite —
  * `system.access.audit` alone runs ~46M rows/week on this workspace, so nothing here is ever
  * pulled raw. See [[Queries]] for the SQL that produces each row and
  * [[guild.databricksusage.DatabricksJdbc]] for how the JDBC read is cast to match.
  */
/** One row per day, USD estimated from `usage_quantity * list_prices.pricing.default` joined on
  * the price effective at `usage_start_time` — the standard cost-estimation join Databricks'
  * own system-tables docs recommend, the closest analog to Snowflake's
  * `ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY`. Unlike Snowflake's real-billed-dollars view,
  * this is a list-price estimate — it won't reflect negotiated discounts.
  */
case class DailyCostRow(day: String, usdAmount: Double)

/** Total cost and raw DBU quantity per SKU (e.g. `STANDARD_ALL_PURPOSE_COMPUTE`,
  * `SERVERLESS_SQL`) over the whole lookback window — analog to Snowflake's Cost by Usage Type.
  */
case class CostBySkuRow(skuName: String, usdAmount: Double, dbuQuantity: Double)

/** One row per (day, `billing_origin_product`) — Databricks' own coarse product grouping
  * (JOBS, SQL, ALL_PURPOSE, etc.), a level up from individual SKU.
  */
case class DailyCostByProductRow(day: String, product: String, usdAmount: Double)

/** Single-row overview stats over the whole lookback window, from `system.query.history` —
  * `activeUsers` counts distinct `executed_by` identities, person and service principal alike
  * (no user-type classification pass yet, unlike the Snowflake report's `USERS.TYPE`-based split).
  */
case class OverviewStatsRow(totalQueries: Long, activeUsers: Long)

case class DailyQueryVolumeRow(day: String, queryCount: Long, activeUsers: Long)

/** Precomputed per-day duration quartiles via `approx_percentile` — same precomputed-quartile
  * box-trace shape as the Snowflake report's `DailyDurationStatsRow`, adopted from day one here
  * rather than relearned; `total_duration_ms` is a `BIGINT` and Databricks' `approx_percentile`
  * returns the input column's own type for a single-percentile call, so these stay `Long`
  * (Snowflake's analog used `Double` — its `APPROX_PERCENTILE` always returns a fractional type).
  */
case class DailyDurationStatsRow(day: String, minMs: Long, q1Ms: Long, medianMs: Long, q3Ms: Long, maxMs: Long)

/** One row per compute endpoint (`compute.type` = `WAREHOUSE` or `SERVERLESS_COMPUTE`, so far —
  * `CLUSTER` may appear for notebook-run queries not yet seen on this workspace).
  * `warehouseId`/`clusterId` are both `None` for serverless compute, which has no fixed endpoint
  * id; display code falls back to `computeType` as the label in that case. Mirrors Snowflake's
  * per-warehouse `WarehouseExecStatsRow` — the all-queries total is the denominator for a
  * $/second-of-query-time rate, the failed-only figures price out wasted compute.
  */
case class ComputeExecStatsRow(
  computeType: String,
  warehouseId: Option[String],
  clusterId: Option[String],
  totalQueries: Long,
  totalExecMs: Long,
  failedCount: Long,
  failedExecMs: Long
)

/** Per-compute-endpoint spill/queueing signals — `spilled_local_bytes` (disk spill, an
  * undersized-compute signal) and `waiting_for_compute_duration_ms` (queueing, an
  * oversubscribed-compute signal) are query.history's direct analogs to Snowflake's
  * spilled-bytes/queued-ms resource-mismatch pair.
  */
case class ComputeResourceSignalRow(
  computeType: String,
  warehouseId: Option[String],
  clusterId: Option[String],
  queryCount: Long,
  queriesWithSpill: Long,
  totalSpilledBytes: Long,
  queriesWithQueueing: Long,
  totalQueuedMs: Long
)

/** `errorClass` comes from the leading `[BRACKETED_ERROR_CLASS]` Spark/Databricks SQL error
  * messages carry when present (e.g. `TABLE_OR_VIEW_NOT_FOUND`) — falls back to `"OTHER"` for
  * messages without that prefix (e.g. plain `PERMISSION_DENIED: ...` text), same
  * representative-not-first `ANY_VALUE` semantics as Snowflake's `ErrorReasonRow`.
  */
case class QueryErrorReasonRow(errorClass: String, sampleMessage: String, count: Long)

/** `system.access.audit` is `~46M rows/week` on this workspace, by far the largest table
  * probed — every pull below is `GROUP BY`-aggregated in SQL, no exceptions. `actorType` is
  * `"person"` when `user_identity.email` looks like an email address, else `"service"` (a UUID
  * service-principal application id, or the literal `"System-User"` for platform-initiated
  * actions) — see [[Queries.actorTypeExpr]].
  */
case class DailyActiveUsersRow(day: String, actorType: String, distinctUsers: Long, actionCount: Long)

/** Top individual actors by raw action count — `actor` is an email for person users, a UUID
  * service-principal application id otherwise (no SCIM/accounts-API lookup to resolve UUIDs to
  * display names yet, unlike Snowflake's `USERS` table having names built in).
  */
case class TopActorRow(actor: String, actionCount: Long)

/** `system.access.audit`'s coarse activity-type dimension (`unityCatalog`, `jobs`, `clusters`,
  * `workspace`, etc.) — the closest Databricks analog to Snowflake's client-application
  * breakdown, though it's *what kind of action*, not *what tool initiated it*.
  */
case class ServiceActionCountRow(serviceName: String, actionCount: Long)

/** One row per (direction, table), from `system.access.table_lineage` — `accessCount` is a row
  * count of lineage edge events touching that table, the closest available proxy to Snowflake's
  * `COUNT(DISTINCT query_id)` (`table_lineage` has no single stable query/statement id column
  * shared across both read and write sides the way Snowflake's unified `access_history` union
  * does). `direction` is `"READ"` (table appeared as a lineage source) or `"WRITE"` (target).
  */
case class TableLineageAccessRow(direction: String, tableName: String, accessCount: Long)

/** Current-state object counts by `table_type` (`MANAGED`/`EXTERNAL`/`VIEW`/`STREAMING_TABLE`/
  * `MATERIALIZED_VIEW`/`FOREIGN`), from `system.information_schema.tables` — spans every Unity
  * Catalog catalog in the metastore (this view isn't per-catalog the way Hive metastore's
  * `information_schema` was), the closest analog to Snowflake's `UNION ALL`-of-inventory-views
  * `ObjectInventoryRow`.
  */
case class ObjectInventoryRow(tableType: String, objectCount: Long)

/** One row per Lakeflow job, from `system.lakeflow.job_run_timeline` — Databricks Workflows,
  * the closest analog to Snowflake's `TASK_HISTORY`. `avgDurationSeconds` averages
  * `execution_duration_seconds` across all runs in the window, not just successful ones.
  */
case class JobRunSummaryRow(
  jobId: String,
  totalRuns: Long,
  succeeded: Long,
  failed: Long,
  avgDurationSeconds: Double,
  totalExecSeconds: Double
)

/** One row per day, from `system.lakeflow.job_run_timeline` — the daily job-run-volume trend,
  * analog to Snowflake's `dailyQueryStats` shape but for job runs instead of queries.
  */
case class DailyJobRunRow(day: String, totalRuns: Long, succeeded: Long)

/** Per-actor USD cost, from `system.billing.usage.identity_metadata.run_as` joined to
  * `list_prices` — the closest Databricks analog to Snowflake's `QUERY_ATTRIBUTION_HISTORY`.
  * Unlike Snowflake's attribution view, a meaningful share of usage records have `run_as = NULL`
  * (unattributed to any specific identity) — those are excluded here, not folded into a
  * catch-all bucket, so `SUM(usdAmount)` across this row type will be less than the report's
  * total cost tile.
  */
case class CostByUserRow(actor: String, usdAmount: Double)

/** One row per (month, SKU) — the line-item granularity for the itemized monthly bill, same
  * shape as Snowflake's `CostRow` grouped by month instead of day.
  */
case class MonthlyCostBySkuRow(month: String, skuName: String, usdAmount: Double)

/** One row per (day, actor type), from `system.query.history.executed_by` — the query-volume
  * analog to [[DailyActiveUsersRow]] (which counts `system.access.audit` actions, a different
  * and much larger activity stream). `actorType` uses the same email-vs-not heuristic.
  */
case class DailyQueryVolumeByTypeRow(day: String, actorType: String, queryCount: Long)

/** Per-service-actor query stats, from `system.query.history` filtered to non-email
  * `executed_by` values — direct analog to Snowflake's `ServiceAccountQueryStatsRow`.
  * `computeCount` counts distinct `COALESCE(compute.warehouse_id, compute.cluster_id)` values,
  * the Databricks analog to Snowflake's `warehouseCount`.
  */
case class ServiceAccountQueryStatsRow(actor: String, queryCount: Long, computeCount: Long, firstDay: String, lastDay: String)

/** Tables accessed by service actors, from `system.access.table_lineage` filtered to non-email
  * `created_by` values — `tableName` is whichever of source/target is non-null for that lineage
  * edge (no read/write split here, matching Snowflake's `ServiceAccountTableAccess` shape).
  */
case class ServiceAccountTableAccessRow(actor: String, tableName: String, accessCount: Long)

/** One row per (direction, actor, table), from `system.access.table_lineage.created_by` —
  * feeds the person-user table-access breakdown (service actors are covered separately by
  * [[ServiceAccountTableAccessRow]]); `direction` is `"READ"` or `"WRITE"`, set in Scala per
  * query ([[guild.databricksusage.DatabricksJdbc.loadUserTableReads]]/`loadUserTableWrites`),
  * mirroring how [[TableLineageAccessRow]]'s direction is assigned.
  */
case class UserTableAccessRow(actor: String, tableName: String, direction: String, accessCount: Long)

/** Real file/byte size for one table, from `DESCRIBE DETAIL` — Unity Catalog has no system
  * table exposing per-table size the way Snowflake's `TABLE_STORAGE_METRICS` does, so sizes are
  * fetched individually, one call per table, for every real storage-bearing table (see
  * [[guild.databricksusage.DatabricksJdbc.loadAllTableSizes]]) rather than via a single
  * `GROUP BY` pull. `format` is `"delta"` or `"iceberg"` — this workspace has both, including
  * several Snowflake-managed Iceberg tables already mounted into Unity Catalog.
  */
case class TableSizeRow(qualifiedName: String, format: String, numFiles: Long, sizeBytes: Long)

/** Generic (label, count) pair — used for catalog/schema counts alongside the table-type
  * breakdown in [[ObjectInventoryRow]], kept as its own type since "table type" would be a
  * misleading field name for a `"CATALOG"`/`"SCHEMA"` label.
  */
case class CatalogSchemaCountRow(objectType: String, objectCount: Long)

/** `applicationId` is the UUID that shows up as `executed_by`/`user_identity.email`/`created_by`
  * everywhere else in this tool for a service principal — this is the only place a
  * human-readable `displayName` for it exists. Not from a `system.*` table at all; from the
  * workspace SCIM API (`GET /api/2.0/preview/scim/v2/ServicePrincipals`), since Unity Catalog's
  * system tables only ever record the identity as its raw UUID. See
  * [[guild.databricksusage.DatabricksJdbc.loadServicePrincipals]].
  */
case class ServicePrincipalRow(applicationId: String, displayName: String)

/** Current-state `job_id` -> `name` lookup, from `system.lakeflow.jobs` — the only place a
  * Lakeflow job's actual name lives; `job_run_timeline`/`job_run_summary` only ever carry the
  * numeric `job_id`. `WHERE delete_time IS NULL` excludes deleted jobs, matching the
  * still-current-state semantics of every other inventory pull in this tool.
  */
case class JobNameRow(jobId: String, name: String)

/** Current-state `warehouse_id` -> `name`, from the same `GET /api/2.0/sql/warehouses` REST
  * response already used for warehouse auto-discovery — a second small call, not a reuse of the
  * first, since that one runs before the report's data-fetch phase even starts. See
  * [[guild.databricksusage.DatabricksJdbc.loadWarehouseNames]].
  */
case class WarehouseNameRow(warehouseId: String, name: String)

/** Current-state `cluster_id` -> `cluster_name`, from `system.compute.clusters`. In practice this
  * workspace's `system.query.history` rows are all `WAREHOUSE`/`SERVERLESS_COMPUTE` compute type
  * (no `CLUSTER` observed), so this lookup currently has nothing to resolve against — kept for
  * completeness/robustness rather than because it's been seen doing anything on real data yet.
  */
case class ClusterNameRow(clusterId: String, clusterName: String)

/** Single-row totals over `system.lakeflow.job_run_timeline` for the WHOLE lookback window,
  * independent of [[JobRunSummaryRow]]'s `LIMIT` — the per-job summary table only shows the top
  * 20 jobs by run count, so summing across just those would understate true total run hours.
  */
case class JobRunTotalsRow(distinctJobsRun: Long, totalRuns: Long, succeeded: Long, totalExecSeconds: Double)

/** One row per (job, task) for the same bounded top-N jobs [[JobRunSummaryRow]] already shows —
  * from the Jobs REST API (`GET /api/2.0/jobs/get`), not a `system.*` table; no system table
  * records how a job's tasks are actually configured (notebook/JAR/Python wheel/SQL/dbt/etc.),
  * only that they ran. `taskType` is whichever of the task JSON's `*_task` keys is present (e.g.
  * `notebook_task`, `spark_jar_task`, `python_wheel_task`) — see
  * [[guild.databricksusage.DatabricksJdbc.loadJobTasks]].
  */
case class JobTaskRow(jobId: String, taskKey: String, taskType: String)

/** Single-row summary over `system.mlflow.{experiments_latest,runs_latest}` — MLflow (experiment
  * tracking + model registry) is genuinely Databricks-native with no Snowflake equivalent
  * measured in that report (Snowflake has Snowpark ML's own model registry as a separate
  * offering, not pulled into the Snowflake tool). `totalExperiments` is current-state
  * (`WHERE delete_time IS NULL`); `totalRuns`/`finishedRuns` are scoped to the lookback window.
  */
case class MlflowSummaryRow(totalExperiments: Long, totalRuns: Long, finishedRuns: Long)

/** Current-state count of active Model Serving endpoints by `entity_type`
  * (`CUSTOM_MODEL`/`FOUNDATION_MODEL`/`EXTERNAL_MODEL`), from `system.serving.served_entities`.
  */
case class ServingEndpointTypeRow(entityType: String, count: Long)

/** Single-row summary over `system.serving.endpoint_usage` for the lookback window —
  * `totalTokens` sums input+output tokens (`COALESCE`d — traditional, non-LLM served models
  * leave these null, not zero, which would otherwise silently drop their request from the sum's
  * contribution though not its count).
  */
case class ServingUsageSummaryRow(totalRequests: Long, totalTokens: Long)

/** One row per (destination type, destination name) AI Gateway routed to — e.g.
  * `PAY_PER_TOKEN_FOUNDATION_MODEL` / `system.ai.databricks-claude-sonnet-5`. On this workspace,
  * AI Gateway traffic is dominated by Databricks' own hosted foundation models, not third-party
  * providers — `COALESCE`d to `"UNKNOWN"` since a real fraction of requests carry no destination
  * at all (likely failed/pre-routing requests, not a parsing gap).
  */
case class AiGatewayUsageRow(destinationType: String, destinationName: String, requestCount: Long, totalTokens: Long)

/** Single-row summary over `system.data_quality_monitoring.table_results` for the lookback
  * window — Databricks' Lakehouse Monitoring, a genuine Databricks-native capability gap on the
  * Snowflake side (no built-in equivalent; would typically be dbt tests or custom SQL checks
  * there). `healthyRuns` counts `status = 'Healthy'`; real values seen also include `'Unhealthy'`
  * and `'Training'` (a monitor still establishing its baseline).
  */
case class DataQualityMonitoringSummaryRow(tablesMonitored: Long, checkRuns: Long, healthyRuns: Long)

/** Single-row summary over `system.data_classification.results` — Unity Catalog's automated
  * PII/sensitive-data tagging. Unlike the other sections here, this ISN'T a Databricks-only
  * capability (Snowflake has its own native classification feature) — real usage on this
  * workspace is minimal (a handful of results), included for completeness rather than because
  * it's a major signal.
  */
case class DataClassificationSummaryRow(totalResults: Long, tablesClassified: Long)
