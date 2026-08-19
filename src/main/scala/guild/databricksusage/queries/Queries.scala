package guild.databricksusage.queries


/** SQL text for each `system.*` pull, mirroring [[guild.snowflakeusage.queries.Queries]]'s
  * shape: every function returns a `GROUP BY`-aggregated query string, never a raw-row pull —
  * see [[Rows]] for why that matters on this workspace specifically.
  */
object Queries {

  private def usageWhere(lookbackDays: Int): String =
    s"usage_date >= current_date() - INTERVAL $lookbackDays DAYS"

  private def queryHistoryWhere(lookbackDays: Int): String =
    s"start_time >= current_timestamp() - INTERVAL $lookbackDays DAYS"

  /** Joins `billing.usage` to `billing.list_prices` on the price effective at
    * `usage_start_time` — the join Databricks' own system-tables cost-estimation docs recommend.
    * List price, not negotiated/discounted price — see [[DailyCostRow]].
    */
  private def pricedUsageCte(lookbackDays: Int): String =
    s"""
       |priced AS (
       |  SELECT
       |    u.usage_date,
       |    u.sku_name,
       |    u.billing_origin_product,
       |    u.identity_metadata.run_as AS run_as,
       |    u.usage_quantity AS dbu_quantity,
       |    u.usage_quantity * lp.pricing.default AS usd
       |  FROM system.billing.usage u
       |  JOIN system.billing.list_prices lp
       |    ON u.sku_name = lp.sku_name
       |   AND u.usage_start_time >= lp.price_start_time
       |   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
       |  WHERE u.${usageWhere(lookbackDays)}
       |)
    """.stripMargin

  def dailyCost(lookbackDays: Int): String =
    s"""
       |WITH ${pricedUsageCte(lookbackDays)}
       |SELECT
       |  date_format(usage_date, 'yyyy-MM-dd') AS day,
       |  SUM(usd) AS usd_amount
       |FROM priced
       |GROUP BY usage_date
       |ORDER BY usage_date
    """.stripMargin

  def costBySku(lookbackDays: Int): String =
    s"""
       |WITH ${pricedUsageCte(lookbackDays)}
       |SELECT
       |  sku_name,
       |  SUM(usd) AS usd_amount,
       |  SUM(dbu_quantity) AS dbu_quantity
       |FROM priced
       |GROUP BY sku_name
       |ORDER BY usd_amount DESC
    """.stripMargin

  def dailyCostByProduct(lookbackDays: Int): String =
    s"""
       |WITH ${pricedUsageCte(lookbackDays)}
       |SELECT
       |  date_format(usage_date, 'yyyy-MM-dd') AS day,
       |  billing_origin_product AS product,
       |  SUM(usd) AS usd_amount
       |FROM priced
       |GROUP BY usage_date, billing_origin_product
       |ORDER BY usage_date
    """.stripMargin

  def overviewStats(lookbackDays: Int): String =
    s"""
       |SELECT
       |  COUNT(*) AS total_queries,
       |  COUNT(DISTINCT executed_by) AS active_users
       |FROM system.query.history
       |WHERE ${queryHistoryWhere(lookbackDays)}
    """.stripMargin

  def dailyQueryVolume(lookbackDays: Int): String =
    s"""
       |SELECT
       |  date_format(start_time, 'yyyy-MM-dd') AS day,
       |  COUNT(*) AS query_count,
       |  COUNT(DISTINCT executed_by) AS active_users
       |FROM system.query.history
       |WHERE ${queryHistoryWhere(lookbackDays)}
       |GROUP BY 1
       |ORDER BY 1
    """.stripMargin

  /** `total_duration_ms > 0` excludes cache-hit/instant queries the same way the Snowflake
    * report's `execution_time > 0` filter did — a wall of zero-duration points would otherwise
    * compress every real query's box flat against the axis floor.
    */
  def dailyDurationStats(lookbackDays: Int): String =
    s"""
       |SELECT
       |  date_format(start_time, 'yyyy-MM-dd') AS day,
       |  MIN(total_duration_ms) AS min_ms,
       |  approx_percentile(total_duration_ms, 0.25) AS q1_ms,
       |  approx_percentile(total_duration_ms, 0.5) AS median_ms,
       |  approx_percentile(total_duration_ms, 0.75) AS q3_ms,
       |  MAX(total_duration_ms) AS max_ms
       |FROM system.query.history
       |WHERE ${queryHistoryWhere(lookbackDays)} AND total_duration_ms > 0
       |GROUP BY 1
       |ORDER BY 1
    """.stripMargin

  def computeExecStats(lookbackDays: Int): String =
    s"""
       |SELECT
       |  compute.type AS compute_type,
       |  compute.warehouse_id AS warehouse_id,
       |  compute.cluster_id AS cluster_id,
       |  COUNT(*) AS total_queries,
       |  SUM(total_duration_ms) AS total_exec_ms,
       |  SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
       |  SUM(CASE WHEN execution_status = 'FAILED' THEN total_duration_ms ELSE 0 END) AS failed_exec_ms
       |FROM system.query.history
       |WHERE ${queryHistoryWhere(lookbackDays)}
       |GROUP BY compute.type, compute.warehouse_id, compute.cluster_id
       |ORDER BY total_queries DESC
    """.stripMargin

  def computeResourceSignals(lookbackDays: Int): String =
    s"""
       |SELECT
       |  compute.type AS compute_type,
       |  compute.warehouse_id AS warehouse_id,
       |  compute.cluster_id AS cluster_id,
       |  COUNT(*) AS query_count,
       |  SUM(CASE WHEN spilled_local_bytes > 0 THEN 1 ELSE 0 END) AS queries_with_spill,
       |  SUM(spilled_local_bytes) AS total_spilled_bytes,
       |  SUM(CASE WHEN waiting_for_compute_duration_ms > 0 THEN 1 ELSE 0 END) AS queries_with_queueing,
       |  SUM(waiting_for_compute_duration_ms) AS total_queued_ms
       |FROM system.query.history
       |WHERE ${queryHistoryWhere(lookbackDays)}
       |GROUP BY compute.type, compute.warehouse_id, compute.cluster_id
    """.stripMargin

  /** `error_message` has no stable error-code column the way Snowflake's `query_history` does —
    * Databricks/Spark SQL error messages carry a leading `[BRACKETED_ERROR_CLASS]` when the
    * error originates from a classified Spark exception; anything else (e.g. a plain
    * `PERMISSION_DENIED: ...` message with no brackets) folds into `"OTHER"` rather than
    * fragmenting into near-duplicate groups by raw message text.
    */
  def topErrorReasons(lookbackDays: Int): String =
    s"""
       |SELECT
       |  COALESCE(NULLIF(regexp_extract(error_message, '^\\\\[([A-Z_0-9]+)\\\\]', 1), ''), 'OTHER') AS error_class,
       |  ANY_VALUE(error_message) AS sample_message,
       |  COUNT(*) AS query_count
       |FROM system.query.history
       |WHERE ${queryHistoryWhere(lookbackDays)} AND execution_status = 'FAILED'
       |GROUP BY error_class
       |ORDER BY query_count DESC
       |LIMIT 15
    """.stripMargin

  private def auditWhere(lookbackDays: Int): String =
    s"event_date >= current_date() - INTERVAL $lookbackDays DAYS"

  /** `"person"` when `user_identity.email` looks like an email address, else `"service"` — a UUID
    * service-principal application id or the literal `"System-User"` both fall through to
    * `"service"`. `"person"` matches Snowflake's `PERSON`/`SERVICE` terminology (see
    * `guild.snowflakeusage.aggregate.Aggregations.classifyUserType`). See
    * [[guild.databricksusage.queries.Rows.DailyActiveUsersRow]].
    */
  private val actorTypeExpr =
    "CASE WHEN user_identity.email LIKE '%@%' THEN 'person' ELSE 'service' END"

  def dailyActiveUsers(lookbackDays: Int): String =
    s"""
       |SELECT
       |  date_format(event_time, 'yyyy-MM-dd') AS day,
       |  $actorTypeExpr AS actor_type,
       |  COUNT(DISTINCT user_identity.email) AS distinct_users,
       |  COUNT(*) AS action_count
       |FROM system.access.audit
       |WHERE ${auditWhere(lookbackDays)}
       |GROUP BY 1, 2
       |ORDER BY 1
    """.stripMargin

  /** `limit` defaults well past the 20 ultimately shown — bucketing multiple real employees into
    * one "Employee@guild.com" row happens client-side after this pull, and could otherwise push
    * a real top-20 actor out of a too-tight SQL `LIMIT` (same "generous buffer" pattern as
    * Snowflake's `topUsersByQueryCount`).
    */
  def topActorsByActions(lookbackDays: Int, limit: Int = 50): String =
    s"""
       |SELECT
       |  user_identity.email AS actor,
       |  COUNT(*) AS action_count
       |FROM system.access.audit
       |WHERE ${auditWhere(lookbackDays)}
       |GROUP BY user_identity.email
       |ORDER BY action_count DESC
       |LIMIT $limit
    """.stripMargin

  def actionsByService(lookbackDays: Int): String =
    s"""
       |SELECT
       |  service_name,
       |  COUNT(*) AS action_count
       |FROM system.access.audit
       |WHERE ${auditWhere(lookbackDays)}
       |GROUP BY service_name
       |ORDER BY action_count DESC
    """.stripMargin

  private def lineageWhere(lookbackDays: Int): String =
    s"event_date >= current_date() - INTERVAL $lookbackDays DAYS"

  /** `left(schemaCol, 2) != '__'` excludes Databricks' own platform-internal schemas
    * (`__internal_logging`, `__internal_data_quality_monitoring`) — a leading double-underscore
    * is Databricks' convention for hidden/internal schemas, distinct from the real, legitimate
    * `system.*` schemas this tool itself queries (`billing`, `access`, `lakeflow`, etc.). These
    * internal-logging rows have no resolvable actor identity (they're written by Databricks'
    * own platform processes, not anything visible via the workspace's SCIM API), so filtering
    * them out here is the actual fix — not a name lookup that will never have anywhere to look.
    */
  private def notInternalSchema(schemaCol: String): String = s"($schemaCol IS NULL OR left($schemaCol, 2) != '__')"

  def topReadTables(lookbackDays: Int, limit: Int = 20): String =
    s"""
       |SELECT
       |  source_table_full_name AS table_name,
       |  COUNT(*) AS access_count
       |FROM system.access.table_lineage
       |WHERE ${lineageWhere(lookbackDays)} AND source_table_full_name IS NOT NULL
       |  AND ${notInternalSchema("source_table_schema")}
       |GROUP BY source_table_full_name
       |ORDER BY access_count DESC
       |LIMIT $limit
    """.stripMargin

  def topWriteTables(lookbackDays: Int, limit: Int = 20): String =
    s"""
       |SELECT
       |  target_table_full_name AS table_name,
       |  COUNT(*) AS access_count
       |FROM system.access.table_lineage
       |WHERE ${lineageWhere(lookbackDays)} AND target_table_full_name IS NOT NULL
       |  AND ${notInternalSchema("target_table_schema")}
       |GROUP BY target_table_full_name
       |ORDER BY access_count DESC
       |LIMIT $limit
    """.stripMargin

  /** Current-state inventory, not lookback-scoped — same treatment as Snowflake's
    * `SHOW`-command inventories (streams/shares/lifecycle policies).
    */
  def objectInventory(): String =
    s"""
       |SELECT
       |  table_type,
       |  COUNT(*) AS object_count
       |FROM system.information_schema.tables
       |GROUP BY table_type
       |ORDER BY object_count DESC
    """.stripMargin

  def jobRunSummary(lookbackDays: Int, limit: Int = 20): String =
    s"""
       |SELECT
       |  job_id,
       |  COUNT(*) AS total_runs,
       |  SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
       |  SUM(CASE WHEN result_state IS NULL OR result_state != 'SUCCEEDED' THEN 1 ELSE 0 END) AS failed,
       |  AVG(execution_duration_seconds) AS avg_duration_seconds,
       |  SUM(execution_duration_seconds) AS total_exec_seconds
       |FROM system.lakeflow.job_run_timeline
       |WHERE period_start_time >= current_timestamp() - INTERVAL $lookbackDays DAYS
       |GROUP BY job_id
       |ORDER BY total_runs DESC
       |LIMIT $limit
    """.stripMargin

  /** Unlike [[jobRunSummary]], no `LIMIT` and no `GROUP BY` — a single-row total across every
    * job run in the window, so none of these figures understate reality by only counting the
    * top 20 jobs shown in that table (mixing a true total next to a top-20-only figure in the
    * same stat row would be actively misleading, not just incomplete).
    */
  def jobRunTotals(lookbackDays: Int): String =
    s"""
       |SELECT
       |  COUNT(DISTINCT job_id) AS distinct_jobs_run,
       |  COUNT(*) AS total_runs,
       |  SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
       |  SUM(execution_duration_seconds) AS total_exec_seconds
       |FROM system.lakeflow.job_run_timeline
       |WHERE period_start_time >= current_timestamp() - INTERVAL $lookbackDays DAYS
    """.stripMargin

  /** Current-state, not lookback-scoped — same treatment as [[objectInventory]]. */
  def jobNames(): String =
    "SELECT job_id, name FROM system.lakeflow.jobs WHERE delete_time IS NULL"

  /** Current-state, not lookback-scoped. */
  def clusterNames(): String =
    "SELECT cluster_id, cluster_name FROM system.compute.clusters WHERE delete_time IS NULL"

  def dailyJobRuns(lookbackDays: Int): String =
    s"""
       |SELECT
       |  date_format(period_start_time, 'yyyy-MM-dd') AS day,
       |  COUNT(*) AS total_runs,
       |  SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded
       |FROM system.lakeflow.job_run_timeline
       |WHERE period_start_time >= current_timestamp() - INTERVAL $lookbackDays DAYS
       |GROUP BY 1
       |ORDER BY 1
    """.stripMargin

  /** No `LIMIT` — bounded by distinct `run_as` identities (small), not usage-record volume;
    * bucketing/name resolution (see [[Aggregations.displayName]]) and top-N selection happen in Scala after this
    * pull, same pattern as Snowflake's unlimited `queryAttributionByUser`. `run_as IS NOT NULL`
    * excludes unattributed usage — see [[Rows.CostByUserRow]] for why that's a real gap, not a
    * bug to paper over with a catch-all bucket.
    */
  def costByUser(lookbackDays: Int): String =
    s"""
       |WITH ${pricedUsageCte(lookbackDays)}
       |SELECT
       |  run_as AS actor,
       |  SUM(usd) AS usd_amount
       |FROM priced
       |WHERE run_as IS NOT NULL
       |GROUP BY run_as
    """.stripMargin

  def monthlyCostBySku(lookbackDays: Int): String =
    s"""
       |WITH ${pricedUsageCte(lookbackDays)}
       |SELECT
       |  date_format(usage_date, 'yyyy-MM') AS month,
       |  sku_name,
       |  SUM(usd) AS usd_amount
       |FROM priced
       |GROUP BY 1, 2
    """.stripMargin

  def dailyQueryVolumeByType(lookbackDays: Int): String =
    s"""
       |SELECT
       |  date_format(start_time, 'yyyy-MM-dd') AS day,
       |  CASE WHEN executed_by LIKE '%@%' THEN 'person' ELSE 'service' END AS actor_type,
       |  COUNT(*) AS query_count
       |FROM system.query.history
       |WHERE ${queryHistoryWhere(lookbackDays)}
       |GROUP BY 1, 2
       |ORDER BY 1
    """.stripMargin

  def serviceAccountQueryStats(lookbackDays: Int): String =
    s"""
       |SELECT
       |  executed_by AS actor,
       |  COUNT(*) AS query_count,
       |  COUNT(DISTINCT COALESCE(compute.warehouse_id, compute.cluster_id)) AS compute_count,
       |  MIN(date_format(start_time, 'yyyy-MM-dd')) AS first_day,
       |  MAX(date_format(start_time, 'yyyy-MM-dd')) AS last_day
       |FROM system.query.history
       |WHERE ${queryHistoryWhere(lookbackDays)} AND executed_by NOT LIKE '%@%'
       |GROUP BY executed_by
       |ORDER BY query_count DESC
    """.stripMargin

  def serviceAccountTableAccess(lookbackDays: Int, limit: Int = 30): String =
    s"""
       |SELECT
       |  created_by AS actor,
       |  COALESCE(source_table_full_name, target_table_full_name) AS table_name,
       |  COUNT(*) AS access_count
       |FROM system.access.table_lineage
       |WHERE ${lineageWhere(lookbackDays)} AND created_by IS NOT NULL AND created_by NOT LIKE '%@%'
       |  AND ${notInternalSchema("source_table_schema")} AND ${notInternalSchema("target_table_schema")}
       |GROUP BY actor, table_name
       |ORDER BY access_count DESC
       |LIMIT $limit
    """.stripMargin

  def userTableReads(lookbackDays: Int, limit: Int = 150): String =
    s"""
       |SELECT
       |  created_by AS actor,
       |  source_table_full_name AS table_name,
       |  COUNT(*) AS access_count
       |FROM system.access.table_lineage
       |WHERE ${lineageWhere(lookbackDays)} AND created_by IS NOT NULL AND source_table_full_name IS NOT NULL
       |  AND ${notInternalSchema("source_table_schema")}
       |GROUP BY actor, table_name
       |ORDER BY access_count DESC
       |LIMIT $limit
    """.stripMargin

  def userTableWrites(lookbackDays: Int, limit: Int = 150): String =
    s"""
       |SELECT
       |  created_by AS actor,
       |  target_table_full_name AS table_name,
       |  COUNT(*) AS access_count
       |FROM system.access.table_lineage
       |WHERE ${lineageWhere(lookbackDays)} AND created_by IS NOT NULL AND target_table_full_name IS NOT NULL
       |  AND ${notInternalSchema("target_table_schema")}
       |GROUP BY actor, table_name
       |ORDER BY access_count DESC
       |LIMIT $limit
    """.stripMargin

  def catalogSchemaCounts(): String =
    s"""
       |SELECT 'CATALOG' AS object_type, COUNT(*) AS object_count FROM system.information_schema.catalogs
       |UNION ALL
       |SELECT 'SCHEMA' AS object_type, COUNT(*) AS object_count FROM system.information_schema.schemata
    """.stripMargin

  /** Backtick-quotes each of the 3 Unity Catalog namespace segments individually — several real
    * catalog names on this workspace contain hyphens (e.g. `snowflake-iceberg-prod-scratch`),
    * which `DESCRIBE DETAIL` would otherwise parse as subtraction.
    */
  def describeDetail(qualifiedName: String): String = {
    val quoted = qualifiedName.split("\\.").map(seg => s"`$seg`").mkString(".")
    s"DESCRIBE DETAIL $quoted"
  }

  /** Every table type that occupies real physical storage under Databricks/cloud-storage
    * control — `VIEW` has none of its own, and `FOREIGN` tables' data lives in the source
    * system, not in this workspace's storage. Feeds a full `DESCRIBE DETAIL` sweep (see
    * [[guild.databricksusage.DatabricksJdbc.loadAllTableSizes]]) — unlike
    * [[guild.databricksusage.aggregate.Aggregations.topTableNamesForSizing]]'s bounded top-N,
    * this is every real candidate, since there's no cheaper way to get a true storage total
    * (no system table tracks size at all — see [[Rows.TableSizeRow]]).
    */
  def storageBearingTableNames(): String =
    """
      |SELECT table_catalog, table_schema, table_name
      |FROM system.information_schema.tables
      |WHERE table_type IN ('MANAGED', 'EXTERNAL', 'STREAMING_TABLE', 'MATERIALIZED_VIEW')
    """.stripMargin

  def mlflowSummary(lookbackDays: Int): String =
    s"""
       |SELECT
       |  (SELECT COUNT(*) FROM system.mlflow.experiments_latest WHERE delete_time IS NULL) AS total_experiments,
       |  COUNT(*) AS total_runs,
       |  SUM(CASE WHEN status = 'FINISHED' THEN 1 ELSE 0 END) AS finished_runs
       |FROM system.mlflow.runs_latest
       |WHERE start_time >= current_timestamp() - INTERVAL $lookbackDays DAYS
    """.stripMargin

  /** Current-state, not lookback-scoped. */
  def servingEndpointTypes(): String =
    """
      |SELECT entity_type, COUNT(*) AS count
      |FROM system.serving.served_entities
      |WHERE endpoint_delete_time IS NULL
      |GROUP BY entity_type
    """.stripMargin

  def servingUsageSummary(lookbackDays: Int): String =
    s"""
       |SELECT
       |  COUNT(*) AS total_requests,
       |  SUM(COALESCE(input_token_count, 0) + COALESCE(output_token_count, 0)) AS total_tokens
       |FROM system.serving.endpoint_usage
       |WHERE request_time >= current_timestamp() - INTERVAL $lookbackDays DAYS
    """.stripMargin

  def aiGatewayUsage(lookbackDays: Int): String =
    s"""
       |SELECT
       |  COALESCE(destination_type, 'UNKNOWN') AS destination_type,
       |  COALESCE(destination_name, 'UNKNOWN') AS destination_name,
       |  COUNT(*) AS request_count,
       |  SUM(COALESCE(total_tokens, 0)) AS total_tokens
       |FROM system.ai_gateway.usage
       |WHERE event_time >= current_timestamp() - INTERVAL $lookbackDays DAYS
       |GROUP BY destination_type, destination_name
       |ORDER BY request_count DESC
    """.stripMargin

  def dataQualityMonitoringSummary(lookbackDays: Int): String =
    s"""
       |SELECT
       |  COUNT(DISTINCT table_id) AS tables_monitored,
       |  COUNT(*) AS check_runs,
       |  SUM(CASE WHEN status = 'Healthy' THEN 1 ELSE 0 END) AS healthy_runs
       |FROM system.data_quality_monitoring.table_results
       |WHERE event_time >= current_timestamp() - INTERVAL $lookbackDays DAYS
    """.stripMargin

  /** Current-state, not lookback-scoped. */
  def dataClassificationSummary(): String =
    """
      |SELECT COUNT(*) AS total_results, COUNT(DISTINCT table_id) AS tables_classified
      |FROM system.data_classification.results
    """.stripMargin
}
