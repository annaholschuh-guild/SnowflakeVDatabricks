package guild.snowflakeusage.queries


/** SQL pulls against `SNOWFLAKE.ACCOUNT_USAGE`. SELECT lists are trimmed to exactly the
  * columns [[Rows]] declares (and [[guild.snowflakeusage.aggregate.Aggregations]] consumes) —
  * filter-only columns (e.g. `deleted_on`) are referenced in `WHERE` but not selected back.
  *
  * Every column is aliased with a **quoted** lowercase identifier (`AS "user_name"`, not
  * `AS user_name`). Snowflake's JDBC driver does a case-sensitive column-label lookup (unlike
  * the usual JDBC case-insensitive contract), and an unquoted alias gets upper-cased by
  * Snowflake — so without the quotes, `rs.getString("user_name")` in [[guild.snowflakeusage.SnowflakeJdbc]]
  * would fail with "Column not found" against the actual `USER_NAME` label.
  *
  * `lookbackDays` is an internally-computed Int (from [[guild.snowflakeusage.Config]]), never
  * user-supplied text, so it's interpolated directly rather than bound as a JDBC parameter.
  *
  * Notes on Snowflake edition requirements:
  *   - `ACCESS_HISTORY` requires Enterprise Edition or higher. On Standard Edition the query
  *     below will fail with an access error — [[guild.snowflakeusage.UsageReportRunner]] treats
  *     that pull as optional and continues without it.
  *   - `ACCOUNT_USAGE` views have replication latency of up to ~3 hours and retain history for
  *     up to 365 days (1 year), so `lookbackDays` beyond that will silently return less data.
  */
object Queries {

  def users: String =
    """
      |SELECT
      |  name AS "user_name",
      |  disabled AS "disabled",
      |  last_success_login AS "last_success_login",
      |  created_on AS "created_on"
      |FROM snowflake.account_usage.users
      |WHERE deleted_on IS NULL
    """.stripMargin

  /** `query_history` is never pulled as raw rows — every shape below is a `GROUP BY` done in
    * Snowflake, so the pull stays small (bounded by days×warehouses/users/databases, not by raw
    * query count) regardless of lookback window. This is what makes a full year feasible; see
    * the Phase 1 rewrite plan. Each function here corresponds 1:1 to a row type in [[Rows]].
    */
  private def queryHistoryWhere(lookbackDays: Int): String =
    s"start_time >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())"

  def dailyQueryStats(lookbackDays: Int): String =
    s"""
       |SELECT
       |  TO_DATE(start_time) AS "day",
       |  COUNT(*) AS "query_count",
       |  COUNT(DISTINCT user_name) AS "active_users"
       |FROM snowflake.account_usage.query_history
       |WHERE ${queryHistoryWhere(lookbackDays)}
       |GROUP BY 1
    """.stripMargin

  /** Feeds both the person-vs-service daily query-volume split and the daily-active-users-by-type
    * split — a row here already implies the user was active that day, so counting rows per
    * (day, type) after folding `user_name` through `classifyUserType` gives both answers.
    */
  def dailyQueryByUser(lookbackDays: Int): String =
    s"""
       |SELECT
       |  TO_DATE(start_time) AS "day",
       |  user_name AS "user_name",
       |  COUNT(*) AS "query_count"
       |FROM snowflake.account_usage.query_history
       |WHERE ${queryHistoryWhere(lookbackDays)}
       |GROUP BY 1, 2
    """.stripMargin

  /** Buffered well beyond the top 20 ultimately shown — employee-bucketing happens client-side
    * after this, and a too-tight SQL `LIMIT` could drop a real top-20 user before bucketing.
    */
  def topUsersByQueryCount(lookbackDays: Int, limit: Int = 300): String =
    s"""
       |SELECT
       |  user_name AS "user_name",
       |  COUNT(*) AS "query_count"
       |FROM snowflake.account_usage.query_history
       |WHERE ${queryHistoryWhere(lookbackDays)}
       |GROUP BY 1
       |ORDER BY 2 DESC
       |LIMIT $limit
    """.stripMargin

  def databaseActivityByDay(lookbackDays: Int): String =
    s"""
       |SELECT
       |  TO_DATE(start_time) AS "day",
       |  database_name AS "database_name",
       |  COUNT(*) AS "query_count"
       |FROM snowflake.account_usage.query_history
       |WHERE ${queryHistoryWhere(lookbackDays)} AND database_name IS NOT NULL
       |GROUP BY 1, 2
    """.stripMargin

  /** Snowflake's analog to a Databricks cluster-size mismatch: spilling means the working set
    * didn't fit in memory, queueing means not enough concurrent capacity.
    */
  def warehouseResourceSignals(lookbackDays: Int): String =
    s"""
       |SELECT
       |  warehouse_name AS "warehouse_name",
       |  COUNT(*) AS "query_count",
       |  SUM(CASE WHEN (bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage) > 0 THEN 1 ELSE 0 END) AS "queries_with_spill",
       |  SUM(bytes_spilled_to_local_storage + bytes_spilled_to_remote_storage) AS "total_spilled_bytes",
       |  SUM(CASE WHEN (queued_provisioning_time + queued_overload_time) > 0 THEN 1 ELSE 0 END) AS "queries_with_queueing",
       |  SUM(queued_provisioning_time + queued_overload_time) AS "total_queued_ms"
       |FROM snowflake.account_usage.query_history
       |WHERE ${queryHistoryWhere(lookbackDays)}
       |GROUP BY 1
    """.stripMargin

  /** All-queries totals (the denominator for a per-warehouse $/second-of-query-time rate) and
    * failed-only totals (to price out wasted compute), per warehouse, in one pass.
    */
  def warehouseExecStats(lookbackDays: Int): String =
    s"""
       |SELECT
       |  warehouse_name AS "warehouse_name",
       |  COUNT(*) AS "total_queries",
       |  SUM(execution_time) AS "total_exec_ms",
       |  SUM(CASE WHEN execution_status != 'SUCCESS' THEN 1 ELSE 0 END) AS "failed_count",
       |  SUM(CASE WHEN execution_status != 'SUCCESS' THEN execution_time ELSE 0 END) AS "failed_exec_ms"
       |FROM snowflake.account_usage.query_history
       |WHERE ${queryHistoryWhere(lookbackDays)}
       |GROUP BY 1
    """.stripMargin

  /** `ANY_VALUE` picks an arbitrary (not "first") row's message per error code — the same
    * non-deterministic-but-representative semantics the old driver-side "first row seen" logic
    * had anyway (Spark's partition order was never guaranteed either).
    */
  def topErrorReasons(lookbackDays: Int): String =
    s"""
       |SELECT
       |  COALESCE(error_code, 'UNKNOWN') AS "error_code",
       |  ANY_VALUE(error_message) AS "sample_message",
       |  COUNT(*) AS "count"
       |FROM snowflake.account_usage.query_history
       |WHERE ${queryHistoryWhere(lookbackDays)} AND execution_status != 'SUCCESS'
       |GROUP BY 1
       |ORDER BY 3 DESC
       |LIMIT 15
    """.stripMargin


  /** `APPROX_PERCENTILE` (sketch-based) for all three percentile points, including the median —
    * deliberately not the exact `MEDIAN`/`PERCENTILE_CONT` aggregate, which would force an
    * expensive full sort per day and undercut the point of aggregating server-side. Likely more
    * accurate than the box plot this replaces, which computed exact quartiles from a
    * ≤2000-point/day client-side subsample rather than the full day's data.
    */
  def dailyDurationStats(lookbackDays: Int): String =
    s"""
       |SELECT
       |  TO_DATE(start_time) AS "day",
       |  MIN(execution_time) AS "min_ms",
       |  APPROX_PERCENTILE(execution_time, 0.25) AS "q1_ms",
       |  APPROX_PERCENTILE(execution_time, 0.5) AS "median_ms",
       |  APPROX_PERCENTILE(execution_time, 0.75) AS "q3_ms",
       |  MAX(execution_time) AS "max_ms"
       |FROM snowflake.account_usage.query_history
       |WHERE ${queryHistoryWhere(lookbackDays)} AND execution_time > 0
       |GROUP BY 1
    """.stripMargin

  /** Filtered to just the (already-classified) service-account user names — small regardless of
    * overall query volume. `userNames` comes from `USERS.TYPE`, not end-user input, but is still
    * quote-escaped defensively before interpolation.
    */
  def serviceAccountQueryStats(lookbackDays: Int, userNames: Seq[String]): String = {
    val inList = userNames.map(n => "'" + n.replace("'", "''") + "'").mkString(",")
    s"""
       |SELECT
       |  user_name AS "user_name",
       |  COUNT(*) AS "query_count",
       |  COUNT(DISTINCT warehouse_name) AS "warehouse_count",
       |  MIN(TO_DATE(start_time)) AS "first_day",
       |  MAX(TO_DATE(start_time)) AS "last_day"
       |FROM snowflake.account_usage.query_history
       |WHERE ${queryHistoryWhere(lookbackDays)} AND user_name IN ($inList)
       |GROUP BY 1
    """.stripMargin
  }

  def warehouseMeteringHistory(lookbackDays: Int): String =
    s"""
       |SELECT
       |  warehouse_name AS "warehouse_name",
       |  start_time AS "start_time",
       |  credits_used AS "credits_used"
       |FROM snowflake.account_usage.warehouse_metering_history
       |WHERE start_time >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
    """.stripMargin

  def tableStorageMetrics: String =
    """
      |SELECT
      |  table_catalog AS "table_catalog",
      |  table_schema AS "table_schema",
      |  table_name AS "table_name",
      |  active_bytes AS "active_bytes",
      |  time_travel_bytes AS "time_travel_bytes",
      |  failsafe_bytes AS "failsafe_bytes",
      |  retained_for_clone_bytes AS "retained_for_clone_bytes"
      |FROM snowflake.account_usage.table_storage_metrics
      |WHERE deleted = FALSE
    """.stripMargin

  def databaseStorageUsageHistory(lookbackDays: Int): String =
    s"""
       |SELECT
       |  usage_date AS "usage_date",
       |  database_name AS "database_name",
       |  average_database_bytes AS "average_database_bytes"
       |FROM snowflake.account_usage.database_storage_usage_history
       |WHERE usage_date >= DATEADD(day, -$lookbackDays, CURRENT_DATE())
    """.stripMargin

  /** Flattens both `base_objects_accessed` (reads) and `objects_modified` (writes) — each a
    * VARIANT array of `{objectName, objectDomain, ...}` — into one row per (query, object,
    * action) triple, `user_name` carried along so read/write activity can be broken down per
    * table AND per user (see [[guild.snowflakeusage.aggregate.Aggregations.tableUsageSection]]).
    */
  /** Never queried raw — always wrapped as a CTE by [[tableAccessCounts]]/[[tableReadWriteByUser]]
    * below, which `GROUP BY` the union in one outer aggregate. Aggregating each branch (reads,
    * writes) separately and summing `COUNT(DISTINCT query_id)` would double-count any query that
    * both reads and writes the same table — the union must happen before the aggregate, not after.
    */
  private def accessHistoryUnion(lookbackDays: Int): String =
    s"""
       |SELECT
       |  ah.query_id AS "query_id",
       |  ah.user_name AS "user_name",
       |  obj.value:"objectName"::string AS "object_name",
       |  obj.value:"objectDomain"::string AS "object_domain",
       |  'READ' AS "action"
       |FROM snowflake.account_usage.access_history ah,
       |LATERAL FLATTEN(input => ah.base_objects_accessed) obj
       |WHERE ah.query_start_time >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
       |UNION ALL
       |SELECT
       |  ah.query_id AS "query_id",
       |  ah.user_name AS "user_name",
       |  obj.value:"objectName"::string AS "object_name",
       |  obj.value:"objectDomain"::string AS "object_domain",
       |  'WRITE' AS "action"
       |FROM snowflake.account_usage.access_history ah,
       |LATERAL FLATTEN(input => ah.objects_modified) obj
       |WHERE ah.query_start_time >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
    """.stripMargin

  /** Per-table distinct-query access counts — feeds most-queried-tables, dead-table detection
    * (anti-joined against `table_storage_metrics` in Scala), and size-vs-access. Bounded by
    * distinct tables touched, not by raw access_history row count.
    */
  def tableAccessCounts(lookbackDays: Int): String =
    s"""
       |WITH unioned AS (${accessHistoryUnion(lookbackDays)})
       |SELECT "object_name" AS "object_name", COUNT(DISTINCT "query_id") AS "distinct_queries"
       |FROM unioned
       |WHERE "object_domain" = 'Table'
       |GROUP BY "object_name"
    """.stripMargin

  /** Per (table, user, action) distinct-query counts — feeds both the Table Usage section's
    * read/write-by-table breakdown AND the Service Accounts section's per-account table access
    * (filtered to service-account user names client-side, since this pull is already small — no
    * separate SQL round-trip needed for that). Bounded by distinct (table, user) pairs actually
    * touched, not by raw access_history row count.
    */
  def tableReadWriteByUser(lookbackDays: Int): String =
    s"""
       |WITH unioned AS (${accessHistoryUnion(lookbackDays)})
       |SELECT "object_name" AS "object_name", "user_name" AS "user_name", "action" AS "action", COUNT(DISTINCT "query_id") AS "distinct_queries"
       |FROM unioned
       |WHERE "object_domain" = 'Table'
       |GROUP BY "object_name", "user_name", "action"
    """.stripMargin

  /** Per-user total reads/writes, regardless of which table(s) each query touched. NOT the same
    * as summing [[tableReadWriteByUser]]'s per-table counts across tables for a user — a query
    * touching N tables would be counted N times that way, since "distinct query_id" per table
    * doesn't stay distinct once you sum across tables. This needs its own `GROUP BY` grain
    * (no `object_name`) to stay correct.
    */
  def userActionCounts(lookbackDays: Int): String =
    s"""
       |WITH unioned AS (${accessHistoryUnion(lookbackDays)})
       |SELECT "user_name" AS "user_name", "action" AS "action", COUNT(DISTINCT "query_id") AS "distinct_queries"
       |FROM unioned
       |WHERE "object_domain" = 'Table'
       |GROUP BY "user_name", "action"
    """.stripMargin

  /** Per (user, table) distinct query count, action-agnostic — a query that both reads and
    * writes the same table counts once, not twice, same rationale as [[userActionCounts]].
    */
  def userTableAccessCounts(lookbackDays: Int): String =
    s"""
       |WITH unioned AS (${accessHistoryUnion(lookbackDays)})
       |SELECT "user_name" AS "user_name", "object_name" AS "object_name", COUNT(DISTINCT "query_id") AS "distinct_queries"
       |FROM unioned
       |WHERE "object_domain" = 'Table'
       |GROUP BY "user_name", "object_name"
    """.stripMargin

  /** Real billed cost for this account, in the contract's currency — `ORGANIZATION_USAGE` spans
    * every account in the org, so this is filtered down to just `accountLocator`. `usage_type`
    * and `usage_in_currency` are summed per day; negative rows are legitimate credits/adjustments
    * (e.g. included cloud-services waivers) and net out correctly when summed, not excluded.
    *
    * `accountLocator` is validated as alphanumeric before interpolation — it's operator-supplied
    * (a CLI flag/env var), not end-user input, but there's no reason to risk string-building SQL
    * with an unvalidated value when a one-line check removes the risk entirely.
    */
  def costUsageDaily(lookbackDays: Int, accountLocator: String): String = {
    require(accountLocator.matches("[A-Za-z0-9_]+"), s"Invalid account locator: $accountLocator")
    s"""
       |SELECT
       |  usage_date AS "usage_date",
       |  usage_type AS "usage_type",
       |  usage_in_currency AS "usd_amount"
       |FROM snowflake.organization_usage.usage_in_currency_daily
       |WHERE account_locator = '$accountLocator'
       |  AND currency = 'USD'
       |  AND usage_date >= DATEADD(day, -$lookbackDays, CURRENT_DATE())
    """.stripMargin
  }

  /** Not present on every Snowflake account/edition — pulled separately from `users` so a
    * failure here doesn't break that mandatory fetch.
    */
  def userTypes: String =
    """
      |SELECT
      |  name AS "user_name",
      |  type AS "user_type"
      |FROM snowflake.account_usage.users
      |WHERE deleted_on IS NULL
    """.stripMargin

  /** Requires Enterprise Edition or higher. Aggregated in SQL (one row per user, not per query)
    * since `query_attribution_history` can otherwise run to millions of rows.
    */
  def queryAttributionByUser(lookbackDays: Int): String =
    s"""
       |SELECT
       |  user_name AS "user_name",
       |  SUM(credits_attributed_compute) AS "credits"
       |FROM snowflake.account_usage.query_attribution_history
       |WHERE start_time >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
       |GROUP BY user_name
    """.stripMargin

  /** Requires Enterprise Edition or higher (Tasks are a Serverless/Enterprise+ feature in
    * practice, though the view itself may reject on lower editions with an access error).
    */
  def taskHistory(lookbackDays: Int): String =
    s"""
       |SELECT
       |  name AS "task_name",
       |  database_name AS "database_name",
       |  schema_name AS "schema_name",
       |  state AS "state",
       |  scheduled_time AS "scheduled_time",
       |  query_text AS "query_text"
       |FROM snowflake.account_usage.task_history
       |WHERE scheduled_time >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
    """.stripMargin

  def pipeUsageHistory(lookbackDays: Int): String =
    s"""
       |SELECT
       |  pipe_name AS "pipe_name",
       |  start_time AS "start_time",
       |  credits_used AS "credits_used",
       |  bytes_inserted AS "bytes_inserted",
       |  files_inserted AS "files_inserted"
       |FROM snowflake.account_usage.pipe_usage_history
       |WHERE start_time >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
    """.stripMargin

  /** There's no `ACCOUNT_USAGE.STREAMS` view (unlike tables/pipes/tasks) — `SHOW STREAMS` is the
    * only inventory source, and its result columns are all VARCHAR (including "stale", which is
    * the literal string "true"/"false", not a JDBC boolean) — see [[guild.snowflakeusage.SnowflakeJdbc.loadStreamsInventory]].
    * Requires the role to have visibility into every database's streams account-wide.
    */
  def streamsInventory: String = "SHOW STREAMS IN ACCOUNT"

  /** There's no `ACCOUNT_USAGE.SHARES` view either — same situation as `streamsInventory`.
    * `SHOW SHARES` lists both directions: shares this account created (`kind = OUTBOUND`) and
    * shares this account consumes from someone else (`kind = INBOUND`).
    */
  def sharesInventory: String = "SHOW SHARES"

  /** Requires Enterprise Edition or higher, same tier as Tasks/`ACCESS_HISTORY`. Unlike Tasks,
    * this view has no success/failure state column — just start/end time and `credits_used`.
    */
  def materializedViewRefreshHistory(lookbackDays: Int): String =
    s"""
       |SELECT
       |  table_name AS "view_name",
       |  database_name AS "database_name",
       |  schema_name AS "schema_name",
       |  start_time AS "start_time",
       |  end_time AS "end_time",
       |  credits_used AS "credits_used"
       |FROM snowflake.account_usage.materialized_view_refresh_history
       |WHERE start_time >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
    """.stripMargin

  def dataTransferHistory(lookbackDays: Int): String =
    s"""
       |SELECT
       |  start_time AS "start_time",
       |  source_cloud AS "source_cloud",
       |  source_region AS "source_region",
       |  target_cloud AS "target_cloud",
       |  target_region AS "target_region",
       |  transfer_type AS "transfer_type",
       |  bytes_transferred AS "bytes_transferred"
       |FROM snowflake.account_usage.data_transfer_history
       |WHERE start_time >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
    """.stripMargin

  /** One lightweight `UNION ALL` round trip instead of six separate pulls — all we need per
    * object type is a count. Every source view tracks soft-deletes via `deleted_on`/`deleted`,
    * so each branch excludes those consistently with the `users`/`tableStorageMetrics` pulls.
    */
  def objectInventoryCounts: String =
    """
      |SELECT 'Databases' AS "object_type", COUNT(*) AS "object_count"
      |FROM snowflake.account_usage.databases WHERE deleted IS NULL
      |UNION ALL
      |SELECT 'Schemas' AS "object_type", COUNT(*) AS "object_count"
      |FROM snowflake.account_usage.schemata WHERE deleted IS NULL
      |UNION ALL
      |SELECT 'Tables' AS "object_type", COUNT(*) AS "object_count"
      |FROM snowflake.account_usage.tables WHERE deleted IS NULL
      |UNION ALL
      |SELECT 'Views' AS "object_type", COUNT(*) AS "object_count"
      |FROM snowflake.account_usage.views WHERE deleted IS NULL
      |UNION ALL
      |SELECT 'Stored Procedures' AS "object_type", COUNT(*) AS "object_count"
      |FROM snowflake.account_usage.procedures WHERE deleted IS NULL
      |UNION ALL
      |SELECT 'Functions' AS "object_type", COUNT(*) AS "object_count"
      |FROM snowflake.account_usage.functions WHERE deleted IS NULL
    """.stripMargin

  /** `clientApplicationId` is what actually needs a re-point in a Databricks migration (BI
    * tools, JDBC/ODBC drivers) — native Snowflake-only tooling doesn't carry over at all.
    */
  /** Pulled pre-aggregated (`GROUP BY` in Snowflake) — the version-collapsing
    * (`Aggregations.baseClientName`) is inherently Scala logic (regex), not naturally SQL, so
    * this stays small (bounded by distinct raw `client_application_id` values — a few dozen —
    * not by raw session count) and the classification happens client-side on that tiny result.
    */
  def sessionsByClientApp(lookbackDays: Int): String =
    s"""
       |SELECT
       |  client_application_id AS "client_application_id",
       |  COUNT(*) AS "session_count"
       |FROM snowflake.account_usage.sessions
       |WHERE created_on >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
       |GROUP BY 1
    """.stripMargin

  /** `OPTIONS` is an OBJECT column holding `ARCHIVE_FOR_DAYS`/`ARCHIVE_TIER` among other settings
    * — this view lists policy *definitions*, not which tables they're attached to (that needs
    * `INFORMATION_SCHEMA.POLICY_REFERENCES()`, out of scope here — the definition list alone is
    * enough to answer "does this account use cold storage at all").
    */
  def storageLifecyclePolicies: String =
    """
      |SELECT
      |  name AS "policy_name",
      |  database AS "database_name",
      |  schema AS "schema_name",
      |  options:"ARCHIVE_TIER"::string AS "archive_tier",
      |  options:"ARCHIVE_FOR_DAYS"::int AS "archive_for_days"
      |FROM snowflake.account_usage.storage_lifecycle_policies
      |WHERE deleted_on IS NULL
    """.stripMargin

  /** `is_success` is the literal string `"YES"`/`"NO"`, not a JDBC boolean — same gotcha as
    * `SHOW STREAMS.stale`. `client_ip` is what gets matched against known network-policy IP
    * allowlists (Twingate, dbt Cloud, Fivetran, etc.) to attribute a login to a channel.
    */
  /** Pulled pre-aggregated (`GROUP BY` in Snowflake) — the IP-CIDR channel classification
    * (`Aggregations.classifyLoginChannel`) is inherently Scala logic, not naturally SQL, so this
    * stays small (bounded by distinct (IP, client type) pairs, not by raw login count) and both
    * the channel and client-type breakdowns are computed client-side on that tiny result.
    */
  def loginsByIpAndClientType(lookbackDays: Int): String =
    s"""
       |SELECT
       |  client_ip AS "client_ip",
       |  reported_client_type AS "reported_client_type",
       |  COUNT(*) AS "login_count"
       |FROM snowflake.account_usage.login_history
       |WHERE event_timestamp >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
       |GROUP BY 1, 2
    """.stripMargin

  /** `first_authentication_factor`/`second_authentication_factor` are the real auth-method
    * signal LOGIN_HISTORY carries (e.g. `PASSWORD`, `OAUTH_ACCESS_TOKEN`, `SAML2_ASSERTION`,
    * `KEY_PAIR_AUTHENTICATION`; `second_authentication_factor` is the MFA factor, null when MFA
    * wasn't used) — distinct from `reported_client_type` (what tool connected) and `client_ip`
    * (what network), which is what `loginsByIpAndClientType` above already covers.
    */
  def authFactorBreakdown(lookbackDays: Int): String =
    s"""
       |SELECT
       |  first_authentication_factor AS "first_factor",
       |  second_authentication_factor AS "second_factor",
       |  is_success AS "is_success",
       |  COUNT(*) AS "login_count"
       |FROM snowflake.account_usage.login_history
       |WHERE event_timestamp >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
       |GROUP BY 1, 2, 3
    """.stripMargin

  /** Real cross-region/cross-cloud DR-replication usage, if any — `REPLICATION_GROUP_USAGE_HISTORY`
    * has one row per (replication group, time bucket); zero rows is itself a real, meaningful
    * answer (no replication/failover group configured), same precedent as
    * `storageLifecyclePolicies` above.
    */
  def replicationGroupUsage(lookbackDays: Int): String =
    s"""
       |SELECT
       |  replication_group_name AS "replication_group_name",
       |  SUM(credits_used) AS "total_credits",
       |  SUM(bytes_transferred) AS "total_bytes_transferred",
       |  COUNT(*) AS "event_count"
       |FROM snowflake.account_usage.replication_group_usage_history
       |WHERE start_time >= DATEADD(day, -$lookbackDays, CURRENT_TIMESTAMP())
       |GROUP BY 1
    """.stripMargin
}
