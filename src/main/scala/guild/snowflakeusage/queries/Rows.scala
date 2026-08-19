package guild.snowflakeusage.queries

import java.sql.Timestamp


/** Typed rows for each `ACCOUNT_USAGE` pull. Fields are trimmed to exactly what
  * `guild.snowflakeusage.aggregate.Aggregations` consumes — see [[Queries]] for the SQL that
  * produces them and [[guild.snowflakeusage.SnowflakeJdbc]] for how the JDBC read is cast to
  * match these schemas before `.as[T]`.
  */
case class UserRow(
  userName: String,
  disabled: Boolean,
  lastSuccessLogin: Option[Timestamp],
  createdOn: Timestamp
)

/** `query_history` is pulled pre-aggregated (`GROUP BY` in Snowflake), never as raw rows — at a
  * year's lookback that's 300M+ rows, infeasible to materialize client-side over JDBC. Each row
  * type below corresponds to one `GROUP BY` shape in [[Queries]]; see
  * [[guild.snowflakeusage.aggregate.Aggregations]] for how each feeds the dashboard.
  */
case class DailyQueryStatsRow(day: String, queryCount: Long, activeUsers: Long)

/** One row per (day, user) — feeds both the person-vs-service query-volume split and the
  * daily-active-users-by-type split, since a row here already implies "this user was active
  * this day."
  */
case class DailyUserQueryRow(day: String, userName: String, queryCount: Long)

/** Top individual users by raw query count — pulled with a generous buffer beyond the 20
  * ultimately shown, since employee-bucketing (many real users folding into one
  * "Employee@GUILDEDUCATION.com" row) happens client-side after this and could otherwise push a
  * real top-20 user out of a too-tight SQL `LIMIT`.
  */
case class UserQueryCountRow(userName: String, queryCount: Long)

case class DatabaseActivityRawRow(day: String, databaseName: String, queryCount: Long)

/** Per-warehouse spill/queueing signals — Snowflake's analog to a Databricks cluster-size
  * mismatch. Byte/ms sums, not GB/seconds — unit conversion stays in Scala.
  */
case class WarehouseResourceSignalRow(
  warehouseName: String,
  queryCount: Long,
  queriesWithSpill: Long,
  totalSpilledBytes: Long,
  queriesWithQueueing: Long,
  totalQueuedMs: Long
)

/** Per-warehouse totals across ALL queries and the FAILED-only subset, in one pass — the
  * all-queries total is the denominator for a $/second-of-query-time rate (see
  * [[guild.snowflakeusage.aggregate.Aggregations.wastedComputeCostUsd]]); the failed-only figures
  * price out wasted compute.
  */
case class WarehouseExecStatsRow(
  warehouseName: String,
  totalQueries: Long,
  totalExecMs: Long,
  failedCount: Long,
  failedExecMs: Long
)

/** `sampleMessage` comes from `ANY_VALUE` — an arbitrary (not "first") row's message per error
  * code, same non-deterministic-but-representative semantics as the driver-side "first row seen"
  * this replaced.
  */
case class ErrorReasonRow(errorCode: String, sampleMessage: String, count: Long)

/** Precomputed per-day duration quartiles (`APPROX_PERCENTILE`, sketch-based) — replaces the old
  * client-side-subsampled-then-Plotly-computed box plot. `q1Ms`/`medianMs`/`q3Ms` are `Double`
  * (percentile functions return fractional values even over integer input); `minMs`/`maxMs` are
  * exact. See [[guild.reportkit.dashboard.PlotlyUtil]] for the precomputed-quartile box
  * trace this feeds — whiskers are true min/max, not the classical 1.5×IQR convention, and
  * individual outlier points are no longer rendered (Plotly can't plot outliers without raw
  * samples).
  */
case class DailyDurationStatsRow(day: String, minMs: Long, q1Ms: Double, medianMs: Double, q3Ms: Double, maxMs: Long)

/** Per-service-account query stats — only fetched for the user names `USERS.TYPE` already
  * classified as service accounts (see [[guild.snowflakeusage.aggregate.Aggregations.classifyUserType]]),
  * via a `WHERE user_name IN (...)` filter, so this pull is small regardless of overall query
  * volume.
  */
case class ServiceAccountQueryStatsRow(userName: String, queryCount: Long, warehouseCount: Long, firstDay: String, lastDay: String)

case class WarehouseMeteringRow(
  warehouseName: String,
  startTime: Timestamp,
  creditsUsed: Double
)

case class TableStorageRow(
  tableCatalog: String,
  tableSchema: String,
  tableName: String,
  activeBytes: Long,
  timeTravelBytes: Long,
  failsafeBytes: Long,
  retainedForCloneBytes: Long
) {
  def qualifiedName: String = s"$tableCatalog.$tableSchema.$tableName"
}

case class DatabaseStorageUsageRow(
  usageDate: java.sql.Date,
  databaseName: String,
  averageDatabaseBytes: Double
)

/** `access_history` is pulled pre-aggregated (`GROUP BY` in Snowflake, over the unioned
  * reads+writes flatten), never as raw rows — see [[Queries.tableAccessCounts]]/
  * [[Queries.tableReadWriteByUser]]. Bounded by distinct tables/users touched, not by raw
  * access_history row count (which can run well past query_history's own row count once
  * per-query object arrays are flattened).
  */
case class TableAccessCountRow(objectName: String, distinctQueries: Long)

/** `action` is `"READ"` or `"WRITE"`. */
case class TableUserActionCountRow(objectName: String, userName: String, action: String, distinctQueries: Long)

/** Per-user total reads/writes — NOT derivable by summing `TableUserActionCountRow` across
  * tables for a user (that double-counts multi-table queries); see
  * [[Queries.userActionCounts]].
  */
case class UserActionCountRow(userName: String, action: String, distinctQueries: Long)

/** Per (user, table) distinct query count, action-agnostic; see [[Queries.userTableAccessCounts]]. */
case class UserTableAccessCountRow(userName: String, objectName: String, distinctQueries: Long)

/** One row per (day, usage type) for this account, from `ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY`
  * — real billed dollars at the account's contracted rate, not a credit-price estimate. Requires
  * the role to have access to `ORGANIZATION_USAGE` (broader than plain `ACCOUNT_USAGE` access).
  */
case class CostRow(
  usageDate: java.sql.Date,
  usageType: String,
  usdAmount: Double
)

/** From `ACCOUNT_USAGE.USERS.TYPE` — not present on every Snowflake account/edition, so pulled
  * as its own optional query rather than folded into [[UserRow]] (a failure here shouldn't
  * break the mandatory `users` fetch).
  */
case class UserTypeRow(userName: String, userType: String)

/** Per-user credit attribution from `ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY` — requires
  * Enterprise Edition or higher, same tier gate as [[AccessHistoryRow]].
  */
case class UserCreditsRow(userName: String, credits: Double)

/** One row per task run, from `ACCOUNT_USAGE.TASK_HISTORY`. Tasks are Snowflake-native scheduled
  * orchestration with no 1:1 Databricks equivalent (closest is Databricks Workflows/Jobs) —
  * relevant to migration scoping, not just cost.
  */
/** `queryText` is the SQL the task actually ran that run — used to classify the task as a
  * stored-procedure call (`CALL ...`) vs. plain SQL (`INSERT`/`MERGE`/`COPY INTO`/etc.), see
  * [[guild.snowflakeusage.aggregate.Aggregations.classifyTaskType]].
  */
case class TaskRunRow(
  taskName: String,
  databaseName: String,
  schemaName: String,
  state: String,
  scheduledTime: Timestamp,
  queryText: String
)

/** One row per Snowpipe ingestion batch, from `ACCOUNT_USAGE.PIPE_USAGE_HISTORY`. */
case class PipeUsageRow(pipeName: String, startTime: Timestamp, creditsUsed: Double, bytesInserted: Long, filesInserted: Long)

/** Current-state stream inventory, from `ACCOUNT_USAGE.STREAMS`. `stale = true` means the
  * stream's change-data offset has expired past retention and it's now broken until recreated.
  */
case class StreamInventoryRow(streamName: String, databaseName: String, schemaName: String, stale: Boolean)

/** Current-state share inventory, from `SHOW SHARES` — there's no `ACCOUNT_USAGE.SHARES` view,
  * same situation as [[StreamInventoryRow]]. `kind` is `"OUTBOUND"` (this account is sharing
  * out) or `"INBOUND"` (this account is consuming someone else's share).
  */
case class ShareInventoryRow(shareName: String, kind: String, databaseName: String, toAccounts: String)

/** One row per materialized view refresh, from `ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY`
  * — a Snowflake-native automated-maintenance feature (closest Databricks analog: a scheduled
  * job refreshing a table), relevant to migration scoping alongside Tasks/Snowpipe/Streams.
  * Unlike Tasks, there's no success/failure state column here — just timing and credits.
  */
case class MaterializedViewRefreshRow(
  viewName: String,
  databaseName: String,
  schemaName: String,
  startTime: Timestamp,
  endTime: Timestamp,
  creditsUsed: Double
)

/** One row per cross-cloud/cross-region data transfer, from `ACCOUNT_USAGE.DATA_TRANSFER_HISTORY`
  * — relevant to migration planning since egress out of Snowflake's cloud/region can carry its
  * own cost independent of compute/storage.
  */
case class DataTransferRow(
  startTime: Timestamp,
  sourceCloud: String,
  sourceRegion: String,
  targetCloud: String,
  targetRegion: String,
  transferType: String,
  bytesTransferred: Long
)

/** One row per account object type, from a `UNION ALL` of the `ACCOUNT_USAGE` inventory views
  * (`DATABASES`/`SCHEMATA`/`TABLES`/`VIEWS`/`PROCEDURES`/`FUNCTIONS`) — a single lightweight
  * round trip rather than six separate pulls, since all we need is a count per type.
  */
case class ObjectInventoryRow(objectType: String, objectCount: Long)

/** One row per storage lifecycle policy (COOL/COLD archive tiering), from
  * `ACCOUNT_USAGE.STORAGE_LIFECYCLE_POLICIES` — this is Snowflake's actual cold-storage feature
  * (attached per-table, not automatic); a 0-row pull means the account genuinely isn't using it,
  * this view isn't role-scoped the way `SHOW STREAMS` is.
  */
case class StorageLifecyclePolicyRow(
  policyName: String,
  databaseName: String,
  schemaName: String,
  archiveTier: Option[String],
  archiveForDays: Option[Int]
)

/** `login_history` is pulled pre-aggregated (`GROUP BY client_ip, reported_client_type` in
  * Snowflake), never as raw rows — the IP-CIDR channel classification and client-type breakdown
  * are inherently Scala logic, so aggregating the raw dimensions first keeps this bounded by
  * distinct (IP, client type) pairs rather than raw login count. See
  * [[Queries.loginsByIpAndClientType]].
  */
case class LoginByIpAndTypeRow(clientIp: String, reportedClientType: String, loginCount: Long)

/** `secondFactor` is `None` when MFA wasn't used for that (firstFactor, isSuccess) bucket. */
case class AuthFactorRow(firstFactor: String, secondFactor: Option[String], isSuccess: Boolean, loginCount: Long)

/** Zero rows back from the query this maps means no replication/failover group is configured on
  * this account — a real, meaningful answer, not missing data. */
case class ReplicationGroupUsageRow(
  replicationGroupName: String,
  totalCredits: Double,
  totalBytesTransferred: Long,
  eventCount: Long
)

/** `sessions` is pulled pre-aggregated too, for the same reason — `clientApplicationId`'s
  * version-collapsing (see `Aggregations.baseClientName`) is Scala regex logic, not SQL. See
  * [[Queries.sessionsByClientApp]].
  */
case class SessionByClientRow(clientApplicationId: String, sessionCount: Long)
