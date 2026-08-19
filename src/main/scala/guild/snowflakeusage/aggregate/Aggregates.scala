package guild.snowflakeusage.aggregate

import guild.snowflakeusage.queries._

import org.apache.spark.sql.Dataset


/** Raw typed pulls from `SNOWFLAKE.ACCOUNT_USAGE`. `tableAccessCounts`/`tableReadWriteByUser`/
  * `costUsage`/`userTypes`/`queryAttribution` may all be missing (edition/grant-gated) — see
  * [[Aggregations]] for how each section degrades when its input is absent.
  */
case class RawUsageData(
  users: Dataset[UserRow],
  // query_history is pulled pre-aggregated (GROUP BY in Snowflake) — see Phase 1 rewrite plan.
  // Each field below is one GROUP BY shape; none of them scale with raw query count.
  dailyQueryStats: Dataset[DailyQueryStatsRow],
  dailyQueryByUser: Dataset[DailyUserQueryRow],
  topUsersByQueryCount: Dataset[UserQueryCountRow],
  databaseActivityByDay: Dataset[DatabaseActivityRawRow],
  warehouseResourceSignals: Dataset[WarehouseResourceSignalRow],
  warehouseExecStats: Dataset[WarehouseExecStatsRow],
  topErrorReasons: Dataset[ErrorReasonRow],
  dailyDurationStats: Dataset[DailyDurationStatsRow],
  // Only present when userTypes classified at least one service account — see UsageReportRunner.
  serviceAccountQueryStats: Option[Dataset[ServiceAccountQueryStatsRow]],
  warehouseMetering: Dataset[WarehouseMeteringRow],
  tableStorage: Dataset[TableStorageRow],
  databaseStorageHistory: Dataset[DatabaseStorageUsageRow],
  // access_history is pulled pre-aggregated too (GROUP BY the unioned reads+writes flatten in
  // Snowflake) — see Queries.tableAccessCounts/tableReadWriteByUser.
  tableAccessCounts: Option[Dataset[TableAccessCountRow]],
  tableReadWriteByUser: Option[Dataset[TableUserActionCountRow]],
  // Per-user rollups need their own GROUP BY grain — summing tableReadWriteByUser across tables
  // for a user would double-count multi-table queries. See Queries.userActionCounts/userTableAccessCounts.
  userActionCounts: Option[Dataset[UserActionCountRow]],
  userTableAccessCounts: Option[Dataset[UserTableAccessCountRow]],
  costUsage: Option[Dataset[CostRow]],
  userTypes: Option[Dataset[UserTypeRow]],
  queryAttribution: Option[Dataset[UserCreditsRow]],
  taskHistory: Option[Dataset[TaskRunRow]],
  pipeUsage: Option[Dataset[PipeUsageRow]],
  streamsInventory: Option[Dataset[StreamInventoryRow]],
  sharesInventory: Option[Dataset[ShareInventoryRow]],
  materializedViewRefreshHistory: Option[Dataset[MaterializedViewRefreshRow]],
  dataTransferHistory: Option[Dataset[DataTransferRow]],
  objectInventory: Option[Dataset[ObjectInventoryRow]],
  // sessions/login_history are pulled pre-aggregated too — see Queries.sessionsByClientApp/
  // loginsByIpAndClientType.
  sessionsByClientApp: Option[Dataset[SessionByClientRow]],
  storageLifecyclePolicies: Option[Dataset[StorageLifecyclePolicyRow]],
  loginsByIpAndClientType: Option[Dataset[LoginByIpAndTypeRow]]
)

case class DailyQueryVolume(day: String, queryCount: Long)

/** Same day-level query counts as [[DailyQueryVolume]], split by person vs. service account —
  * empty when `USERS.TYPE` isn't available on this account/edition (same gate as
  * [[UsersSection.dailyActiveByType]]).
  */
case class DailyQueryVolumeByType(day: String, userType: String, queryCount: Long)
/** Precomputed quartiles (seconds), not raw samples — see `PlotlyUtil`'s precomputed-quartile box
  * trace. Whiskers (`minSeconds`/`maxSeconds`) are true min/max, not the classical 1.5×IQR
  * convention; there are no individual outlier points to render (Plotly can't plot outliers
  * without raw samples) — the separate top-slow-queries table covers that instead.
  */
case class DailyDurationSample(
  day: String,
  minSeconds: Double,
  q1Seconds: Double,
  medianSeconds: Double,
  q3Seconds: Double,
  maxSeconds: Double
)
case class WarehouseCredits(warehouseName: String, totalCredits: Double)
case class DailyCredits(day: String, credits: Double)
case class DatabaseActivityTrend(day: String, databaseName: String, queryCount: Long)

case class DailyActiveUsers(day: String, activeUsers: Long)
case class DailyActiveByType(day: String, userType: String, activeUsers: Long)
case class TopUser(userName: String, queryCount: Long)

case class TopTableBySize(qualifiedName: String, activeGb: Double, timeTravelGb: Double, failsafeGb: Double)
case class DatabaseStorageTrend(day: String, databaseName: String, avgGb: Double)
case class StorageSummary(
  totalActiveGb: Double,
  totalTimeTravelGb: Double,
  totalFailsafeGb: Double,
  totalRetainedForCloneGb: Double
)

case class TopAccessedTable(objectName: String, distinctQueries: Long)
case class DeadTable(qualifiedName: String, activeGb: Double)

/** Per-warehouse signals that the warehouse may be the wrong size/type for its workload —
  * Snowflake's analog to running a Databricks job on a cluster that doesn't fit the job.
  * Spilling means the working set didn't fit in memory (warehouse too small); queueing means
  * not enough concurrent capacity (too small, or not configured for multi-cluster auto-scale).
  */
case class WarehouseResourceSignal(
  warehouseName: String,
  queryCount: Long,
  queriesWithSpill: Long,
  totalSpilledGb: Double,
  queriesWithQueueing: Long,
  totalQueuedSeconds: Double
)

/** Wasted compute from queries that never succeeded, per warehouse — a failed query can still
  * burn real warehouse time before it dies, so this isn't just a reliability signal, it's a
  * cost-efficiency one too.
  */
case class FailedQueryWaste(warehouseName: String, failedQueryCount: Long, wastedSeconds: Double)

/** One row per distinct error code among failed queries, with a representative sample message
  * (not aggregated on message text — Snowflake error messages often embed query-specific detail
  * like table/object names, which would fragment otherwise-identical failures into many groups).
  */
case class TopErrorReason(errorCode: String, sampleMessage: String, count: Long)

/** Total query execution time per warehouse, across ALL queries (not just failed ones) — the
  * denominator for turning a warehouse's real credit consumption into a $/second-of-query-time
  * rate, used to price out wasted compute from failed queries. See [[Aggregations.costSection]].
  */
case class WarehouseExecSeconds(warehouseName: String, totalExecSeconds: Double)

/** Query & warehouse activity: how hard the account is actually being worked, and by what. */
case class ActivitySection(
  dailyQueryVolume: Seq[DailyQueryVolume],
  dailyQueryVolumeByType: Seq[DailyQueryVolumeByType],
  dailyDurationSamples: Seq[DailyDurationSample],
  totalCredits: Double,
  dailyTotalCredits: Seq[DailyCredits],
  topWarehousesByCredits: Seq[WarehouseCredits],
  databaseActivityTrend: Seq[DatabaseActivityTrend],
  resourceMismatchSignals: Seq[WarehouseResourceSignal],
  failedQueryCount: Long,
  wastedComputeByWarehouse: Seq[FailedQueryWaste],
  totalExecSecondsByWarehouse: Seq[WarehouseExecSeconds],
  topErrorReasons: Seq[TopErrorReason]
)

/** Active users: who is actually logged in and querying.
  * `dailyActiveByType` is empty when `USERS.TYPE` isn't available on this account/edition.
  */
case class UsersSection(
  dailyActiveUsers: Seq[DailyActiveUsers],
  dailyActiveByType: Seq[DailyActiveByType],
  topUsersByQueryCount: Seq[TopUser],
  totalNonDisabledUsers: Long
)

/** Storage & table sizes: current-state footprint, and how it's trending. */
case class StorageSection(
  topTablesBySize: Seq[TopTableBySize],
  databaseStorageTrend: Seq[DatabaseStorageTrend],
  summary: StorageSummary
)

case class TableSizeVsAccess(qualifiedName: String, activeGb: Double, distinctQueries: Long)

/** One row per (table, user) — who's reading vs. writing a given table, and how often. Reads and
  * writes are separate count columns (not a combined "action" row) so both directions are visible
  * at a glance for the same table/user, without needing to scan two separate rows. Rendered as a
  * client-side sortable/filterable table (see `HTMLUtil.sortableFilterableTable`) rather than a
  * fixed top-N chart, since "sortable by size and frequency" is exactly what a static report can
  * offer without a backend.
  */
case class TableAccessDetail(qualifiedName: String, activeGb: Double, userName: String, readCount: Long, writeCount: Long)

/** Table usage / access patterns: what's actually queried vs. dead weight not worth migrating.
  * `None` when `ACCESS_HISTORY` wasn't available (requires Enterprise Edition+).
  */
case class TableUsageSection(
  topAccessedTables: Seq[TopAccessedTable],
  deadTables: Seq[DeadTable],
  sizeVsAccess: Seq[TableSizeVsAccess],
  readWriteDetails: Seq[TableAccessDetail]
)

/** One (month, usage type) cell of the monthly "bill" table — real billed dollars only
  * (`isEstimate = false`); the credit-price estimate doesn't carry enough per-usage-type detail
  * over time to build a meaningful line-item breakdown, so this is empty in that case.
  */
case class MonthlyCostLineItem(month: String, usageType: String, usd: Double)

case class DailyCost(day: String, usd: Double)
case class CostByUsageType(usageType: String, usd: Double)
case class WarehouseCost(warehouseName: String, usd: Double)
case class TopUserCost(userName: String, usd: Double)

/** Real or estimated dollar cost. `isEstimate = true` means it's a credit-price/storage-price
  * approximation (no `ORGANIZATION_USAGE` access) rather than actual billed dollars — the
  * dashboard must label this distinction, never present an estimate as billed fact.
  *
  * `costByWarehouse`/`topUsersByCost` are derived by applying an implied or supplied $/credit
  * rate to already-known credit breakdowns (`ORGANIZATION_USAGE` has no per-warehouse or
  * per-user grain) — an approximation even when `isEstimate = false`, since the account-wide
  * rate is applied uniformly rather than each warehouse's/user's actual negotiated slice.
  */
case class CostSection(
  isEstimate: Boolean,
  totalUsd: Double,
  dailyTotal: Seq[DailyCost],
  cumulativeDaily: Seq[DailyCost],
  byUsageType: Seq[CostByUsageType],
  costByWarehouse: Seq[WarehouseCost],
  topUsersByCost: Seq[TopUserCost],
  topUsersByCostPieSlices: Seq[TopUserCost],
  wastedComputeCostUsd: Double,
  monthlyBill: Seq[MonthlyCostLineItem]
)

/** `taskType` classifies what the task actually runs — `"Stored Procedure Call"` for a `CALL`
  * statement, or the leading SQL verb (`INSERT`, `MERGE`, `COPY INTO`, ...) otherwise. Derived
  * from a single sample run's `query_text` per task, not every run — a task's definition (and so
  * its statement) rarely changes between runs; see [[guild.snowflakeusage.aggregate.Aggregations.classifyTaskType]].
  */
case class TaskRunSummary(taskName: String, taskType: String, totalRuns: Long, succeeded: Long, failed: Long)
case class PipeIngestSummary(pipeName: String, totalCredits: Double, totalFilesInserted: Long, totalBytesInsertedGb: Double)
case class StreamsSummary(totalStreams: Long, staleStreams: Long)

/** Snowflake-native automation/orchestration objects with no 1:1 Databricks equivalent — Tasks
  * (closest analog: Databricks Workflows/Jobs) and Snowpipe (closest analog: Auto Loader) are
  * directly relevant to migration scoping, not just cost. `taskRuns`/`pipes` are empty and
  * `streams` is `None` when their respective views aren't available on this account/edition.
  */
case class AutomationSection(
  taskRuns: Seq[TaskRunSummary],
  pipes: Seq[PipeIngestSummary],
  streams: Option[StreamsSummary]
)

/** Deep-dive into non-person logins: how active they are, and (when `ACCESS_HISTORY` is
  * available) which tables they actually touch — the "what are they doing" follow-up to the
  * people-vs-service split in [[UsersSection.dailyActiveByType]].
  */
/** `readCount`/`writeCount` are distinct queries touching at least one table for that action
  * (0/0 when `ACCESS_HISTORY` isn't available — Enterprise Edition+ only, same gate as
  * [[ServiceAccountTableAccess]]).
  */
case class ServiceAccountSummary(
  userName: String,
  queryCount: Long,
  warehouseCount: Long,
  firstActiveDay: String,
  lastActiveDay: String,
  readCount: Long,
  writeCount: Long
)
case class ServiceAccountTableAccess(userName: String, tableName: String, distinctQueries: Long)
case class ServiceAccountsSection(
  topServiceAccounts: Seq[ServiceAccountSummary],
  topTablesByServiceAccount: Seq[ServiceAccountTableAccess]
)

/** Secure Data Sharing inventory — `shares` covers both directions (`kind` is `OUTBOUND`/`INBOUND`).
  * No 1:1 Databricks equivalent (closest analog: Delta Sharing), relevant to migration scoping.
  */
case class ShareSummary(shareName: String, kind: String, databaseName: String, toAccounts: String)
case class DataSharingSection(totalShares: Long, outboundShares: Long, inboundShares: Long, shares: Seq[ShareSummary])

/** Materialized view refresh activity — Snowflake-native automated maintenance, grouped with
  * Tasks/Snowpipe/Streams in the Automation & Orchestration section.
  */
case class MvRefreshSummary(viewName: String, totalRefreshes: Long, totalCredits: Double, avgDurationSeconds: Double)
case class MaterializedViewsSection(refreshes: Seq[MvRefreshSummary])

/** Cross-cloud/cross-region data transfer (egress) — relevant to migration planning since moving
  * data out of Snowflake's cloud/region can carry its own cost independent of compute/storage.
  */
/** `transferType` is the kind of operation (`DATA_LAKE`, `COPY`, `COPY_FILES`, ...), NOT a
  * direction — `DATA_TRANSFER_HISTORY` carries no explicit ingress/egress flag, only source and
  * target cloud/region, so direction has to be read off comparing those two directly.
  * `crossRegion = false` (source == target) is external-stage I/O within one region, not network
  * egress/ingress in the inter-region sense, even though Snowflake logs/bills it here.
  */
case class DataTransferSummary(
  transferType: String,
  sourceCloud: String,
  sourceRegion: String,
  targetCloud: String,
  targetRegion: String,
  crossRegion: Boolean,
  totalGb: Double
)
case class DataTransferSection(totalGb: Double, byDestination: Seq[DataTransferSummary])

/** Account-wide object counts — the full inventory surface area a migration needs to cover,
  * beyond just the tables already tracked in [[StorageSection]].
  */
case class ObjectCount(objectType: String, count: Long)
case class ObjectInventorySection(counts: Seq[ObjectCount])

/** What's actually connecting to Snowflake, from `ACCOUNT_USAGE.SESSIONS.CLIENT_APPLICATION_ID`
  * — BI tools and drivers need a re-point in a Databricks migration; this is the inventory of
  * what needs one.
  */
case class ClientBreakdown(clientApplicationId: String, sessionCount: Long)
case class ClientToolSection(byClient: Seq[ClientBreakdown])

/** Snowflake's actual cold-storage feature — opt-in per table, not automatic. A report showing
  * `totalPolicies = 0` means this account genuinely isn't using it (this view is account-wide,
  * not role-scoped the way `SHOW STREAMS` is), not "not visible to this role."
  */
case class LifecyclePolicySummary(policyName: String, databaseName: String, schemaName: String, archiveTier: String, archiveForDays: Int)
case class StorageLifecycleSection(totalPolicies: Long, policies: Seq[LifecyclePolicySummary])

/** Attributes each login to a known channel by matching `CLIENT_IP` against this account's
  * network-policy IP allowlists (Twingate, dbt Cloud, Fivetran, etc.) — see
  * [[guild.snowflakeusage.aggregate.Aggregations.KNOWN_NETWORK_POLICIES]]. `"Unmatched"` means
  * the login's IP didn't fall in any allowlist this report knows about (most logins from a
  * direct/non-allowlisted network route land here, not necessarily anything suspicious).
  */
case class LoginChannelSummary(channel: String, loginCount: Long)

/** Separate axis from `byChannel` — `LOGIN_HISTORY.REPORTED_CLIENT_TYPE` identifies WHAT
  * connected (`SNOWFLAKE_UI`, `JDBC_DRIVER`, `PYTHON_DRIVER`, ...), independent of WHICH network
  * it came from. A Snowsight login could show up under any IP channel depending on the network
  * the person was on when they opened it.
  */
case class LoginClientTypeSummary(clientType: String, loginCount: Long)

case class LoginChannelSection(totalLogins: Long, byChannel: Seq[LoginChannelSummary], byClientType: Seq[LoginClientTypeSummary])

case class UsageAggregates(
  lookbackDays: Int,
  activity: ActivitySection,
  users: UsersSection,
  storage: StorageSection,
  tableUsage: Option[TableUsageSection],
  cost: Option[CostSection],
  automation: AutomationSection,
  serviceAccounts: Option[ServiceAccountsSection],
  dataSharing: Option[DataSharingSection],
  materializedViews: Option[MaterializedViewsSection],
  dataTransfer: Option[DataTransferSection],
  objectInventory: Option[ObjectInventorySection],
  clientTools: Option[ClientToolSection],
  storageLifecycle: Option[StorageLifecycleSection],
  loginChannels: Option[LoginChannelSection]
)
