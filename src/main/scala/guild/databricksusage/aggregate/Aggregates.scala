package guild.databricksusage.aggregate

import guild.databricksusage.queries._

import org.apache.spark.sql.Dataset


/** Raw pulls handed to [[Aggregations.compute]] — every field is already `GROUP BY`-aggregated
  * in SQL (see [[guild.databricksusage.queries.Queries]]), so nothing here needs Spark-side
  * `groupByKey`/`mapGroups` the way the Snowflake tool's raw pulls originally did. The one
  * exception is `tableSizes`, a full `DESCRIBE DETAIL` sweep (see
  * [[guild.databricksusage.DatabricksJdbc.loadAllTableSizes]]) rather than a `GROUP BY`.
  */
case class RawUsageData(
  dailyCost: Dataset[DailyCostRow],
  costBySku: Dataset[CostBySkuRow],
  dailyCostByProduct: Dataset[DailyCostByProductRow],
  overviewStats: OverviewStatsRow,
  dailyQueryVolume: Dataset[DailyQueryVolumeRow],
  dailyDurationStats: Dataset[DailyDurationStatsRow],
  computeExecStats: Dataset[ComputeExecStatsRow],
  computeResourceSignals: Dataset[ComputeResourceSignalRow],
  topErrorReasons: Dataset[QueryErrorReasonRow],
  dailyActiveUsers: Dataset[DailyActiveUsersRow],
  topActorsByActions: Dataset[TopActorRow],
  actionsByService: Dataset[ServiceActionCountRow],
  topReadTables: Dataset[TableLineageAccessRow],
  topWriteTables: Dataset[TableLineageAccessRow],
  objectInventory: Dataset[ObjectInventoryRow],
  jobRunSummary: Dataset[JobRunSummaryRow],
  dailyJobRuns: Dataset[DailyJobRunRow],
  costByUser: Dataset[CostByUserRow],
  monthlyCostBySku: Dataset[MonthlyCostBySkuRow],
  dailyQueryVolumeByType: Dataset[DailyQueryVolumeByTypeRow],
  serviceAccountQueryStats: Dataset[ServiceAccountQueryStatsRow],
  serviceAccountTableAccess: Dataset[ServiceAccountTableAccessRow],
  userTableReads: Dataset[UserTableAccessRow],
  userTableWrites: Dataset[UserTableAccessRow],
  catalogSchemaCounts: Dataset[CatalogSchemaCountRow],
  tableSizes: Dataset[TableSizeRow],
  servicePrincipals: Dataset[ServicePrincipalRow],
  jobRunTotals: JobRunTotalsRow,
  jobNames: Dataset[JobNameRow],
  warehouseNames: Dataset[WarehouseNameRow],
  clusterNames: Dataset[ClusterNameRow],
  jobTasks: Dataset[JobTaskRow],
  mlflowSummary: MlflowSummaryRow,
  servingEndpointTypes: Dataset[ServingEndpointTypeRow],
  servingUsageSummary: ServingUsageSummaryRow,
  aiGatewayUsage: Dataset[AiGatewayUsageRow],
  dataQualityMonitoringSummary: DataQualityMonitoringSummaryRow,
  dataClassificationSummary: DataClassificationSummaryRow,
  lookbackDays: Int
)

case class DailyCost(day: String, usd: Double)

/** Total cost and raw DBU quantity per SKU over the lookback window. */
case class CostBySku(skuName: String, usd: Double, dbuQuantity: Double)

case class DailyCostByProduct(day: String, product: String, usd: Double)

/** `actor` resolved through [[Aggregations.displayName]] — see there for why. */
case class TopUserCost(actor: String, usd: Double)

/** One (month, SKU) cell of the itemized monthly bill — see
  * [[guild.databricksusage.queries.MonthlyCostBySkuRow]] for why this is list-price, not real
  * billed dollars, unlike Snowflake's equivalent.
  */
case class MonthlyCostLineItem(month: String, sku: String, usd: Double)

/** `totalUsd`/`dailyTotal`/`bySku`/`dailyByProduct`/`topUsersByCost`/`monthlyBill` are all
  * list-price estimates, not real billed dollars — see
  * [[guild.databricksusage.queries.DailyCostRow]]. There's no per-SKU real-dollar breakdown
  * available, so they stay list-price even when `realTotalUsd` is present.
  * `topUsersByCost`/`topUsersByCostPieSlices` exclude usage with no `identity_metadata.run_as`
  * attribution, so `topUsersByCost.map(_.usd).sum` will be less than `totalUsd`.
  *
  * `realTotalUsd` is the one exception: real, negotiated-rate total cost for the same lookback
  * window, from a manually-exported Account Console CSV (see [[guild.databricksusage.ManualBillableUsage]]).
  * `None` when no export is present or it doesn't cover the full window.
  */
case class CostSection(
  totalUsd: Double,
  dailyTotal: Seq[DailyCost],
  bySku: Seq[CostBySku],
  dailyByProduct: Seq[DailyCostByProduct],
  topUsersByCost: Seq[TopUserCost],
  topUsersByCostPieSlices: Seq[TopUserCost],
  monthlyBill: Seq[MonthlyCostLineItem],
  realTotalUsd: Option[Double] = None
)

case class OverviewSection(lookbackDays: Int, totalQueries: Long, activeUsers: Long, totalCostUsd: Double)

case class DailyQueryVolume(day: String, queryCount: Long, activeUsers: Long)

case class DailyQueryVolumeByType(day: String, actorType: String, queryCount: Long)

/** `minMs`/`q1Ms`/`medianMs`/`q3Ms`/`maxMs` stay `Long` end to end — see
  * [[guild.databricksusage.queries.DailyDurationStatsRow]] for why (Databricks'
  * `approx_percentile` over a `BIGINT` column returns `BIGINT`, unlike Snowflake's always-
  * fractional `APPROX_PERCENTILE`). Cast to `Double` only at the chart-rendering boundary.
  */
case class DailyDurationSample(day: String, minMs: Long, q1Ms: Long, medianMs: Long, q3Ms: Long, maxMs: Long)

/** `computeId` is the warehouse/cluster's real name where [[Aggregations.resolveComputeName]]
  * found one, else the raw `warehouseId`/`clusterId`, else `computeType` — serverless compute has
  * no fixed id of either kind, so there's nothing to resolve or fall back to there.
  */
case class ComputeExecSeconds(
  computeId: String,
  computeType: String,
  totalQueries: Long,
  totalExecMs: Long,
  failedCount: Long,
  failedExecMs: Long
)

case class ComputeResourceSignal(
  computeId: String,
  computeType: String,
  queryCount: Long,
  queriesWithSpill: Long,
  totalSpilledBytes: Long,
  queriesWithQueueing: Long,
  totalQueuedMs: Long
)

case class TopErrorReason(errorClass: String, sampleMessage: String, count: Long)

case class ActivitySection(
  dailyQueryVolume: Seq[DailyQueryVolume],
  dailyQueryVolumeByType: Seq[DailyQueryVolumeByType],
  dailyDurationSamples: Seq[DailyDurationSample],
  computeExecStats: Seq[ComputeExecSeconds],
  computeResourceSignals: Seq[ComputeResourceSignal],
  topErrorReasons: Seq[TopErrorReason]
)

case class DailyActiveUsers(day: String, actorType: String, distinctUsers: Long, actionCount: Long)

/** `actor` resolved through [[Aggregations.displayName]]. */
case class TopActor(actor: String, actionCount: Long)
case class ServiceActionCount(serviceName: String, actionCount: Long)

case class UsersSection(dailyActiveUsers: Seq[DailyActiveUsers], topActors: Seq[TopActor], byService: Seq[ServiceActionCount])

case class TableLineageAccess(direction: String, tableName: String, accessCount: Long)

/** `actor` resolved through [[Aggregations.displayName]]; `sizeBytes` covers essentially every
  * real table now that the storage sweep is a full inventory, not a bounded sample — `None`
  * only for the rare table a `DESCRIBE DETAIL` genuinely failed against (see `StorageSection`),
  * not a false zero.
  */
case class TableAccessDetail(actor: String, tableName: String, readCount: Long, writeCount: Long, sizeBytes: Option[Long])

case class TableAccessSection(
  topReadTables: Seq[TableLineageAccess],
  topWriteTables: Seq[TableLineageAccess],
  readWriteDetails: Seq[TableAccessDetail]
)

case class ObjectInventoryByType(tableType: String, objectCount: Long)

case class ObjectInventorySection(byTableType: Seq[ObjectInventoryByType], catalogCount: Long, schemaCount: Long)

/** `jobName` falls back to the raw `jobId` when `system.lakeflow.jobs` has no current-state
  * match (a job deleted since it last ran). `totalHours` is the real per-job total, not
  * `avgDurationSeconds * totalRuns` — summed directly in SQL.
  */
case class JobRunSummary(
  jobId: String,
  jobName: String,
  totalRuns: Long,
  succeeded: Long,
  failed: Long,
  avgDurationSeconds: Double,
  totalHours: Double
)
case class DailyJobRun(day: String, totalRuns: Long, succeeded: Long)

/** Friendly label for one of a job's task-JSON `*_task` keys (e.g. `"Notebook"` for
  * `notebook_task`) — see [[guild.databricksusage.DatabricksJdbc.loadJobTasks]] for why this can
  * only ever cover the same bounded top-N jobs [[AutomationSection.jobRuns]] shows.
  */
case class JobTaskTypeCount(taskType: String, count: Long)

/** `totalRunHours`/`distinctJobsRun`/`totalRuns`/`successRatePercent` are unbounded totals over
  * the WHOLE lookback window (see [[guild.databricksusage.queries.JobRunTotalsRow]]) — NOT
  * derived from `jobRuns`, which is capped to the top 20 by run count and would understate all
  * four (mixing a true total next to a top-20-only figure in the same stat row would be actively
  * misleading, not just incomplete). `taskTypeBreakdown` is the one exception to "every figure
  * here is a true total": it only covers the jobs in `jobRuns`, a real, called-out scope limit.
  */
case class AutomationSection(
  jobRuns: Seq[JobRunSummary],
  dailyJobRuns: Seq[DailyJobRun],
  totalRunHours: Double,
  distinctJobsRun: Long,
  totalRuns: Long,
  successRatePercent: Double,
  taskTypeBreakdown: Seq[JobTaskTypeCount]
)

/** `actor` here is a service principal by construction (`executed_by NOT LIKE '%@%'`), so
  * [[Aggregations.displayName]] never applies the `Employee@guild.com` bucket to it — but it IS
  * still resolved from a raw UUID to its real service-principal name where one exists.
  */
case class ServiceAccountSummary(actor: String, queryCount: Long, computeCount: Long, firstDay: String, lastDay: String)
case class ServiceAccountTableAccess(actor: String, tableName: String, accessCount: Long)

case class ServiceAccountsSection(topServiceAccounts: Seq[ServiceAccountSummary], topTablesByServiceAccount: Seq[ServiceAccountTableAccess])

case class TableSize(qualifiedName: String, format: String, numFiles: Long, sizeBytes: Long)

/** No Unity Catalog system table exposes per-table size the way Snowflake's
  * `TABLE_STORAGE_METRICS` does — Databricks delegates most storage to the customer's own cloud
  * account and doesn't meter it centrally. `tableSizes`/`totalSizeBytes` come from a real, full
  * `DESCRIBE DETAIL` sweep across every `MANAGED`/`EXTERNAL`/`STREAMING_TABLE`/
  * `MATERIALIZED_VIEW` table (see [[guild.databricksusage.DatabricksJdbc.loadAllTableSizes]]),
  * not a bounded sample — `totalSizeBytes` is a genuine account-wide total, comparable to
  * Snowflake's `StorageSummary.totalActiveGb`. `VIEW`s have no storage of their own and
  * `FOREIGN` tables' data lives in the source system, not here, so neither is counted.
  */
case class StorageSection(tableSizes: Seq[TableSize], totalSizeBytes: Long, tablesCounted: Long)

case class MlEndpointTypeCount(entityType: String, count: Long)
case class AiGatewayDestination(destinationType: String, destinationName: String, requestCount: Long, totalTokens: Long)

/** Real usage of Databricks-native platform capabilities that either have no Snowflake
  * equivalent measured in that report (MLflow, Model Serving, AI Gateway, Lakehouse Monitoring)
  * or are included for completeness despite Snowflake having its own native answer (Data
  * Classification — see [[guild.databricksusage.queries.Rows.DataClassificationSummaryRow]]).
  * Feeds the Comparison report's capability-parity matrix with real numbers, not just a
  * qualitative row.
  */
case class DatabricksNativeSection(
  totalExperiments: Long,
  totalMlflowRuns: Long,
  finishedMlflowRuns: Long,
  activeServingEndpoints: Long,
  servingEndpointsByType: Seq[MlEndpointTypeCount],
  servingRequests: Long,
  servingTokens: Long,
  aiGatewayRequests: Long,
  aiGatewayTokens: Long,
  aiGatewayByDestination: Seq[AiGatewayDestination],
  tablesUnderQualityMonitoring: Long,
  qualityCheckRuns: Long,
  healthyQualityRuns: Long,
  dataClassificationResults: Long,
  tablesClassified: Long
)

case class DatabricksUsageAggregates(
  overview: OverviewSection,
  cost: CostSection,
  activity: ActivitySection,
  users: UsersSection,
  tableAccess: TableAccessSection,
  objectInventory: ObjectInventorySection,
  automation: AutomationSection,
  serviceAccounts: ServiceAccountsSection,
  storage: StorageSection,
  databricksNative: DatabricksNativeSection
)
