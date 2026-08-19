package guild.databricksusage.aggregate

import guild.databricksusage.queries._

import org.apache.spark.sql.Dataset


/** Turns [[RawUsageData]]'s already-`GROUP BY`-aggregated pulls into [[DatabricksUsageAggregates]]
  * — every pull here is already small (bounded by days × SKUs/compute-endpoints/actors, not by
  * usage-event volume), so this is plain Scala `Seq` reshaping, no further Spark aggregation needed.
  */
object Aggregations {

  private val TOP_USERS_BY_COST_LIMIT = 10
  private val TOP_ACTORS_LIMIT = 20
  private val TOP_TABLES_BY_SIZE_LIMIT = 20

  def compute(raw: RawUsageData): DatabricksUsageAggregates = {
    val spNames: Map[String, String] = raw.servicePrincipals.collect().map(r => r.applicationId -> r.displayName).toMap
    def resolve(actor: String): String = displayName(actor, spNames)

    val warehouseNames: Map[String, String] = raw.warehouseNames.collect().map(r => r.warehouseId -> r.name).toMap
    val clusterNames: Map[String, String] = raw.clusterNames.collect().map(r => r.clusterId -> r.clusterName).toMap
    def computeName(warehouseId: Option[String], clusterId: Option[String], computeType: String): String =
      resolveComputeName(warehouseId, clusterId, computeType, warehouseNames, clusterNames)

    val jobNames: Map[String, String] = raw.jobNames.collect().map(r => r.jobId -> r.name).toMap

    val dailyTotal = collectDailyCost(raw.dailyCost)
    val bySku = collectCostBySku(raw.costBySku)
    val dailyByProduct = collectDailyCostByProduct(raw.dailyCostByProduct)
    val totalUsd = dailyTotal.map(_.usd).sum

    val bucketedByUser = raw.costByUser
      .collect()
      .toSeq
      .groupBy(r => resolve(r.actor))
      .toSeq
      .map { case (actor, rows) => TopUserCost(actor, rows.map(_.usdAmount).sum) }
      .sortBy(-_.usd)
    val topUsersByCost = bucketedByUser.take(TOP_USERS_BY_COST_LIMIT)
    val topUsersByCostPieSlices =
      if (bucketedByUser.length <= TOP_USERS_BY_COST_LIMIT + 1) bucketedByUser
      else topUsersByCost :+ TopUserCost("Other", bucketedByUser.drop(TOP_USERS_BY_COST_LIMIT).map(_.usd).sum)

    val monthlyBill = raw.monthlyCostBySku.collect().map(r => MonthlyCostLineItem(r.month, r.skuName, r.usdAmount)).toSeq

    val topActors = raw.topActorsByActions
      .collect()
      .toSeq
      .groupBy(r => resolve(r.actor))
      .toSeq
      .map { case (actor, rows) => TopActor(actor, rows.map(_.actionCount).sum) }
      .sortBy(-_.actionCount)
      .take(TOP_ACTORS_LIMIT)

    val topReadTables = collectLineageAccess(raw.topReadTables)
    val topWriteTables = collectLineageAccess(raw.topWriteTables)

    val readRows = raw.userTableReads.collect().toSeq
    val writeRows = raw.userTableWrites.collect().toSeq
    val sizeByTable: Map[String, Long] = raw.tableSizes.collect().map(r => r.qualifiedName -> r.sizeBytes).toMap
    val readWriteDetails = (readRows.map(r => (resolve(r.actor), r.tableName) -> (r.accessCount, 0L)) ++
      writeRows.map(r => (resolve(r.actor), r.tableName) -> (0L, r.accessCount)))
      .groupBy(_._1)
      .toSeq
      .map { case ((actor, tableName), rows) =>
        val readCount = rows.map(_._2._1).sum
        val writeCount = rows.map(_._2._2).sum
        TableAccessDetail(actor, tableName, readCount, writeCount, sizeByTable.get(tableName))
      }
      .sortBy(r => -(r.readCount + r.writeCount))
      .take(150)

    DatabricksUsageAggregates(
      overview = OverviewSection(
        lookbackDays = raw.lookbackDays,
        totalQueries = raw.overviewStats.totalQueries,
        activeUsers = raw.overviewStats.activeUsers,
        totalCostUsd = totalUsd
      ),
      cost = CostSection(
        totalUsd = totalUsd,
        dailyTotal = dailyTotal,
        bySku = bySku,
        dailyByProduct = dailyByProduct,
        topUsersByCost = topUsersByCost,
        topUsersByCostPieSlices = topUsersByCostPieSlices,
        monthlyBill = monthlyBill
      ),
      activity = ActivitySection(
        dailyQueryVolume = collectDailyQueryVolume(raw.dailyQueryVolume),
        dailyQueryVolumeByType = raw.dailyQueryVolumeByType
          .collect()
          .map(r => DailyQueryVolumeByType(r.day, r.actorType, r.queryCount))
          .toSeq
          .sortBy(_.day),
        dailyDurationSamples = collectDailyDurationStats(raw.dailyDurationStats),
        computeExecStats = collectComputeExecStats(raw.computeExecStats, computeName),
        computeResourceSignals = collectComputeResourceSignals(raw.computeResourceSignals, computeName),
        topErrorReasons = collectTopErrorReasons(raw.topErrorReasons)
      ),
      users = UsersSection(
        dailyActiveUsers = raw.dailyActiveUsers
          .collect()
          .map(r => DailyActiveUsers(r.day, r.actorType, r.distinctUsers, r.actionCount))
          .toSeq
          .sortBy(_.day),
        topActors = topActors,
        byService = raw.actionsByService
          .collect()
          .map(r => ServiceActionCount(r.serviceName, r.actionCount))
          .toSeq
          .sortBy(-_.actionCount)
      ),
      tableAccess = TableAccessSection(
        topReadTables = topReadTables,
        topWriteTables = topWriteTables,
        readWriteDetails = readWriteDetails
      ),
      objectInventory = ObjectInventorySection(
        byTableType = raw.objectInventory
          .collect()
          .map(r => ObjectInventoryByType(r.tableType, r.objectCount))
          .toSeq
          .sortBy(-_.objectCount),
        catalogCount = raw.catalogSchemaCounts.collect().find(_.objectType == "CATALOG").map(_.objectCount).getOrElse(0L),
        schemaCount = raw.catalogSchemaCounts.collect().find(_.objectType == "SCHEMA").map(_.objectCount).getOrElse(0L)
      ),
      automation = AutomationSection(
        jobRuns = raw.jobRunSummary
          .collect()
          .map(r =>
            JobRunSummary(
              jobId = r.jobId,
              jobName = jobNames.getOrElse(r.jobId, r.jobId),
              totalRuns = r.totalRuns,
              succeeded = r.succeeded,
              failed = r.failed,
              avgDurationSeconds = r.avgDurationSeconds,
              totalHours = r.totalExecSeconds / 3600.0
            )
          )
          .toSeq
          .sortBy(-_.totalRuns),
        dailyJobRuns = raw.dailyJobRuns.collect().map(r => DailyJobRun(r.day, r.totalRuns, r.succeeded)).toSeq.sortBy(_.day),
        totalRunHours = raw.jobRunTotals.totalExecSeconds / 3600.0,
        distinctJobsRun = raw.jobRunTotals.distinctJobsRun,
        totalRuns = raw.jobRunTotals.totalRuns,
        successRatePercent =
          if (raw.jobRunTotals.totalRuns > 0) raw.jobRunTotals.succeeded.toDouble / raw.jobRunTotals.totalRuns * 100.0 else 0.0,
        taskTypeBreakdown = raw.jobTasks
          .collect()
          .toSeq
          .groupBy(t => friendlyTaskType(t.taskType))
          .map { case (label, rows) => JobTaskTypeCount(label, rows.length.toLong) }
          .toSeq
          .sortBy(-_.count)
      ),
      serviceAccounts = ServiceAccountsSection(
        topServiceAccounts = raw.serviceAccountQueryStats
          .collect()
          .map(r => ServiceAccountSummary(resolve(r.actor), r.queryCount, r.computeCount, r.firstDay, r.lastDay))
          .toSeq
          .sortBy(-_.queryCount),
        topTablesByServiceAccount = raw.serviceAccountTableAccess
          .collect()
          .map(r => ServiceAccountTableAccess(resolve(r.actor), r.tableName, r.accessCount))
          .toSeq
          .sortBy(-_.accessCount)
      ),
      storage = {
        val allSizes = raw.tableSizes.collect().map(r => TableSize(r.qualifiedName, r.format, r.numFiles, r.sizeBytes)).toSeq
        StorageSection(
          // The full sweep can cover ~1,900 tables — the dashboard only ever needs the largest
          // few to chart, but totalSizeBytes/tablesCounted below still reflect every one of them.
          tableSizes = allSizes.sortBy(-_.sizeBytes).take(TOP_TABLES_BY_SIZE_LIMIT),
          totalSizeBytes = allSizes.map(_.sizeBytes).sum,
          tablesCounted = allSizes.length
        )
      },
      databricksNative = DatabricksNativeSection(
        totalExperiments = raw.mlflowSummary.totalExperiments,
        totalMlflowRuns = raw.mlflowSummary.totalRuns,
        finishedMlflowRuns = raw.mlflowSummary.finishedRuns,
        activeServingEndpoints = raw.servingEndpointTypes.collect().map(_.count).sum,
        servingEndpointsByType = raw.servingEndpointTypes
          .collect()
          .map(r => MlEndpointTypeCount(r.entityType, r.count))
          .toSeq
          .sortBy(-_.count),
        servingRequests = raw.servingUsageSummary.totalRequests,
        servingTokens = raw.servingUsageSummary.totalTokens,
        aiGatewayRequests = raw.aiGatewayUsage.collect().map(_.requestCount).sum,
        aiGatewayTokens = raw.aiGatewayUsage.collect().map(_.totalTokens).sum,
        aiGatewayByDestination = raw.aiGatewayUsage
          .collect()
          .map(r => AiGatewayDestination(r.destinationType, r.destinationName, r.requestCount, r.totalTokens))
          .toSeq
          .sortBy(-_.requestCount),
        tablesUnderQualityMonitoring = raw.dataQualityMonitoringSummary.tablesMonitored,
        qualityCheckRuns = raw.dataQualityMonitoringSummary.checkRuns,
        healthyQualityRuns = raw.dataQualityMonitoringSummary.healthyRuns,
        dataClassificationResults = raw.dataClassificationSummary.totalResults,
        tablesClassified = raw.dataClassificationSummary.tablesClassified
      )
    )
  }


  /** Resolves a raw actor identity into what the report should actually display, in priority
    * order: (1) an `@guild.com` employee login is lumped into one bucket, mirroring
    * [[guild.snowflakeusage.aggregate.Aggregations.bucketPeople]]'s exact convention (adjusted
    * for this workspace's real domain); (2) a UUID service-principal application id resolved via
    * `servicePrincipalNames` (from [[guild.databricksusage.DatabricksJdbc.loadServicePrincipals]])
    * to its real, individually-distinguishable name — unlike the employee case, service
    * principals are NOT collapsed into each other, since which specific service is doing what is
    * exactly what's interesting to see; (3) anything else (a non-`@guild.com` person's email from
    * some other domain, the literal `"System-User"`, or a UUID with no SCIM match) passes through
    * unchanged.
    */
  def displayName(actor: String, servicePrincipalNames: Map[String, String]): String =
    if (isEmployeeEmail(actor)) "Employee@guild.com" else servicePrincipalNames.getOrElse(actor, actor)

  private def isEmployeeEmail(actor: String): Boolean = Option(actor).exists(_.toLowerCase.endsWith("@guild.com"))

  private def collectDailyCost(ds: Dataset[DailyCostRow]): Seq[DailyCost] =
    ds.collect().map(r => DailyCost(r.day, r.usdAmount)).sortBy(_.day)

  private def collectCostBySku(ds: Dataset[CostBySkuRow]): Seq[CostBySku] =
    ds.collect().map(r => CostBySku(r.skuName, r.usdAmount, r.dbuQuantity)).sortBy(-_.usd)

  private def collectDailyCostByProduct(ds: Dataset[DailyCostByProductRow]): Seq[DailyCostByProduct] =
    ds.collect().map(r => DailyCostByProduct(r.day, r.product, r.usdAmount)).sortBy(_.day)

  private def collectDailyQueryVolume(ds: Dataset[DailyQueryVolumeRow]): Seq[DailyQueryVolume] =
    ds.collect().map(r => DailyQueryVolume(r.day, r.queryCount, r.activeUsers)).sortBy(_.day)

  private def collectDailyDurationStats(ds: Dataset[DailyDurationStatsRow]): Seq[DailyDurationSample] =
    ds.collect().map(r => DailyDurationSample(r.day, r.minMs, r.q1Ms, r.medianMs, r.q3Ms, r.maxMs)).sortBy(_.day)

  /** Warehouse/cluster's real name if `warehouseId`/`clusterId` matches one in the respective
    * name map, else the raw id itself, else `computeType` — serverless compute has no fixed id
    * of either kind, so there's nothing to resolve or fall back to there.
    */
  def resolveComputeName(
    warehouseId: Option[String],
    clusterId: Option[String],
    computeType: String,
    warehouseNames: Map[String, String],
    clusterNames: Map[String, String]
  ): String =
    warehouseId
      .map(id => warehouseNames.getOrElse(id, id))
      .orElse(clusterId.map(id => clusterNames.getOrElse(id, id)))
      .getOrElse(computeType)

  /** Maps a job task's `*_task` JSON key to a human label — anything not in this list (a task
    * type added to the Jobs API after this was written) falls back to a title-cased version of
    * the raw key rather than silently disappearing from the breakdown.
    */
  private val TASK_TYPE_LABELS: Map[String, String] = Map(
    "notebook_task" -> "Notebook",
    "spark_jar_task" -> "JAR",
    "python_wheel_task" -> "Python Wheel",
    "spark_python_task" -> "Python Script",
    "spark_submit_task" -> "Spark Submit",
    "sql_task" -> "SQL",
    "dbt_task" -> "dbt",
    "pipeline_task" -> "DLT Pipeline",
    "run_job_task" -> "Run Job",
    "for_each_task" -> "For Each",
    "condition_task" -> "Condition",
    "dashboard_task" -> "Dashboard",
    "unknown_task" -> "Unknown"
  )

  private def friendlyTaskType(taskType: String): String =
    TASK_TYPE_LABELS.getOrElse(taskType, taskType.stripSuffix("_task").split("_").map(_.capitalize).mkString(" "))

  private def collectComputeExecStats(
    ds: Dataset[ComputeExecStatsRow],
    computeName: (Option[String], Option[String], String) => String
  ): Seq[ComputeExecSeconds] =
    ds.collect()
      .map(r =>
        ComputeExecSeconds(
          computeId = computeName(r.warehouseId, r.clusterId, r.computeType),
          computeType = r.computeType,
          totalQueries = r.totalQueries,
          totalExecMs = r.totalExecMs,
          failedCount = r.failedCount,
          failedExecMs = r.failedExecMs
        )
      )
      .sortBy(-_.totalQueries)

  private def collectComputeResourceSignals(
    ds: Dataset[ComputeResourceSignalRow],
    computeName: (Option[String], Option[String], String) => String
  ): Seq[ComputeResourceSignal] =
    ds.collect()
      .map(r =>
        ComputeResourceSignal(
          computeId = computeName(r.warehouseId, r.clusterId, r.computeType),
          computeType = r.computeType,
          queryCount = r.queryCount,
          queriesWithSpill = r.queriesWithSpill,
          totalSpilledBytes = r.totalSpilledBytes,
          queriesWithQueueing = r.queriesWithQueueing,
          totalQueuedMs = r.totalQueuedMs
        )
      )
      .sortBy(-_.queryCount)

  private def collectTopErrorReasons(ds: Dataset[QueryErrorReasonRow]): Seq[TopErrorReason] =
    ds.collect().map(r => TopErrorReason(r.errorClass, r.sampleMessage, r.count)).sortBy(-_.count)

  private def collectLineageAccess(ds: Dataset[TableLineageAccessRow]): Seq[TableLineageAccess] =
    ds.collect().map(r => TableLineageAccess(r.direction, r.tableName, r.accessCount)).sortBy(-_.accessCount)
}
