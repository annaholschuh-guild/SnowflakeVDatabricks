package guild.databricksusage

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import guild.databricksusage.aggregate.{Aggregations, DatabricksUsageAggregates, RawUsageData}
import guild.databricksusage.dashboard.DatabricksDashboard
import guild.databricksusage.queries._
import guild.reportkit.{PlatformSummary, PlatformSummaryIo}

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.io.File
import java.time.{Instant, LocalDate}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}


/** Entry point: pull `system.*` usage tables from Databricks (or reuse a cached local pull),
  * aggregate with Spark, and write the offline HTML/Plotly report — the Databricks analog to
  * [[guild.snowflakeusage.UsageReportRunner]]. Currently covers Overview + Cost only (Phase 2,
  * Milestone 2 of the migration-tooling plan); further `system.*` sections land in later
  * milestones as separate batches, same discipline as the Snowflake tool's own Phase 1 rewrite.
  *
  * Usage (reads host/token from `~/.databrickscfg`, the same file `databricks auth login` writes):
  *   sbt "runMain guild.databricksusage.DatabricksUsageReportRunner --lookback-days 30"
  *
  * Rebuild the report from the last pull without hitting Databricks again:
  *   sbt "runMain guild.databricksusage.DatabricksUsageReportRunner --skip-fetch"
  */
object DatabricksUsageReportRunner {

  def main(args: Array[String]): Unit = {
    val cfg = new DatabricksConfig(args)
    val spark = DatabricksJdbc.buildSparkSession()
    spark.sparkContext.setLogLevel("WARN")

    try {
      val raw = if (cfg.skipFetch()) loadFromCache(spark, cfg) else fetchAndCache(spark, cfg)
      val agg = withRealBilledCost(Aggregations.compute(raw), cfg)
      DatabricksDashboard.generate(agg, cfg.outputPath(), cfg.host.toOption.getOrElse("UNKNOWN"))
      PlatformSummaryIo.write(buildPlatformSummary(agg), "output/databricks-summary.json")
    } finally {
      spark.stop()
    }
  }

  /** Overlays a real (negotiated-rate) total cost from a manually-exported Account Console CSV
    * onto `agg.cost.realTotalUsd`, if one is present and covers the full lookback window — see
    * [[ManualBillableUsage]] for why this can't be pulled automatically. A no-op (returns `agg`
    * unchanged) when no export is configured/present, so this is safe to call unconditionally.
    */
  private def withRealBilledCost(agg: DatabricksUsageAggregates, cfg: DatabricksConfig): DatabricksUsageAggregates = {
    // Windowed off agg.cost.dailyTotal's own min/max day, not agg.overview.lookbackDays or
    // cfg.lookbackDays() — under --skip-fetch, the configured lookback can silently disagree with
    // what the cached data was actually fetched at (loadFromCache never persisted the fetch's own
    // lookback), so the list-price data's own date range is the only ground truth for what window
    // the real total needs to cover.
    val realTotalUsd = for {
      days <- Some(agg.cost.dailyTotal.map(_.day)).filter(_.nonEmpty)
      rows <- ManualBillableUsage.loadIfPresent(cfg.billableUsageCsv())
      total <- ManualBillableUsage.totalForDateRange(rows, LocalDate.parse(days.min), LocalDate.parse(days.max))
    } yield total
    realTotalUsd match {
      case Some(usd) =>
        println(f"Real billed total from ${cfg.billableUsageCsv()}: $$$usd%,.2f (list-price estimate was $$${agg.cost.totalUsd}%,.2f)")
        agg.copy(cost = agg.cost.copy(realTotalUsd = Some(usd)))
      case None => agg
    }
  }

  /** A small, cross-platform-comparable slice of `agg` — see [[guild.reportkit.PlatformSummary]]
    * for what this feeds (`guild.comparison.ComparisonReportRunner`) and why only these fields.
    */
  private def buildPlatformSummary(agg: DatabricksUsageAggregates): PlatformSummary = {
    val byType = agg.users.dailyActiveUsers.groupBy(_.actorType)
    def avgActiveUsers(actorType: String): Double = {
      val rows = byType.getOrElse(actorType, Seq.empty)
      if (rows.isEmpty) 0.0 else rows.map(_.distinctUsers).sum.toDouble / rows.map(_.day).distinct.size.max(1)
    }

    PlatformSummary(
      platform = "Databricks",
      lookbackDays = agg.overview.lookbackDays,
      generatedAt = Instant.now().toString,
      // Real (negotiated-rate) when a matching Account Console export is present, else the same
      // list-price estimate as before — see withRealBilledCost.
      totalCostUsd = agg.cost.realTotalUsd.getOrElse(agg.cost.totalUsd),
      costIsEstimate = agg.cost.realTotalUsd.isEmpty,
      // Compute-only cost has no real per-SKU breakdown available even when the real TOTAL is
      // known — it's always derived from list-price SKU filtering, so it stays an estimate
      // regardless of totalCostUsd's basis.
      computeCostIsEstimate = true,
      // Account-wide real/list ratio — see PlatformSummary.realBilledDiscountRatio for why this
      // exists and its important caveat (it's observed across ALL usage types, not confirmed to
      // apply uniformly to compute specifically).
      realBilledDiscountRatio = agg.cost.realTotalUsd.map(_ / agg.cost.totalUsd),
      // Every real SKU name on this workspace that represents compute contains "COMPUTE"
      // (STANDARD_ALL_PURPOSE_COMPUTE, ENTERPRISE_SERVERLESS_SQL_COMPUTE_*, etc.) — same
      // compute-only intent as Snowflake's costByWarehouse, via the one pattern that reliably
      // holds across this workspace's real data rather than an exhaustive SKU allowlist.
      computeCostUsd = agg.cost.bySku.filter(_.skuName.contains("COMPUTE")).map(_.usd).sum,
      totalComputeSeconds = agg.activity.computeExecStats.map(_.totalExecMs).sum / 1000.0,
      totalQueries = agg.overview.totalQueries,
      activeUsersHuman = avgActiveUsers("person"),
      activeUsersService = avgActiveUsers("service"),
      failedQueryCount = agg.activity.computeExecStats.map(_.failedCount).sum,
      totalTables = agg.objectInventory.byTableType.map(_.objectCount).sum,
      totalSchemas = agg.objectInventory.schemaCount,
      totalDatabasesOrCatalogs = agg.objectInventory.catalogCount,
      // A real, complete account-wide total now (a full DESCRIBE DETAIL sweep, not a bounded
      // sample) — genuinely comparable to Snowflake's totalActiveGb.
      totalStorageGb = Some(agg.storage.totalSizeBytes / (1024.0 * 1024.0 * 1024.0)),
      automationRunCount = agg.automation.totalRuns,
      automationSuccessRatePercent = agg.automation.successRatePercent
    )
  }

  private def fetchAndCache(spark: SparkSession, cfg: DatabricksConfig): RawUsageData = {
    import spark.implicits._
    val lookback = cfg.lookbackDays()
    val dir = cfg.cacheDir()

    println("Connecting to Databricks (auto-discovering a SQL warehouse)...")
    val conn = DatabricksJdbc.openConnection(cfg)
    try {
      println("Fetching overview stats...")
      val overviewStats = DatabricksJdbc.loadOverviewStats(conn, lookback)
      cacheScalar(overviewStats, dir, "overview_stats")

      println("Fetching daily cost...")
      val dailyCost = DatabricksJdbc.loadDailyCost(spark, conn, lookback)
      cache(dailyCost, dir, "daily_cost")

      println("Fetching cost by SKU...")
      val costBySku = DatabricksJdbc.loadCostBySku(spark, conn, lookback)
      cache(costBySku, dir, "cost_by_sku")

      println("Fetching daily cost by product...")
      val dailyCostByProduct = DatabricksJdbc.loadDailyCostByProduct(spark, conn, lookback)
      cache(dailyCostByProduct, dir, "daily_cost_by_product")

      // system.query.history is pulled pre-aggregated (GROUP BY in Databricks SQL), never as raw
      // rows — system.access.audit alone runs ~46M rows/week on this workspace, and query.history
      // is headed the same direction at longer lookbacks, so this batch adopts the Snowflake
      // tool's Phase 1 discipline from day one rather than relearning it.
      println("Fetching daily query volume...")
      val dailyQueryVolume = DatabricksJdbc.loadDailyQueryVolume(spark, conn, lookback)
      cache(dailyQueryVolume, dir, "daily_query_volume")

      println("Fetching daily duration stats...")
      val dailyDurationStats = DatabricksJdbc.loadDailyDurationStats(spark, conn, lookback)
      cache(dailyDurationStats, dir, "daily_duration_stats")

      println("Fetching compute exec stats...")
      val computeExecStats = DatabricksJdbc.loadComputeExecStats(spark, conn, lookback)
      cache(computeExecStats, dir, "compute_exec_stats")

      println("Fetching compute resource signals...")
      val computeResourceSignals = DatabricksJdbc.loadComputeResourceSignals(spark, conn, lookback)
      cache(computeResourceSignals, dir, "compute_resource_signals")

      println("Fetching top error reasons...")
      val topErrorReasons = DatabricksJdbc.loadTopErrorReasons(spark, conn, lookback)
      cache(topErrorReasons, dir, "top_error_reasons")

      // system.access.audit is by far the largest table on this workspace (~46M rows/week) —
      // every pull below is GROUP BY-aggregated in SQL, no exceptions.
      println("Fetching daily active users...")
      val dailyActiveUsers = DatabricksJdbc.loadDailyActiveUsers(spark, conn, lookback)
      cache(dailyActiveUsers, dir, "daily_active_users")

      println("Fetching top actors by actions...")
      val topActorsByActions = DatabricksJdbc.loadTopActorsByActions(spark, conn, lookback)
      cache(topActorsByActions, dir, "top_actors_by_actions")

      println("Fetching actions by service area...")
      val actionsByService = DatabricksJdbc.loadActionsByService(spark, conn, lookback)
      cache(actionsByService, dir, "actions_by_service")

      println("Fetching top read tables (table lineage)...")
      val topReadTables = DatabricksJdbc.loadTopReadTables(spark, conn, lookback)
      cache(topReadTables, dir, "top_read_tables")

      println("Fetching top write tables (table lineage)...")
      val topWriteTables = DatabricksJdbc.loadTopWriteTables(spark, conn, lookback)
      cache(topWriteTables, dir, "top_write_tables")

      println("Fetching object inventory...")
      val objectInventory = DatabricksJdbc.loadObjectInventory(spark, conn)
      cache(objectInventory, dir, "object_inventory")

      println("Fetching job run summary...")
      val jobRunSummary = DatabricksJdbc.loadJobRunSummary(spark, conn, lookback)
      cache(jobRunSummary, dir, "job_run_summary")

      println("Fetching daily job runs...")
      val dailyJobRuns = DatabricksJdbc.loadDailyJobRuns(spark, conn, lookback)
      cache(dailyJobRuns, dir, "daily_job_runs")

      println("Fetching job run totals (unbounded)...")
      val jobRunTotals = DatabricksJdbc.loadJobRunTotals(conn, lookback)
      cacheScalar(jobRunTotals, dir, "job_run_totals")

      println("Fetching job names...")
      val jobNames = DatabricksJdbc.loadJobNames(spark, conn)
      cache(jobNames, dir, "job_names")

      println("Fetching cluster names...")
      val clusterNames = DatabricksJdbc.loadClusterNames(spark, conn)
      cache(clusterNames, dir, "cluster_names")

      // A REST call, not SQL — no system table carries a warehouse's name, only its id.
      println("Fetching warehouse names...")
      val warehouseNames = DatabricksJdbc.loadWarehouseNames(spark, cfg)
      cache(warehouseNames, dir, "warehouse_names")

      // Bounded to the same top-N jobs jobRunSummary already shows — see loadJobTasks.
      val topJobIds = jobRunSummary.collect().map(_.jobId).toSeq
      println(s"Fetching task configuration for ${topJobIds.length} top jobs...")
      val jobTasks = DatabricksJdbc.loadJobTasks(spark, cfg, topJobIds)
      cache(jobTasks, dir, "job_tasks")

      println("Fetching cost by user...")
      val costByUser = DatabricksJdbc.loadCostByUser(spark, conn, lookback)
      cache(costByUser, dir, "cost_by_user")

      println("Fetching monthly cost by SKU...")
      val monthlyCostBySku = DatabricksJdbc.loadMonthlyCostBySku(spark, conn, lookback)
      cache(monthlyCostBySku, dir, "monthly_cost_by_sku")

      println("Fetching daily query volume by user type...")
      val dailyQueryVolumeByType = DatabricksJdbc.loadDailyQueryVolumeByType(spark, conn, lookback)
      cache(dailyQueryVolumeByType, dir, "daily_query_volume_by_type")

      println("Fetching service account query stats...")
      val serviceAccountQueryStats = DatabricksJdbc.loadServiceAccountQueryStats(spark, conn, lookback)
      cache(serviceAccountQueryStats, dir, "service_account_query_stats")

      println("Fetching service account table access...")
      val serviceAccountTableAccess = DatabricksJdbc.loadServiceAccountTableAccess(spark, conn, lookback)
      cache(serviceAccountTableAccess, dir, "service_account_table_access")

      println("Fetching user table reads...")
      val userTableReads = DatabricksJdbc.loadUserTableReads(spark, conn, lookback)
      cache(userTableReads, dir, "user_table_reads")

      println("Fetching user table writes...")
      val userTableWrites = DatabricksJdbc.loadUserTableWrites(spark, conn, lookback)
      cache(userTableWrites, dir, "user_table_writes")

      println("Fetching catalog/schema counts...")
      val catalogSchemaCounts = DatabricksJdbc.loadCatalogSchemaCounts(spark, conn)
      cache(catalogSchemaCounts, dir, "catalog_schema_counts")

      // A plain REST call (SCIM), not SQL — no system table carries a service principal's
      // human-readable name, only the raw UUID every other pull above sees as executed_by/
      // created_by/user_identity.email.
      println("Fetching service principal display names...")
      val servicePrincipals = DatabricksJdbc.loadServicePrincipals(spark, cfg)
      cache(servicePrincipals, dir, "service_principals")

      // A full inventory, not a sample — there's no system table exposing size across every
      // table at once (see Rows.TableSizeRow), but DESCRIBE DETAIL spread over several parallel
      // connections makes a real, complete total feasible in a few minutes rather than the ~30
      // it'd take sequentially.
      val storageBearingTableNames = DatabricksJdbc.loadStorageBearingTableNames(conn)
      println(s"Fetching table sizes for all ${storageBearingTableNames.length} storage-bearing tables (DESCRIBE DETAIL, parallelized)...")
      val tableSizes = spark.createDataset(DatabricksJdbc.loadAllTableSizes(cfg, storageBearingTableNames))
      cache(tableSizes, dir, "table_sizes")

      // Databricks-native platform capabilities with no Snowflake equivalent measured in that
      // report (MLflow, Model Serving, AI Gateway, Lakehouse Monitoring) — see
      // guild.databricksusage.aggregate.Aggregates.DatabricksNativeSection. Each is Try-wrapped:
      // at least system.data_quality_monitoring lives in storage that some warehouse
      // configurations can't reach ("Databricks Default Storage cannot be accessed using
      // Classic Compute" — a real infrastructure constraint, not a bug), and there's no reason a
      // hiccup on one of these optional, newer system tables should sink the whole report.
      println("Fetching MLflow summary...")
      val mlflowSummary = tryOrDefault("MLflow summary", MlflowSummaryRow(0L, 0L, 0L)) {
        DatabricksJdbc.loadMlflowSummary(conn, lookback)
      }
      cacheScalar(mlflowSummary, dir, "mlflow_summary")

      println("Fetching Model Serving endpoint types...")
      val servingEndpointTypes = tryOrDefault("Model Serving endpoint types", spark.createDataset(Seq.empty[ServingEndpointTypeRow])) {
        DatabricksJdbc.loadServingEndpointTypes(spark, conn)
      }
      cache(servingEndpointTypes, dir, "serving_endpoint_types")

      println("Fetching Model Serving usage summary...")
      val servingUsageSummary = tryOrDefault("Model Serving usage summary", ServingUsageSummaryRow(0L, 0L)) {
        DatabricksJdbc.loadServingUsageSummary(conn, lookback)
      }
      cacheScalar(servingUsageSummary, dir, "serving_usage_summary")

      println("Fetching AI Gateway usage...")
      val aiGatewayUsage = tryOrDefault("AI Gateway usage", spark.createDataset(Seq.empty[AiGatewayUsageRow])) {
        DatabricksJdbc.loadAiGatewayUsage(spark, conn, lookback)
      }
      cache(aiGatewayUsage, dir, "ai_gateway_usage")

      println("Fetching data quality monitoring summary...")
      val dataQualityMonitoringSummary =
        tryOrDefault("data quality monitoring summary", DataQualityMonitoringSummaryRow(0L, 0L, 0L)) {
          DatabricksJdbc.loadDataQualityMonitoringSummary(conn, lookback)
        }
      cacheScalar(dataQualityMonitoringSummary, dir, "data_quality_monitoring_summary")

      println("Fetching data classification summary...")
      val dataClassificationSummary = tryOrDefault("data classification summary", DataClassificationSummaryRow(0L, 0L)) {
        DatabricksJdbc.loadDataClassificationSummary(conn)
      }
      cacheScalar(dataClassificationSummary, dir, "data_classification_summary")

      RawUsageData(
        dailyCost = dailyCost,
        costBySku = costBySku,
        dailyCostByProduct = dailyCostByProduct,
        overviewStats = overviewStats,
        dailyQueryVolume = dailyQueryVolume,
        dailyDurationStats = dailyDurationStats,
        computeExecStats = computeExecStats,
        computeResourceSignals = computeResourceSignals,
        topErrorReasons = topErrorReasons,
        dailyActiveUsers = dailyActiveUsers,
        topActorsByActions = topActorsByActions,
        actionsByService = actionsByService,
        topReadTables = topReadTables,
        topWriteTables = topWriteTables,
        objectInventory = objectInventory,
        jobRunSummary = jobRunSummary,
        dailyJobRuns = dailyJobRuns,
        costByUser = costByUser,
        monthlyCostBySku = monthlyCostBySku,
        dailyQueryVolumeByType = dailyQueryVolumeByType,
        serviceAccountQueryStats = serviceAccountQueryStats,
        serviceAccountTableAccess = serviceAccountTableAccess,
        userTableReads = userTableReads,
        userTableWrites = userTableWrites,
        catalogSchemaCounts = catalogSchemaCounts,
        tableSizes = tableSizes,
        servicePrincipals = servicePrincipals,
        jobRunTotals = jobRunTotals,
        jobNames = jobNames,
        warehouseNames = warehouseNames,
        clusterNames = clusterNames,
        jobTasks = jobTasks,
        mlflowSummary = mlflowSummary,
        servingEndpointTypes = servingEndpointTypes,
        servingUsageSummary = servingUsageSummary,
        aiGatewayUsage = aiGatewayUsage,
        dataQualityMonitoringSummary = dataQualityMonitoringSummary,
        dataClassificationSummary = dataClassificationSummary,
        lookbackDays = lookback
      )
    } finally conn.close()
  }

  /** Runs `action`, falling back to `default` (and printing a short warning, not crashing the
    * whole report) if it fails — for optional, newer system tables where a real access
    * constraint on one (e.g. a warehouse configuration that can't reach
    * `system.data_quality_monitoring`'s storage) shouldn't sink everything else.
    */
  private def tryOrDefault[T](label: String, default: T)(action: => T): T =
    Try(action) match {
      case Success(value) => value
      case Failure(e) =>
        println(s"  Skipping $label: ${Option(e.getMessage).getOrElse(e.toString).linesIterator.next()}")
        default
    }

  private def cache[T](ds: Dataset[T], cacheDir: String, name: String): Unit =
    ds.write.mode("overwrite").parquet(s"$cacheDir/$name.parquet")

  private val scalarMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /** `overviewStats`/`jobRunTotals` are single-row values, not `Dataset[T]`s — a Parquet
    * round-trip for one row is pure overhead, so these go through a plain JSON file instead
    * (same `ObjectMapper` pattern as [[guild.reportkit.PlatformSummaryIo]]). Previously these
    * weren't cached at all and silently reset to zero under `--skip-fetch` — a real bug (it
    * broke the Automation section's run-count/success-rate stats, and the Overview section's
    * query count, on any cache-only rebuild), caught while wiring up the Phase 3 comparison
    * export and fixed here rather than just in that export.
    */
  private def cacheScalar[T <: Product](value: T, cacheDir: String, name: String): Unit = {
    val file = new File(s"$cacheDir/$name.json")
    Option(file.getParentFile).foreach(_.mkdirs())
    scalarMapper.writerWithDefaultPrettyPrinter().writeValue(file, value)
  }

  private def loadScalar[T: ClassTag](cacheDir: String, name: String): T =
    scalarMapper.readValue(new File(s"$cacheDir/$name.json"), implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])

  private def loadFromCache(spark: SparkSession, cfg: DatabricksConfig): RawUsageData = {
    import spark.implicits._
    val dir = cfg.cacheDir()

    def load[T <: Product: Encoder](name: String): Dataset[T] =
      spark.read.parquet(s"$dir/$name.parquet").as[T]

    RawUsageData(
      dailyCost = load[DailyCostRow]("daily_cost"),
      costBySku = load[CostBySkuRow]("cost_by_sku"),
      dailyCostByProduct = load[DailyCostByProductRow]("daily_cost_by_product"),
      overviewStats = loadScalar[OverviewStatsRow](dir, "overview_stats"),
      dailyQueryVolume = load[DailyQueryVolumeRow]("daily_query_volume"),
      dailyDurationStats = load[DailyDurationStatsRow]("daily_duration_stats"),
      computeExecStats = load[ComputeExecStatsRow]("compute_exec_stats"),
      computeResourceSignals = load[ComputeResourceSignalRow]("compute_resource_signals"),
      topErrorReasons = load[QueryErrorReasonRow]("top_error_reasons"),
      dailyActiveUsers = load[DailyActiveUsersRow]("daily_active_users"),
      topActorsByActions = load[TopActorRow]("top_actors_by_actions"),
      actionsByService = load[ServiceActionCountRow]("actions_by_service"),
      topReadTables = load[TableLineageAccessRow]("top_read_tables"),
      topWriteTables = load[TableLineageAccessRow]("top_write_tables"),
      objectInventory = load[ObjectInventoryRow]("object_inventory"),
      jobRunSummary = load[JobRunSummaryRow]("job_run_summary"),
      dailyJobRuns = load[DailyJobRunRow]("daily_job_runs"),
      costByUser = load[CostByUserRow]("cost_by_user"),
      monthlyCostBySku = load[MonthlyCostBySkuRow]("monthly_cost_by_sku"),
      dailyQueryVolumeByType = load[DailyQueryVolumeByTypeRow]("daily_query_volume_by_type"),
      serviceAccountQueryStats = load[ServiceAccountQueryStatsRow]("service_account_query_stats"),
      serviceAccountTableAccess = load[ServiceAccountTableAccessRow]("service_account_table_access"),
      userTableReads = load[UserTableAccessRow]("user_table_reads"),
      userTableWrites = load[UserTableAccessRow]("user_table_writes"),
      catalogSchemaCounts = load[CatalogSchemaCountRow]("catalog_schema_counts"),
      tableSizes = load[TableSizeRow]("table_sizes"),
      servicePrincipals = load[ServicePrincipalRow]("service_principals"),
      jobRunTotals = loadScalar[JobRunTotalsRow](dir, "job_run_totals"),
      jobNames = load[JobNameRow]("job_names"),
      warehouseNames = load[WarehouseNameRow]("warehouse_names"),
      clusterNames = load[ClusterNameRow]("cluster_names"),
      jobTasks = load[JobTaskRow]("job_tasks"),
      mlflowSummary = loadScalar[MlflowSummaryRow](dir, "mlflow_summary"),
      servingEndpointTypes = load[ServingEndpointTypeRow]("serving_endpoint_types"),
      servingUsageSummary = loadScalar[ServingUsageSummaryRow](dir, "serving_usage_summary"),
      aiGatewayUsage = load[AiGatewayUsageRow]("ai_gateway_usage"),
      dataQualityMonitoringSummary = loadScalar[DataQualityMonitoringSummaryRow](dir, "data_quality_monitoring_summary"),
      dataClassificationSummary = loadScalar[DataClassificationSummaryRow](dir, "data_classification_summary"),
      lookbackDays = cfg.lookbackDays()
    )
  }
}
