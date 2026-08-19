package guild.snowflakeusage

import guild.snowflakeusage.aggregate.{Aggregations, RawUsageData, UsageAggregates}
import guild.snowflakeusage.dashboard.UsageDashboard
import guild.snowflakeusage.queries._
import guild.reportkit.{PlatformSummary, PlatformSummaryIo}

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.io.File
import java.time.Instant
import scala.util.{Failure, Success, Try}


/** Entry point: pull `ACCOUNT_USAGE` from Snowflake (or reuse a cached local pull), aggregate
  * it with Spark, and write the offline HTML/Plotly report.
  *
  * Usage (SSO):
  *   sbt "run --account xy12345.us-east-1 --user anna.holschuh@guild.com --warehouse COMPUTE_WH"
  *
  * Rebuild the report from the last pull without hitting Snowflake again:
  *   sbt "run --skip-fetch"
  */
object UsageReportRunner {

  def main(args: Array[String]): Unit = {
    val cfg = new Config(args)
    val spark = SnowflakeJdbc.buildSparkSession()
    spark.sparkContext.setLogLevel("WARN")

    try {
      val raw = if (cfg.skipFetch()) loadFromCache(spark, cfg) else fetchAndCache(spark, cfg)
      val agg = Aggregations.compute(
        spark,
        raw,
        cfg.lookbackDays(),
        cfg.creditPriceUsd.toOption,
        cfg.storagePricePerTbUsd.toOption
      )
      // Only unset when --skip-fetch ran without --account (accountLocator has no fallback
      // then) — "UNKNOWN" is just a placeholder for the reference SQL display, never sent to
      // Snowflake at this point.
      UsageDashboard.generate(agg, cfg.outputPath(), cfg.accountLocator.toOption.getOrElse("UNKNOWN"))
      PlatformSummaryIo.write(buildPlatformSummary(agg), "output/snowflake-summary.json")
    } finally {
      spark.stop()
    }
  }

  /** A small, cross-platform-comparable slice of `agg` — see [[guild.reportkit.PlatformSummary]]
    * for what this feeds (`guild.comparison.ComparisonReportRunner`) and why only these fields.
    */
  private def buildPlatformSummary(agg: UsageAggregates): PlatformSummary = {
    val objectCounts = agg.objectInventory.map(_.counts).getOrElse(Seq.empty)
    def countOf(objectType: String): Long = objectCounts.find(_.objectType == objectType).map(_.count).getOrElse(0L)

    // Snowflake's userType is "PERSON"/"SERVICE"/"Unclassified" (see Aggregations.classifyUserType) —
    // "Unclassified" folds into the service bucket here as a conservative default, same as how
    // isServiceAccountType treats anything that isn't cleanly PERSON.
    val byType = agg.users.dailyActiveByType.groupBy(_.userType)
    def avgActiveUsers(matches: String => Boolean): Double = {
      val rows = byType.filterKeys(matches).values.flatten.toSeq
      if (rows.isEmpty) 0.0 else rows.map(_.activeUsers).sum.toDouble / rows.map(_.day).distinct.size.max(1)
    }

    val totalTaskRuns = agg.automation.taskRuns.map(_.totalRuns).sum
    val succeededTaskRuns = agg.automation.taskRuns.map(_.succeeded).sum

    PlatformSummary(
      platform = "Snowflake",
      lookbackDays = agg.lookbackDays,
      generatedAt = Instant.now().toString,
      totalCostUsd = agg.cost.map(_.totalUsd).getOrElse(0.0),
      costIsEstimate = agg.cost.forall(_.isEstimate),
      // Same source (ORGANIZATION_USAGE) as the total, so real/estimate travels with it — unlike
      // Databricks, where a real account-wide total is possible without a real per-SKU breakdown.
      computeCostIsEstimate = agg.cost.forall(_.isEstimate),
      // No list-price/real-billed split on the Snowflake side — ORGANIZATION_USAGE is real
      // billed dollars directly, nothing to compute a discount ratio against.
      realBilledDiscountRatio = None,
      computeCostUsd = agg.cost.map(_.costByWarehouse.map(_.usd).sum).getOrElse(0.0),
      totalComputeSeconds = agg.activity.totalExecSecondsByWarehouse.map(_.totalExecSeconds).sum,
      totalQueries = agg.activity.dailyQueryVolume.map(_.queryCount).sum,
      activeUsersHuman = avgActiveUsers(_ == "PERSON"),
      activeUsersService = avgActiveUsers(_ != "PERSON"),
      failedQueryCount = agg.activity.failedQueryCount,
      totalTables = countOf("Tables") + countOf("Views"),
      totalSchemas = countOf("Schemas"),
      totalDatabasesOrCatalogs = countOf("Databases"),
      totalStorageGb = Some(agg.storage.summary.totalActiveGb),
      automationRunCount = totalTaskRuns,
      automationSuccessRatePercent = if (totalTaskRuns > 0) succeededTaskRuns.toDouble / totalTaskRuns * 100.0 else 0.0
    )
  }

  private def fetchAndCache(spark: SparkSession, cfg: Config): RawUsageData = {
    val lookback = cfg.lookbackDays()
    val dir = cfg.cacheDir()

    println("Connecting to Snowflake (one session, one SSO prompt for the whole run)...")
    val conn = SnowflakeJdbc.openConnection(cfg)
    try {
      println("Fetching users...")
      val users = SnowflakeJdbc.loadUsers(spark, conn)
      cache(users, dir, "users")

      // Fetched here (not later, alongside the other optional pulls) because the
      // service-account query_history pull below needs the classified user list to build its
      // `WHERE user_name IN (...)` filter before it can even be issued.
      println("Fetching user types (people vs. service accounts, not on every edition)...")
      val userTypes = Try {
        val ds = SnowflakeJdbc.loadUserTypes(spark, conn)
        cache(ds, dir, "user_types")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: USERS.TYPE unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      // query_history is pulled pre-aggregated (GROUP BY in Snowflake), never as raw rows — see
      // the Phase 1 rewrite plan. Each pull below is small (bounded by days x warehouses/users/
      // databases, not raw query count) regardless of lookback window.
      println("Fetching daily query stats...")
      val dailyQueryStats = SnowflakeJdbc.loadDailyQueryStats(spark, conn, lookback)
      cache(dailyQueryStats, dir, "daily_query_stats")

      println("Fetching daily query-by-user stats...")
      val dailyQueryByUser = SnowflakeJdbc.loadDailyQueryByUser(spark, conn, lookback)
      cache(dailyQueryByUser, dir, "daily_query_by_user")

      println("Fetching top users by query count...")
      val topUsersByQueryCount = SnowflakeJdbc.loadTopUsersByQueryCount(spark, conn, lookback)
      cache(topUsersByQueryCount, dir, "top_users_by_query_count")

      println("Fetching database activity by day...")
      val databaseActivityByDay = SnowflakeJdbc.loadDatabaseActivityByDay(spark, conn, lookback)
      cache(databaseActivityByDay, dir, "database_activity_by_day")

      println("Fetching warehouse resource signals...")
      val warehouseResourceSignals = SnowflakeJdbc.loadWarehouseResourceSignals(spark, conn, lookback)
      cache(warehouseResourceSignals, dir, "warehouse_resource_signals")

      println("Fetching warehouse exec stats...")
      val warehouseExecStats = SnowflakeJdbc.loadWarehouseExecStats(spark, conn, lookback)
      cache(warehouseExecStats, dir, "warehouse_exec_stats")

      println("Fetching top error reasons...")
      val topErrorReasons = SnowflakeJdbc.loadTopErrorReasons(spark, conn, lookback)
      cache(topErrorReasons, dir, "top_error_reasons")

      println("Fetching daily duration stats...")
      val dailyDurationStats = SnowflakeJdbc.loadDailyDurationStats(spark, conn, lookback)
      cache(dailyDurationStats, dir, "daily_duration_stats")

      println("Fetching service-account query stats...")
      val serviceAccountNames = userTypes match {
        case Some(typesDs) =>
          typesDs
            .collect()
            .toSeq
            .filter(t => Aggregations.isServiceAccountType(t.userName, t.userType))
            .map(_.userName)
        case None => Seq.empty
      }
      val serviceAccountQueryStats =
        if (serviceAccountNames.isEmpty) {
          println("  Skipping: no service accounts classified (USERS.TYPE unavailable or none found)")
          None
        } else
          Try {
            val ds = SnowflakeJdbc.loadServiceAccountQueryStats(spark, conn, lookback, serviceAccountNames)
            cache(ds, dir, "service_account_query_stats")
            ds
          } match {
            case Success(ds) => Some(ds)
            case Failure(e) =>
              println(s"  Skipping: service-account query stats unavailable (${e.getMessage.linesIterator.next()})")
              None
          }

      println("Fetching warehouse_metering_history...")
      val warehouseMetering = SnowflakeJdbc.loadWarehouseMetering(spark, conn, lookback)
      cache(warehouseMetering, dir, "warehouse_metering_history")

      println("Fetching table_storage_metrics...")
      val tableStorage = SnowflakeJdbc.loadTableStorage(spark, conn)
      cache(tableStorage, dir, "table_storage_metrics")

      println("Fetching database_storage_usage_history...")
      val databaseStorageHistory = SnowflakeJdbc.loadDatabaseStorageHistory(spark, conn, lookback)
      cache(databaseStorageHistory, dir, "database_storage_usage_history")

      // access_history is pulled pre-aggregated too (GROUP BY the unioned reads+writes flatten
      // in Snowflake) — see Queries.tableAccessCounts/tableReadWriteByUser. Both come from the
      // same underlying flatten, so they succeed/fail together in practice, but each pull is
      // independently Try-wrapped like every other optional pull.
      println("Fetching table access counts (requires Enterprise Edition+)...")
      val tableAccessCounts = Try {
        val ds = SnowflakeJdbc.loadTableAccessCounts(spark, conn, lookback)
        cache(ds, dir, "table_access_counts")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: ACCESS_HISTORY unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching table read/write by user (requires Enterprise Edition+)...")
      val tableReadWriteByUser = Try {
        val ds = SnowflakeJdbc.loadTableReadWriteByUser(spark, conn, lookback)
        cache(ds, dir, "table_read_write_by_user")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: ACCESS_HISTORY unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      // Per-user rollups need their own GROUP BY grain — see Queries.userActionCounts/
      // userTableAccessCounts for why summing tableReadWriteByUser across tables would be wrong.
      println("Fetching per-user action counts (requires Enterprise Edition+)...")
      val userActionCounts = Try {
        val ds = SnowflakeJdbc.loadUserActionCounts(spark, conn, lookback)
        cache(ds, dir, "user_action_counts")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: ACCESS_HISTORY unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching per-user table access counts (requires Enterprise Edition+)...")
      val userTableAccessCounts = Try {
        val ds = SnowflakeJdbc.loadUserTableAccessCounts(spark, conn, lookback)
        cache(ds, dir, "user_table_access_counts")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: ACCESS_HISTORY unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println(
        s"Fetching cost data (requires ORGANIZATION_USAGE access, locator=${cfg.accountLocator()}, " +
          s"cost-lookback-days=${cfg.costLookbackDays()})..."
      )
      val costUsage = Try {
        val ds = SnowflakeJdbc.loadCostUsage(spark, conn, cfg.costLookbackDays(), cfg.accountLocator())
        cache(ds, dir, "cost_usage")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: ORGANIZATION_USAGE unavailable (${e.getMessage.linesIterator.next()})")
          if (cfg.creditPriceUsd.isDefined || cfg.storagePricePerTbUsd.isDefined)
            println("  Falling back to the --credit-price-usd/--storage-price-per-tb-usd estimate.")
          else
            println("  No cost estimate flags set either — the report will skip the Cost section.")
          None
      }

      println("Fetching query_attribution_history (requires Enterprise Edition+, for cost-by-user)...")
      val queryAttribution = Try {
        val ds = SnowflakeJdbc.loadQueryAttribution(spark, conn, lookback)
        cache(ds, dir, "query_attribution")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: QUERY_ATTRIBUTION_HISTORY unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching task_history (Tasks — Enterprise Edition+ required)...")
      val taskHistory = Try {
        val ds = SnowflakeJdbc.loadTaskHistory(spark, conn, lookback)
        cache(ds, dir, "task_history")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: TASK_HISTORY unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching pipe_usage_history (Snowpipe)...")
      val pipeUsage = Try {
        val ds = SnowflakeJdbc.loadPipeUsage(spark, conn, lookback)
        cache(ds, dir, "pipe_usage")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: PIPE_USAGE_HISTORY unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching streams inventory...")
      val streamsInventory = Try {
        val ds = SnowflakeJdbc.loadStreamsInventory(spark, conn)
        cache(ds, dir, "streams_inventory")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: STREAMS unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching shares inventory (Secure Data Sharing)...")
      val sharesInventory = Try {
        val ds = SnowflakeJdbc.loadSharesInventory(spark, conn)
        cache(ds, dir, "shares_inventory")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: SHARES unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching materialized_view_refresh_history (requires Enterprise Edition+)...")
      val materializedViewRefreshHistory = Try {
        val ds = SnowflakeJdbc.loadMaterializedViewRefreshHistory(spark, conn, lookback)
        cache(ds, dir, "materialized_view_refresh_history")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: MATERIALIZED_VIEW_REFRESH_HISTORY unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching data_transfer_history (egress)...")
      val dataTransferHistory = Try {
        val ds = SnowflakeJdbc.loadDataTransferHistory(spark, conn, lookback)
        cache(ds, dir, "data_transfer_history")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: DATA_TRANSFER_HISTORY unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching object inventory counts...")
      val objectInventory = Try {
        val ds = SnowflakeJdbc.loadObjectInventory(spark, conn)
        cache(ds, dir, "object_inventory")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: object inventory unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching sessions by client app (client/tool breakdown)...")
      val sessionsByClientApp = Try {
        val ds = SnowflakeJdbc.loadSessionsByClientApp(spark, conn, lookback)
        cache(ds, dir, "sessions_by_client_app")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: SESSIONS unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching storage_lifecycle_policies (cold storage — opt-in per table)...")
      val storageLifecyclePolicies = Try {
        val ds = SnowflakeJdbc.loadStorageLifecyclePolicies(spark, conn)
        cache(ds, dir, "storage_lifecycle_policies")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: STORAGE_LIFECYCLE_POLICIES unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      println("Fetching logins by IP and client type (channel attribution)...")
      val loginsByIpAndClientType = Try {
        val ds = SnowflakeJdbc.loadLoginsByIpAndClientType(spark, conn, lookback)
        cache(ds, dir, "logins_by_ip_and_client_type")
        ds
      } match {
        case Success(ds) => Some(ds)
        case Failure(e) =>
          println(s"  Skipping: LOGIN_HISTORY unavailable (${e.getMessage.linesIterator.next()})")
          None
      }

      RawUsageData(
        users = users,
        dailyQueryStats = dailyQueryStats,
        dailyQueryByUser = dailyQueryByUser,
        topUsersByQueryCount = topUsersByQueryCount,
        databaseActivityByDay = databaseActivityByDay,
        warehouseResourceSignals = warehouseResourceSignals,
        warehouseExecStats = warehouseExecStats,
        topErrorReasons = topErrorReasons,
        dailyDurationStats = dailyDurationStats,
        serviceAccountQueryStats = serviceAccountQueryStats,
        warehouseMetering = warehouseMetering,
        tableStorage = tableStorage,
        databaseStorageHistory = databaseStorageHistory,
        tableAccessCounts = tableAccessCounts,
        tableReadWriteByUser = tableReadWriteByUser,
        userActionCounts = userActionCounts,
        userTableAccessCounts = userTableAccessCounts,
        costUsage = costUsage,
        userTypes = userTypes,
        queryAttribution = queryAttribution,
        taskHistory = taskHistory,
        pipeUsage = pipeUsage,
        streamsInventory = streamsInventory,
        sharesInventory = sharesInventory,
        materializedViewRefreshHistory = materializedViewRefreshHistory,
        dataTransferHistory = dataTransferHistory,
        objectInventory = objectInventory,
        sessionsByClientApp = sessionsByClientApp,
        storageLifecyclePolicies = storageLifecyclePolicies,
        loginsByIpAndClientType = loginsByIpAndClientType
      )
    } finally conn.close()
  }

  private def cache[T](ds: Dataset[T], cacheDir: String, name: String): Unit =
    ds.write.mode("overwrite").parquet(s"$cacheDir/$name.parquet")

  private def loadFromCache(spark: SparkSession, cfg: Config): RawUsageData = {
    import spark.implicits._
    val dir = cfg.cacheDir()

    def load[T <: Product: Encoder](name: String): Dataset[T] =
      spark.read.parquet(s"$dir/$name.parquet").as[T]

    def loadIfPresent[T <: Product: Encoder](name: String): Option[Dataset[T]] =
      if (new File(s"$dir/$name.parquet").exists()) Some(load[T](name)) else None

    RawUsageData(
      users = load[UserRow]("users"),
      dailyQueryStats = load[DailyQueryStatsRow]("daily_query_stats"),
      dailyQueryByUser = load[DailyUserQueryRow]("daily_query_by_user"),
      topUsersByQueryCount = load[UserQueryCountRow]("top_users_by_query_count"),
      databaseActivityByDay = load[DatabaseActivityRawRow]("database_activity_by_day"),
      warehouseResourceSignals = load[WarehouseResourceSignalRow]("warehouse_resource_signals"),
      warehouseExecStats = load[WarehouseExecStatsRow]("warehouse_exec_stats"),
      topErrorReasons = load[ErrorReasonRow]("top_error_reasons"),
      dailyDurationStats = load[DailyDurationStatsRow]("daily_duration_stats"),
      serviceAccountQueryStats = loadIfPresent[ServiceAccountQueryStatsRow]("service_account_query_stats"),
      warehouseMetering = load[WarehouseMeteringRow]("warehouse_metering_history"),
      tableStorage = load[TableStorageRow]("table_storage_metrics"),
      databaseStorageHistory = load[DatabaseStorageUsageRow]("database_storage_usage_history"),
      tableAccessCounts = loadIfPresent[TableAccessCountRow]("table_access_counts"),
      tableReadWriteByUser = loadIfPresent[TableUserActionCountRow]("table_read_write_by_user"),
      userActionCounts = loadIfPresent[UserActionCountRow]("user_action_counts"),
      userTableAccessCounts = loadIfPresent[UserTableAccessCountRow]("user_table_access_counts"),
      costUsage = loadIfPresent[CostRow]("cost_usage"),
      userTypes = loadIfPresent[UserTypeRow]("user_types"),
      queryAttribution = loadIfPresent[UserCreditsRow]("query_attribution"),
      taskHistory = loadIfPresent[TaskRunRow]("task_history"),
      pipeUsage = loadIfPresent[PipeUsageRow]("pipe_usage"),
      streamsInventory = loadIfPresent[StreamInventoryRow]("streams_inventory"),
      sharesInventory = loadIfPresent[ShareInventoryRow]("shares_inventory"),
      materializedViewRefreshHistory = loadIfPresent[MaterializedViewRefreshRow]("materialized_view_refresh_history"),
      dataTransferHistory = loadIfPresent[DataTransferRow]("data_transfer_history"),
      objectInventory = loadIfPresent[ObjectInventoryRow]("object_inventory"),
      sessionsByClientApp = loadIfPresent[SessionByClientRow]("sessions_by_client_app"),
      storageLifecyclePolicies = loadIfPresent[StorageLifecyclePolicyRow]("storage_lifecycle_policies"),
      loginsByIpAndClientType = loadIfPresent[LoginByIpAndTypeRow]("logins_by_ip_and_client_type")
    )
  }
}
