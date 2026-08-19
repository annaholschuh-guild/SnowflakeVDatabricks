package guild.snowflakeusage.aggregate

import guild.snowflakeusage.queries.{
  CostRow,
  DailyDurationStatsRow,
  DailyQueryStatsRow,
  DailyUserQueryRow,
  DatabaseActivityRawRow,
  DatabaseStorageUsageRow,
  DataTransferRow,
  ErrorReasonRow,
  LoginByIpAndTypeRow,
  MaterializedViewRefreshRow,
  ObjectInventoryRow,
  PipeUsageRow,
  ServiceAccountQueryStatsRow,
  SessionByClientRow,
  ShareInventoryRow,
  StorageLifecyclePolicyRow,
  StreamInventoryRow,
  TableAccessCountRow,
  TableStorageRow,
  TableUserActionCountRow,
  TaskRunRow,
  UserActionCountRow,
  UserCreditsRow,
  UserQueryCountRow,
  UserRow,
  UserTableAccessCountRow,
  UserTypeRow,
  WarehouseExecStatsRow,
  WarehouseMeteringRow,
  WarehouseResourceSignalRow
}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.desc

import java.sql.Timestamp
import scala.util.Try


/** Turns the raw typed `ACCOUNT_USAGE` pulls into the small, driver-side result sets the
  * dashboard renders.
  *
  * `query_history`/`warehouse_metering_history`/`access_history` can run to millions of rows
  * over a 90-day lookback, so those stay on Spark via `groupByKey`/`mapGroups` until reduced to
  * daily rollups or top-N lists. `users`/`table_storage_metrics`/`database_storage_usage_history`
  * are bounded by account inventory (not activity volume), so they're collected once and
  * processed as plain (still strongly-typed) Scala collections.
  */
object Aggregations {

  private val BYTES_PER_GB = 1024.0 * 1024.0 * 1024.0
  private val MS_PER_SECOND = 1000.0

  def compute(
    spark: SparkSession,
    raw: RawUsageData,
    lookbackDays: Int,
    creditPriceUsd: Option[Double] = None,
    storagePricePerTbUsd: Option[Double] = None
  ): UsageAggregates = {
    val activity = activitySection(
      spark,
      raw.dailyQueryStats,
      raw.dailyQueryByUser,
      raw.databaseActivityByDay,
      raw.warehouseResourceSignals,
      raw.warehouseExecStats,
      raw.topErrorReasons,
      raw.dailyDurationStats,
      raw.warehouseMetering,
      raw.userTypes
    )
    val users = usersSection(raw.users, raw.dailyQueryStats, raw.dailyQueryByUser, raw.topUsersByQueryCount, raw.userTypes)

    val allTables = raw.tableStorage.collect().toSeq
    val storage = storageSection(allTables, raw.databaseStorageHistory)
    val tableUsage = for {
      counts <- raw.tableAccessCounts
      rw <- raw.tableReadWriteByUser
    } yield tableUsageSection(allTables, counts, rw)
    val cost = costSection(raw.costUsage, activity, storage, raw.queryAttribution, creditPriceUsd, storagePricePerTbUsd)
    val automation =
      automationSection(spark, raw.taskHistory, raw.pipeUsage, raw.streamsInventory)
    val serviceAccounts =
      serviceAccountsSection(raw.userTypes, raw.serviceAccountQueryStats, raw.userActionCounts, raw.userTableAccessCounts)
    val dataSharing = dataSharingSection(raw.sharesInventory)
    val materializedViews = materializedViewsSection(spark, raw.materializedViewRefreshHistory)
    val dataTransfer = dataTransferSection(raw.dataTransferHistory)
    val objectInventory = objectInventorySection(raw.objectInventory)
    val clientTools = clientToolSection(raw.sessionsByClientApp)
    val storageLifecycle = storageLifecycleSection(raw.storageLifecyclePolicies)
    val loginChannels = loginChannelSection(raw.loginsByIpAndClientType)

    UsageAggregates(
      lookbackDays,
      activity,
      users,
      storage,
      tableUsage,
      cost,
      automation,
      serviceAccounts,
      dataSharing,
      materializedViews,
      dataTransfer,
      objectInventory,
      clientTools,
      storageLifecycle,
      loginChannels
    )
  }

  // ************************************************************************************
  // Query & warehouse activity
  // ************************************************************************************

  private def activitySection(
    spark: SparkSession,
    dailyQueryStats: Dataset[DailyQueryStatsRow],
    dailyQueryByUser: Dataset[DailyUserQueryRow],
    databaseActivityByDay: Dataset[DatabaseActivityRawRow],
    warehouseResourceSignals: Dataset[WarehouseResourceSignalRow],
    warehouseExecStats: Dataset[WarehouseExecStatsRow],
    topErrorReasonsRaw: Dataset[ErrorReasonRow],
    dailyDurationStatsRaw: Dataset[DailyDurationStatsRow],
    warehouseMetering: Dataset[WarehouseMeteringRow],
    userTypes: Option[Dataset[UserTypeRow]]
  ): ActivitySection = {
    import spark.implicits._

    val dailyVolume = dailyQueryStats.collect().toSeq.sortBy(_.day).map(s => DailyQueryVolume(s.day, s.queryCount))

    val dailyByUser = dailyQueryByUser.collect().toSeq

    // Same person-vs-service split as UsersSection.dailyActiveByType, applied to query COUNT
    // instead of distinct active users. dailyByUser is one row per (day, user) — already small
    // (bounded by days x distinct users, not query count) — so the classification fold happens
    // here in plain Scala rather than as a Spark job.
    val dailyVolumeByType = userTypes match {
      case Some(typesDs) =>
        val typeByUser = typesDs.collect().map(t => t.userName -> classifyUserType(t.userName, t.userType)).toMap
        dailyByUser
          .groupBy(r => (r.day, typeByUser.getOrElse(r.userName, classifyUserType(r.userName, null))))
          .toSeq
          .map { case ((day, userType), rows) => DailyQueryVolumeByType(day, userType, rows.map(_.queryCount).sum) }
          .sortBy(_.day)
      case None => Seq.empty
    }

    // Precomputed per-day quartiles (APPROX_PERCENTILE, server-side) — see DailyDurationStatsRow.
    val dailyDurationSamples = dailyDurationStatsRaw
      .collect()
      .toSeq
      .sortBy(_.day)
      .map(s =>
        DailyDurationSample(
          s.day,
          s.minMs / MS_PER_SECOND,
          s.q1Ms / MS_PER_SECOND,
          s.medianMs / MS_PER_SECOND,
          s.q3Ms / MS_PER_SECOND,
          s.maxMs / MS_PER_SECOND
        )
      )

    val allCredits = warehouseMetering.map(_.creditsUsed).collect().toSeq
    val totalCredits = allCredits.sum

    val dailyTotalCredits = warehouseMetering
      .groupByKey(w => dayOf(w.startTime))
      .mapGroups { case (day, rows) => DailyCredits(day, rows.map(_.creditsUsed).sum) }
      .orderBy("day")
      .collect()
      .toSeq

    val creditsByWarehouse = warehouseMetering
      .groupByKey(_.warehouseName)
      .mapGroups { case (wh, rows) => WarehouseCredits(wh, rows.map(_.creditsUsed).sum) }
      .orderBy(desc("totalCredits"))
      .limit(15)
      .collect()
      .toSeq

    val dbActivity = databaseActivityByDay.collect().toSeq
    val totalsByDatabase = dbActivity.groupBy(_.databaseName).map { case (db, rows) => db -> rows.map(_.queryCount).sum }
    val topDatabaseSet = totalsByDatabase.toSeq.sortBy(-_._2).take(6).map(_._1).toSet

    val topDatabaseActivityTrend = dbActivity
      .filter(r => topDatabaseSet.contains(r.databaseName))
      .map(r => DatabaseActivityTrend(r.day, r.databaseName, r.queryCount))

    // Everything outside the top 6, folded into one "Other" series so the stack height still
    // reflects the true daily total rather than silently dropping the long tail of databases.
    val otherDatabaseActivityTrend = dbActivity
      .filterNot(r => topDatabaseSet.contains(r.databaseName))
      .groupBy(_.day)
      .toSeq
      .map { case (day, rows) => DatabaseActivityTrend(day, "Other", rows.map(_.queryCount).sum) }

    val databaseActivityTrend = (topDatabaseActivityTrend ++ otherDatabaseActivityTrend).sortBy(_.day)

    val resourceMismatchSignals = warehouseResourceSignals
      .collect()
      .toSeq
      .map(r =>
        WarehouseResourceSignal(
          warehouseName = r.warehouseName,
          queryCount = r.queryCount,
          queriesWithSpill = r.queriesWithSpill,
          totalSpilledGb = r.totalSpilledBytes.toDouble / BYTES_PER_GB,
          queriesWithQueueing = r.queriesWithQueueing,
          totalQueuedSeconds = r.totalQueuedMs / MS_PER_SECOND
        )
      )
      .filter(s => s.totalSpilledGb > 0.0 || s.totalQueuedSeconds > 0.0)
      .sortBy(s => -(s.queriesWithSpill + s.queriesWithQueueing))
      .take(15)

    val execStats = warehouseExecStats.collect().toSeq
    val failedQueryCount = execStats.map(_.failedCount).sum

    val wastedComputeByWarehouse = execStats
      .filter(_.failedCount > 0)
      .map(r => FailedQueryWaste(r.warehouseName, r.failedCount, r.failedExecMs / MS_PER_SECOND))
      .sortBy(-_.wastedSeconds)
      .take(15)

    // Total execution time per warehouse across ALL queries (not just failed) — the denominator
    // for a per-warehouse $/second-of-query-time rate, used to price out wasted compute.
    val totalExecSecondsByWarehouse = execStats.map(r => WarehouseExecSeconds(r.warehouseName, r.totalExecMs / MS_PER_SECOND))

    val topErrorReasons = topErrorReasonsRaw
      .collect()
      .toSeq
      .map(r => TopErrorReason(r.errorCode, r.sampleMessage, r.count))
      .sortBy(-_.count)

    ActivitySection(
      dailyVolume,
      dailyVolumeByType,
      dailyDurationSamples,
      totalCredits,
      dailyTotalCredits,
      creditsByWarehouse,
      databaseActivityTrend,
      resourceMismatchSignals,
      failedQueryCount,
      wastedComputeByWarehouse,
      totalExecSecondsByWarehouse,
      topErrorReasons
    )
  }

  // ************************************************************************************
  // Active users
  // ************************************************************************************

  private def usersSection(
    users: Dataset[UserRow],
    dailyQueryStats: Dataset[DailyQueryStatsRow],
    dailyQueryByUser: Dataset[DailyUserQueryRow],
    topUsersByQueryCount: Dataset[UserQueryCountRow],
    userTypes: Option[Dataset[UserTypeRow]]
  ): UsersSection = {
    val dailyActive = dailyQueryStats.collect().toSeq.sortBy(_.day).map(s => DailyActiveUsers(s.day, s.activeUsers))

    // dailyQueryByUser is one row per (day, user) by construction (GROUP BY day, user_name in
    // SQL) — counting rows per (day, type) bucket after classification IS counting distinct
    // active users of that type that day.
    val dailyActiveByType = userTypes match {
      case Some(typesDs) =>
        val typeByUser = typesDs.collect().map(t => t.userName -> classifyUserType(t.userName, t.userType)).toMap
        dailyQueryByUser
          .collect()
          .toSeq
          .groupBy(r => (r.day, typeByUser.getOrElse(r.userName, classifyUserType(r.userName, null))))
          .toSeq
          .map { case ((day, userType), rows) => DailyActiveByType(day, userType, rows.length.toLong) }
          .sortBy(_.day)
      case None => Seq.empty
    }

    // topUsersByQueryCount is pulled with a generous buffer beyond 20 (see Queries.topUsersByQueryCount)
    // so employee-bucketing here doesn't push a real top-20 user out before this re-sort/take.
    val topUsers = topUsersByQueryCount
      .collect()
      .toSeq
      .groupBy(r => bucketPeople(r.userName))
      .toSeq
      .map { case (user, rows) => TopUser(user, rows.map(_.queryCount).sum) }
      .sortBy(-_.queryCount)
      .take(20)

    val nonDisabled = users.collect().toSeq.filterNot(_.disabled)

    UsersSection(dailyActive, dailyActiveByType, topUsers, nonDisabled.length.toLong)
  }

  // ************************************************************************************
  // Storage & table sizes
  // ************************************************************************************

  private def storageSection(
    allTables: Seq[TableStorageRow],
    databaseStorageHistory: Dataset[DatabaseStorageUsageRow]
  ): StorageSection = {
    val topTables = allTables
      .sortBy(t => -t.activeBytes)
      .take(20)
      .map(t =>
        TopTableBySize(t.qualifiedName, t.activeBytes / BYTES_PER_GB, t.timeTravelBytes / BYTES_PER_GB, t.failsafeBytes / BYTES_PER_GB)
      )

    val summary = StorageSummary(
      totalActiveGb = allTables.map(_.activeBytes).sum / BYTES_PER_GB,
      totalTimeTravelGb = allTables.map(_.timeTravelBytes).sum / BYTES_PER_GB,
      totalFailsafeGb = allTables.map(_.failsafeBytes).sum / BYTES_PER_GB,
      totalRetainedForCloneGb = allTables.map(_.retainedForCloneBytes).sum / BYTES_PER_GB
    )

    val allDbHistory = databaseStorageHistory.collect().toSeq

    val latestBytesByDatabase = allDbHistory
      .groupBy(_.databaseName)
      .mapValues { rows =>
        val latestDate = rows.map(_.usageDate.toLocalDate).maxBy(_.toEpochDay)
        rows.filter(_.usageDate.toLocalDate == latestDate).map(_.averageDatabaseBytes).sum
      }
      .toMap

    // Capped at 6 — the largest series count the categorical palette validates for a
    // multi-line/area chart; see dashboard/PlotlyUtil.scala CATEGORICAL.
    val topDatabaseNames = latestBytesByDatabase.toSeq.sortBy(-_._2).take(6).map(_._1).toSet

    val topTrend = allDbHistory
      .filter(d => topDatabaseNames.contains(d.databaseName))
      .groupBy(d => (d.usageDate.toString, d.databaseName))
      .toSeq
      .map { case ((day, db), rows) => DatabaseStorageTrend(day, db, rows.map(_.averageDatabaseBytes).sum / BYTES_PER_GB) }

    // Everything outside the top 6, folded into one "Other" series so the stack height still
    // reflects true total storage rather than silently dropping the long tail of databases.
    val otherTrend = allDbHistory
      .filter(d => !topDatabaseNames.contains(d.databaseName))
      .groupBy(_.usageDate.toString)
      .toSeq
      .map { case (day, rows) => DatabaseStorageTrend(day, "Other", rows.map(_.averageDatabaseBytes).sum / BYTES_PER_GB) }

    val trend = (topTrend ++ otherTrend).sortBy(_.day)

    StorageSection(topTables, trend, summary)
  }

  // ************************************************************************************
  // Table usage / access patterns (Enterprise Edition+ only — requires ACCESS_HISTORY)
  // ************************************************************************************

  private def tableUsageSection(
    allTables: Seq[TableStorageRow],
    tableAccessCounts: Dataset[TableAccessCountRow],
    tableReadWriteByUser: Dataset[TableUserActionCountRow]
  ): TableUsageSection = {
    val accessCounts = tableAccessCounts.collect().toSeq

    val topAccessed = accessCounts
      .sortBy(-_.distinctQueries)
      .take(30)
      .map(r => TopAccessedTable(r.objectName, r.distinctQueries))

    // Full (not top-30-limited) per-table access counts, keyed upper-case to match qualifiedName
    // joins below — both the dead-table anti-join and the size-vs-access scatter need every
    // accessed table, not just the top-ranked ones.
    val accessCountByUpperName: Map[String, Long] = accessCounts.map(r => r.objectName.toUpperCase -> r.distinctQueries).toMap

    val dead = allTables
      .filterNot(t => accessCountByUpperName.contains(t.qualifiedName.toUpperCase))
      .sortBy(t => -t.activeBytes)
      .take(30)
      .map(t => DeadTable(t.qualifiedName, t.activeBytes / BYTES_PER_GB))

    // Capped to the 200 largest tables — biggest-first is what matters most for migration
    // sizing, and the long tail of small tables would just clutter the scatter with no real
    // prioritization value.
    val sizeVsAccess = allTables
      .sortBy(t => -t.activeBytes)
      .take(200)
      .map(t =>
        TableSizeVsAccess(
          t.qualifiedName,
          t.activeBytes / BYTES_PER_GB,
          accessCountByUpperName.getOrElse(t.qualifiedName.toUpperCase, 0L)
        )
      )

    val activeGbByUpperName: Map[String, Double] = allTables.map(t => t.qualifiedName.toUpperCase -> t.activeBytes / BYTES_PER_GB).toMap

    // Bucketed by user (Employees lumped together), same privacy convention as every other
    // per-user ranking in this report. One row per (table, user) with separate read/write
    // counts — not one row per (table, user, action) — so both directions are visible for the
    // same table/user without scanning two rows. Capped at 150 rows by total activity (not 500)
    // so the scrollable table (see HTMLUtil.sortableFilterableTable) stays a reasonable size;
    // the filter box lets you dig into any slice of what's shown, not into rows never included.
    // Summing distinctQueries across bucketed individual users is safe — query_id is unique per
    // query, so per-user distinct-query sets never overlap.
    val readWriteDetails = tableReadWriteByUser
      .collect()
      .toSeq
      .groupBy(r => (r.objectName, bucketPeople(r.userName)))
      .toSeq
      .map { case ((tableName, user), rows) =>
        val readCount = rows.filter(_.action == "READ").map(_.distinctQueries).sum
        val writeCount = rows.filter(_.action == "WRITE").map(_.distinctQueries).sum
        TableUserRWCount(tableName, user, readCount, writeCount)
      }
      .sortBy(r => -(r.readCount + r.writeCount))
      .take(150)
      .map(r =>
        TableAccessDetail(r.tableName, activeGbByUpperName.getOrElse(r.tableName.toUpperCase, 0.0), r.userName, r.readCount, r.writeCount)
      )

    TableUsageSection(topAccessed, dead, sizeVsAccess, readWriteDetails)
  }

  // ************************************************************************************
  // Automation & orchestration — Tasks/Snowpipe have no 1:1 Databricks equivalent, so this
  // matters for migration scoping beyond just cost. Every input here is optional/edition-gated
  // except query_type, which rides along on the mandatory query_history pull.
  // ************************************************************************************

  private def automationSection(
    spark: SparkSession,
    taskHistory: Option[Dataset[TaskRunRow]],
    pipeUsage: Option[Dataset[PipeUsageRow]],
    streamsInventory: Option[Dataset[StreamInventoryRow]]
  ): AutomationSection = {
    import spark.implicits._

    val taskRuns = taskHistory
      .map(
        _.groupByKey(_.taskName)
          .mapGroups { case (name, rows) =>
            val all = rows.toSeq
            TaskRunSummary(
              name,
              taskType = classifyTaskType(all.headOption.map(_.queryText).getOrElse("")),
              totalRuns = all.size.toLong,
              succeeded = all.count(_.state == "SUCCEEDED").toLong,
              failed = all.count(_.state == "FAILED").toLong
            )
          }
          .orderBy(desc("totalRuns"))
          .limit(20)
          .collect()
          .toSeq
      )
      .getOrElse(Seq.empty)

    val pipes = pipeUsage
      .map(
        _.groupByKey(_.pipeName)
          .mapGroups { case (name, rows) =>
            val all = rows.toSeq
            PipeIngestSummary(
              name,
              totalCredits = all.map(_.creditsUsed).sum,
              totalFilesInserted = all.map(_.filesInserted).sum,
              totalBytesInsertedGb = all.map(_.bytesInserted).sum / BYTES_PER_GB
            )
          }
          .orderBy(desc("totalCredits"))
          .limit(20)
          .collect()
          .toSeq
      )
      .getOrElse(Seq.empty)

    val streams = streamsInventory.map { ds =>
      val all = ds.collect().toSeq
      StreamsSummary(all.size.toLong, all.count(_.stale).toLong)
    }

    AutomationSection(taskRuns, pipes, streams)
  }

  // ************************************************************************************
  // Service accounts deep-dive — how active they are, and (when ACCESS_HISTORY is available)
  // which tables they actually touch. Gated on USERS.TYPE being available at all (same gate as
  // UsersSection.dailyActiveByType); the table-access half degrades further if ACCESS_HISTORY
  // specifically isn't available.
  // ************************************************************************************

  // NOT private: Spark's codegen generates a deserializer in a different class that needs to
  // access these fields directly — a `private case class` here fails at runtime with
  // "Private member cannot be accessed from type ...SpecificSafeProjection" even though it
  // compiles fine (Datasets are the one place a private nested case class actively breaks).
  case class TableUserRWCount(tableName: String, userName: String, readCount: Long, writeCount: Long)

  /** Not capped at a top-N: unlike per-person rankings (which can run to hundreds of users and
    * genuinely need a cutoff), the number of distinct service accounts in an account is small
    * and bounded by nature — every one of them is worth showing.
    *
    * No join against query_history needed for the table-access half — `AccessHistoryRow` already
    * carries `userName` directly, so filtering `accessHistory` to the service-account set and
    * grouping by `(userName, objectName)`/`(userName, action)` gets there directly.
    */
  private def serviceAccountsSection(
    userTypes: Option[Dataset[UserTypeRow]],
    serviceAccountQueryStats: Option[Dataset[ServiceAccountQueryStatsRow]],
    userActionCounts: Option[Dataset[UserActionCountRow]],
    userTableAccessCounts: Option[Dataset[UserTableAccessCountRow]]
  ): Option[ServiceAccountsSection] =
    for {
      _ <- userTypes
      statsDs <- serviceAccountQueryStats
    } yield {
      val baseStats = statsDs.collect().toSeq.sortBy(-_.queryCount)
      val allServiceUserSet = baseStats.map(_.userName).toSet

      // Both pulls are already small (bounded by distinct users/tables, not raw access_history
      // rows) and already domain-filtered to "Table" in SQL — filter to service accounts
      // client-side rather than a separate SQL round-trip. Each uses its own correct GROUP BY
      // grain (no summing across a collapsed dimension) — see Queries.userActionCounts/
      // userTableAccessCounts for why that matters.
      val readWriteByUser = userActionCounts match {
        case Some(ds) if allServiceUserSet.nonEmpty =>
          ds.collect().toSeq.filter(r => allServiceUserSet.contains(r.userName)).map(r => (r.userName, r.action) -> r.distinctQueries).toMap
        case _ => Map.empty[(String, String), Long]
      }

      val tableAccess = userTableAccessCounts match {
        case Some(ds) if allServiceUserSet.nonEmpty =>
          ds.collect()
            .toSeq
            .filter(r => allServiceUserSet.contains(r.userName))
            .map(r => ServiceAccountTableAccess(r.userName, r.objectName, r.distinctQueries))
            .sortBy(-_.distinctQueries)
            .take(50)
        case _ => Seq.empty
      }

      val summaries = baseStats.map(b =>
        ServiceAccountSummary(
          b.userName,
          b.queryCount,
          b.warehouseCount,
          b.firstDay,
          b.lastDay,
          readCount = readWriteByUser.getOrElse((b.userName, "READ"), 0L),
          writeCount = readWriteByUser.getOrElse((b.userName, "WRITE"), 0L)
        )
      )

      ServiceAccountsSection(summaries, tableAccess)
    }

  // ************************************************************************************
  // Secure Data Sharing — no ACCOUNT_USAGE view, so this is a `SHOW SHARES` inventory rather
  // than a time-series usage history, same situation as the Streams inventory above.
  // ************************************************************************************

  private def dataSharingSection(sharesInventory: Option[Dataset[ShareInventoryRow]]): Option[DataSharingSection] =
    sharesInventory.map { ds =>
      val all = ds.collect().toSeq
      DataSharingSection(
        totalShares = all.size.toLong,
        outboundShares = all.count(_.kind.equalsIgnoreCase("OUTBOUND")).toLong,
        inboundShares = all.count(_.kind.equalsIgnoreCase("INBOUND")).toLong,
        shares = all
          .map(s => ShareSummary(s.shareName, s.kind, s.databaseName, s.toAccounts))
          .sortBy(_.shareName)
      )
    }

  // ************************************************************************************
  // Materialized view refresh activity — Snowflake-native automated maintenance, Enterprise
  // Edition+ only.
  // ************************************************************************************

  private def materializedViewsSection(
    spark: SparkSession,
    refreshHistory: Option[Dataset[MaterializedViewRefreshRow]]
  ): Option[MaterializedViewsSection] = refreshHistory.map { ds =>
    import spark.implicits._
    val refreshes = ds
      .groupByKey(_.viewName)
      .mapGroups { case (name, rows) =>
        var total = 0L
        var totalCredits = 0.0
        var totalDurationMs = 0L
        while (rows.hasNext) {
          val r = rows.next()
          total += 1
          totalCredits += r.creditsUsed
          totalDurationMs += r.endTime.getTime - r.startTime.getTime
        }
        val avgSeconds = if (total > 0) totalDurationMs / MS_PER_SECOND / total else 0.0
        MvRefreshSummary(name, total, totalCredits, avgSeconds)
      }
      .orderBy(desc("totalRefreshes"))
      .limit(20)
      .collect()
      .toSeq
    MaterializedViewsSection(refreshes)
  }

  // ************************************************************************************
  // Data transfer (egress) — cross-cloud/cross-region transfer can carry its own cost
  // independent of compute/storage, relevant to migration planning.
  // ************************************************************************************

  private def dataTransferSection(transferHistory: Option[Dataset[DataTransferRow]]): Option[DataTransferSection] =
    transferHistory.map { ds =>
      val rows = ds.collect().toSeq
      val totalGb = rows.map(_.bytesTransferred).sum / BYTES_PER_GB
      val byDestination = rows
        .groupBy(r => (r.transferType, r.sourceCloud, r.sourceRegion, r.targetCloud, r.targetRegion))
        .toSeq
        .map { case ((transferType, srcCloud, srcRegion, tgtCloud, tgtRegion), rs) =>
          DataTransferSummary(
            transferType,
            srcCloud,
            srcRegion,
            tgtCloud,
            tgtRegion,
            crossRegion = srcCloud != tgtCloud || srcRegion != tgtRegion,
            rs.map(_.bytesTransferred).sum / BYTES_PER_GB
          )
        }
        .sortBy(-_.totalGb)
        .take(20)
      DataTransferSection(totalGb, byDestination)
    }

  // ************************************************************************************
  // Object inventory — account-wide counts, the full surface area a migration needs to cover.
  // ************************************************************************************

  private def objectInventorySection(inventory: Option[Dataset[ObjectInventoryRow]]): Option[ObjectInventorySection] =
    inventory.map(ds => ObjectInventorySection(ds.collect().toSeq.map(r => ObjectCount(r.objectType, r.objectCount))))

  // ************************************************************************************
  // Client/tool breakdown — what's actually connecting to Snowflake, from SESSIONS. BI tools
  // and drivers need a re-point in a migration; this is the inventory of what needs one.
  // ************************************************************************************

  private def clientToolSection(sessionsByClientApp: Option[Dataset[SessionByClientRow]]): Option[ClientToolSection] =
    sessionsByClientApp.map { ds =>
      val byClient = ds
        .collect()
        .toSeq
        .groupBy(r => baseClientName(r.clientApplicationId))
        .toSeq
        .map { case (client, rows) => ClientBreakdown(client, rows.map(_.sessionCount).sum) }
        .sortBy(-_.sessionCount)
      ClientToolSection(byClient)
    }

  // ************************************************************************************
  // Storage lifecycle policies — Snowflake's actual cold-storage feature (opt-in per table).
  // ************************************************************************************

  private def storageLifecycleSection(
    policies: Option[Dataset[StorageLifecyclePolicyRow]]
  ): Option[StorageLifecycleSection] =
    policies.map { ds =>
      val all = ds.collect().toSeq
      val summaries = all.map(p =>
        LifecyclePolicySummary(
          p.policyName,
          p.databaseName,
          p.schemaName,
          p.archiveTier.getOrElse("Unknown"),
          p.archiveForDays.getOrElse(0)
        )
      )
      StorageLifecycleSection(summaries.length.toLong, summaries)
    }

  // ************************************************************************************
  // Login channel attribution — matches LOGIN_HISTORY.CLIENT_IP against this account's known
  // network-policy IP allowlists (hand-maintained from Guild's Terraform; keep in sync if the
  // account's network policies change — this is NOT derived from Snowflake metadata, there's no
  // ACCOUNT_USAGE view mapping an IP to "which network policy matched"). Excludes Okta's ~300-IP
  // allowlist (that's an identity-provider integration, not a human/tool access channel) and the
  // Terraform's "Combinations" policies (pure unions of the base policies already listed here).
  // First match wins where the same IP appears in more than one real policy (e.g. Twingate and
  // Omni share 35.163.43.7 in the actual config) — order below is not otherwise significant.
  // ************************************************************************************

  private val KNOWN_NETWORK_POLICIES: Seq[(String, Seq[String])] = Seq(
    "Twingate VPN" -> Seq("54.212.60.202", "35.163.43.7", "35.164.133.188", "34.223.32.205", "52.25.210.109", "54.185.90.54"),
    "Legacy Guild VPN" -> Seq("38.104.32.10/32"),
    "dbt Cloud" -> Seq("52.3.77.232", "3.214.191.130", "34.233.79.135", "52.45.144.63", "54.81.134.249", "52.22.161.231"),
    "Fivetran" -> Seq(
      "35.227.135.0/29",
      "35.234.176.144/29",
      "52.0.2.4/32",
      "35.172.213.182",
      "52.91.21.146",
      "35.80.36.104/29",
      "3.239.194.48/29",
      "34.82.204.231"
    ),
    "Looker" -> Seq(
      "54.208.10.167",
      "54.209.116.191",
      "52.1.5.228",
      "52.1.157.156",
      "54.83.113.5",
      "52.44.187.22",
      "18.213.96.40",
      "23.22.133.206",
      "35.168.173.238",
      "54.162.175.244",
      "54.80.5.17"
    ),
    "Segment" -> Seq("52.25.130.38", "34.223.203.0/28"),
    "Databricks (AWS_DATABRICKS_PROD)" -> Seq("54.68.31.47", "35.82.18.175", "18.246.106.0/24"),
    "Atlan" -> Seq("34.194.9.164", "52.70.111.3"),
    "Celigo" -> Seq("44.204.21.0/24"),
    "Omni" -> Seq(
      "3.211.115.21",
      "34.215.77.3",
      "44.199.154.198",
      "52.205.119.255",
      "52.33.167.28",
      "54.172.168.164",
      "54.213.140.155",
      "54.213.219.131"
    ),
    "Tableau" -> Seq("34.218.129.202", "52.40.235.24"),
    "Astronomer" -> Seq("35.245.140.149", "35.245.44.221", "34.86.203.139", "35.199.31.94", "52.24.52.182", "54.190.182.111"),
    "GitHub Runner" -> Seq("4.249.211.128/28", "20.22.11.208/28"),
    "Zapier" -> Seq("44.214.195.64/28", "18.246.81.208/28"),
    "FullStory" -> Seq(
      "35.222.108.170",
      "34.123.155.165",
      "34.66.61.70",
      "34.132.0.62",
      "35.188.33.35",
      "34.173.20.8",
      "34.133.56.70",
      "34.171.5.86",
      "34.173.220.42",
      "34.68.140.85",
      "8.35.195.0/29",
      "34.89.210.80/29"
    ),
    "Hex" -> Seq("3.129.36.245", "3.13.16.99", "3.18.79.139"),
    "Iterable Smart Ingest" -> Seq("54.196.30.169", "52.72.201.213", "18.213.226.96", "3.224.126.197", "3.217.26.199"),
    "Lightbeam" -> Seq(
      "44.233.51.147",
      "34.198.104.197",
      "54.202.15.240",
      "44.241.15.127",
      "54.190.149.207",
      "52.41.7.242",
      "52.24.87.32",
      "35.160.86.251",
      "52.37.68.177",
      "52.38.105.94",
      "52.25.14.100",
      "35.82.112.80",
      "54.69.182.72"
    ),
    "Rally" -> Seq("44.196.80.52", "107.21.141.153", "54.226.107.60", "98.89.199.176", "34.204.160.35", "52.2.175.121"),
    "AWS Devops" -> Seq(
      "54.200.165.16",
      "44.232.216.121",
      "44.241.92.97",
      "54.71.73.237",
      "34.213.24.117",
      "52.10.98.46",
      "52.33.151.14",
      "54.191.38.213",
      "54.201.5.147"
    ),
    "AWS Dev" -> Seq("34.215.159.56", "52.24.65.245", "52.43.9.23"),
    "AWS Staging" -> Seq("52.88.154.206", "44.232.119.19", "35.167.106.225"),
    "AWS Prod" -> Seq("35.160.86.251", "52.38.105.94", "52.37.68.177"),
    "PC Hunters" -> Seq(
      "18.192.165.147",
      "18.203.212.46",
      "34.208.97.32",
      "34.213.101.49",
      "34.223.186.164",
      "34.223.20.125",
      "34.223.221.217",
      "35.162.98.78",
      "35.163.166.14",
      "35.83.242.177",
      "44.236.244.86",
      "52.211.30.86",
      "52.214.31.50",
      "52.32.222.121",
      "52.35.219.75",
      "52.35.55.27",
      "52.39.212.48",
      "52.40.78.172",
      "52.89.191.5",
      "54.186.74.45",
      "54.187.196.247",
      "54.202.110.6",
      "54.212.81.93",
      "54.214.94.117",
      "54.220.191.11",
      "54.68.155.124",
      "54.72.125.231",
      "54.73.199.243",
      "54.75.50.99"
    )
  )

  private def ipToLong(ip: String): Long =
    ip.split("\\.").map(_.toInt).foldLeft(0L) { case (acc, octet) => (acc << 8) | (octet & 0xFF) }

  private def ipInCidr(ip: String, cidr: String): Boolean =
    Try {
      val (base, prefix) = if (cidr.contains("/")) {
        val parts = cidr.split("/")
        (parts(0), parts(1).toInt)
      } else (cidr, 32)
      val mask = if (prefix == 0) 0L else (-1L << (32 - prefix)) & 0xFFFFFFFFL
      (ipToLong(ip) & mask) == (ipToLong(base) & mask)
    }.getOrElse(false)

  private def classifyLoginChannel(clientIp: String): String =
    if (clientIp == null || clientIp.isEmpty) "Unknown"
    else
      KNOWN_NETWORK_POLICIES
        .find { case (_, ips) => ips.exists(ipInCidr(clientIp, _)) }
        .map(_._1)
        .getOrElse("Unmatched")

  private def loginChannelSection(loginsByIpAndClientType: Option[Dataset[LoginByIpAndTypeRow]]): Option[LoginChannelSection] =
    loginsByIpAndClientType.map { ds =>
      val rows = ds.collect().toSeq

      val byChannel = rows
        .groupBy(r => classifyLoginChannel(r.clientIp))
        .toSeq
        .map { case (channel, rs) => LoginChannelSummary(channel, rs.map(_.loginCount).sum) }
        .sortBy(-_.loginCount)

      val byClientType = rows
        .groupBy(r => Option(r.reportedClientType).filter(_.nonEmpty).getOrElse("Unknown"))
        .toSeq
        .map { case (clientType, rs) => LoginClientTypeSummary(clientType, rs.map(_.loginCount).sum) }
        .sortBy(-_.loginCount)

      LoginChannelSection(byChannel.map(_.loginCount).sum, byChannel, byClientType)
    }

  // ************************************************************************************
  // Cost — prefers actual billed USD from ORGANIZATION_USAGE, falls back to a credit-price/
  // storage-price estimate when that view isn't accessible, omits the section if neither is.
  //
  // `costByWarehouse`/`topUsersByCost` apply one account-wide $/credit rate to per-warehouse/
  // per-user credit totals — ORGANIZATION_USAGE has no per-warehouse or per-user grain, so this
  // is an approximation (uniform rate) even when the account-wide total is actual billed dollars.
  // ************************************************************************************

  private def costSection(
    costUsage: Option[Dataset[CostRow]],
    activity: ActivitySection,
    storage: StorageSection,
    queryAttribution: Option[Dataset[UserCreditsRow]],
    creditPriceUsd: Option[Double],
    storagePricePerTbUsd: Option[Double]
  ): Option[CostSection] = costUsage match {
    case Some(ds) =>
      val rows = ds.collect().toSeq

      val dailyTotal = rows
        .groupBy(_.usageDate.toString)
        .toSeq
        .map { case (day, rs) => DailyCost(day, rs.map(_.usdAmount).sum) }
        .sortBy(_.day)

      val byUsageType = rows
        .groupBy(_.usageType)
        .toSeq
        .map { case (usageType, rs) => CostByUsageType(usageType, rs.map(_.usdAmount).sum) }
        .sortBy(-_.usd)

      val computeUsdTotal = rows.filter(_.usageType == "compute").map(_.usdAmount).sum
      val ratePerCredit = if (activity.totalCredits > 0) computeUsdTotal / activity.totalCredits else 0.0
      val (cumulative, costByWarehouse, topUsersByCost, topUsersPieSlices) =
        deriveCostBreakdowns(ratePerCredit, dailyTotal, activity, queryAttribution)

      // One row per (month, usage type) — a real monthly bill, including negative adjustment
      // lines (e.g. Snowflake's automatic "included cloud services" credit) exactly as billed,
      // not netted out — that's how the real invoice reads.
      val monthlyBill = rows
        .groupBy(r => (monthOf(r.usageDate), r.usageType))
        .toSeq
        .map { case ((month, usageType), rs) => MonthlyCostLineItem(month, usageType, rs.map(_.usdAmount).sum) }

      Some(
        CostSection(
          isEstimate = false,
          totalUsd = rows.map(_.usdAmount).sum,
          dailyTotal,
          cumulative,
          byUsageType,
          costByWarehouse,
          topUsersByCost,
          topUsersPieSlices,
          wastedComputeCostUsd(activity, ratePerCredit),
          monthlyBill
        )
      )

    case None if creditPriceUsd.isDefined || storagePricePerTbUsd.isDefined =>
      val creditPrice = creditPriceUsd.getOrElse(0.0)
      val storagePricePerGbMonth = storagePricePerTbUsd.getOrElse(0.0) / 1024.0

      val computeCostUsd = activity.totalCredits * creditPrice
      val storageCostUsd = storage.summary.totalActiveGb * storagePricePerGbMonth
      val dailyTotal = activity.dailyTotalCredits.map(d => DailyCost(d.day, d.credits * creditPrice))
      val byUsageType = Seq(
        CostByUsageType("compute (estimated)", computeCostUsd),
        CostByUsageType("storage (estimated)", storageCostUsd)
      ).filter(_.usd > 0.0)
      val (cumulative, costByWarehouse, topUsersByCost, topUsersPieSlices) =
        deriveCostBreakdowns(creditPrice, dailyTotal, activity, queryAttribution)

      Some(
        CostSection(
          isEstimate = true,
          totalUsd = computeCostUsd + storageCostUsd,
          dailyTotal,
          cumulative,
          byUsageType,
          costByWarehouse,
          topUsersByCost,
          topUsersPieSlices,
          wastedComputeCostUsd(activity, creditPrice),
          monthlyBill = Seq.empty
        )
      )

    case None => None
  }

  /** Prices out wasted compute (failed queries) using each warehouse's OWN credits-per-second
    * rate — derived from that warehouse's actual credit consumption divided by its total query
    * execution time across ALL queries — rather than one account-wide average, since warehouse
    * size (and therefore $/second) varies.
    */
  private def wastedComputeCostUsd(activity: ActivitySection, ratePerCredit: Double): Double = {
    val creditsByWarehouse = activity.topWarehousesByCredits.map(w => w.warehouseName -> w.totalCredits).toMap
    val execSecondsByWarehouse = activity.totalExecSecondsByWarehouse.map(w => w.warehouseName -> w.totalExecSeconds).toMap
    activity.wastedComputeByWarehouse.map { waste =>
      val credits = creditsByWarehouse.getOrElse(waste.warehouseName, 0.0)
      val execSeconds = execSecondsByWarehouse.getOrElse(waste.warehouseName, 0.0)
      val creditsPerSecond = if (execSeconds > 0) credits / execSeconds else 0.0
      waste.wastedSeconds * creditsPerSecond * ratePerCredit
    }.sum
  }

  private val TOP_USERS_BY_COST_LIMIT = 10

  private def deriveCostBreakdowns(
    ratePerCredit: Double,
    dailyTotal: Seq[DailyCost],
    activity: ActivitySection,
    queryAttribution: Option[Dataset[UserCreditsRow]]
  ): (Seq[DailyCost], Seq[WarehouseCost], Seq[TopUserCost], Seq[TopUserCost]) = {
    val cumulative = dailyTotal
      .sortBy(_.day)
      .scanLeft(DailyCost("", 0.0)) { case (acc, d) => DailyCost(d.day, acc.usd + d.usd) }
      .drop(1)

    val costByWarehouse = activity.topWarehousesByCredits.map(w => WarehouseCost(w.warehouseName, w.totalCredits * ratePerCredit))

    val allAttribution = queryAttribution.map(_.collect().toSeq).getOrElse(Seq.empty)

    // Bucketed by user (Employees lumped together), full ranking — the top N feeds both the bar
    // chart and (top N + one "Other" for the remainder) the pie, same pattern as costByWarehouse.
    val bucketedByUser = allAttribution
      .groupBy(r => bucketPeople(r.userName))
      .toSeq
      .map { case (bucket, rows) => TopUserCost(bucket, rows.map(_.credits).sum * ratePerCredit) }
      .sortBy(-_.usd)

    val topUsersByCost = bucketedByUser.take(TOP_USERS_BY_COST_LIMIT)

    val topUsersPieSlices =
      if (bucketedByUser.length <= TOP_USERS_BY_COST_LIMIT + 1) bucketedByUser
      else topUsersByCost :+ TopUserCost("Other", bucketedByUser.drop(TOP_USERS_BY_COST_LIMIT).map(_.usd).sum)

    (cumulative, costByWarehouse, topUsersByCost, topUsersPieSlices)
  }

  /** Collapses e.g. `"JDBC 3.27.1"` / `"PythonConnector 4.6.0"` down to `"JDBC"` / "PythonConnector"`
    * — `CLIENT_APPLICATION_ID` embeds a trailing driver version, which would otherwise fragment
    * one tool into dozens of near-duplicate buckets (one per version ever used). Names with no
    * trailing version (e.g. `"Snowsight"`, `"SQLAPI"`) pass through unchanged.
    */
  private def baseClientName(rawId: String): String = {
    val trimmed = Option(rawId).map(_.trim).filter(_.nonEmpty).getOrElse("")
    if (trimmed.isEmpty) "Unknown"
    else trimmed.replaceAll("""\s+v?[0-9][0-9a-zA-Z.\-]*$""", "")
  }

  /** Classifies a task's statement as a stored-procedure call or the leading SQL verb — a rough
    * but useful "what kind of thing does this task actually run" signal for migration scoping.
    */
  private def classifyTaskType(queryText: String): String = {
    val trimmed = Option(queryText).map(_.trim.toUpperCase).getOrElse("")
    if (trimmed.isEmpty) "Unknown"
    else if (trimmed.startsWith("CALL")) "Stored Procedure Call"
    else if (trimmed.startsWith("EXECUTE IMMEDIATE")) "EXECUTE IMMEDIATE"
    else if (trimmed.startsWith("MERGE")) "MERGE"
    else if (trimmed.startsWith("INSERT")) "INSERT"
    else if (trimmed.startsWith("UPDATE")) "UPDATE"
    else if (trimmed.startsWith("DELETE")) "DELETE"
    else if (trimmed.startsWith("COPY")) "COPY INTO"
    else if (trimmed.startsWith("CREATE")) "CREATE"
    else if (trimmed.startsWith("ALTER")) "ALTER"
    else if (trimmed.startsWith("TRUNCATE")) "TRUNCATE"
    else "Other SQL"
  }

  private def dayOf(ts: Timestamp): String = ts.toLocalDateTime.toLocalDate.toString

  private def monthOf(date: java.sql.Date): String = date.toLocalDate.toString.substring(0, 7)

  private def isEmployeeEmail(userName: String): Boolean = userName.toLowerCase.endsWith("@guildeducation.com")

  /** USERS.TYPE is blank (not null — an empty string) for accounts predating Snowflake's
    * user-type classification feature — in practice this account's blank-type users are 100%
    * @guildeducation.com employee logins (verified against real data), so an email-shaped blank
    * falls back to PERSON rather than an opaque "Unclassified" bucket. Only a blank AND
    * non-email-shaped login (never observed here, but possible in principle) falls back to
    * "Unclassified". A top-level method rather than a closure-local `def` — the latter dragged a
    * `NotSerializableException` on `Aggregations$` into the Spark closure that called it.
    */
  def classifyUserType(userName: String, rawType: String): String =
    if (rawType != null && rawType.trim.nonEmpty) rawType
    else if (isEmployeeEmail(userName)) "PERSON"
    else "Unclassified"

  /** Not private — `UsageReportRunner` needs this to build the service-account user list for
    * `Queries.serviceAccountQueryStats`'s `WHERE user_name IN (...)` filter before that pull can
    * even be issued.
    */
  def isServiceAccountType(userName: String, rawType: String): Boolean =
    classifyUserType(userName, rawType).toUpperCase.contains("SERVICE")

  /** Lumps every `@guildeducation.com` login into one bucket so a handful of heavy human users
    * don't crowd out the top-N ranking — service accounts and other non-standard identities are
    * what's actually interesting to see individually there.
    */
  private def bucketPeople(userName: String): String =
    if (isEmployeeEmail(userName)) "Employee@GUILDEDUCATION.com" else userName
}
