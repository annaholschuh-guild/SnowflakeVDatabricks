package guild.snowflakeusage

import guild.snowflakeusage.queries._

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.net.URLEncoder
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/** Connects to Snowflake over a single, directly-managed JDBC connection and maps each pull
  * into a typed `Dataset[T]` matching one of the row classes in [[guild.snowflakeusage.queries.Rows]].
  *
  * Deliberately NOT using Spark's `format("jdbc")` reader here: that data source opens a fresh
  * physical connection per query — at least twice each (once to resolve the schema via a
  * `WHERE 1=0` probe, once to actually read) — which meant a fresh `externalbrowser` SSO prompt
  * every time, up to a dozen browser windows for a single report run. [[openConnection]] instead
  * authenticates once; every `loadXxx` below reuses that same `Connection`.
  */
object SnowflakeJdbc {

  private val DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver"

  def buildSparkSession(): SparkSession =
    SparkSession
      .builder()
      .appName("snowflake-usage-report")
      .master("local[*]")
      .getOrCreate()

  def jdbcUrl(cfg: Config): String = {
    val params = Seq(
      "warehouse" -> cfg.warehouse(),
      "role" -> cfg.role(),
      "authenticator" -> cfg.authenticator()
    ).map { case (k, v) => s"$k=${URLEncoder.encode(v, "UTF-8")}" }.mkString("&")
    s"jdbc:snowflake://${cfg.account()}.snowflakecomputing.com/?$params"
  }

  /** Opens and authenticates one JDBC connection — callers should hold onto it for the whole
    * run and close it once at the end (see [[UsageReportRunner]]).
    */
  def openConnection(cfg: Config): Connection = {
    Class.forName(DRIVER)
    val props = new Properties()
    props.put("user", cfg.user())
    if (cfg.authenticator() == "snowflake") props.put("password", cfg.password())
    DriverManager.getConnection(jdbcUrl(cfg), props)
  }

  private val ROWS_PER_PARTITION = 10000
  private val MAX_PARTITIONS = 200

  /** Runs `sql` over `conn`, maps each row with `mapRow`, and lifts the (already-materialized,
    * driver-local) result into a Spark `Dataset[T]`.
    *
    * Goes through `sparkContext.parallelize(rows, n)` rather than `spark.createDataset(seq)`
    * directly — the latter embeds the whole local Seq as a single `LocalRelation`/task, which
    * for `query_history`-sized pulls (hundreds of thousands of rows) serialized into one
    * multi-hundred-MB task and blew the driver heap. Partitioning first keeps each task small.
    */
  private def query[T <: Product: Encoder: ClassTag](spark: SparkSession, conn: Connection, sql: String)(
    mapRow: ResultSet => T
  ): Dataset[T] = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(sql)
      val rows = ArrayBuffer.empty[T]
      while (rs.next()) rows += mapRow(rs)
      val numPartitions = math.max(1, math.min(MAX_PARTITIONS, rows.size / ROWS_PER_PARTITION + 1))
      spark.createDataset(spark.sparkContext.parallelize(rows.toSeq, numPartitions))
    } finally stmt.close()
  }

  def loadUsers(spark: SparkSession, conn: Connection): Dataset[UserRow] = {
    import spark.implicits._
    query(spark, conn, Queries.users) { rs =>
      UserRow(
        userName = rs.getString("user_name"),
        disabled = rs.getBoolean("disabled"),
        lastSuccessLogin = Option(rs.getTimestamp("last_success_login")),
        createdOn = rs.getTimestamp("created_on")
      )
    }
  }

  def loadDailyQueryStats(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[DailyQueryStatsRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dailyQueryStats(lookbackDays)) { rs =>
      DailyQueryStatsRow(
        day = rs.getDate("day").toLocalDate.toString,
        queryCount = rs.getLong("query_count"),
        activeUsers = rs.getLong("active_users")
      )
    }
  }

  def loadDailyQueryByUser(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[DailyUserQueryRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dailyQueryByUser(lookbackDays)) { rs =>
      DailyUserQueryRow(
        day = rs.getDate("day").toLocalDate.toString,
        userName = rs.getString("user_name"),
        queryCount = rs.getLong("query_count")
      )
    }
  }

  def loadTopUsersByQueryCount(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[UserQueryCountRow] = {
    import spark.implicits._
    query(spark, conn, Queries.topUsersByQueryCount(lookbackDays)) { rs =>
      UserQueryCountRow(userName = rs.getString("user_name"), queryCount = rs.getLong("query_count"))
    }
  }

  def loadDatabaseActivityByDay(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[DatabaseActivityRawRow] = {
    import spark.implicits._
    query(spark, conn, Queries.databaseActivityByDay(lookbackDays)) { rs =>
      DatabaseActivityRawRow(
        day = rs.getDate("day").toLocalDate.toString,
        databaseName = rs.getString("database_name"),
        queryCount = rs.getLong("query_count")
      )
    }
  }

  def loadWarehouseResourceSignals(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[WarehouseResourceSignalRow] = {
    import spark.implicits._
    query(spark, conn, Queries.warehouseResourceSignals(lookbackDays)) { rs =>
      WarehouseResourceSignalRow(
        warehouseName = rs.getString("warehouse_name"),
        queryCount = rs.getLong("query_count"),
        queriesWithSpill = rs.getLong("queries_with_spill"),
        totalSpilledBytes = rs.getLong("total_spilled_bytes"),
        queriesWithQueueing = rs.getLong("queries_with_queueing"),
        totalQueuedMs = rs.getLong("total_queued_ms")
      )
    }
  }

  def loadWarehouseExecStats(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[WarehouseExecStatsRow] = {
    import spark.implicits._
    query(spark, conn, Queries.warehouseExecStats(lookbackDays)) { rs =>
      WarehouseExecStatsRow(
        warehouseName = rs.getString("warehouse_name"),
        totalQueries = rs.getLong("total_queries"),
        totalExecMs = rs.getLong("total_exec_ms"),
        failedCount = rs.getLong("failed_count"),
        failedExecMs = rs.getLong("failed_exec_ms")
      )
    }
  }

  def loadTopErrorReasons(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[ErrorReasonRow] = {
    import spark.implicits._
    query(spark, conn, Queries.topErrorReasons(lookbackDays)) { rs =>
      ErrorReasonRow(
        errorCode = rs.getString("error_code"),
        sampleMessage = Option(rs.getString("sample_message")).getOrElse("").take(160),
        count = rs.getLong("count")
      )
    }
  }

  def loadDailyDurationStats(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[DailyDurationStatsRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dailyDurationStats(lookbackDays)) { rs =>
      DailyDurationStatsRow(
        day = rs.getDate("day").toLocalDate.toString,
        minMs = rs.getLong("min_ms"),
        q1Ms = rs.getDouble("q1_ms"),
        medianMs = rs.getDouble("median_ms"),
        q3Ms = rs.getDouble("q3_ms"),
        maxMs = rs.getLong("max_ms")
      )
    }
  }

  /** Only called once the service-account user list is known from `USERS.TYPE` — see
    * [[guild.snowflakeusage.UsageReportRunner]] for the fetch ordering this requires.
    */
  def loadServiceAccountQueryStats(
    spark: SparkSession,
    conn: Connection,
    lookbackDays: Int,
    userNames: Seq[String]
  ): Dataset[ServiceAccountQueryStatsRow] = {
    import spark.implicits._
    query(spark, conn, Queries.serviceAccountQueryStats(lookbackDays, userNames)) { rs =>
      ServiceAccountQueryStatsRow(
        userName = rs.getString("user_name"),
        queryCount = rs.getLong("query_count"),
        warehouseCount = rs.getLong("warehouse_count"),
        firstDay = rs.getDate("first_day").toLocalDate.toString,
        lastDay = rs.getDate("last_day").toLocalDate.toString
      )
    }
  }

  def loadWarehouseMetering(
    spark: SparkSession,
    conn: Connection,
    lookbackDays: Int
  ): Dataset[WarehouseMeteringRow] = {
    import spark.implicits._
    query(spark, conn, Queries.warehouseMeteringHistory(lookbackDays)) { rs =>
      WarehouseMeteringRow(
        warehouseName = rs.getString("warehouse_name"),
        startTime = rs.getTimestamp("start_time"),
        creditsUsed = rs.getDouble("credits_used")
      )
    }
  }

  def loadTableStorage(spark: SparkSession, conn: Connection): Dataset[TableStorageRow] = {
    import spark.implicits._
    query(spark, conn, Queries.tableStorageMetrics) { rs =>
      TableStorageRow(
        tableCatalog = rs.getString("table_catalog"),
        tableSchema = rs.getString("table_schema"),
        tableName = rs.getString("table_name"),
        activeBytes = rs.getLong("active_bytes"),
        timeTravelBytes = rs.getLong("time_travel_bytes"),
        failsafeBytes = rs.getLong("failsafe_bytes"),
        retainedForCloneBytes = rs.getLong("retained_for_clone_bytes")
      )
    }
  }

  def loadDatabaseStorageHistory(
    spark: SparkSession,
    conn: Connection,
    lookbackDays: Int
  ): Dataset[DatabaseStorageUsageRow] = {
    import spark.implicits._
    query(spark, conn, Queries.databaseStorageUsageHistory(lookbackDays)) { rs =>
      DatabaseStorageUsageRow(
        usageDate = rs.getDate("usage_date"),
        databaseName = rs.getString("database_name"),
        averageDatabaseBytes = rs.getDouble("average_database_bytes")
      )
    }
  }

  /** Requires Enterprise Edition or higher — callers should catch and treat as optional. */
  def loadTableAccessCounts(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[TableAccessCountRow] = {
    import spark.implicits._
    query(spark, conn, Queries.tableAccessCounts(lookbackDays)) { rs =>
      TableAccessCountRow(objectName = rs.getString("object_name"), distinctQueries = rs.getLong("distinct_queries"))
    }
  }

  /** Requires Enterprise Edition or higher — callers should catch and treat as optional. */
  def loadTableReadWriteByUser(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[TableUserActionCountRow] = {
    import spark.implicits._
    query(spark, conn, Queries.tableReadWriteByUser(lookbackDays)) { rs =>
      TableUserActionCountRow(
        objectName = rs.getString("object_name"),
        userName = rs.getString("user_name"),
        action = rs.getString("action"),
        distinctQueries = rs.getLong("distinct_queries")
      )
    }
  }

  /** Requires Enterprise Edition or higher — callers should catch and treat as optional. */
  def loadUserActionCounts(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[UserActionCountRow] = {
    import spark.implicits._
    query(spark, conn, Queries.userActionCounts(lookbackDays)) { rs =>
      UserActionCountRow(
        userName = rs.getString("user_name"),
        action = rs.getString("action"),
        distinctQueries = rs.getLong("distinct_queries")
      )
    }
  }

  /** Requires Enterprise Edition or higher — callers should catch and treat as optional. */
  def loadUserTableAccessCounts(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[UserTableAccessCountRow] = {
    import spark.implicits._
    query(spark, conn, Queries.userTableAccessCounts(lookbackDays)) { rs =>
      UserTableAccessCountRow(
        userName = rs.getString("user_name"),
        objectName = rs.getString("object_name"),
        distinctQueries = rs.getLong("distinct_queries")
      )
    }
  }

  /** Requires `ORGANIZATION_USAGE` access (typically ORGADMIN or an explicit grant) — callers
    * should catch and treat as optional, falling back to a credit-price estimate.
    */
  def loadCostUsage(
    spark: SparkSession,
    conn: Connection,
    lookbackDays: Int,
    accountLocator: String
  ): Dataset[CostRow] = {
    import spark.implicits._
    query(spark, conn, Queries.costUsageDaily(lookbackDays, accountLocator)) { rs =>
      CostRow(
        usageDate = rs.getDate("usage_date"),
        usageType = rs.getString("usage_type"),
        usdAmount = rs.getDouble("usd_amount")
      )
    }
  }

  /** Not present on every Snowflake account/edition — callers should catch and treat as
    * optional (falls back to an undifferentiated active-users count).
    */
  def loadUserTypes(spark: SparkSession, conn: Connection): Dataset[UserTypeRow] = {
    import spark.implicits._
    query(spark, conn, Queries.userTypes) { rs =>
      UserTypeRow(userName = rs.getString("user_name"), userType = rs.getString("user_type"))
    }
  }

  /** Requires Enterprise Edition or higher — callers should catch and treat as optional. */
  def loadQueryAttribution(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[UserCreditsRow] = {
    import spark.implicits._
    query(spark, conn, Queries.queryAttributionByUser(lookbackDays)) { rs =>
      UserCreditsRow(userName = rs.getString("user_name"), credits = rs.getDouble("credits"))
    }
  }

  /** Requires Enterprise Edition or higher — callers should catch and treat as optional. */
  def loadTaskHistory(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[TaskRunRow] = {
    import spark.implicits._
    query(spark, conn, Queries.taskHistory(lookbackDays)) { rs =>
      TaskRunRow(
        taskName = rs.getString("task_name"),
        databaseName = rs.getString("database_name"),
        schemaName = rs.getString("schema_name"),
        state = rs.getString("state"),
        scheduledTime = rs.getTimestamp("scheduled_time"),
        queryText = rs.getString("query_text")
      )
    }
  }

  def loadPipeUsage(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[PipeUsageRow] = {
    import spark.implicits._
    query(spark, conn, Queries.pipeUsageHistory(lookbackDays)) { rs =>
      PipeUsageRow(
        pipeName = rs.getString("pipe_name"),
        startTime = rs.getTimestamp("start_time"),
        creditsUsed = rs.getDouble("credits_used"),
        bytesInserted = rs.getLong("bytes_inserted"),
        filesInserted = rs.getLong("files_inserted")
      )
    }
  }

  /** `SHOW STREAMS` result columns are all VARCHAR (not JDBC-typed) — "stale" is literally the
    * string "true"/"false", so it's compared as a string rather than read via `getBoolean`.
    */
  def loadStreamsInventory(spark: SparkSession, conn: Connection): Dataset[StreamInventoryRow] = {
    import spark.implicits._
    query(spark, conn, Queries.streamsInventory) { rs =>
      StreamInventoryRow(
        streamName = rs.getString("name"),
        databaseName = rs.getString("database_name"),
        schemaName = rs.getString("schema_name"),
        stale = "true".equalsIgnoreCase(rs.getString("stale"))
      )
    }
  }

  /** `SHOW SHARES` result columns are all VARCHAR, same as `SHOW STREAMS`. */
  def loadSharesInventory(spark: SparkSession, conn: Connection): Dataset[ShareInventoryRow] = {
    import spark.implicits._
    query(spark, conn, Queries.sharesInventory) { rs =>
      ShareInventoryRow(
        shareName = rs.getString("name"),
        kind = rs.getString("kind"),
        databaseName = rs.getString("database_name"),
        toAccounts = rs.getString("to")
      )
    }
  }

  /** Requires Enterprise Edition or higher — callers should catch and treat as optional. */
  def loadMaterializedViewRefreshHistory(
    spark: SparkSession,
    conn: Connection,
    lookbackDays: Int
  ): Dataset[MaterializedViewRefreshRow] = {
    import spark.implicits._
    query(spark, conn, Queries.materializedViewRefreshHistory(lookbackDays)) { rs =>
      MaterializedViewRefreshRow(
        viewName = rs.getString("view_name"),
        databaseName = rs.getString("database_name"),
        schemaName = rs.getString("schema_name"),
        startTime = rs.getTimestamp("start_time"),
        endTime = rs.getTimestamp("end_time"),
        creditsUsed = rs.getDouble("credits_used")
      )
    }
  }

  def loadDataTransferHistory(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[DataTransferRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dataTransferHistory(lookbackDays)) { rs =>
      DataTransferRow(
        startTime = rs.getTimestamp("start_time"),
        sourceCloud = rs.getString("source_cloud"),
        sourceRegion = rs.getString("source_region"),
        targetCloud = rs.getString("target_cloud"),
        targetRegion = rs.getString("target_region"),
        transferType = rs.getString("transfer_type"),
        bytesTransferred = rs.getLong("bytes_transferred")
      )
    }
  }

  def loadObjectInventory(spark: SparkSession, conn: Connection): Dataset[ObjectInventoryRow] = {
    import spark.implicits._
    query(spark, conn, Queries.objectInventoryCounts) { rs =>
      ObjectInventoryRow(objectType = rs.getString("object_type"), objectCount = rs.getLong("object_count"))
    }
  }

  def loadSessionsByClientApp(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[SessionByClientRow] = {
    import spark.implicits._
    query(spark, conn, Queries.sessionsByClientApp(lookbackDays)) { rs =>
      SessionByClientRow(clientApplicationId = rs.getString("client_application_id"), sessionCount = rs.getLong("session_count"))
    }
  }

  def loadStorageLifecyclePolicies(spark: SparkSession, conn: Connection): Dataset[StorageLifecyclePolicyRow] = {
    import spark.implicits._
    query(spark, conn, Queries.storageLifecyclePolicies) { rs =>
      val archiveForDays = rs.getInt("archive_for_days")
      StorageLifecyclePolicyRow(
        policyName = rs.getString("policy_name"),
        databaseName = rs.getString("database_name"),
        schemaName = rs.getString("schema_name"),
        archiveTier = Option(rs.getString("archive_tier")),
        archiveForDays = if (rs.wasNull()) None else Some(archiveForDays)
      )
    }
  }

  def loadLoginsByIpAndClientType(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[LoginByIpAndTypeRow] = {
    import spark.implicits._
    query(spark, conn, Queries.loginsByIpAndClientType(lookbackDays)) { rs =>
      LoginByIpAndTypeRow(
        clientIp = rs.getString("client_ip"),
        reportedClientType = rs.getString("reported_client_type"),
        loginCount = rs.getLong("login_count")
      )
    }
  }

  /** `is_success` is `'YES'`/`'NO'`, same string-not-boolean gotcha as `SHOW STREAMS.stale`. */
  def loadAuthFactorBreakdown(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[AuthFactorRow] = {
    import spark.implicits._
    query(spark, conn, Queries.authFactorBreakdown(lookbackDays)) { rs =>
      AuthFactorRow(
        firstFactor = Option(rs.getString("first_factor")).getOrElse("Unknown"),
        secondFactor = Option(rs.getString("second_factor")),
        isSuccess = "YES".equalsIgnoreCase(rs.getString("is_success")),
        loginCount = rs.getLong("login_count")
      )
    }
  }

  def loadReplicationGroupUsage(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[ReplicationGroupUsageRow] = {
    import spark.implicits._
    query(spark, conn, Queries.replicationGroupUsage(lookbackDays)) { rs =>
      ReplicationGroupUsageRow(
        replicationGroupName = rs.getString("replication_group_name"),
        totalCredits = rs.getDouble("total_credits"),
        totalBytesTransferred = rs.getLong("total_bytes_transferred"),
        eventCount = rs.getLong("event_count")
      )
    }
  }
}
