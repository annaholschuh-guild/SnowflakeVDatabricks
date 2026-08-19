package guild.databricksusage

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import guild.databricksusage.queries._
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag


/** Connects to a Databricks SQL Warehouse over JDBC, mirroring
  * [[guild.snowflakeusage.SnowflakeJdbc]]'s single-shared-`Connection` pattern. Auth is a plain
  * personal access token — no browser-SSO lifecycle to manage, so (unlike Snowflake) there's no
  * reason to avoid Spark's built-in `format("jdbc")` reader other than consistency; this keeps
  * the same typed-loader shape as the Snowflake side for the eventual Phase 3 comparison report.
  */
object DatabricksJdbc {

  private val DRIVER = "com.databricks.client.jdbc.Driver"

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val httpClient = HttpClient.newHttpClient()

  def buildSparkSession(): SparkSession =
    SparkSession
      .builder()
      .appName("databricks-usage-report")
      .master("local[*]")
      .getOrCreate()

  /** `GET /api/2.0/sql/warehouses` and pick a warehouse to run queries against — a RUNNING one
    * first (no cold-start wait), else a serverless one (auto-starts in seconds), else whatever's
    * first. Skipped entirely when `--databricks-warehouse-id` pins a specific warehouse.
    */
  private def discoverWarehouseId(cfg: DatabricksConfig): String =
    cfg.warehouseId.toOption.getOrElse {
      val request = HttpRequest
        .newBuilder(URI.create(s"https://${cfg.host()}/api/2.0/sql/warehouses"))
        .header("Authorization", s"Bearer ${cfg.token()}")
        .GET()
        .build()
      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      if (response.statusCode() != 200)
        throw new RuntimeException(
          s"Failed to list Databricks SQL warehouses (HTTP ${response.statusCode()}): ${response.body()}"
        )

      val root = mapper.readTree(response.body())
      val warehouses = root.path("warehouses").elements().asScala.toSeq
      if (warehouses.isEmpty)
        throw new RuntimeException(
          "No SQL warehouses found in this workspace — create one or pass --databricks-warehouse-id"
        )

      def idOf(node: com.fasterxml.jackson.databind.JsonNode): String = node.path("id").asText()

      warehouses
        .find(_.path("state").asText() == "RUNNING")
        .orElse(warehouses.find(_.path("enable_serverless_compute").asBoolean(false)))
        .orElse(warehouses.headOption)
        .map(idOf)
        .get
    }

  /** `GET /api/2.0/preview/scim/v2/ServicePrincipals` — a plain REST call, not SQL, since no
    * `system.*` table carries a service principal's human-readable name (`system.query.history`/
    * `system.access.audit`/`system.access.table_lineage` all only ever record the raw UUID
    * `applicationId`). This workspace has 69 service principals, well under SCIM's default page
    * size, so one unpaginated call is enough — confirmed via `totalResults == itemsPerPage ==
    * Resources.length` in a throwaway probe; would need `startIndex` paging past ~100.
    */
  def loadServicePrincipals(spark: SparkSession, cfg: DatabricksConfig): Dataset[ServicePrincipalRow] = {
    import spark.implicits._
    val request = HttpRequest
      .newBuilder(URI.create(s"https://${cfg.host()}/api/2.0/preview/scim/v2/ServicePrincipals"))
      .header("Authorization", s"Bearer ${cfg.token()}")
      .GET()
      .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200)
      throw new RuntimeException(
        s"Failed to list service principals (HTTP ${response.statusCode()}): ${response.body()}"
      )

    val root = mapper.readTree(response.body())
    val rows = root
      .path("Resources")
      .elements()
      .asScala
      .toSeq
      .map(node => ServicePrincipalRow(applicationId = node.path("applicationId").asText(), displayName = node.path("displayName").asText()))
    spark.createDataset(rows)
  }

  def jdbcUrl(cfg: DatabricksConfig): String = {
    val warehouseId = discoverWarehouseId(cfg)
    s"jdbc:databricks://${cfg.host()}:443/default;transportMode=http;ssl=1;AuthMech=3;" +
      s"httpPath=/sql/1.0/warehouses/$warehouseId"
  }

  /** Opens and authenticates one JDBC connection — callers should hold onto it for the whole run
    * and close it once at the end (see the future `DatabricksUsageReportRunner`).
    */
  def openConnection(cfg: DatabricksConfig): Connection = {
    Class.forName(DRIVER)
    val props = new Properties()
    props.put("UID", "token")
    props.put("PWD", cfg.token())
    DriverManager.getConnection(jdbcUrl(cfg), props)
  }

  private val ROWS_PER_PARTITION = 10000
  private val MAX_PARTITIONS = 200

  /** Runs `sql` over `conn`, maps each row with `mapRow`, and lifts the (already-materialized,
    * driver-local) result into a Spark `Dataset[T]` — see
    * [[guild.snowflakeusage.SnowflakeJdbc.query]] for why this goes through
    * `sparkContext.parallelize` rather than `spark.createDataset(seq)` directly. Every Databricks
    * pull is aggregated in SQL from the start (see [[Queries]]), so result sets here stay small
    * regardless of `--lookback-days` — no raw-row-volume problem to solve twice.
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

  def loadDailyCost(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[DailyCostRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dailyCost(lookbackDays)) { rs =>
      DailyCostRow(day = rs.getString("day"), usdAmount = rs.getDouble("usd_amount"))
    }
  }

  def loadCostBySku(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[CostBySkuRow] = {
    import spark.implicits._
    query(spark, conn, Queries.costBySku(lookbackDays)) { rs =>
      CostBySkuRow(
        skuName = rs.getString("sku_name"),
        usdAmount = rs.getDouble("usd_amount"),
        dbuQuantity = rs.getDouble("dbu_quantity")
      )
    }
  }

  def loadDailyCostByProduct(
    spark: SparkSession,
    conn: Connection,
    lookbackDays: Int
  ): Dataset[DailyCostByProductRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dailyCostByProduct(lookbackDays)) { rs =>
      DailyCostByProductRow(
        day = rs.getString("day"),
        product = rs.getString("product"),
        usdAmount = rs.getDouble("usd_amount")
      )
    }
  }

  def loadOverviewStats(conn: Connection, lookbackDays: Int): OverviewStatsRow = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(Queries.overviewStats(lookbackDays))
      rs.next()
      OverviewStatsRow(totalQueries = rs.getLong("total_queries"), activeUsers = rs.getLong("active_users"))
    } finally stmt.close()
  }

  def loadDailyQueryVolume(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[DailyQueryVolumeRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dailyQueryVolume(lookbackDays)) { rs =>
      DailyQueryVolumeRow(
        day = rs.getString("day"),
        queryCount = rs.getLong("query_count"),
        activeUsers = rs.getLong("active_users")
      )
    }
  }

  def loadDailyDurationStats(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[DailyDurationStatsRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dailyDurationStats(lookbackDays)) { rs =>
      DailyDurationStatsRow(
        day = rs.getString("day"),
        minMs = rs.getLong("min_ms"),
        q1Ms = rs.getLong("q1_ms"),
        medianMs = rs.getLong("median_ms"),
        q3Ms = rs.getLong("q3_ms"),
        maxMs = rs.getLong("max_ms")
      )
    }
  }

  def loadComputeExecStats(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[ComputeExecStatsRow] = {
    import spark.implicits._
    query(spark, conn, Queries.computeExecStats(lookbackDays)) { rs =>
      ComputeExecStatsRow(
        computeType = rs.getString("compute_type"),
        warehouseId = Option(rs.getString("warehouse_id")),
        clusterId = Option(rs.getString("cluster_id")),
        totalQueries = rs.getLong("total_queries"),
        totalExecMs = rs.getLong("total_exec_ms"),
        failedCount = rs.getLong("failed_count"),
        failedExecMs = rs.getLong("failed_exec_ms")
      )
    }
  }

  def loadComputeResourceSignals(
    spark: SparkSession,
    conn: Connection,
    lookbackDays: Int
  ): Dataset[ComputeResourceSignalRow] = {
    import spark.implicits._
    query(spark, conn, Queries.computeResourceSignals(lookbackDays)) { rs =>
      ComputeResourceSignalRow(
        computeType = rs.getString("compute_type"),
        warehouseId = Option(rs.getString("warehouse_id")),
        clusterId = Option(rs.getString("cluster_id")),
        queryCount = rs.getLong("query_count"),
        queriesWithSpill = rs.getLong("queries_with_spill"),
        totalSpilledBytes = rs.getLong("total_spilled_bytes"),
        queriesWithQueueing = rs.getLong("queries_with_queueing"),
        totalQueuedMs = rs.getLong("total_queued_ms")
      )
    }
  }

  def loadTopErrorReasons(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[QueryErrorReasonRow] = {
    import spark.implicits._
    query(spark, conn, Queries.topErrorReasons(lookbackDays)) { rs =>
      QueryErrorReasonRow(
        errorClass = rs.getString("error_class"),
        sampleMessage = rs.getString("sample_message"),
        count = rs.getLong("query_count")
      )
    }
  }

  def loadDailyActiveUsers(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[DailyActiveUsersRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dailyActiveUsers(lookbackDays)) { rs =>
      DailyActiveUsersRow(
        day = rs.getString("day"),
        actorType = rs.getString("actor_type"),
        distinctUsers = rs.getLong("distinct_users"),
        actionCount = rs.getLong("action_count")
      )
    }
  }

  def loadTopActorsByActions(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[TopActorRow] = {
    import spark.implicits._
    query(spark, conn, Queries.topActorsByActions(lookbackDays)) { rs =>
      TopActorRow(actor = Option(rs.getString("actor")).getOrElse("(unknown)"), actionCount = rs.getLong("action_count"))
    }
  }

  def loadActionsByService(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[ServiceActionCountRow] = {
    import spark.implicits._
    query(spark, conn, Queries.actionsByService(lookbackDays)) { rs =>
      ServiceActionCountRow(serviceName = rs.getString("service_name"), actionCount = rs.getLong("action_count"))
    }
  }

  def loadTopReadTables(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[TableLineageAccessRow] = {
    import spark.implicits._
    query(spark, conn, Queries.topReadTables(lookbackDays)) { rs =>
      TableLineageAccessRow(direction = "READ", tableName = rs.getString("table_name"), accessCount = rs.getLong("access_count"))
    }
  }

  def loadTopWriteTables(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[TableLineageAccessRow] = {
    import spark.implicits._
    query(spark, conn, Queries.topWriteTables(lookbackDays)) { rs =>
      TableLineageAccessRow(direction = "WRITE", tableName = rs.getString("table_name"), accessCount = rs.getLong("access_count"))
    }
  }

  def loadObjectInventory(spark: SparkSession, conn: Connection): Dataset[ObjectInventoryRow] = {
    import spark.implicits._
    query(spark, conn, Queries.objectInventory()) { rs =>
      ObjectInventoryRow(tableType = rs.getString("table_type"), objectCount = rs.getLong("object_count"))
    }
  }

  def loadJobRunSummary(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[JobRunSummaryRow] = {
    import spark.implicits._
    query(spark, conn, Queries.jobRunSummary(lookbackDays)) { rs =>
      JobRunSummaryRow(
        jobId = rs.getString("job_id"),
        totalRuns = rs.getLong("total_runs"),
        succeeded = rs.getLong("succeeded"),
        failed = rs.getLong("failed"),
        avgDurationSeconds = rs.getDouble("avg_duration_seconds"),
        totalExecSeconds = rs.getDouble("total_exec_seconds")
      )
    }
  }

  def loadJobRunTotals(conn: Connection, lookbackDays: Int): JobRunTotalsRow = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(Queries.jobRunTotals(lookbackDays))
      rs.next()
      JobRunTotalsRow(
        distinctJobsRun = rs.getLong("distinct_jobs_run"),
        totalRuns = rs.getLong("total_runs"),
        succeeded = rs.getLong("succeeded"),
        totalExecSeconds = rs.getDouble("total_exec_seconds")
      )
    } finally stmt.close()
  }

  def loadJobNames(spark: SparkSession, conn: Connection): Dataset[JobNameRow] = {
    import spark.implicits._
    query(spark, conn, Queries.jobNames()) { rs =>
      JobNameRow(jobId = rs.getString("job_id"), name = rs.getString("name"))
    }
  }

  def loadClusterNames(spark: SparkSession, conn: Connection): Dataset[ClusterNameRow] = {
    import spark.implicits._
    query(spark, conn, Queries.clusterNames()) { rs =>
      ClusterNameRow(clusterId = rs.getString("cluster_id"), clusterName = rs.getString("cluster_name"))
    }
  }

  /** A second, independent `GET /api/2.0/sql/warehouses` call — the first (inside
    * [[discoverWarehouseId]]) runs before the report's data-fetch phase even starts and its
    * result isn't threaded through, so this just asks again rather than plumbing state across
    * that boundary for what's a cheap, small call either way.
    */
  def loadWarehouseNames(spark: SparkSession, cfg: DatabricksConfig): Dataset[WarehouseNameRow] = {
    import spark.implicits._
    val request = HttpRequest
      .newBuilder(URI.create(s"https://${cfg.host()}/api/2.0/sql/warehouses"))
      .header("Authorization", s"Bearer ${cfg.token()}")
      .GET()
      .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200)
      throw new RuntimeException(s"Failed to list Databricks SQL warehouses (HTTP ${response.statusCode()}): ${response.body()}")

    val root = mapper.readTree(response.body())
    val rows = root
      .path("warehouses")
      .elements()
      .asScala
      .toSeq
      .map(node => WarehouseNameRow(warehouseId = node.path("id").asText(), name = node.path("name").asText()))
    spark.createDataset(rows)
  }

  /** One REST call per job (`GET /api/2.0/jobs/get`), bounded to the same top-N job list
    * [[loadJobRunSummary]] already shows — there's no system table recording how a job's tasks
    * are configured, only that they ran, so this is the only way to get it, and it isn't worth
    * paying for beyond the jobs already surfaced elsewhere in the report. Each job's tasks JSON
    * has exactly one `*_task`-suffixed key indicating its type (`notebook_task`,
    * `spark_jar_task`, `python_wheel_task`, etc.) — a job whose lookup fails (e.g. deleted since
    * the run-summary pull) or that has no matching key just contributes no rows, not an error.
    */
  def loadJobTasks(spark: SparkSession, cfg: DatabricksConfig, jobIds: Seq[String]): Dataset[JobTaskRow] = {
    import spark.implicits._
    val rows = jobIds.flatMap { jobId =>
      try {
        val request = HttpRequest
          .newBuilder(URI.create(s"https://${cfg.host()}/api/2.0/jobs/get?job_id=$jobId"))
          .header("Authorization", s"Bearer ${cfg.token()}")
          .GET()
          .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 200) Seq.empty
        else {
          val root = mapper.readTree(response.body())
          root
            .path("settings")
            .path("tasks")
            .elements()
            .asScala
            .toSeq
            .map { task =>
              val taskType = task.fieldNames().asScala.find(_.endsWith("_task")).getOrElse("unknown_task")
              JobTaskRow(jobId = jobId, taskKey = task.path("task_key").asText(), taskType = taskType)
            }
        }
      } catch {
        case _: Exception => Seq.empty
      }
    }
    spark.createDataset(rows)
  }

  def loadDailyJobRuns(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[DailyJobRunRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dailyJobRuns(lookbackDays)) { rs =>
      DailyJobRunRow(day = rs.getString("day"), totalRuns = rs.getLong("total_runs"), succeeded = rs.getLong("succeeded"))
    }
  }

  def loadCostByUser(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[CostByUserRow] = {
    import spark.implicits._
    query(spark, conn, Queries.costByUser(lookbackDays)) { rs =>
      CostByUserRow(actor = rs.getString("actor"), usdAmount = rs.getDouble("usd_amount"))
    }
  }

  def loadMonthlyCostBySku(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[MonthlyCostBySkuRow] = {
    import spark.implicits._
    query(spark, conn, Queries.monthlyCostBySku(lookbackDays)) { rs =>
      MonthlyCostBySkuRow(month = rs.getString("month"), skuName = rs.getString("sku_name"), usdAmount = rs.getDouble("usd_amount"))
    }
  }

  def loadDailyQueryVolumeByType(
    spark: SparkSession,
    conn: Connection,
    lookbackDays: Int
  ): Dataset[DailyQueryVolumeByTypeRow] = {
    import spark.implicits._
    query(spark, conn, Queries.dailyQueryVolumeByType(lookbackDays)) { rs =>
      DailyQueryVolumeByTypeRow(
        day = rs.getString("day"),
        actorType = rs.getString("actor_type"),
        queryCount = rs.getLong("query_count")
      )
    }
  }

  def loadServiceAccountQueryStats(
    spark: SparkSession,
    conn: Connection,
    lookbackDays: Int
  ): Dataset[ServiceAccountQueryStatsRow] = {
    import spark.implicits._
    query(spark, conn, Queries.serviceAccountQueryStats(lookbackDays)) { rs =>
      ServiceAccountQueryStatsRow(
        actor = rs.getString("actor"),
        queryCount = rs.getLong("query_count"),
        computeCount = rs.getLong("compute_count"),
        firstDay = rs.getString("first_day"),
        lastDay = rs.getString("last_day")
      )
    }
  }

  def loadServiceAccountTableAccess(
    spark: SparkSession,
    conn: Connection,
    lookbackDays: Int
  ): Dataset[ServiceAccountTableAccessRow] = {
    import spark.implicits._
    query(spark, conn, Queries.serviceAccountTableAccess(lookbackDays)) { rs =>
      ServiceAccountTableAccessRow(
        actor = rs.getString("actor"),
        tableName = rs.getString("table_name"),
        accessCount = rs.getLong("access_count")
      )
    }
  }

  def loadUserTableReads(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[UserTableAccessRow] = {
    import spark.implicits._
    query(spark, conn, Queries.userTableReads(lookbackDays)) { rs =>
      UserTableAccessRow(
        actor = rs.getString("actor"),
        tableName = rs.getString("table_name"),
        direction = "READ",
        accessCount = rs.getLong("access_count")
      )
    }
  }

  def loadUserTableWrites(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[UserTableAccessRow] = {
    import spark.implicits._
    query(spark, conn, Queries.userTableWrites(lookbackDays)) { rs =>
      UserTableAccessRow(
        actor = rs.getString("actor"),
        tableName = rs.getString("table_name"),
        direction = "WRITE",
        accessCount = rs.getLong("access_count")
      )
    }
  }

  def loadCatalogSchemaCounts(spark: SparkSession, conn: Connection): Dataset[CatalogSchemaCountRow] = {
    import spark.implicits._
    query(spark, conn, Queries.catalogSchemaCounts()) { rs =>
      CatalogSchemaCountRow(objectType = rs.getString("object_type"), objectCount = rs.getLong("object_count"))
    }
  }

  /** Unlike every other loader, this issues one `DESCRIBE DETAIL` statement per table rather than
    * a single `GROUP BY` — there's no system table exposing size across tables at once (see
    * [[Rows.TableSizeRow]]). Bounded to a small top-N table list by the caller, and each
    * individual `DESCRIBE DETAIL` is wrapped in `Try` — a table dropped since the lineage pull,
    * or one this warehouse's principal can't read, shouldn't abort the whole report.
    */
  /** Plain query, not a `Dataset` — this drives an immediate `DESCRIBE DETAIL` sweep in the same
    * runner, not something cached/reused across a Spark job, so there's nothing distributed to
    * gain from wrapping it in Spark.
    */
  def loadStorageBearingTableNames(conn: Connection): Seq[String] = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(Queries.storageBearingTableNames())
      val names = ArrayBuffer.empty[String]
      while (rs.next())
        names += s"${rs.getString("table_catalog")}.${rs.getString("table_schema")}.${rs.getString("table_name")}"
      names.toSeq
    } finally stmt.close()
  }

  /** Runs `DESCRIBE DETAIL` across every table in `tableNames`, spread over `parallelism`
    * independent JDBC connections (a `Connection` isn't safe to share across concurrent
    * statements) — the only way to get a real storage total, since no `system.*` table tracks
    * per-table size at all (see [[guild.databricksusage.queries.Rows.TableSizeRow]]). At ~1,900
    * real storage-bearing tables on this workspace, sequential would take the better part of an
    * hour; spread across 10 connections it's a few minutes. A table whose `DESCRIBE DETAIL` fails
    * (dropped since the inventory pull, or a format that doesn't support it) just contributes no
    * row, same as the old bounded top-N version did.
    */
  def loadAllTableSizes(cfg: DatabricksConfig, tableNames: Seq[String], parallelism: Int = 10): Seq[TableSizeRow] = {
    val pool = Executors.newFixedThreadPool(parallelism)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    val completed = new AtomicInteger(0)
    val total = tableNames.length

    try {
      val connections = Seq.fill(parallelism)(openConnection(cfg))
      try {
        val batches = tableNames.zipWithIndex.groupBy(_._2 % parallelism).toSeq.map { case (i, items) =>
          connections(i) -> items.map(_._1)
        }
        val futures = batches.map { case (conn, names) =>
          Future {
            names.flatMap { name =>
              val stmt = conn.createStatement()
              val row =
                try {
                  val rs = stmt.executeQuery(Queries.describeDetail(name))
                  if (rs.next())
                    Some(
                      TableSizeRow(
                        qualifiedName = name,
                        format = rs.getString("format"),
                        numFiles = rs.getLong("numFiles"),
                        sizeBytes = rs.getLong("sizeInBytes")
                      )
                    )
                  else None
                } catch {
                  case _: Exception => None
                } finally stmt.close()
              val done = completed.incrementAndGet()
              if (done % 200 == 0) println(s"  ...$done/$total table sizes fetched")
              row
            }
          }
        }
        Await.result(Future.sequence(futures), 30.minutes).flatten
      } finally connections.foreach(_.close())
    } finally pool.shutdown()
  }

  def loadMlflowSummary(conn: Connection, lookbackDays: Int): MlflowSummaryRow = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(Queries.mlflowSummary(lookbackDays))
      rs.next()
      MlflowSummaryRow(
        totalExperiments = rs.getLong("total_experiments"),
        totalRuns = rs.getLong("total_runs"),
        finishedRuns = rs.getLong("finished_runs")
      )
    } finally stmt.close()
  }

  def loadServingEndpointTypes(spark: SparkSession, conn: Connection): Dataset[ServingEndpointTypeRow] = {
    import spark.implicits._
    query(spark, conn, Queries.servingEndpointTypes()) { rs =>
      ServingEndpointTypeRow(entityType = rs.getString("entity_type"), count = rs.getLong("count"))
    }
  }

  def loadServingUsageSummary(conn: Connection, lookbackDays: Int): ServingUsageSummaryRow = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(Queries.servingUsageSummary(lookbackDays))
      rs.next()
      ServingUsageSummaryRow(totalRequests = rs.getLong("total_requests"), totalTokens = rs.getLong("total_tokens"))
    } finally stmt.close()
  }

  def loadAiGatewayUsage(spark: SparkSession, conn: Connection, lookbackDays: Int): Dataset[AiGatewayUsageRow] = {
    import spark.implicits._
    query(spark, conn, Queries.aiGatewayUsage(lookbackDays)) { rs =>
      AiGatewayUsageRow(
        destinationType = rs.getString("destination_type"),
        destinationName = rs.getString("destination_name"),
        requestCount = rs.getLong("request_count"),
        totalTokens = rs.getLong("total_tokens")
      )
    }
  }

  def loadDataQualityMonitoringSummary(conn: Connection, lookbackDays: Int): DataQualityMonitoringSummaryRow = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(Queries.dataQualityMonitoringSummary(lookbackDays))
      rs.next()
      DataQualityMonitoringSummaryRow(
        tablesMonitored = rs.getLong("tables_monitored"),
        checkRuns = rs.getLong("check_runs"),
        healthyRuns = rs.getLong("healthy_runs")
      )
    } finally stmt.close()
  }

  def loadDataClassificationSummary(conn: Connection): DataClassificationSummaryRow = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(Queries.dataClassificationSummary())
      rs.next()
      DataClassificationSummaryRow(totalResults = rs.getLong("total_results"), tablesClassified = rs.getLong("tables_classified"))
    } finally stmt.close()
  }
}
