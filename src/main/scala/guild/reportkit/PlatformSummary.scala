package guild.reportkit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.File


/** A small, stable, hand-picked slice of one platform's usage-report output — just the fields
  * that are genuinely comparable across Snowflake and Databricks, not the full aggregate object
  * either report actually renders. Written by each platform's own runner right after it builds
  * its own HTML report, and read back (with no database/warehouse connection at all) by
  * `guild.comparison.ComparisonReportRunner` to build the side-by-side comparison.
  *
  * `costIsEstimate`/`totalStorageGb: Option` exist because the two platforms' underlying data
  * isn't equally complete — Snowflake's storage total is a real, complete account-wide figure;
  * Databricks' only ever covers a bounded top-N sample (see
  * [[guild.databricksusage.aggregate.StorageSection]]), so it's `None` here rather than a
  * precise-looking number that quietly understates reality.
  */
case class PlatformSummary(
  platform: String,
  lookbackDays: Int,
  generatedAt: String,
  totalCostUsd: Double,
  costIsEstimate: Boolean,
  computeCostUsd: Double,
  // Independent from `costIsEstimate`: on Databricks, the account-wide TOTAL can be real
  // (a manually-exported Account Console figure) while the COMPUTE-ONLY breakdown stays a
  // list-price estimate (no per-SKU real-dollar data available) — see
  // guild.databricksusage.DatabricksUsageReportRunner.withRealBilledCost.
  computeCostIsEstimate: Boolean,
  // real totalCostUsd / list-price totalCostUsd, when a real total is available (Databricks
  // only, today) — lets the comparison report's migration-load projection apply the SAME
  // observed discount to its list-price-derived compute rate, instead of silently pricing a
  // migration target's workload at list price while the rest of that side is now labeled real.
  // `None` when there's no real total to compute a ratio from (including always, on Snowflake).
  realBilledDiscountRatio: Option[Double],
  totalComputeSeconds: Double,
  totalQueries: Long,
  activeUsersHuman: Double,
  activeUsersService: Double,
  failedQueryCount: Long,
  totalTables: Long,
  totalSchemas: Long,
  totalDatabasesOrCatalogs: Long,
  totalStorageGb: Option[Double],
  automationRunCount: Long,
  automationSuccessRatePercent: Double
)

object PlatformSummaryIo {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def write(summary: PlatformSummary, path: String): Unit = {
    val file = new File(path)
    Option(file.getParentFile).foreach(_.mkdirs())
    mapper.writerWithDefaultPrettyPrinter().writeValue(file, summary)
  }

  def read(path: String): PlatformSummary = {
    val file = new File(path)
    if (!file.exists())
      throw new RuntimeException(
        s"Missing $path — run that platform's own report first " +
          "(it writes this summary automatically after generating its HTML report)."
      )
    mapper.readValue(file, classOf[PlatformSummary])
  }
}
