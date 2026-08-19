package guild.comparison

import guild.reportkit.PlatformSummaryIo

import org.rogach.scallop._


/** Entry point for the Snowflake-vs-Databricks comparison report. Unlike
  * [[guild.snowflakeusage.UsageReportRunner]]/[[guild.databricksusage.DatabricksUsageReportRunner]],
  * this makes no database/warehouse connection and starts no Spark session — it only composes
  * two small `PlatformSummary` JSON files each of those runners already writes after generating
  * their own report, so there's no distributed data here to justify either.
  *
  * Usage (run both platform reports at least once first — this reads what they wrote):
  *   sbt "runMain guild.comparison.ComparisonReportRunner"
  */
object ComparisonReportRunner {

  def main(args: Array[String]): Unit = {
    val cfg = new ComparisonConfig(args)

    val snowflake = PlatformSummaryIo.read(cfg.snowflakeSummary())
    val databricks = PlatformSummaryIo.read(cfg.databricksSummary())

    val result = ComparisonAggregations.compute(snowflake, databricks)
    result.lookbackMismatchWarning.foreach(msg => println(s"WARNING: $msg"))

    ComparisonDashboard.generate(result, cfg.outputPath())
  }
}

class ComparisonConfig(arguments: Seq[String]) extends ScallopConf(arguments) {

  val snowflakeSummary: ScallopOption[String] = opt[String](
    default = Some("output/snowflake-summary.json"),
    descr = "Path to the Snowflake platform summary JSON (written by UsageReportRunner)"
  )

  val databricksSummary: ScallopOption[String] = opt[String](
    default = Some("output/databricks-summary.json"),
    descr = "Path to the Databricks platform summary JSON (written by DatabricksUsageReportRunner)"
  )

  val outputPath: ScallopOption[String] = opt[String](
    default = Some("output/comparison-report.html"),
    descr = "Path to write the generated HTML comparison report"
  )

  verify()
}
