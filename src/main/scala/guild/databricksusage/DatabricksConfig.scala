package guild.databricksusage

import org.rogach.scallop._

import scala.io.Source
import scala.util.Try


/** CLI + environment-variable configuration for the Databricks usage report.
  *
  * Auth defaults to whatever the Databricks CLI already has configured in `~/.databrickscfg`
  * (the same file `databricks auth login`/`databricks configure` write) — the host/token pair
  * is read straight from there, so no PAT needs to be re-entered by hand. `--databricks-host`/
  * `--databricks-token` (or their env vars) override the cfg file when set; either path works if
  * the user has neither configured.
  */
class DatabricksConfig(arguments: Seq[String]) extends ScallopConf(arguments) {

  val databricksProfile: ScallopOption[String] = opt[String](
    default = Some(sys.env.getOrElse("DATABRICKS_CONFIG_PROFILE", "DEFAULT")),
    descr = "Profile name in ~/.databrickscfg to read host/token from"
  )

  private lazy val cfgProfile: Map[String, String] = DatabricksConfig.readCfgProfile(databricksProfile())

  val host: ScallopOption[String] = opt[String](
    default = sys.env.get("DATABRICKS_HOST").orElse(cfgProfile.get("host")).map(DatabricksConfig.stripScheme),
    descr = "Databricks workspace hostname, e.g. dbc-xxxxxxxx-xxxx.cloud.databricks.com " +
      "(default: read from ~/.databrickscfg)"
  )

  val token: ScallopOption[String] = opt[String](
    default = sys.env.get("DATABRICKS_TOKEN").orElse(cfgProfile.get("token")),
    descr = "Databricks personal access token (default: read from ~/.databrickscfg)"
  )

  /** JDBC needs a running SQL Warehouse's `httpPath`, not just the workspace host — left unset,
    * [[DatabricksJdbc]] auto-discovers one via `GET /api/2.0/sql/warehouses` (preferring a
    * RUNNING warehouse) rather than requiring UI copy-paste for every run.
    */
  val warehouseId: ScallopOption[String] = opt[String](
    default = sys.env.get("DATABRICKS_WAREHOUSE_ID"),
    descr = "SQL warehouse id to run queries against (default: auto-discovered, preferring a RUNNING warehouse)"
  )

  val lookbackDays: ScallopOption[Int] = opt[Int](
    default = Some(sys.env.get("LOOKBACK_DAYS").map(_.toInt).getOrElse(90)),
    descr = "Days of system-table history to pull"
  )

  val cacheDir: ScallopOption[String] = opt[String](
    default = Some(sys.env.getOrElse("CACHE_DIR", "data/raw-databricks")),
    descr = "Directory to cache pulled data as Parquet, so the report can be rebuilt without re-querying"
  )

  val skipFetch: ScallopOption[Boolean] = opt[Boolean](
    default = Some(false),
    descr = "Skip Databricks entirely and rebuild the report from --cache-dir"
  )

  val outputPath: ScallopOption[String] = opt[String](
    default = Some(sys.env.getOrElse("OUTPUT_PATH", "output/databricks-usage-report.html")),
    descr = "Path to write the generated HTML report"
  )

  /** Optional real-dollars override — see [[ManualBillableUsage]] for why this can't be fetched
    * automatically. Missing file is fine; cost reporting just stays list-price-only.
    */
  val billableUsageCsv: ScallopOption[String] = opt[String](
    default = Some(sys.env.getOrElse("BILLABLE_USAGE_CSV", "data/manual/databricks-billable-usage.csv")),
    descr = "Path to a manually-exported Account Console billable-usage CSV (timestamp,dbus,dollars) " +
      "for real (not list-price) total cost, if present"
  )

  verify()

  if (!skipFetch()) {
    require(
      host.isDefined,
      "Missing Databricks host: pass --databricks-host, set DATABRICKS_HOST, or configure `databricks auth login`"
    )
    require(
      token.isDefined,
      "Missing Databricks token: pass --databricks-token, set DATABRICKS_TOKEN, or configure `databricks auth login`"
    )
  }
}

object DatabricksConfig {

  private def stripScheme(host: String): String =
    host.stripPrefix("https://").stripPrefix("http://").stripSuffix("/")

  /** Minimal INI parser for `~/.databrickscfg` — the same file the Databricks CLI reads/writes.
    * Returns an empty map (not an error) if the file or profile section is missing, so the CLI
    * flags/env vars above remain a valid fallback.
    */
  private def readCfgProfile(profileName: String): Map[String, String] = {
    val path = new java.io.File(sys.props("user.home"), ".databrickscfg")
    if (!path.exists()) return Map.empty

    val lines = Try(Source.fromFile(path).getLines().toList).getOrElse(Nil)
    val sectionStarts = lines.zipWithIndex.collect {
      case (line, idx) if line.trim.startsWith("[") && line.trim.endsWith("]") =>
        (line.trim.stripPrefix("[").stripSuffix("]"), idx)
    }
    val targetSection = sectionStarts.find(_._1.equalsIgnoreCase(profileName))
    targetSection match {
      case None => Map.empty
      case Some((_, startIdx)) =>
        val endIdx = sectionStarts.map(_._2).filter(_ > startIdx).headOption.getOrElse(lines.size)
        lines
          .slice(startIdx + 1, endIdx)
          .flatMap { line =>
            val trimmed = line.trim
            if (trimmed.isEmpty || trimmed.startsWith("#") || trimmed.startsWith(";")) None
            else
              trimmed.split("=", 2) match {
                case Array(k, v) => Some(k.trim.toLowerCase -> v.trim)
                case _           => None
              }
          }
          .toMap
    }
  }
}
