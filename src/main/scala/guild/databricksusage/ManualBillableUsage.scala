package guild.databricksusage

import java.io.File
import java.time.LocalDate
import scala.io.Source

/** One day of real billed usage, manually exported from the Databricks Account Console's Usage
  * page (`https://accounts.cloud.databricks.com/usage/legacy`) — a browser-SSO-authenticated view
  * that reflects the account's actual negotiated rate, unlike every other cost figure in this
  * report (which comes from `system.billing.list_prices`, list price only; see
  * [[guild.databricksusage.queries.DailyCostRow]]). There's no equivalent API reachable with a
  * workspace-scoped token — the account-level billing/usage-download APIs all 404 against it (see
  * databricks_usage_report_tool memory) — so this file has to be exported and dropped in by hand.
  */
case class BillableUsageDay(date: LocalDate, dbus: Double, dollars: Double)

/** Optional: if the export isn't present, cost reporting falls back to the list-price estimate
  * only, exactly as before this existed. */
object ManualBillableUsage {

  def loadIfPresent(path: String): Option[Seq[BillableUsageDay]] = {
    val file = new File(path)
    if (!file.exists()) None
    else {
      val lines = Source.fromFile(file).getLines().toList
      val rows = lines.drop(1).filter(_.nonEmpty).map { line =>
        val Array(timestamp, dbus, dollars) = line.split(",")
        BillableUsageDay(LocalDate.parse(timestamp.take(10)), dbus.toDouble, dollars.toDouble)
      }
      Some(rows)
    }
  }

  /** Real total for an explicit `[start, end]` date range (inclusive) — deliberately NOT a
    * "trailing lookbackDays from today" computation: under `--skip-fetch`, `agg.overview
    * .lookbackDays` reflects whatever `--lookback-days` this rebuild-from-cache invocation
    * passed/defaulted to, not necessarily the window the cached data was actually fetched at (a
    * real pre-existing gap — `loadFromCache` never persisted the fetch's own lookback). Callers
    * should instead derive `start`/`end` from the list-price data's own date range
    * (`agg.cost.dailyTotal`), which is always ground truth for what the report is covering.
    * Returns `None` — never a partial/understated sum — if the export doesn't reach back to
    * `start`.
    */
  def totalForDateRange(rows: Seq[BillableUsageDay], start: LocalDate, end: LocalDate): Option[Double] = {
    val earliestAvailable = if (rows.isEmpty) None else Some(rows.minBy(_.date.toEpochDay).date)
    if (earliestAvailable.isEmpty || earliestAvailable.exists(_.isAfter(start))) None
    else Some(rows.filter(r => !r.date.isBefore(start) && !r.date.isAfter(end)).map(_.dollars).sum)
  }
}
