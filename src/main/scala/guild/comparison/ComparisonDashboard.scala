package guild.comparison

import guild.reportkit.dashboard.HTMLUtil._

import java.io.{BufferedWriter, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


/** Assembles the self-contained HTML Snowflake-vs-Databricks comparison report from a
  * [[ComparisonResult]] — reuses [[guild.reportkit.dashboard.HTMLUtil]] verbatim, same as both
  * platform-specific dashboards. No Plotly charts here: almost everything being shown is a
  * single aggregate number per platform, which a comparison table renders more clearly than a
  * chart would.
  */
object ComparisonDashboard {

  def generate(result: ComparisonResult, outputPath: String): Unit = {
    val file = new java.io.File(outputPath)
    Option(file.getParentFile).foreach(_.mkdirs())

    val html = buildHtml(result)
    val writer = new BufferedWriter(new FileWriter(file))
    try writer.write(html)
    finally writer.close()
    println(s"Report written to: $outputPath")
  }

  private def buildHtml(r: ComparisonResult): String = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val title = "Snowflake vs. Databricks — Usage Comparison"

    val mismatchBanner = r.lookbackMismatchWarning match {
      case Some(msg) => div("note")(msg)
      case None      => ""
    }

    htmlPage(title, hasPlotly = false) {
      header {
        div("")(h(1)(title)) +
          div("meta")(
            s"Generated $timestamp &middot; Snowflake: trailing ${r.snowflake.lookbackDays} days " +
              s"(as of ${r.snowflake.generatedAt}) &middot; Databricks: trailing ${r.databricks.lookbackDays} " +
              s"days (as of ${r.databricks.generatedAt})"
          )
      } +
        wrap {
          mismatchBanner + "\n\n" +
            buildOverviewSection(r) + "\n\n" +
            buildCostSection(r) + "\n\n" +
            buildProjectionSection(r) + "\n\n" +
            buildActivitySection(r) + "\n\n" +
            buildStorageSection(r) + "\n\n" +
            buildAutomationSection(r) + "\n\n" +
            buildObjectInventorySection(r) + "\n\n" +
            buildCapabilityMatrixSection(r)
        } +
        footer(s"$title &middot; $timestamp")
    }
  }

  private def comparisonTable(rows: Seq[(String, String, String)]): String =
    table(Seq("Metric", "Snowflake", "Databricks"), rows.map { case (m, sf, dbx) => Seq(m, sf, dbx) })

  private def buildOverviewSection(r: ComparisonResult): String = {
    val sf = r.snowflake
    val dbx = r.databricks
    collapsibleSection("Overview", "cmpOverviewBody") {
      grid(1) {
        div("card")(
          h(3)("At a Glance") +
            comparisonTable(
              Seq(
                ("Lookback Window", s"${sf.lookbackDays} days", s"${dbx.lookbackDays} days"),
                ("Total Queries", fmtN(sf.totalQueries), fmtN(dbx.totalQueries)),
                (
                  "Total Cost",
                  fmtUsdWithBasis(sf.totalCostUsd, sf.costIsEstimate),
                  fmtUsdWithBasis(dbx.totalCostUsd, dbx.costIsEstimate)
                )
              )
            )
        )
      }
    }
  }

  private def buildCostSection(r: ComparisonResult): String = {
    val sf = r.snowflake
    val dbx = r.databricks
    collapsibleSection("Cost", "cmpCostBody") {
      div("rpt-note")(
        "The (real billed) vs. (est.) qualifier on every dollar figure below matters: Snowflake's is " +
          s"${if (sf.costIsEstimate) "an estimate" else "real billed dollars"}; Databricks' Total Cost is " +
          s"${if (dbx.costIsEstimate) "a list-price estimate (no negotiated-discount data available via system tables)"
            else "real billed dollars (manually exported from the Account Console — no API access to it exists yet)"}" +
          ", but its Compute-Only Cost always stays a list-price estimate — there's no per-SKU real-dollar " +
          "breakdown available even when the total is real. See each platform's own report for the exact basis."
      ) +
        grid(1) {
          div("card")(
            h(3)("Cost Breakdown") +
              comparisonTable(
                Seq(
                  (
                    "Total Cost",
                    fmtUsdWithBasis(sf.totalCostUsd, sf.costIsEstimate),
                    fmtUsdWithBasis(dbx.totalCostUsd, dbx.costIsEstimate)
                  ),
                  (
                    "Compute-Only Cost",
                    fmtUsdWithBasis(sf.computeCostUsd, sf.computeCostIsEstimate),
                    fmtUsdWithBasis(dbx.computeCostUsd, dbx.computeCostIsEstimate)
                  )
                )
              )
          )
        }
    }
  }

  private def buildProjectionSection(r: ComparisonResult): String = {
    val p = r.projection
    collapsibleSection("Migration Load Projection", "cmpProjectionBody") {
      div("rpt-note")(
        "Method: take Snowflake's actual compute-seconds workload for this window and price it at " +
          "Databricks' own OBSERVED $-per-compute-second rate today (its real compute-only cost " +
          "divided by its real total compute-seconds) — not an assumed or industry-average " +
          "multiplier. Does NOT account for: query-engine efficiency differences between the two " +
          "platforms, concurrency/auto-scaling behavior, storage or egress cost, or the fact that " +
          "both platforms' own $/compute-second figures are themselves blended rates across many " +
          "warehouse sizes/SKUs, not a single like-for-like unit. Treat this as a rough order-of-" +
          "magnitude signal, not a quote."
      ) +
        grid(1) {
          div("card")(
            h(3)("If Snowflake's Current SQL Workload Ran on Databricks") +
              comparisonTable(
                Seq(
                  ("Compute Time (this window)", fmtSeconds(p.snowflakeComputeSeconds), fmtSeconds(p.snowflakeComputeSeconds)),
                  (
                    "Actual/Observed Compute Cost",
                    fmtUsdWithBasis(p.snowflakeComputeCostUsd, r.snowflake.computeCostIsEstimate),
                    fmtUsdRate(p.databricksObservedRatePerSecond) + " (est.)"
                  ),
                  (
                    "Projected Cost for Snowflake's Workload",
                    fmtUsdWithBasis(p.snowflakeComputeCostUsd, r.snowflake.computeCostIsEstimate),
                    fmtUsd(p.projectedDatabricksCostUsd) + " (projected est.)"
                  )
                ) ++ p.projectedDatabricksCostUsdRealAdjusted.map { adjusted =>
                  (
                    "Projected Cost, Adjusted for Observed Discount",
                    "—",
                    fmtUsd(adjusted) + " (projected, discount-adjusted est.)"
                  )
                }
              ) +
              div("rpt-note")(
                projectionDeltaNote(p.snowflakeComputeCostUsd, p.projectedDatabricksCostUsd)
              ) +
              (for {
                ratio <- p.realBilledDiscountRatioApplied
                adjusted <- p.projectedDatabricksCostUsdRealAdjusted
              } yield {
                val discountPercent = (1.0 - ratio) * 100.0
                div("rpt-note")(
                  f"The discount-adjusted row above applies Databricks' observed real/list-price ratio " +
                    f"account-wide ($discountPercent%,.1f%% off list) to the list-price projection — an " +
                    "extrapolation, not a second real number: that discount was observed across the WHOLE " +
                    "bill (storage, serverless, model serving, everything), never confirmed to apply " +
                    "uniformly to compute specifically. " +
                    projectionDeltaNote(p.snowflakeComputeCostUsd, adjusted)
                )
              }).getOrElse("")
          )
        }
    }
  }

  private def projectionDeltaNote(snowflakeUsd: Double, projectedUsd: Double): String = {
    if (snowflakeUsd <= 0.0) "Snowflake's compute cost for this window is $0 — nothing to project."
    else {
      val deltaPercent = math.abs((projectedUsd - snowflakeUsd) / snowflakeUsd * 100.0)
      val direction = if (projectedUsd >= snowflakeUsd) "more" else "less"
      f"At Databricks' current observed rate, this workload would cost about $deltaPercent%,.0f%% $direction than it does on Snowflake today."
    }
  }

  private def buildActivitySection(r: ComparisonResult): String = {
    val sf = r.snowflake
    val dbx = r.databricks
    def failureRate(failed: Long, total: Long): String = if (total > 0) f"${failed.toDouble / total * 100.0}%.2f%%" else "0.00%"

    collapsibleSection("Query & Compute Activity", "cmpActivityBody") {
      grid(1) {
        div("card")(
          h(3)("Activity") +
            comparisonTable(
              Seq(
                ("Total Queries", fmtN(sf.totalQueries), fmtN(dbx.totalQueries)),
                ("Failed Queries", fmtN(sf.failedQueryCount), fmtN(dbx.failedQueryCount)),
                ("Failure Rate", failureRate(sf.failedQueryCount, sf.totalQueries), failureRate(dbx.failedQueryCount, dbx.totalQueries)),
                ("Total Compute Time", fmtSeconds(sf.totalComputeSeconds), fmtSeconds(dbx.totalComputeSeconds)),
                ("Avg. Daily Active Person Users", f"${sf.activeUsersHuman}%,.1f", f"${dbx.activeUsersHuman}%,.1f"),
                ("Avg. Daily Active Service Identities", f"${sf.activeUsersService}%,.1f", f"${dbx.activeUsersService}%,.1f")
              )
            )
        )
      }
    }
  }

  private def buildStorageSection(r: ComparisonResult): String = {
    val sf = r.snowflake
    val dbx = r.databricks
    collapsibleSection("Storage", "cmpStorageBody") {
      div("rpt-note")(
        "Both figures are real, complete account-wide totals. Snowflake's comes from " +
          "TABLE_STORAGE_METRICS directly; Databricks has no equivalent system table (it " +
          "delegates most storage to the workspace's own cloud account rather than metering it " +
          "centrally), so its total comes from a full DESCRIBE DETAIL sweep across every " +
          "MANAGED/EXTERNAL/STREAMING_TABLE/MATERIALIZED_VIEW table instead — see its own " +
          "report's Storage section for the per-table breakdown."
      ) +
        grid(1) {
          div("card")(
            h(3)("Total Storage") +
              comparisonTable(
                Seq(
                  (
                    "Total Active Storage",
                    sf.totalStorageGb.map(fmtGb).getOrElse("Not available"),
                    dbx.totalStorageGb.map(fmtGb).getOrElse("Not available")
                  )
                )
              )
          )
        }
    }
  }

  private def buildAutomationSection(r: ComparisonResult): String = {
    val sf = r.snowflake
    val dbx = r.databricks
    collapsibleSection("Automation & Orchestration", "cmpAutomationBody") {
      div("rpt-note")("Snowflake Tasks vs. Databricks Jobs (Lakeflow) — see the Capability Parity Matrix below for how these actually differ, not just in run count.") +
        grid(1) {
          div("card")(
            h(3)("Scheduled Runs") +
              comparisonTable(
                Seq(
                  ("Total Runs (Tasks / Jobs)", fmtN(sf.automationRunCount), fmtN(dbx.automationRunCount)),
                  ("Success Rate", f"${sf.automationSuccessRatePercent}%.1f%%", f"${dbx.automationSuccessRatePercent}%.1f%%")
                )
              )
          )
        }
    }
  }

  private def buildObjectInventorySection(r: ComparisonResult): String = {
    val sf = r.snowflake
    val dbx = r.databricks
    collapsibleSection("Object Inventory", "cmpObjectInventoryBody") {
      grid(1) {
        div("card")(
          h(3)("Object Counts") +
            comparisonTable(
              Seq(
                ("Databases / Catalogs", fmtN(sf.totalDatabasesOrCatalogs), fmtN(dbx.totalDatabasesOrCatalogs)),
                ("Schemas", fmtN(sf.totalSchemas), fmtN(dbx.totalSchemas)),
                ("Tables + Views", fmtN(sf.totalTables), fmtN(dbx.totalTables))
              )
            )
        )
      }
    }
  }

  private def buildCapabilityMatrixSection(r: ComparisonResult): String =
    collapsibleSection("Capability Parity Matrix", "cmpParityBody") {
      div("rpt-note")(
        "Hand-authored from product knowledge, not pulled from either report — this is about what " +
          "each platform's feature actually IS, not a number either system tracks."
      ) +
        grid(1) {
          div("card")(
            table(
              Seq("Feature", "Snowflake", "Databricks", "Note"),
              r.capabilityMatrix.map(c => Seq(c.feature, c.snowflake, c.databricks, c.note))
            )
          )
        }
    }

  private def fmtN(n: Long): String = f"$n%,d"

  private def fmtUsd(usd: Double): String = f"$$$usd%,.2f"

  /** Appends the estimate/real qualifier directly onto the dollar figure itself — a separate
    * "basis" row/note below the number is too easy to miss at a glance, and $ figures with no
    * inline qualifier read as equally authoritative even when one is real billed dollars and the
    * other is a list-price estimate.
    */
  private def fmtUsdWithBasis(usd: Double, isEstimate: Boolean): String =
    fmtUsd(usd) + (if (isEstimate) " (est.)" else " (real billed)")

  private def fmtUsdRate(usdPerSecond: Double): String = f"$$$usdPerSecond%,.4f / sec"

  private def fmtSeconds(seconds: Double): String =
    if (seconds >= 3600) f"${seconds / 3600.0}%,.1f hrs" else f"$seconds%,.0f sec"

  private def fmtGb(gb: Double): String =
    if (gb >= 1024.0 * 1024.0) f"${gb / (1024.0 * 1024.0)}%,.1f PB"
    else if (gb >= 1024.0) f"${gb / 1024.0}%,.1f TB"
    else f"$gb%,.1f GB"
}
