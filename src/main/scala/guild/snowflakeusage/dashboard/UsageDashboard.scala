package guild.snowflakeusage.dashboard

import guild.reportkit.dashboard.HTMLUtil._
import guild.reportkit.dashboard.JSUtil.{escapeHtml, truncateLabel}
import guild.reportkit.dashboard.PlotlyUtil
import guild.snowflakeusage.aggregate._
import guild.snowflakeusage.queries.Queries

import java.io.{BufferedWriter, FileWriter}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter


/** Assembles a self-contained HTML/Plotly usage report from [[UsageAggregates]].
  *
  * Every section builder returns `(html, js)`: `html` is the card/table markup with empty
  * chart placeholders, `js` is the matching `Plotly.newPlot` calls — concatenated once into a
  * single `<script>` block at the end of the page, following the same pattern as the
  * talent-graph pipeline's dashboard generator.
  *
  * Layout rule of thumb: any horizontal bar chart (long category labels on the y-axis) gets its
  * own full-width row — squeezed into a two-column grid, warehouse/table/user names overlap and
  * become illegible. Line/multi-line/box charts have no such label, so they pair fine at half width.
  */
object UsageDashboard {

  def generate(agg: UsageAggregates, outputPath: String, accountLocator: String): Unit = {
    val file = new java.io.File(outputPath)
    Option(file.getParentFile).foreach(_.mkdirs())

    val html = buildHtml(agg, accountLocator)
    val writer = new BufferedWriter(new FileWriter(file))
    try writer.write(html)
    finally writer.close()
    println(s"Report written to: $outputPath")
  }

  private def buildHtml(agg: UsageAggregates, accountLocator: String): String = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val title = "Snowflake Usage Report"
    val (actualLookbackDays, _) = actualDataSpan(agg)

    val sections: Seq[(String, String)] =
      agg.cost.map(buildCostSection).toSeq ++
        Seq(
          buildActivitySection(agg.activity, agg.cost),
          buildUsersSection(agg.users)
        ) ++ agg.serviceAccounts.map(buildServiceAccountsSection).toSeq ++
        Seq(
          buildStorageSection(agg.storage, agg.activity)
        ) ++ agg.tableUsage.map(buildTableUsageSection).toSeq ++
        agg.storageLifecycle.map(buildStorageLifecycleSection).toSeq ++
        Seq(buildAutomationSection(agg.automation, agg.materializedViews)) ++
        (if (agg.dataSharing.isDefined || agg.dataTransfer.isDefined)
           Seq(buildDataSharingSection(agg.dataSharing, agg.dataTransfer))
         else Seq.empty) ++
        agg.clientTools.map(buildClientToolSection).toSeq ++
        agg.loginChannels.map(buildLoginChannelSection).toSeq ++
        agg.objectInventory.map(buildObjectInventorySection).toSeq

    val sectionsHtml = sections.map(_._1).mkString("\n\n")
    val sectionsJs = sections.map(_._2).filter(_.nonEmpty).mkString("\n\n")

    val tableUsageNote =
      if (agg.tableUsage.isEmpty)
        div("note")(
          "Table usage / access-pattern analysis requires ACCESS_HISTORY, available on " +
            "Snowflake Enterprise Edition and above. Skipped for this account."
        )
      else ""

    val costNote =
      if (agg.cost.isEmpty)
        div("note")(
          "Cost analysis requires either ORGANIZATION_USAGE access (typically ORGADMIN or an " +
            "explicit grant) or --credit-price-usd/--storage-price-per-tb-usd for an estimate. " +
            "Skipped for this run."
        )
      else ""

    htmlPage(title) {
      header {
        div("")(h(1)(title)) +
          div("meta")(s"Generated $timestamp &middot; trailing $actualLookbackDays days")
      } +
        wrap {
          buildOverviewStats(agg) + "\n\n" + sectionsHtml + "\n\n" + tableUsageNote + "\n\n" + costNote + "\n\n" +
            buildQueriesUsedSection(actualLookbackDays, actualCostDataSpan(agg)._1, accountLocator)
        } +
        footer(s"$title &middot; $actualLookbackDays-day lookback &middot; $timestamp") +
        script(sectionsJs)
    }
  }

  /** The actual calendar span present in the data (day count + date-range string), never the
    * configured `--lookback-days` value — those can disagree whenever the report is rebuilt
    * from cache (`--skip-fetch`) with a different `--lookback-days` than the original fetch
    * used, or because the SQL filter is a rolling timestamp cutoff (not a calendar-day
    * boundary), which can make the observed range span one more calendar date than requested.
    * Falls back to the configured value only if there's literally no query data at all.
    */
  private def actualDataSpan(agg: UsageAggregates): (Int, String) = spanFromDays(agg.activity.dailyQueryVolume.map(_.day), agg.lookbackDays)

  /** Cost/billing data has its own independently-configurable lookback (`--cost-lookback-days`,
    * see [[guild.snowflakeusage.Config]]) — it can span a full year while `query_history` and
    * everything else stays at a few weeks, so it needs its own actual-span derivation rather than
    * reusing [[actualDataSpan]]'s day count.
    */
  private def actualCostDataSpan(agg: UsageAggregates): (Int, String) =
    spanFromDays(agg.cost.map(_.dailyTotal.map(_.day)).getOrElse(Seq.empty), agg.lookbackDays)

  /** The actual calendar span present in a series of "yyyy-MM-dd" day strings (day count +
    * date-range string), never a configured lookback value — those can disagree whenever the
    * report is rebuilt from cache (`--skip-fetch`) with a different lookback than the original
    * fetch used, or because the SQL filter is a rolling timestamp cutoff (not a calendar-day
    * boundary), which can make the observed range span one more calendar date than requested.
    * Falls back to `fallbackDays` only if there's literally no data at all.
    */
  private def spanFromDays(dayStrings: Seq[String], fallbackDays: Int): (Int, String) = {
    val days = dayStrings.sorted
    (days.headOption, days.lastOption) match {
      case (Some(first), Some(last)) =>
        val span = (LocalDate.parse(last).toEpochDay - LocalDate.parse(first).toEpochDay + 1).toInt
        val tag = if (first == last) first else s"$first to $last"
        (span, tag)
      case _ => (fallbackDays, "")
    }
  }

  // ************************************************************************************
  // Overview
  // ************************************************************************************

  private def buildOverviewStats(agg: UsageAggregates): String = {
    val totalQueries = agg.activity.dailyQueryVolume.map(_.queryCount).sum

    val (actualLookbackDays, dateRangeTag) = actualDataSpan(agg)
    val lookbackTile = stat("Day Lookback Window", sub = dateRangeTag) { actualLookbackDays.toString }

    // Cost supersedes Credits as the headline compute-usage number when it's available (it also
    // covers storage/snowpipe/etc. that warehouse credits alone don't) — Credits only appears as
    // a fallback for accounts without ORGANIZATION_USAGE access. Never show both; that's the
    // redundant pairing. Credits still gets its own charts in the Activity section either way.
    //
    // Cost has its own independently-configurable lookback (--cost-lookback-days) which can be
    // much longer than the main --lookback-days shown in the tile above — the label/sub-tag here
    // must reflect COST's own actual span, never implied to match the Day Lookback Window tile.
    val costOrCreditsTile = agg.cost match {
      case Some(c) =>
        val (costDays, costRangeTag) = actualCostDataSpan(agg)
        stat(
          if (c.isEstimate) s"Est. Cost ($costDays d)" else s"Cost ($costDays d)",
          sub = costRangeTag
        ) { f"$$${c.totalUsd}%,.0f" }
      case None =>
        stat("Credits Used") { fmtN(math.round(agg.activity.totalCredits)) }
    }

    val tiles = Seq(
      lookbackTile,
      stat("Queries (lookback)") { fmtN(totalQueries) },
      costOrCreditsTile,
      stat("Active Users") { agg.users.totalNonDisabledUsers.toString },
      stat("Active Storage") { fmtGb(agg.storage.summary.totalActiveGb) }
    )
    stats(5, "stats-square") { tiles.mkString }
  }

  // ************************************************************************************
  // Cost — actual billed USD (ORGANIZATION_USAGE) or a labeled credit-price/storage-price
  // estimate. costByWarehouse/topUsersByCost apply one account-wide $/credit rate to credit
  // breakdowns (ORGANIZATION_USAGE has no per-warehouse/per-user grain) — see
  // aggregate/Aggregations.scala costSection for exactly how that rate is derived.
  // ************************************************************************************

  private def buildCostSection(c: CostSection): (String, String) = {
    val trendJs =
      PlotlyUtil.plotlyBarLineCombo(
        "chartCostTrend",
        c.dailyTotal.map(_.day),
        c.dailyTotal.map(_.usd),
        "Daily cost",
        PlotlyUtil.SEQUENTIAL_BLUE,
        c.cumulativeDaily.map(_.day),
        c.cumulativeDaily.map(_.usd),
        "Cumulative cost",
        PlotlyUtil.ACCENT_ORANGE,
        "USD"
      )

    val byTypeJs =
      PlotlyUtil.plotlyHBar(
        "chartCostByType",
        c.byUsageType.map(_.usageType),
        c.byUsageType.map(_.usd),
        PlotlyUtil.ACCENT_ORANGE,
        "USD",
        valueFormat = "$%{x:,.2f}"
      )
    val byTypePieSlices = capToOther(c.byUsageType.map(t => t.usageType -> t.usd))
    val byTypePieJs =
      if (byTypePieSlices.nonEmpty)
        PlotlyUtil.plotlyDonut("chartCostByTypePie", byTypePieSlices.map(_._1), byTypePieSlices.map(_._2))
      else ""

    val warehouseJs =
      if (c.costByWarehouse.nonEmpty)
        PlotlyUtil.plotlyHBar(
          "chartCostByWarehouse",
          c.costByWarehouse.map(_.warehouseName),
          c.costByWarehouse.map(_.usd),
          PlotlyUtil.SEQUENTIAL_BLUE,
          "USD",
          valueFormat = "$%{x:,.2f}"
        )
      else ""
    val warehousePieSlices = capToOther(c.costByWarehouse.map(w => w.warehouseName -> w.usd))
    val warehousePieJs =
      if (warehousePieSlices.nonEmpty)
        PlotlyUtil.plotlyDonut("chartCostByWarehousePie", warehousePieSlices.map(_._1), warehousePieSlices.map(_._2))
      else ""

    val topUsersJs =
      if (c.topUsersByCost.nonEmpty)
        PlotlyUtil.plotlyHBar(
          "chartTopUsersByCost",
          c.topUsersByCost.map(_.userName),
          c.topUsersByCost.map(_.usd),
          PlotlyUtil.SEQUENTIAL_BLUE,
          "USD",
          valueFormat = "$%{x:,.2f}"
        )
      else ""
    val topUsersPieJs =
      if (c.topUsersByCostPieSlices.nonEmpty)
        PlotlyUtil.plotlyDonut(
          "chartTopUsersByCostPie",
          c.topUsersByCostPieSlices.map(_.userName),
          c.topUsersByCostPieSlices.map(_.usd)
        )
      else ""

    val basisNote =
      if (c.isEstimate)
        "Estimated from --credit-price-usd / --storage-price-per-tb-usd — not actual billed cost."
      else
        "Actual billed cost from SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY, at this account's contracted rate."

    def barPiePair(barTitle: String, barId: String, pieTitle: String, pieId: String, pieAvailable: Boolean, missingNote: String): String =
      grid(2) {
        card(barTitle, barId)() +
          (if (pieAvailable) card(pieTitle, pieId)() else div("card")(h(3)(pieTitle) + div("note")(missingNote)))
      }

    val warehousePair =
      if (c.costByWarehouse.isEmpty) ""
      else barPiePair("Cost by Warehouse", "chartCostByWarehouse", "Cost by Warehouse — Breakdown", "chartCostByWarehousePie", warehousePieSlices.nonEmpty, "")

    val topUsersPair =
      if (c.topUsersByCost.isEmpty) ""
      else
        barPiePair(
          "Top 10 Users by Cost",
          "chartTopUsersByCost",
          "Users by Cost — Breakdown",
          "chartTopUsersByCostPie",
          c.topUsersByCostPieSlices.nonEmpty,
          "Requires QUERY_ATTRIBUTION_HISTORY (Enterprise Edition+)"
        )

    // Presentation-only bucketing (the raw CostSection.monthlyBill stays complete) — keeps the
    // bill scannable by folding minor line items into "Other" rather than listing every one.
    val monthlyBillCard =
      if (c.monthlyBill.isEmpty) ""
      else {
        val months = c.monthlyBill.map(_.month).distinct.sorted
        val totalByUsageType = c.monthlyBill.groupBy(_.usageType).map { case (ut, items) => ut -> items.map(_.usd).sum }
        val usageTypesByTotal = totalByUsageType.toSeq.sortBy(-_._2).map(_._1)

        // Adjustment/credit lines (e.g. "adj for incl cloud services") are corrections, not
        // minor usage — always shown as their own row, never folded into "Other" where a large
        // negative number would look unexplained.
        val (adjustmentTypes, nonAdjustmentTypes) = usageTypesByTotal.partition(_.toLowerCase.contains("adj"))
        val topUsageTypes = nonAdjustmentTypes.take(MONTHLY_BILL_TOP_N)
        val otherUsageTypes = nonAdjustmentTypes.drop(MONTHLY_BILL_TOP_N)

        val cellByKey: Map[(String, String), Double] = c.monthlyBill.map(li => (li.month, li.usageType) -> li.usd).toMap
        val totalByMonth: Map[String, Double] = c.monthlyBill.groupBy(_.month).map { case (m, items) => m -> items.map(_.usd).sum }

        def fmtCell(v: Double): String = if (v == 0.0) "–" else f"$$$v%,.2f"
        def dollarTd(v: Double, cls: String = ""): String =
          s"""<td${if (cls.nonEmpty) s""" class="$cls"""" else ""}>${escapeHtml(fmtCell(v))}</td>"""

        val headHtml =
          "<tr>" + (Seq("Line Item", "Yearly Total") ++ months).map(h => s"<th>${escapeHtml(h)}</th>").mkString + "</tr>"

        val topRowsHtml = topUsageTypes.map { ut =>
          val rowTotal = totalByUsageType.getOrElse(ut, 0.0)
          s"<tr><td>${escapeHtml(ut)}</td>" + dollarTd(rowTotal, "rpt-bill-total-col") +
            months.map(m => dollarTd(cellByKey.getOrElse((m, ut), 0.0))).mkString + "</tr>"
        }

        val otherRowHtml =
          if (otherUsageTypes.isEmpty) ""
          else {
            val otherTotal = otherUsageTypes.map(ut => totalByUsageType.getOrElse(ut, 0.0)).sum
            val tooltip = "Includes: " + otherUsageTypes.mkString(", ")
            s"""<tr><td class="rpt-bill-other" title="${escapeHtml(tooltip)}">Other</td>""" +
              dollarTd(otherTotal, "rpt-bill-total-col") +
              months
                .map(m => dollarTd(otherUsageTypes.map(ut => cellByKey.getOrElse((m, ut), 0.0)).sum))
                .mkString + "</tr>"
          }

        val adjustmentRowsHtml = adjustmentTypes.map { ut =>
          val rowTotal = totalByUsageType.getOrElse(ut, 0.0)
          s"<tr><td>${escapeHtml(ut)}</td>" + dollarTd(rowTotal, "rpt-bill-total-col") +
            months.map(m => dollarTd(cellByKey.getOrElse((m, ut), 0.0))).mkString + "</tr>"
        }

        val grandTotal = c.monthlyBill.map(_.usd).sum
        val totalRowHtml =
          s"<tr><td><b>TOTAL</b></td>" + dollarTd(grandTotal, "rpt-bill-total-col") +
            months.map(m => dollarTd(totalByMonth.getOrElse(m, 0.0))).mkString + "</tr>"

        val billTableHtml =
          s"""<table class="rpt-table"><thead>$headHtml</thead><tbody>${topRowsHtml.mkString}$otherRowHtml${adjustmentRowsHtml.mkString}$totalRowHtml</tbody></table>"""

        grid(1) {
          div("card")(
            h(3)(s"Monthly Bill (${months.length} month${if (months.length == 1) "" else "s"})") +
              div("rpt-note")(
                "Real billed dollars by usage type and month — smaller line items are folded into " +
                  "\"Other\" (hover for what's inside); negative adjustment rows (e.g. Snowflake's " +
                  "automatic \"included cloud services\" credit) are included exactly as billed, not " +
                  "netted out. Independent of the report's main lookback — see --cost-lookback-days." +
                  " Line Item and Total stay frozen while scrolling through months."
              ) +
              div("rpt-bill-scroll")(billTableHtml)
          )
        }
      }

    val html = collapsibleSection("Cost", "costBody") {
      div("rpt-note")(basisNote) +
        grid(1) { card("Cost Over Time (daily bars, cumulative line)", "chartCostTrend")() } +
        barPiePair("Cost by Usage Type", "chartCostByType", "Cost by Usage Type — Breakdown", "chartCostByTypePie", byTypePieSlices.nonEmpty, "") +
        warehousePair +
        topUsersPair +
        monthlyBillCard
    }

    (html, Seq(trendJs, byTypeJs, byTypePieJs, warehouseJs, warehousePieJs, topUsersJs, topUsersPieJs).filter(_.nonEmpty).mkString("\n"))
  }

  /** Keeps the top 8 usage types as their own Monthly Bill rows (matches this account's real
    * data — compute/storage/cloud services/ai services/snowpipe/trust center/automatic
    * clustering/query acceleration — everything from "ai functions" down is minor by comparison);
    * the rest fold into "Other" with a hover tooltip listing what's inside.
    */
  private val MONTHLY_BILL_TOP_N = 8

  /** Sorts descending and folds everything past `cap` into an "Other" bucket, so a pie/donut
    * never exceeds the categorical palette's 6 validated slots.
    */
  private def capToOther(items: Seq[(String, Double)], cap: Int = 5): Seq[(String, Double)] = {
    val sorted = items.sortBy(-_._2)
    if (sorted.length <= cap + 1) sorted
    else sorted.take(cap) :+ ("Other" -> sorted.drop(cap).map(_._2).sum)
  }

  // ************************************************************************************
  // Query & warehouse activity
  // ************************************************************************************

  private def buildActivitySection(a: ActivitySection, cost: Option[CostSection]): (String, String) = {
    val volumeJs =
      PlotlyUtil.plotlyLine(
        "chartQueryVolume",
        a.dailyQueryVolume.map(_.day),
        a.dailyQueryVolume.map(_.queryCount.toDouble),
        PlotlyUtil.SEQUENTIAL_BLUE,
        "queries"
      )
    val volumeByTypeJs =
      if (a.dailyQueryVolumeByType.isEmpty) ""
      else
        PlotlyUtil.plotlyStackedBar(
          "chartQueryVolumeByType",
          a.dailyQueryVolumeByType
            .groupBy(_.userType)
            .toSeq
            .map { case (userType, rows) => userType -> rows.sortBy(_.day).map(r => r.day -> r.queryCount.toDouble) },
          "queries"
        )
    val durationJs =
      PlotlyUtil.plotlyBoxPerDayPrecomputed(
        "chartQueryDuration",
        a.dailyDurationSamples.map(d => (d.day, d.minSeconds, d.q1Seconds, d.medianSeconds, d.q3Seconds, d.maxSeconds)),
        PlotlyUtil.ACCENT_ORANGE,
        "seconds"
      )
    val resourceMismatchJs =
      if (a.resourceMismatchSignals.isEmpty) ""
      else
        PlotlyUtil.plotlyScatter(
          "chartResourceMismatch",
          a.resourceMismatchSignals.map(_.warehouseName),
          a.resourceMismatchSignals.map(_.totalQueuedSeconds),
          a.resourceMismatchSignals.map(_.totalSpilledGb),
          "total queued seconds",
          "total spilled GB",
          PlotlyUtil.ACCENT_ORANGE,
          directLabels = true
        )

    val resourceMismatchCard =
      if (a.resourceMismatchSignals.isEmpty) ""
      else
        grid(1) {
          div("card")(
            h(3)("Warehouse Resource Mismatch Signals") +
              div("rpt-note")(
                "Spilling (y-axis) means a query's working set didn't fit in memory — the warehouse may be " +
                  "undersized. Queueing (x-axis) means queries waited for capacity — undersized, or not " +
                  "configured for multi-cluster auto-scale. Top-right is the worst combination of both."
              ) +
              divId("chartResourceMismatch")()
          )
        }

    val totalQueries = a.dailyQueryVolume.map(_.queryCount).sum
    val failureRatePercent = if (totalQueries > 0) a.failedQueryCount.toDouble / totalQueries * 100.0 else 0.0
    val totalWastedSeconds = a.wastedComputeByWarehouse.map(_.wastedSeconds).sum

    val wastedCostTile = cost.map(c =>
      stat(
        if (c.isEstimate) "Est. $ Wasted (failures)" else "$ Wasted (failures)",
        "Estimated using each warehouse's own credits-per-second-of-query-time rate, applied to its wasted seconds."
      ) { f"$$${c.wastedComputeCostUsd}%,.0f" }
    )

    val failureStats =
      if (a.failedQueryCount == 0) ""
      else
        stats(if (wastedCostTile.isDefined) 4 else 3) {
          val tiles = Seq(
            stat("Failed Queries") { fmtN(a.failedQueryCount) },
            stat("Failure Rate") { f"$failureRatePercent%.1f%%" },
            stat("Wasted Compute Time", "Total execution time burned by queries that never succeeded.") {
              f"${totalWastedSeconds / 3600.0}%,.1f hrs"
            }
          ) ++ wastedCostTile.toSeq
          tiles.mkString
        }

    val wastedComputeJs =
      if (a.wastedComputeByWarehouse.isEmpty) ""
      else
        PlotlyUtil.plotlyHBar(
          "chartWastedCompute",
          a.wastedComputeByWarehouse.map(_.warehouseName),
          a.wastedComputeByWarehouse.map(_.wastedSeconds / 3600.0),
          PlotlyUtil.ACCENT_ORANGE,
          "wasted hours"
        )

    val wastedComputeCard =
      if (a.wastedComputeByWarehouse.isEmpty) ""
      else
        grid(1) {
          div("card")(
            h(3)("Wasted Compute by Warehouse (Failed Queries)") +
              div("rpt-note")("Execution time burned by queries that failed — real compute cost with no completed work to show for it.") +
              divId("chartWastedCompute")()
          )
        }

    val errorReasonsTable =
      if (a.topErrorReasons.isEmpty) ""
      else
        grid(1) {
          div("card")(
            h(3)("Top Error Reasons") +
              div("rpt-note")("Grouped by error code — a sample message shows what one instance of that code actually said.") +
              table(
                Seq("Error Code", "Sample Message", "Count"),
                a.topErrorReasons.map(e => Seq(e.errorCode, e.sampleMessage, e.count.toString))
              )
          )
        }

    val volumeByTypeCard =
      if (a.dailyQueryVolumeByType.isEmpty) ""
      else grid(1) { card("Daily Query Volume by User Type (People vs. Service Accounts)", "chartQueryVolumeByType")() }

    val html = collapsibleSection("Query & Warehouse Activity", "activityBody") {
      grid(2) {
          card("Daily Query Volume", "chartQueryVolume")() +
            div("card")(
              h(3)("Query Duration by Day (box plot)") +
                div("rpt-note")(
                  "Quartiles computed server-side (APPROX_PERCENTILE) — whiskers are the true min/max, " +
                    "not the classical 1.5×IQR convention; individual outlier points aren't shown here."
                ) +
                divId("chartQueryDuration")()
            )
        } +
        volumeByTypeCard +
        resourceMismatchCard +
        failureStats +
        wastedComputeCard +
        errorReasonsTable
    }

    (
      html,
      Seq(volumeJs, volumeByTypeJs, durationJs, resourceMismatchJs, wastedComputeJs)
        .filter(_.nonEmpty)
        .mkString("\n")
    )
  }

  // ************************************************************************************
  // Active users
  // ************************************************************************************

  private def buildUsersSection(u: UsersSection): (String, String) = {
    val dailyActiveJs =
      if (u.dailyActiveByType.nonEmpty)
        PlotlyUtil.plotlyStackedBar(
          "chartDailyActiveUsers",
          u.dailyActiveByType
            .groupBy(_.userType)
            .toSeq
            .map { case (userType, rows) => userType -> rows.sortBy(_.day).map(r => r.day -> r.activeUsers.toDouble) },
          "active users"
        )
      else
        PlotlyUtil.plotlyLine(
          "chartDailyActiveUsers",
          u.dailyActiveUsers.map(_.day),
          u.dailyActiveUsers.map(_.activeUsers.toDouble),
          PlotlyUtil.SEQUENTIAL_BLUE,
          "active users"
        )

    val topUsersJs =
      PlotlyUtil.plotlyHBar(
        "chartTopUsers",
        u.topUsersByQueryCount.map(_.userName),
        u.topUsersByQueryCount.map(_.queryCount.toDouble),
        PlotlyUtil.SEQUENTIAL_BLUE,
        "queries"
      )

    val html = collapsibleSection("Active Users", "usersBody") {
      grid(1) {
        card(if (u.dailyActiveByType.nonEmpty) "Daily Active Users (People vs. Service Accounts)" else "Daily Active Users", "chartDailyActiveUsers")()
      } +
        grid(1) {
          card("Top 20 Users by Query Count", "chartTopUsers")()
        }
    }

    (html, Seq(dailyActiveJs, topUsersJs).mkString("\n"))
  }

  // ************************************************************************************
  // Storage & table sizes
  // ************************************************************************************

  private def buildStorageSection(s: StorageSection, a: ActivitySection): (String, String) = {
    val fullNames = s.topTablesBySize.map(_.qualifiedName)
    val topTablesJs =
      PlotlyUtil.plotlyHBar(
        "chartTopTables",
        fullNames.map(truncateLabel(_)),
        s.topTablesBySize.map(_.activeGb),
        PlotlyUtil.SEQUENTIAL_BLUE,
        "active GB",
        fullLabels = Some(fullNames)
      )
    val dbTrendJs =
      PlotlyUtil.plotlyStackedBar(
        "chartDbStorageTrend",
        s.databaseStorageTrend
          .groupBy(_.databaseName)
          .toSeq
          .map { case (db, rows) => db -> rows.sortBy(_.day).map(r => r.day -> r.avgGb) },
        "GB"
      )
    val dbActivityJs =
      PlotlyUtil.plotlyStackedBar(
        "chartDbActivityTrend",
        a.databaseActivityTrend
          .groupBy(_.databaseName)
          .toSeq
          .map { case (db, rows) => db -> rows.sortBy(_.day).map(r => r.day -> r.queryCount.toDouble) },
        "queries"
      )

    val html = collapsibleSection("Storage & Table Sizes", "storageBody") {
      stats(2) {
        stat("Active", "Current live data — what you'd actually query, not counting historical versions.") {
          fmtGb(s.summary.totalActiveGb)
        } +
          stat(
            "Time Travel",
            "Recent historical versions kept so you can query or restore past states (UNDROP, AT/BEFORE) " +
              "within the table's configured retention period."
          ) { fmtGb(s.summary.totalTimeTravelGb) } +
          stat(
            "Fail-safe",
            "An additional fixed 7-day recovery window after Time Travel expires. Not directly accessible " +
              "— only Snowflake support can recover from it, for disaster recovery."
          ) { fmtGb(s.summary.totalFailsafeGb) } +
          stat(
            "Retained for Clone",
            "Storage still referenced by a zero-copy clone's shared micro-partitions, so it can't be freed " +
              "even though it may look deleted from the original object's perspective."
          ) { fmtGb(s.summary.totalRetainedForCloneGb) }
      } +
        grid(1) {
          card("Top 20 Tables by Active Size", "chartTopTables")()
        } +
        grid(2) {
          card("Storage by Database — Daily Trend (Top 6)", "chartDbStorageTrend")() +
            card("Activity by Database — Daily Trend (Top 6)", "chartDbActivityTrend")()
        }
    }

    (html, Seq(topTablesJs, dbTrendJs, dbActivityJs).mkString("\n"))
  }

  // ************************************************************************************
  // Table usage / access patterns
  // ************************************************************************************

  private def buildTableUsageSection(t: TableUsageSection): (String, String) = {
    val fullNames = t.topAccessedTables.map(_.objectName)
    val topAccessedJs =
      PlotlyUtil.plotlyHBar(
        "chartTopAccessed",
        fullNames.map(truncateLabel(_)),
        t.topAccessedTables.map(_.distinctQueries.toDouble),
        PlotlyUtil.SEQUENTIAL_BLUE,
        "distinct queries",
        fullLabels = Some(fullNames)
      )

    val deadTable = table(
      Seq("Table", "Active Size"),
      t.deadTables.map(d => Seq(d.qualifiedName, fmtGb(d.activeGb)))
    )

    val sizeVsAccessJs =
      if (t.sizeVsAccess.isEmpty) ""
      else
        PlotlyUtil.plotlyScatter(
          "chartSizeVsAccess",
          t.sizeVsAccess.map(_.qualifiedName),
          t.sizeVsAccess.map(_.activeGb),
          t.sizeVsAccess.map(_.distinctQueries.toDouble),
          "active GB",
          "distinct queries",
          PlotlyUtil.SEQUENTIAL_BLUE
        )

    val sizeVsAccessCard =
      if (t.sizeVsAccess.isEmpty) ""
      else
        grid(1) {
          div("card")(
            h(3)(s"Table Size vs. Access Frequency (${t.sizeVsAccess.length} largest tables)") +
              div("rpt-note")(
                "Bottom-right = big and cold — migration candidates to deprioritize. Top-right = big and " +
                  "hot — matters most for sizing the new platform. Hover for table name."
              ) +
              divId("chartSizeVsAccess")()
          )
        }

    val readWriteCard =
      if (t.readWriteDetails.isEmpty) ""
      else {
        val readWriteTable = sortableFilterableTable(
          tableId = "tblReadWrite",
          headers = Seq("Table", "Active Size", "User", "Reads", "Writes"),
          numericCols = Set(1, 3, 4),
          rows = t.readWriteDetails.map(d =>
            Seq(
              d.qualifiedName -> d.qualifiedName,
              fmtGb(d.activeGb) -> d.activeGb.toString,
              d.userName -> d.userName,
              fmtN(d.readCount) -> d.readCount.toString,
              fmtN(d.writeCount) -> d.writeCount.toString
            )
          )
        )
        grid(1) {
          div("card")(
            h(3)(s"Read/Write Activity by Table (top ${t.readWriteDetails.length} by total activity)") +
              div("rpt-note")(
                "Reads and writes per table/user, side by side — click a column header to sort (e.g. by " +
                  "Active Size or Writes), or type to filter by table or user name."
              ) +
              readWriteTable
          )
        }
      }

    val html = collapsibleSection("Table Usage / Access Patterns", "tableUsageBody") {
      grid(1) {
        card("Most-Queried Tables", "chartTopAccessed")()
      } +
        sizeVsAccessCard +
        grid(1) {
          div("card")(
            h(3)(s"Not Accessed In Window (${t.deadTables.length} shown, largest first)") +
              div("rpt-note")("Migration candidates to deprioritize or drop — largest storage, zero reads.") +
              deadTable
          )
        } +
        readWriteCard
    }

    (html, Seq(topAccessedJs, sizeVsAccessJs).filter(_.nonEmpty).mkString("\n"))
  }

  // ************************************************************************************
  // Automation & orchestration — Tasks/Snowpipe have no 1:1 Databricks equivalent, so this
  // matters for migration scoping beyond cost. Sub-parts degrade independently: `queryTypeBreakdown`
  // is always present, but the task/pipe tables and stream stats disappear individually if their
  // source view wasn't available (edition/grant gated) rather than hiding the whole section.
  // ************************************************************************************

  private def buildAutomationSection(a: AutomationSection, mv: Option[MaterializedViewsSection]): (String, String) = {
    val taskCard =
      if (a.taskRuns.isEmpty) ""
      else {
        val taskTable = table(
          Seq("Task", "Type", "Total Runs", "Succeeded", "Failed"),
          a.taskRuns.map(t => Seq(t.taskName, t.taskType, t.totalRuns.toString, t.succeeded.toString, t.failed.toString))
        )
        grid(1) {
          div("card")(
            h(3)("Tasks (Scheduled Orchestration — no 1:1 Databricks equivalent)") + taskTable
          )
        }
      }

    val pipeCard =
      if (a.pipes.isEmpty) ""
      else {
        val pipeTable = table(
          Seq("Pipe", "Credits Used", "Files Inserted", "Data Inserted"),
          a.pipes.map(p =>
            Seq(p.pipeName, f"${p.totalCredits}%.2f", fmtN(p.totalFilesInserted), fmtGb(p.totalBytesInsertedGb))
          )
        )
        grid(1) {
          div("card")(h(3)("Snowpipe Ingestion (closest Databricks analog: Auto Loader)") + pipeTable)
        }
      }

    val streamsCard = a.streams
      .map(s =>
        grid(1) {
          div("card")(
            h(3)("Streams") +
              stats(2) {
                stat("Total Streams") { s.totalStreams.toString } +
                  stat("Stale Streams", "Offset expired past retention — broken until recreated.") {
                    s.staleStreams.toString
                  }
              } +
              (if (s.totalStreams == 0)
                 div("rpt-note")(
                   "SHOW STREAMS only lists streams the report's role can see — a 0 here means \"none visible to " +
                     "this role,\" not necessarily \"none exist.\" Confirm with a role that has broader visibility " +
                     "(or check ACCOUNT_USAGE.OBJECT_DEPENDENCIES for referencing_object_domain = 'STREAM') before " +
                     "concluding this account has no streams."
                 )
               else "")
          )
        }
      )
      .getOrElse("")

    val mvCard = mv
      .filter(_.refreshes.nonEmpty)
      .map { m =>
        val mvTable = table(
          Seq("Materialized View", "Total Refreshes", "Credits Used", "Avg Duration (s)"),
          m.refreshes.map(r =>
            Seq(r.viewName, r.totalRefreshes.toString, f"${r.totalCredits}%.2f", f"${r.avgDurationSeconds}%.1f")
          )
        )
        grid(1) {
          div("card")(
            h(3)("Materialized Views (automated refresh — closest Databricks analog: a scheduled refresh job)") + mvTable
          )
        }
      }
      .getOrElse("")

    val missingNote =
      if (a.taskRuns.isEmpty && a.pipes.isEmpty && a.streams.isEmpty && mv.forall(_.refreshes.isEmpty))
        div("note")(
          "Tasks, Snowpipe, Streams, and Materialized Views data weren't available for this account (edition/grant-gated views)."
        )
      else ""

    val html = collapsibleSection("Automation & Orchestration", "automationBody") {
      streamsCard +
        taskCard +
        pipeCard +
        mvCard +
        missingNote
    }

    (html, "")
  }

  // ************************************************************************************
  // Service accounts deep-dive
  // ************************************************************************************

  private def buildServiceAccountsSection(s: ServiceAccountsSection): (String, String) = {
    val topJs =
      PlotlyUtil.plotlyHBar(
        "chartTopServiceAccounts",
        s.topServiceAccounts.map(_.userName),
        s.topServiceAccounts.map(_.queryCount.toDouble),
        PlotlyUtil.SEQUENTIAL_BLUE,
        "queries"
      )

    val hasReadWrite = s.topServiceAccounts.exists(a => a.readCount > 0 || a.writeCount > 0)
    val summaryHeaders =
      if (hasReadWrite) Seq("Service Account", "Queries", "Reads", "Writes", "Warehouses Used", "First Active", "Last Active")
      else Seq("Service Account", "Queries", "Warehouses Used", "First Active", "Last Active")
    val summaryTable = table(
      summaryHeaders,
      s.topServiceAccounts.map { a =>
        val base = Seq(a.userName, fmtN(a.queryCount))
        val readWrite = if (hasReadWrite) Seq(fmtN(a.readCount), fmtN(a.writeCount)) else Seq.empty
        base ++ readWrite ++ Seq(a.warehouseCount.toString, a.firstActiveDay, a.lastActiveDay)
      }
    )

    val tableAccessCard =
      if (s.topTablesByServiceAccount.isEmpty) ""
      else
        grid(1) {
          div("card")(
            h(3)("Top Tables Accessed by Service Accounts") +
              div("rpt-note")("Which tables each service account actually reads or writes — requires ACCESS_HISTORY (Enterprise Edition+).") +
              table(
                Seq("Service Account", "Table", "Distinct Queries"),
                s.topTablesByServiceAccount.map(t => Seq(t.userName, t.tableName, fmtN(t.distinctQueries)))
              )
          )
        }

    val html = collapsibleSection("Service Accounts", "serviceAccountsBody") {
      grid(1) {
        card(s"Service Accounts by Query Count (${s.topServiceAccounts.length} total)", "chartTopServiceAccounts")()
      } +
        grid(1) {
          div("card")(h(3)("Service Account Activity") + summaryTable)
        } +
        tableAccessCard
    }

    (html, topJs)
  }

  // ************************************************************************************
  // Secure Data Sharing & data transfer (egress) — grouped together since both are about data
  // moving beyond this account's own compute/storage, relevant to migration scoping and cost.
  // ************************************************************************************

  private def buildDataSharingSection(
    sharing: Option[DataSharingSection],
    transfer: Option[DataTransferSection]
  ): (String, String) = {
    val sharingBlock = sharing
      .map { d =>
        stats(3) {
          stat("Total Shares") { d.totalShares.toString } +
            stat("Outbound Shares", "Shares this account created for other accounts to consume.") { d.outboundShares.toString } +
            stat("Inbound Shares", "Shares this account consumes from another account.") { d.inboundShares.toString }
        } +
          (if (d.shares.isEmpty) ""
           else
             grid(1) {
               div("card")(
                 h(3)("Share Inventory") +
                   table(
                     Seq("Share", "Kind", "Database", "Shared To"),
                     d.shares.map(s => Seq(s.shareName, s.kind, s.databaseName, s.toAccounts))
                   )
               )
             })
      }
      .getOrElse("")

    val transferBlock = transfer
      .map { d =>
        val hasCrossRegion = d.byDestination.exists(_.crossRegion)
        grid(1) {
          div("card")(
            h(3)(s"Data Transfer — ${fmtGb(d.totalGb)} total") +
              div("rpt-note")(
                "DATA_TRANSFER_HISTORY carries no explicit ingress/egress flag — direction is read off " +
                  "comparing source vs. target below. \"Same region\" rows are external-stage I/O " +
                  (if (hasCrossRegion)
                     "(not real network transfer); only the cross-region rows are true egress out of the account's home region — no inbound (ingress) transfer was observed in this window."
                   else "(not real network transfer) — no cross-region transfer (true egress) was observed in this window.")
              ) +
              table(
                Seq("Transfer Type", "Source", "Target", "Cross-Region?", "Data Transferred"),
                d.byDestination.map(t =>
                  Seq(
                    t.transferType,
                    s"${t.sourceCloud}/${t.sourceRegion}",
                    s"${t.targetCloud}/${t.targetRegion}",
                    if (t.crossRegion) "Yes" else "No",
                    fmtGb(t.totalGb)
                  )
                )
              )
          )
        }
      }
      .getOrElse("")

    val html = collapsibleSection("Data Sharing & Transfer", "dataSharingBody") {
      sharingBlock + transferBlock
    }

    (html, "")
  }

  // ************************************************************************************
  // Client / tool breakdown — what's actually connecting to Snowflake (SESSIONS.CLIENT_APPLICATION_ID)
  // ************************************************************************************

  private def buildClientToolSection(c: ClientToolSection): (String, String) = {
    val slices = capToOther(c.byClient.map(b => b.clientApplicationId -> b.sessionCount.toDouble))

    val barJs =
      PlotlyUtil.plotlyHBar(
        "chartClientTools",
        c.byClient.map(_.clientApplicationId),
        c.byClient.map(_.sessionCount.toDouble),
        PlotlyUtil.SEQUENTIAL_BLUE,
        "sessions"
      )
    val pieJs =
      if (slices.nonEmpty) PlotlyUtil.plotlyDonut("chartClientToolsPie", slices.map(_._1), slices.map(_._2)) else ""

    val html = collapsibleSection("Client / Tool Breakdown", "clientToolsBody") {
      div("rpt-note")(
        "What's actually connecting to Snowflake — BI tools and drivers need a re-point in a Databricks migration; " +
          "native Snowflake-only tooling doesn't carry over."
      ) +
        grid(2) {
          card("Sessions by Client Application", "chartClientTools")() +
            (if (slices.nonEmpty) card("Sessions by Client — Breakdown", "chartClientToolsPie")() else "")
        }
    }

    (html, Seq(barJs, pieJs).filter(_.nonEmpty).mkString("\n"))
  }

  // ************************************************************************************
  // Object inventory — account-wide counts, full migration surface area.
  // ************************************************************************************

  private def buildObjectInventorySection(o: ObjectInventorySection): (String, String) = {
    val tiles = o.counts.map(c => stat(c.objectType) { fmtN(c.count) })
    val html = collapsibleSection("Object Inventory", "objectInventoryBody") {
      stats(math.max(1, o.counts.length), "stats-square") { tiles.mkString }
    }
    (html, "")
  }

  // ************************************************************************************
  // Storage lifecycle policies — Snowflake's actual cold-storage feature. Always renders (even
  // at 0) since this view is account-wide, not role-scoped — a 0 here is reliable evidence the
  // account isn't using it, unlike the Streams tile's caveat.
  // ************************************************************************************

  private def buildStorageLifecycleSection(s: StorageLifecycleSection): (String, String) = {
    val body =
      if (s.policies.isEmpty)
        div("rpt-note")(
          "No storage lifecycle policies (COOL/COLD archive tiers) are configured on this account — " +
            "every table's active storage is billed at the same rate regardless of access recency. " +
            "This is opt-in per table, not automatic, so this is expected unless someone has " +
            "deliberately set one up."
        )
      else
        table(
          Seq("Policy", "Database", "Schema", "Archive Tier", "Archive After (days)"),
          s.policies.map(p => Seq(p.policyName, p.databaseName, p.schemaName, p.archiveTier, p.archiveForDays.toString))
        )

    val html = collapsibleSection(s"Storage Lifecycle Policies (Cold Storage) — ${s.totalPolicies} configured", "storageLifecycleBody") {
      body
    }
    (html, "")
  }

  // ************************************************************************************
  // Login channel attribution — CLIENT_IP matched against known network-policy IP allowlists.
  // See aggregate/Aggregations.scala KNOWN_NETWORK_POLICIES for the hand-maintained IP list and
  // its limitations (not derived from Snowflake metadata, must be kept in sync manually).
  // ************************************************************************************

  private def buildLoginChannelSection(l: LoginChannelSection): (String, String) = {
    val barJs =
      PlotlyUtil.plotlyHBar(
        "chartLoginChannels",
        l.byChannel.map(_.channel),
        l.byChannel.map(_.loginCount.toDouble),
        PlotlyUtil.SEQUENTIAL_BLUE,
        "logins"
      )
    val pieSlices = capToOther(l.byChannel.map(c => c.channel -> c.loginCount.toDouble))
    val pieJs =
      if (pieSlices.nonEmpty) PlotlyUtil.plotlyDonut("chartLoginChannelsPie", pieSlices.map(_._1), pieSlices.map(_._2)) else ""

    val twingateCount = l.byChannel.find(_.channel == "Twingate VPN").map(_.loginCount).getOrElse(0L)
    val twingatePercent = if (l.totalLogins > 0) twingateCount.toDouble / l.totalLogins * 100.0 else 0.0
    val uiCount = l.byClientType.find(_.clientType == "SNOWFLAKE_UI").map(_.loginCount).getOrElse(0L)
    val uiPercent = if (l.totalLogins > 0) uiCount.toDouble / l.totalLogins * 100.0 else 0.0

    val clientTypeBarJs =
      PlotlyUtil.plotlyHBar(
        "chartLoginClientTypes",
        l.byClientType.map(_.clientType),
        l.byClientType.map(_.loginCount.toDouble),
        PlotlyUtil.SEQUENTIAL_BLUE,
        "logins"
      )
    val clientTypePieSlices = capToOther(l.byClientType.map(c => c.clientType -> c.loginCount.toDouble))
    val clientTypePieJs =
      if (clientTypePieSlices.nonEmpty)
        PlotlyUtil.plotlyDonut("chartLoginClientTypesPie", clientTypePieSlices.map(_._1), clientTypePieSlices.map(_._2))
      else ""

    val html = collapsibleSection("Login Channel Attribution", "loginChannelsBody") {
      div("rpt-note")(
        "CLIENT_IP per login, matched against this account's known network-policy IP allowlists " +
          "(Twingate, dbt Cloud, Fivetran, etc. — hand-maintained from Terraform, not derived from " +
          "Snowflake metadata). \"Unmatched\" means the IP didn't fall in any allowlist this report " +
          "knows about — most direct/non-allowlisted logins land here, not necessarily anything unusual. " +
          "\"Logins by Channel\" (network) and \"Logins by Client Type\" (what connected) are independent " +
          "axes — a Snowsight login can come from any network, so it isn't its own channel below."
      ) +
        stats(3) {
          stat("Total Logins") { fmtN(l.totalLogins) } +
            stat("Twingate VPN Logins", "Matched against the Twingate network policy's IP allowlist.") {
              s"${fmtN(twingateCount)} (${f"$twingatePercent%.1f"}%)"
            } +
            stat("Snowflake UI Logins", "REPORTED_CLIENT_TYPE = SNOWFLAKE_UI — logged in via Snowsight, not a driver.") {
              s"${fmtN(uiCount)} (${f"$uiPercent%.1f"}%)"
            }
        } +
        grid(2) {
          card("Logins by Channel (Network)", "chartLoginChannels")() +
            (if (pieSlices.nonEmpty) card("Logins by Channel — Breakdown", "chartLoginChannelsPie")() else "")
        } +
        grid(2) {
          card("Logins by Client Type", "chartLoginClientTypes")() +
            (if (clientTypePieSlices.nonEmpty) card("Logins by Client Type — Breakdown", "chartLoginClientTypesPie")() else "")
        }
    }

    (
      html,
      Seq(barJs, pieJs, clientTypeBarJs, clientTypePieJs).filter(_.nonEmpty).mkString("\n")
    )
  }

  private def fmtN(n: Long): String =
    if (n >= 1000000) f"${n / 1000000.0}%.1fM"
    else if (n >= 1000) f"${n / 1000.0}%.1fk"
    else n.toString

  /** Auto-scales a GB figure to the next metric unit up once it crosses 4 digits — e.g.
    * 181935.1 GB reads as 177.7 TB instead. Only for standalone stat numbers/table cells, which
    * can each pick their own best unit; chart axes keep one fixed unit for the whole chart.
    */
  private def fmtGb(gb: Double): String =
    if (gb >= 1024.0 * 1024.0) f"${gb / (1024.0 * 1024.0)}%,.1f PB"
    else if (gb >= 1024.0) f"${gb / 1024.0}%,.1f TB"
    else f"$gb%,.1f GB"

  // ************************************************************************************
  // Queries used — reference only, collapsed by default. Reuses the exact same Queries
  // methods the runner called to fetch data, so what's shown here can never drift from what
  // actually ran.
  // ************************************************************************************

  private def buildQueriesUsedSection(lookbackDays: Int, costLookbackDays: Int, accountLocator: String): String = {
    val queries: Seq[(String, String)] = Seq(
      "Users" -> Queries.users,
      "Daily Query Stats" -> Queries.dailyQueryStats(lookbackDays),
      "Daily Query By User" -> Queries.dailyQueryByUser(lookbackDays),
      "Top Users By Query Count" -> Queries.topUsersByQueryCount(lookbackDays),
      "Database Activity By Day" -> Queries.databaseActivityByDay(lookbackDays),
      "Warehouse Resource Signals" -> Queries.warehouseResourceSignals(lookbackDays),
      "Warehouse Exec Stats" -> Queries.warehouseExecStats(lookbackDays),
      "Top Error Reasons" -> Queries.topErrorReasons(lookbackDays),
      "Daily Duration Stats" -> Queries.dailyDurationStats(lookbackDays),
      "Warehouse Metering History" -> Queries.warehouseMeteringHistory(lookbackDays),
      "Table Storage Metrics" -> Queries.tableStorageMetrics,
      "Database Storage Usage History" -> Queries.databaseStorageUsageHistory(lookbackDays),
      "Table Access Counts" -> Queries.tableAccessCounts(lookbackDays),
      "Table Read/Write By User" -> Queries.tableReadWriteByUser(lookbackDays),
      "User Action Counts" -> Queries.userActionCounts(lookbackDays),
      "User Table Access Counts" -> Queries.userTableAccessCounts(lookbackDays),
      "Cost Usage (ORGANIZATION_USAGE, independent lookback)" -> Queries.costUsageDaily(costLookbackDays, accountLocator),
      "User Types" -> Queries.userTypes,
      "Query Attribution History" -> Queries.queryAttributionByUser(lookbackDays),
      "Task History" -> Queries.taskHistory(lookbackDays),
      "Pipe Usage History" -> Queries.pipeUsageHistory(lookbackDays),
      "Streams Inventory" -> Queries.streamsInventory,
      "Shares Inventory" -> Queries.sharesInventory,
      "Materialized View Refresh History" -> Queries.materializedViewRefreshHistory(lookbackDays),
      "Data Transfer History" -> Queries.dataTransferHistory(lookbackDays),
      "Object Inventory Counts" -> Queries.objectInventoryCounts,
      "Sessions By Client App (Client/Tool Breakdown)" -> Queries.sessionsByClientApp(lookbackDays),
      "Storage Lifecycle Policies (Cold Storage)" -> Queries.storageLifecyclePolicies,
      "Logins By IP And Client Type (Channel Attribution)" -> Queries.loginsByIpAndClientType(lookbackDays)
    )

    collapsibleSection("Queries Used (Reference)", "queriesUsedBody", startCollapsed = true) {
      div("rpt-note")(
        "The exact SQL run against Snowflake for this report, with this run's actual lookback " +
          "window and account locator substituted in."
      ) +
        queries.map { case (name, sql) => div("card")(h(3)(name) + codeBlock(sql)) }.mkString("\n")
    }
  }
}
