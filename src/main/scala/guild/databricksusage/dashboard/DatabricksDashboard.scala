package guild.databricksusage.dashboard

import guild.databricksusage.aggregate._
import guild.databricksusage.queries.Queries
import guild.reportkit.dashboard.HTMLUtil._
import guild.reportkit.dashboard.JSUtil.escapeHtml
import guild.reportkit.dashboard.PlotlyUtil

import java.io.{BufferedWriter, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


/** Assembles a self-contained HTML/Plotly Databricks usage report from
  * [[DatabricksUsageAggregates]] — mirrors [[guild.snowflakeusage.dashboard.UsageDashboard]]'s
  * shape and reuses its rendering primitives verbatim (see `guild.reportkit.dashboard`).
  */
object DatabricksDashboard {

  def generate(agg: DatabricksUsageAggregates, outputPath: String, workspaceHost: String): Unit = {
    val file = new java.io.File(outputPath)
    Option(file.getParentFile).foreach(_.mkdirs())

    val html = buildHtml(agg, workspaceHost)
    val writer = new BufferedWriter(new FileWriter(file))
    try writer.write(html)
    finally writer.close()
    println(s"Report written to: $outputPath")
  }

  private def buildHtml(agg: DatabricksUsageAggregates, workspaceHost: String): String = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val title = "Databricks Usage Report"
    val lookbackDays = agg.overview.lookbackDays

    val (costHtml, costJs) = buildCostSection(agg.cost)
    val (activityHtml, activityJs) = buildActivitySection(agg.activity)
    val (usersHtml, usersJs) = buildUsersSection(agg.users)
    val (serviceAccountsHtml, serviceAccountsJs) = buildServiceAccountsSection(agg.serviceAccounts)
    val (tableAccessHtml, tableAccessJs) = buildTableAccessSection(agg.tableAccess)
    val (storageHtml, storageJs) = buildStorageSection(agg.storage)
    val (objectInventoryHtml, objectInventoryJs) = buildObjectInventorySection(agg.objectInventory)
    val (automationHtml, automationJs) = buildAutomationSection(agg.automation)
    val (databricksNativeHtml, databricksNativeJs) = buildDatabricksNativeSection(agg.databricksNative)

    htmlPage(title) {
      header {
        div("")(h(1)(title)) +
          div("meta")(s"Generated $timestamp &middot; $workspaceHost &middot; trailing $lookbackDays days")
      } +
        wrap {
          buildOverviewStats(agg.overview, agg.cost) + "\n\n" + costHtml + "\n\n" + activityHtml + "\n\n" +
            usersHtml + "\n\n" + serviceAccountsHtml + "\n\n" + tableAccessHtml + "\n\n" +
            storageHtml + "\n\n" + objectInventoryHtml + "\n\n" + automationHtml + "\n\n" +
            databricksNativeHtml + "\n\n" +
            buildQueriesUsedSection(lookbackDays)
        } +
        footer(s"$title &middot; $lookbackDays-day lookback &middot; $timestamp") +
        script(
          Seq(
            costJs,
            activityJs,
            usersJs,
            serviceAccountsJs,
            tableAccessJs,
            storageJs,
            objectInventoryJs,
            automationJs,
            databricksNativeJs
          ).mkString("\n")
        )
    }
  }

  private def buildOverviewStats(o: OverviewSection, c: CostSection): String =
    stats(3) {
      stat("Day Lookback Window")(o.lookbackDays.toString) +
        stat("Total Queries")(f"${o.totalQueries}%,d") +
        stat("Active Users", tooltip = "Distinct executed_by identities in system.query.history, person and service principal alike")(
          f"${o.activeUsers}%,d"
        ) +
        (c.realTotalUsd match {
          case Some(real) =>
            stat(
              "Total Cost (real billed)",
              tooltip = "Manually exported from the Account Console's Usage page — the account's actual negotiated rate, " +
                "not list price"
            )(f"$$$real%,.2f")
          case None =>
            stat(
              "Estimated Cost",
              tooltip = "List-price estimate from system.billing.usage × system.billing.list_prices — not negotiated/discounted billed dollars"
            )(f"$$${o.totalCostUsd}%,.2f")
        })
    }

  private def barPiePair(
    barTitle: String,
    barId: String,
    pieTitle: String,
    pieId: String,
    pieAvailable: Boolean,
    missingNote: String
  ): String =
    grid(2) {
      card(barTitle, barId)() +
        (if (pieAvailable) card(pieTitle, pieId)() else div("card")(h(3)(pieTitle) + div("note")(missingNote)))
    }

  private val MONTHLY_BILL_TOP_N = 8

  private def buildCostSection(c: CostSection): (String, String) = {
    val trendJs =
      PlotlyUtil.plotlyLine(
        "chartDbxCostTrend",
        c.dailyTotal.map(_.day),
        c.dailyTotal.map(_.usd),
        PlotlyUtil.SEQUENTIAL_BLUE,
        "USD"
      )

    val bySkuJs =
      if (c.bySku.nonEmpty)
        PlotlyUtil.plotlyHBar(
          "chartDbxCostBySku",
          c.bySku.map(_.skuName),
          c.bySku.map(_.usd),
          PlotlyUtil.ACCENT_ORANGE,
          "USD",
          valueFormat = "$%{x:,.2f}"
        )
      else ""

    val bySkuPieSlices = capToOther(c.bySku.map(s => s.skuName -> s.usd))
    val bySkuPieJs =
      if (bySkuPieSlices.nonEmpty)
        PlotlyUtil.plotlyDonut("chartDbxCostBySkuPie", bySkuPieSlices.map(_._1), bySkuPieSlices.map(_._2))
      else ""

    val topUsersJs =
      if (c.topUsersByCost.nonEmpty)
        PlotlyUtil.plotlyHBar(
          "chartDbxTopUsersByCost",
          c.topUsersByCost.map(_.actor),
          c.topUsersByCost.map(_.usd),
          PlotlyUtil.SEQUENTIAL_BLUE,
          "USD",
          valueFormat = "$%{x:,.2f}"
        )
      else ""
    val topUsersPieJs =
      if (c.topUsersByCostPieSlices.nonEmpty)
        PlotlyUtil.plotlyDonut(
          "chartDbxTopUsersByCostPie",
          c.topUsersByCostPieSlices.map(_.actor),
          c.topUsersByCostPieSlices.map(_.usd)
        )
      else ""
    val topUsersPair =
      if (c.topUsersByCost.isEmpty) ""
      else
        barPiePair(
          "Top Users by Cost",
          "chartDbxTopUsersByCost",
          "Users by Cost — Breakdown",
          "chartDbxTopUsersByCostPie",
          c.topUsersByCostPieSlices.nonEmpty,
          ""
        )

    val monthlyBillCard =
      if (c.monthlyBill.isEmpty) ""
      else {
        val months = c.monthlyBill.map(_.month).distinct.sorted
        val totalBySku = c.monthlyBill.groupBy(_.sku).map { case (sku, items) => sku -> items.map(_.usd).sum }
        val skusByTotal = totalBySku.toSeq.sortBy(-_._2).map(_._1)
        val topSkus = skusByTotal.take(MONTHLY_BILL_TOP_N)
        val otherSkus = skusByTotal.drop(MONTHLY_BILL_TOP_N)

        val cellByKey: Map[(String, String), Double] = c.monthlyBill.map(li => (li.month, li.sku) -> li.usd).toMap
        val totalByMonth: Map[String, Double] = c.monthlyBill.groupBy(_.month).map { case (m, items) => m -> items.map(_.usd).sum }

        def dollarTd(v: Double, cls: String = ""): String =
          s"""<td${if (cls.nonEmpty) s""" class="$cls"""" else ""}>${escapeHtml(f"$$$v%,.2f")}</td>"""

        val headHtml = "<tr>" + (Seq("SKU", "Total") ++ months).map(h => s"<th>${escapeHtml(h)}</th>").mkString + "</tr>"

        val topRowsHtml = topSkus.map { sku =>
          s"<tr><td>${escapeHtml(sku)}</td>" + dollarTd(totalBySku.getOrElse(sku, 0.0), "rpt-bill-total-col") +
            months.map(m => dollarTd(cellByKey.getOrElse((m, sku), 0.0))).mkString + "</tr>"
        }
        val otherRowHtml =
          if (otherSkus.isEmpty) ""
          else {
            val otherTotal = otherSkus.map(sku => totalBySku.getOrElse(sku, 0.0)).sum
            val tooltip = "Includes: " + otherSkus.mkString(", ")
            s"""<tr><td class="rpt-bill-other" title="${escapeHtml(tooltip)}">Other</td>""" +
              dollarTd(otherTotal, "rpt-bill-total-col") +
              months.map(m => dollarTd(otherSkus.map(sku => cellByKey.getOrElse((m, sku), 0.0)).sum)).mkString + "</tr>"
          }
        val grandTotal = c.monthlyBill.map(_.usd).sum
        val totalRowHtml = s"<tr><td><b>TOTAL</b></td>" + dollarTd(grandTotal, "rpt-bill-total-col") +
          months.map(m => dollarTd(totalByMonth.getOrElse(m, 0.0))).mkString + "</tr>"

        val billTableHtml =
          s"""<table class="rpt-table"><thead>$headHtml</thead><tbody>${topRowsHtml.mkString}$otherRowHtml$totalRowHtml</tbody></table>"""

        grid(1) {
          div("card")(
            h(3)(s"Itemized Monthly Bill (${months.length} month${if (months.length == 1) "" else "s"}, by SKU)") +
              div("rpt-note")(
                "List-price estimate — smaller SKUs are folded into \"Other\" (hover for what's inside). " +
                  "SKU and Total stay frozen while scrolling through months."
              ) +
              div("rpt-bill-scroll")(billTableHtml)
          )
        }
      }

    val realVsListCard = c.realTotalUsd match {
      case Some(real) =>
        val discountPercent = (1.0 - real / c.totalUsd) * 100.0
        grid(1) {
          div("card")(
            h(3)("Real Billed vs. List Price") +
              stats(3) {
                stat("List-Price Estimate")(f"$$${c.totalUsd}%,.2f") +
                  stat("Real Billed (Account Console export)")(f"$$$real%,.2f") +
                  stat("Effective Discount")(f"$discountPercent%,.1f%%")
              } +
              div("rpt-note")(
                "Real total is manually exported from the Account Console's Usage page (browser-SSO-only — " +
                  "no reachable API with the workspace token this report uses). It's the one real dollar figure " +
                  "in this section; every SKU/user/monthly breakdown below it is still list-price-derived, since " +
                  "there's no per-SKU real-dollar attribution available."
              )
          )
        }
      case None => ""
    }

    val html = collapsibleSection("Cost", "dbxCostBody") {
      div("rpt-note")(
        "List-price estimate from system.billing.usage joined to system.billing.list_prices " +
          "at the price effective when each usage record started — won't reflect negotiated discounts."
      ) +
        realVsListCard +
        grid(1) { card("Daily Cost", "chartDbxCostTrend")() } +
        grid(2) {
          card("Cost by SKU", "chartDbxCostBySku")() +
            (if (bySkuPieSlices.nonEmpty) card("Cost by SKU — Breakdown", "chartDbxCostBySkuPie")() else "")
        } +
        topUsersPair +
        monthlyBillCard
    }

    (html, Seq(trendJs, bySkuJs, bySkuPieJs, topUsersJs, topUsersPieJs).filter(_.nonEmpty).mkString("\n"))
  }

  private def buildActivitySection(a: ActivitySection): (String, String) = {
    val volumeJs =
      PlotlyUtil.plotlyLine(
        "chartDbxDailyQueryVolume",
        a.dailyQueryVolume.map(_.day),
        a.dailyQueryVolume.map(_.queryCount.toDouble),
        PlotlyUtil.SEQUENTIAL_BLUE,
        "queries"
      )

    val byType: Map[String, Seq[DailyQueryVolumeByType]] = a.dailyQueryVolumeByType.groupBy(_.actorType)
    val typeDays = a.dailyQueryVolumeByType.map(_.day).distinct.sorted
    val volumeByTypeSeries = Seq("person", "service").flatMap { t =>
      byType.get(t).map(rows => t -> typeDays.map(d => d -> rows.find(_.day == d).map(_.queryCount.toDouble).getOrElse(0.0)))
    }
    val volumeByTypeJs =
      if (volumeByTypeSeries.nonEmpty) PlotlyUtil.plotlyStackedBar("chartDbxVolumeByType", volumeByTypeSeries, "queries") else ""
    val volumeByTypeCard =
      if (volumeByTypeSeries.isEmpty) ""
      else grid(1) { card("Daily Query Volume by User Type (People vs. Service Accounts)", "chartDbxVolumeByType")() }

    val durationJs =
      PlotlyUtil.plotlyBoxPerDayPrecomputed(
        "chartDbxDailyDuration",
        a.dailyDurationSamples.map(d =>
          (d.day, d.minMs / 1000.0, d.q1Ms / 1000.0, d.medianMs / 1000.0, d.q3Ms / 1000.0, d.maxMs / 1000.0)
        ),
        PlotlyUtil.ACCENT_ORANGE,
        "duration (sec)"
      )

    val byComputeJs =
      if (a.computeExecStats.nonEmpty)
        PlotlyUtil.plotlyHBar(
          "chartDbxQueriesByCompute",
          a.computeExecStats.map(c => s"${c.computeId} (${c.computeType})"),
          a.computeExecStats.map(_.totalQueries.toDouble),
          PlotlyUtil.SEQUENTIAL_BLUE,
          "queries"
        )
      else ""

    val resourceMismatchJs =
      if (a.computeResourceSignals.isEmpty) ""
      else
        PlotlyUtil.plotlyScatter(
          "chartDbxResourceMismatch",
          a.computeResourceSignals.map(_.computeId),
          a.computeResourceSignals.map(_.totalQueuedMs / 1000.0),
          a.computeResourceSignals.map(_.totalSpilledBytes / 1e9),
          "total queued seconds",
          "total spilled GB",
          PlotlyUtil.ACCENT_ORANGE,
          directLabels = true
        )
    val resourceMismatchCard =
      if (a.computeResourceSignals.isEmpty) ""
      else
        grid(1) {
          div("card")(
            h(3)("Compute Resource-Mismatch Signals") +
              div("rpt-note")(
                "Spilling (y-axis) means a query's working set didn't fit in memory — the compute " +
                  "may be undersized. Queueing (x-axis) means queries waited for capacity. " +
                  "Top-right is the worst combination of both — Databricks' analog to Snowflake's " +
                  "warehouse spill/queueing pair."
              ) +
              divId("chartDbxResourceMismatch")()
          )
        }

    val totalQueries = a.dailyQueryVolume.map(_.queryCount).sum
    val totalFailed = a.computeExecStats.map(_.failedCount).sum
    val totalFailedExecMs = a.computeExecStats.map(_.failedExecMs).sum
    val failureRatePercent = if (totalQueries > 0) totalFailed.toDouble / totalQueries * 100.0 else 0.0
    val failureStats =
      if (totalFailed == 0) ""
      else
        stats(3) {
          stat("Failed Queries")(fmtN(totalFailed)) +
            stat("Failure Rate")(f"$failureRatePercent%.1f%%") +
            stat("Wasted Compute Time", "Total execution time burned by queries that never succeeded.") {
              f"${totalFailedExecMs / 3600000.0}%,.1f hrs"
            }
        }

    val errorReasonsTable =
      if (a.topErrorReasons.isEmpty) ""
      else
        grid(1) {
          div("card")(
            h(3)("Top Error Reasons") +
              div("rpt-note")(
                "Grouped by the leading [BRACKETED_ERROR_CLASS] Databricks SQL errors carry when " +
                  "present; messages without one fold into \"OTHER\"."
              ) +
              table(
                Seq("Error Class", "Sample Message", "Count"),
                a.topErrorReasons.map(e => Seq(e.errorClass, e.sampleMessage, e.count.toString))
              )
          )
        }

    val html = collapsibleSection("Query & Compute Activity", "dbxActivityBody") {
      grid(2) {
        card("Daily Query Volume", "chartDbxDailyQueryVolume")() +
          div("card")(
            h(3)("Query Duration by Day (box plot)") +
              div("rpt-note")(
                "Quartiles computed server-side (approx_percentile) — whiskers are the true " +
                  "min/max, not the classical 1.5×IQR convention; individual outlier points aren't shown here."
              ) +
              divId("chartDbxDailyDuration")()
          )
      } +
        volumeByTypeCard +
        (if (a.computeExecStats.nonEmpty) grid(1) { card("Queries by Compute Endpoint", "chartDbxQueriesByCompute")() }
         else "") +
        resourceMismatchCard +
        failureStats +
        errorReasonsTable
    }

    (
      html,
      Seq(volumeJs, volumeByTypeJs, durationJs, byComputeJs, resourceMismatchJs).filter(_.nonEmpty).mkString("\n")
    )
  }

  private def buildUsersSection(u: UsersSection): (String, String) = {
    val byDay: Map[String, Seq[DailyActiveUsers]] = u.dailyActiveUsers.groupBy(_.actorType)
    val days = u.dailyActiveUsers.map(_.day).distinct.sorted
    val series = Seq("person", "service").flatMap { t =>
      byDay.get(t).map(rows => t -> days.map(d => d -> rows.find(_.day == d).map(_.distinctUsers.toDouble).getOrElse(0.0)))
    }
    val dailyActiveJs =
      if (series.nonEmpty) PlotlyUtil.plotlyStackedBar("chartDbxDailyActiveUsers", series, "distinct users") else ""

    val topActorsJs =
      if (u.topActors.nonEmpty)
        PlotlyUtil.plotlyHBar(
          "chartDbxTopActors",
          u.topActors.map(_.actor),
          u.topActors.map(_.actionCount.toDouble),
          PlotlyUtil.SEQUENTIAL_BLUE,
          "actions"
        )
      else ""

    val byServicePieSlices = capToOther(u.byService.map(s => s.serviceName -> s.actionCount.toDouble))
    val byServiceJs =
      if (byServicePieSlices.nonEmpty)
        PlotlyUtil.plotlyDonut("chartDbxActionsByService", byServicePieSlices.map(_._1), byServicePieSlices.map(_._2))
      else ""

    val html = collapsibleSection("Users", "dbxUsersBody") {
      div("rpt-note")(
        "\"person\" vs \"service\" is inferred from system.access.audit's user_identity.email — " +
          "an address means a person, anything else (a UUID service-principal id, or the " +
          "literal System-User) is bucketed as service. Individual @guild.com employees are " +
          "lumped into a single \"Employee@guild.com\" bucket everywhere a person would " +
          "otherwise be named individually; service-principal UUIDs are instead resolved to " +
          "their real names via the workspace's service-principal list, where one exists."
      ) +
        grid(2) {
          card("Daily Active Users (Human vs. Service)", "chartDbxDailyActiveUsers")() +
            (if (u.topActors.nonEmpty) card("Top Actors by Action Count", "chartDbxTopActors")() else "")
        } +
        (if (byServicePieSlices.nonEmpty) grid(1) { card("Actions by Service Area", "chartDbxActionsByService")() } else "")
    }

    (html, Seq(dailyActiveJs, topActorsJs, byServiceJs).filter(_.nonEmpty).mkString("\n"))
  }

  private def buildServiceAccountsSection(s: ServiceAccountsSection): (String, String) = {
    val topJs =
      if (s.topServiceAccounts.isEmpty) ""
      else
        PlotlyUtil.plotlyHBar(
          "chartDbxTopServiceAccounts",
          s.topServiceAccounts.map(_.actor),
          s.topServiceAccounts.map(_.queryCount.toDouble),
          PlotlyUtil.SEQUENTIAL_BLUE,
          "queries"
        )

    val summaryTable =
      if (s.topServiceAccounts.isEmpty) ""
      else
        table(
          Seq("Service Actor", "Queries", "Compute Endpoints Used", "First Active", "Last Active"),
          s.topServiceAccounts.map(a => Seq(a.actor, fmtN(a.queryCount), a.computeCount.toString, a.firstDay, a.lastDay))
        )

    val tableAccessCard =
      if (s.topTablesByServiceAccount.isEmpty) ""
      else
        grid(1) {
          div("card")(
            h(3)("Top Tables Accessed by Service Actors") +
              div("rpt-note")(
                "From system.access.table_lineage.created_by, filtered to non-email (service) " +
                  "identities; excludes Databricks' own internal logging/monitoring schemas " +
                  "(system.__internal_*), which have no resolvable owner. A raw UUID/id below " +
                  "means the actor isn't in this workspace's current service-principal list — " +
                  "likely deleted since, or scoped outside what this token can see."
              ) +
              table(
                Seq("Service Actor", "Table", "Lineage Events"),
                s.topTablesByServiceAccount.map(t => Seq(t.actor, t.tableName, fmtN(t.accessCount)))
              )
          )
        }

    val html = collapsibleSection("Service Accounts", "dbxServiceAccountsBody") {
      div("rpt-note")(
        "\"Service Actor\" is resolved from the raw UUID system.query.history/table_lineage " +
          "record to its real name via the workspace's SCIM service-principal list — shown as " +
          "the bare UUID only when no match exists there (e.g. the literal System-User, or a " +
          "principal since deleted)."
      ) +
        (if (s.topServiceAccounts.nonEmpty) grid(1) { card("Service Actors by Query Count", "chartDbxTopServiceAccounts")() }
         else "") +
        (if (summaryTable.nonEmpty) grid(1) { div("card")(h(3)("Service Actor Activity") + summaryTable) } else "") +
        tableAccessCard
    }

    (html, topJs)
  }

  private def buildTableAccessSection(t: TableAccessSection): (String, String) = {
    val readJs =
      if (t.topReadTables.nonEmpty)
        PlotlyUtil.plotlyHBar(
          "chartDbxTopReadTables",
          t.topReadTables.map(_.tableName),
          t.topReadTables.map(_.accessCount.toDouble),
          PlotlyUtil.SEQUENTIAL_BLUE,
          "lineage events"
        )
      else ""
    val writeJs =
      if (t.topWriteTables.nonEmpty)
        PlotlyUtil.plotlyHBar(
          "chartDbxTopWriteTables",
          t.topWriteTables.map(_.tableName),
          t.topWriteTables.map(_.accessCount.toDouble),
          PlotlyUtil.ACCENT_ORANGE,
          "lineage events"
        )
      else ""

    val readWriteCard =
      if (t.readWriteDetails.isEmpty) ""
      else {
        val readWriteTable = sortableFilterableTable(
          tableId = "dbxTblReadWrite",
          headers = Seq("Table", "Size", "Actor", "Reads", "Writes"),
          numericCols = Set(1, 3, 4),
          rows = t.readWriteDetails.map(d =>
            Seq(
              d.tableName -> d.tableName,
              d.sizeBytes.map(fmtBytes).getOrElse("—") -> d.sizeBytes.getOrElse(0L).toString,
              d.actor -> d.actor,
              fmtN(d.readCount) -> d.readCount.toString,
              fmtN(d.writeCount) -> d.writeCount.toString
            )
          )
        )
        grid(1) {
          div("card")(
            h(3)(s"Read/Write Activity by Table (top ${t.readWriteDetails.length} by total activity)") +
              div("rpt-note")(
                "From system.access.table_lineage.created_by, person actors only (service actors are " +
                  "covered in the Service Accounts section). \"Size\" is only populated for the " +
                  "handful of tables also looked up in the Storage section below — click a column " +
                  "header to sort, or type to filter by table or actor name."
              ) +
              readWriteTable
          )
        }
      }

    val html = collapsibleSection("Table Access", "dbxTableAccessBody") {
      div("rpt-note")(
        "From system.access.table_lineage — a row per lineage edge, counted per source " +
          "(read) or target (destination write) table. No single stable query id spans both " +
          "sides the way Snowflake's access_history union does, so this counts lineage events, " +
          "not distinct queries."
      ) +
        grid(2) {
          card("Top Read Tables", "chartDbxTopReadTables")() +
            card("Top Write Tables", "chartDbxTopWriteTables")()
        } +
        readWriteCard
    }

    (html, Seq(readJs, writeJs).filter(_.nonEmpty).mkString("\n"))
  }

  private def buildStorageSection(s: StorageSection): (String, String) = {
    if (s.tableSizes.isEmpty) ("", "")
    else {
      val sizeJs =
        PlotlyUtil.plotlyHBar(
          "chartDbxTableSizes",
          s.tableSizes.map(_.qualifiedName),
          s.tableSizes.map(t => t.sizeBytes / 1e9),
          PlotlyUtil.SEQUENTIAL_BLUE,
          "GB"
        )

      val html = collapsibleSection("Storage & Table Sizes", "dbxStorageBody") {
        div("rpt-note")(
          "Unity Catalog has no system table exposing per-table size the way Snowflake's " +
            "TABLE_STORAGE_METRICS does — Databricks delegates most storage to the workspace's " +
            "own cloud account rather than metering it centrally. The total below comes from a " +
            s"real, full DESCRIBE DETAIL sweep across all ${fmtN(s.tablesCounted)} " +
            "MANAGED/EXTERNAL/STREAMING_TABLE/MATERIALIZED_VIEW tables (views have no storage of " +
            "their own; foreign tables' data lives in the source system) — a genuine account-wide " +
            "total, not a sample. Includes both Delta and Iceberg tables — several of the largest " +
            "are Snowflake-managed Iceberg tables already mounted into Unity Catalog."
        ) +
          stats(1) { stat("Total Storage (all real tables)")(fmtBytes(s.totalSizeBytes)) } +
          grid(1) { card(s"Largest ${s.tableSizes.length} Tables", "chartDbxTableSizes")() } +
          grid(1) {
            div("card")(
              h(3)(s"Largest ${s.tableSizes.length} Tables — Detail") +
                table(
                  Seq("Table", "Format", "Files", "Size"),
                  s.tableSizes.map(t => Seq(t.qualifiedName, t.format, fmtN(t.numFiles), fmtBytes(t.sizeBytes)))
                )
            )
          }
      }

      (html, sizeJs)
    }
  }

  private def buildObjectInventorySection(o: ObjectInventorySection): (String, String) = {
    val byTypeJs =
      if (o.byTableType.nonEmpty)
        PlotlyUtil.plotlyDonut("chartDbxObjectInventory", o.byTableType.map(_.tableType), o.byTableType.map(_.objectCount.toDouble))
      else ""

    val html = collapsibleSection("Object Inventory", "dbxObjectInventoryBody") {
      stats(3) {
        stat("Catalogs")(fmtN(o.catalogCount)) +
          stat("Schemas")(fmtN(o.schemaCount)) +
          stat("Tables/Views (all types)")(fmtN(o.byTableType.map(_.objectCount).sum))
      } +
        (if (o.byTableType.nonEmpty) grid(1) { card("Objects by Type", "chartDbxObjectInventory")() } else "")
    }

    (html, byTypeJs)
  }

  private def buildAutomationSection(a: AutomationSection): (String, String) = {
    val trendJs =
      if (a.dailyJobRuns.isEmpty) ""
      else
        PlotlyUtil.plotlyBarLineCombo(
          "chartDbxDailyJobRuns",
          a.dailyJobRuns.map(_.day),
          a.dailyJobRuns.map(_.totalRuns.toDouble),
          "Total runs",
          PlotlyUtil.SEQUENTIAL_BLUE,
          a.dailyJobRuns.map(_.day),
          a.dailyJobRuns.map(_.succeeded.toDouble),
          "Succeeded",
          PlotlyUtil.ACCENT_ORANGE,
          "runs"
        )

    val jobsTable =
      if (a.jobRuns.isEmpty) ""
      else
        grid(1) {
          div("card")(
            h(3)(s"Lakeflow Jobs (Databricks Workflows) — Top ${a.jobRuns.length} by Run Count") +
              div("rpt-note")(
                "Databricks' closest analog to Snowflake Tasks — scheduled/triggered orchestration. " +
                  "\"Job Name\" falls back to the raw job id when the job's been deleted since it last ran."
              ) +
              table(
                Seq("Job Name", "Total Runs", "Succeeded", "Failed/Other", "Avg Duration (sec)", "Total Hours"),
                a.jobRuns.map(j =>
                  Seq(
                    j.jobName,
                    f"${j.totalRuns}%,d",
                    f"${j.succeeded}%,d",
                    f"${j.failed}%,d",
                    f"${j.avgDurationSeconds}%,.1f",
                    f"${j.totalHours}%,.1f"
                  )
                )
              )
          )
        }

    val taskTypeJs =
      if (a.taskTypeBreakdown.isEmpty) ""
      else PlotlyUtil.plotlyDonut("chartDbxJobTaskTypes", a.taskTypeBreakdown.map(_.taskType), a.taskTypeBreakdown.map(_.count.toDouble))
    val taskTypeCard =
      if (a.taskTypeBreakdown.isEmpty) ""
      else
        grid(1) {
          div("card")(
            h(3)(s"How Jobs Are Configured (tasks across the top ${a.jobRuns.length} jobs above)") +
              div("rpt-note")(
                "From the Jobs API, not a system table — no system table records task configuration, " +
                  "only that a job ran. Scoped to the jobs shown above, not every job in the workspace."
              ) +
              divId("chartDbxJobTaskTypes")()
          )
        }

    val html = collapsibleSection("Automation & Orchestration", "dbxAutomationBody") {
      (if (a.jobRuns.nonEmpty)
         stats(4) {
           stat("Distinct Jobs Run", "COUNT(DISTINCT job_id) over the whole lookback window, not just the top jobs shown below.")(
             fmtN(a.distinctJobsRun)
           ) +
             stat("Total Runs")(fmtN(a.totalRuns)) +
             stat("Success Rate")(f"${a.successRatePercent}%.1f%%") +
             stat("Total Run Hours", "Sum of execution_duration_seconds over every job run in the window.")(
               f"${a.totalRunHours}%,.1f"
             )
         }
       else "") +
        (if (a.dailyJobRuns.nonEmpty) grid(1) { card("Daily Job Runs (total vs. succeeded)", "chartDbxDailyJobRuns")() } else "") +
        jobsTable +
        taskTypeCard
    }

    (html, Seq(trendJs, taskTypeJs).filter(_.nonEmpty).mkString("\n"))
  }

  private def buildDatabricksNativeSection(n: DatabricksNativeSection): (String, String) = {
    val servingByTypeJs =
      if (n.servingEndpointsByType.isEmpty) ""
      else
        PlotlyUtil.plotlyDonut(
          "chartDbxServingEndpointTypes",
          n.servingEndpointsByType.map(_.entityType),
          n.servingEndpointsByType.map(_.count.toDouble)
        )

    val aiGatewayByDestJs =
      if (n.aiGatewayByDestination.isEmpty) ""
      else
        PlotlyUtil.plotlyHBar(
          "chartDbxAiGatewayDestinations",
          n.aiGatewayByDestination.map(d => s"${d.destinationName} (${d.destinationType})"),
          n.aiGatewayByDestination.map(_.requestCount.toDouble),
          PlotlyUtil.SEQUENTIAL_BLUE,
          "requests"
        )

    val mlflowCard =
      grid(1) {
        div("card")(
          h(3)("MLflow (Experiment Tracking & Model Registry)") +
            div("rpt-note")(
              "Genuinely Databricks-native — no equivalent measured in the Snowflake report " +
                "(Snowflake has its own separate Snowpark ML model registry, not pulled into that tool)."
            ) +
            stats(3) {
              stat("Experiments (current)")(fmtN(n.totalExperiments)) +
                stat("Runs (this window)")(fmtN(n.totalMlflowRuns)) +
                stat("Finished Runs")(fmtN(n.finishedMlflowRuns))
            }
        )
      }

    val servingCard =
      grid(2) {
        div("card")(
          h(3)("Model Serving") +
            div("rpt-note")("Real-time inference endpoints for custom models, agents, and foundation/external models.") +
            stats(3) {
              stat("Active Endpoints")(fmtN(n.activeServingEndpoints)) +
                stat("Requests (this window)")(fmtN(n.servingRequests)) +
                stat("Tokens (this window)")(fmtN(n.servingTokens))
            }
        ) +
          (if (n.servingEndpointsByType.nonEmpty) card("Endpoints by Type", "chartDbxServingEndpointTypes")()
           else div("card")(h(3)("Endpoints by Type") + div("note")("No active endpoints.")))
      }

    val aiGatewayCard =
      if (n.aiGatewayRequests == 0) ""
      else
        grid(1) {
          div("card")(
            h(3)("AI Gateway") +
              div("rpt-note")(
                "Unified routing/rate-limiting/spend-tracking layer for LLM calls — on this " +
                  "workspace, traffic is dominated by Databricks' own hosted foundation models, " +
                  "not third-party providers."
              ) +
              stats(2) {
                stat("Requests (this window)")(fmtN(n.aiGatewayRequests)) +
                  stat("Tokens (this window)")(fmtN(n.aiGatewayTokens))
              } +
              divId("chartDbxAiGatewayDestinations")()
          )
        }

    val dqMonitoringCard =
      if (n.qualityCheckRuns == 0) ""
      else {
        val healthyPercent = if (n.qualityCheckRuns > 0) n.healthyQualityRuns.toDouble / n.qualityCheckRuns * 100.0 else 0.0
        grid(1) {
          div("card")(
            h(3)("Lakehouse Monitoring (Data Quality)") +
              div("rpt-note")(
                "Automated freshness/completeness checks with downstream-impact analysis — a real " +
                  "Databricks-native capability gap on the Snowflake side, not just unmeasured there."
              ) +
              stats(3) {
                stat("Tables Monitored")(fmtN(n.tablesUnderQualityMonitoring)) +
                  stat("Check Runs (this window)")(fmtN(n.qualityCheckRuns)) +
                  stat("Healthy Rate")(f"$healthyPercent%.1f%%")
              }
          )
        }
      }

    val classificationNote =
      if (n.dataClassificationResults == 0) ""
      else
        div("note")(
          s"Data Classification (automated PII/sensitive-data tagging): ${fmtN(n.dataClassificationResults)} " +
            s"result(s) across ${fmtN(n.tablesClassified)} table(s). Unlike the sections above, this " +
            "ISN'T Databricks-only — Snowflake has its own native classification feature — included " +
            "for completeness; real usage here is minimal."
        )

    val html = collapsibleSection("AI/ML Platform Usage (Databricks-Native)", "dbxNativeBody") {
      div("rpt-note")(
        "Real usage of platform capabilities that either have no Snowflake equivalent measured " +
          "in that report, or (Data Classification) are included for completeness despite " +
          "Snowflake having its own native answer — see the Capability Parity Matrix in the " +
          "comparison report for the fuller picture."
      ) +
        mlflowCard +
        servingCard +
        aiGatewayCard +
        dqMonitoringCard +
        classificationNote
    }

    (html, Seq(servingByTypeJs, aiGatewayByDestJs).filter(_.nonEmpty).mkString("\n"))
  }

  private def buildQueriesUsedSection(lookbackDays: Int): String = {
    val queries: Seq[(String, String)] = Seq(
      "Overview Stats" -> Queries.overviewStats(lookbackDays),
      "Daily Cost" -> Queries.dailyCost(lookbackDays),
      "Cost By SKU" -> Queries.costBySku(lookbackDays),
      "Daily Cost By Product" -> Queries.dailyCostByProduct(lookbackDays),
      "Cost By User" -> Queries.costByUser(lookbackDays),
      "Monthly Cost By SKU" -> Queries.monthlyCostBySku(lookbackDays),
      "Daily Query Volume" -> Queries.dailyQueryVolume(lookbackDays),
      "Daily Query Volume By Type" -> Queries.dailyQueryVolumeByType(lookbackDays),
      "Daily Duration Stats" -> Queries.dailyDurationStats(lookbackDays),
      "Compute Exec Stats" -> Queries.computeExecStats(lookbackDays),
      "Compute Resource Signals" -> Queries.computeResourceSignals(lookbackDays),
      "Top Error Reasons" -> Queries.topErrorReasons(lookbackDays),
      "Daily Active Users" -> Queries.dailyActiveUsers(lookbackDays),
      "Top Actors By Actions" -> Queries.topActorsByActions(lookbackDays),
      "Actions By Service" -> Queries.actionsByService(lookbackDays),
      "Service Account Query Stats" -> Queries.serviceAccountQueryStats(lookbackDays),
      "Service Account Table Access" -> Queries.serviceAccountTableAccess(lookbackDays),
      "Top Read Tables (Table Lineage)" -> Queries.topReadTables(lookbackDays),
      "Top Write Tables (Table Lineage)" -> Queries.topWriteTables(lookbackDays),
      "User Table Reads" -> Queries.userTableReads(lookbackDays),
      "User Table Writes" -> Queries.userTableWrites(lookbackDays),
      "Storage-Bearing Table Names (full inventory)" -> Queries.storageBearingTableNames(),
      "Table Size (per table, all storage-bearing tables)" -> Queries.describeDetail("<catalog>.<schema>.<table>"),
      "Object Inventory (Table Types)" -> Queries.objectInventory(),
      "Catalog/Schema Counts" -> Queries.catalogSchemaCounts(),
      "Job Run Summary" -> Queries.jobRunSummary(lookbackDays),
      "Job Run Totals (unbounded)" -> Queries.jobRunTotals(lookbackDays),
      "Daily Job Runs" -> Queries.dailyJobRuns(lookbackDays),
      "Job Names" -> Queries.jobNames(),
      "Cluster Names" -> Queries.clusterNames(),
      "MLflow Summary" -> Queries.mlflowSummary(lookbackDays),
      "Model Serving Endpoint Types" -> Queries.servingEndpointTypes(),
      "Model Serving Usage Summary" -> Queries.servingUsageSummary(lookbackDays),
      "AI Gateway Usage" -> Queries.aiGatewayUsage(lookbackDays),
      "Data Quality Monitoring Summary" -> Queries.dataQualityMonitoringSummary(lookbackDays),
      "Data Classification Summary" -> Queries.dataClassificationSummary()
    )

    val restCalls: Seq[(String, String)] = Seq(
      "Service Principal Display Names" -> "GET /api/2.0/preview/scim/v2/ServicePrincipals",
      "Warehouse Names" -> "GET /api/2.0/sql/warehouses",
      "Job Task Configuration (per job, top jobs only)" -> "GET /api/2.0/jobs/get?job_id=<id>"
    )
    val restCallsCard = restCalls.map { case (name, endpoint) => div("card")(h(3)(name) + codeBlock(endpoint)) }.mkString("\n")

    collapsibleSection("Queries Used (Reference)", "dbxQueriesUsedBody", startCollapsed = true) {
      div("rpt-note")(
        "The exact SQL run against Databricks for this report, with this run's actual lookback " +
          "window substituted in. \"Table Size\" runs once per real storage-bearing table (a full " +
          "inventory, not a sample), and \"Job Task Configuration\" once per top job — both with " +
          "the real id in place of the placeholder shown here. The 3 calls below are plain REST, " +
          "not SQL — no system table carries any of them."
      ) +
        queries.map { case (name, sql) => div("card")(h(3)(name) + codeBlock(sql)) }.mkString("\n") +
        restCallsCard
    }
  }

  private def fmtN(n: Long): String =
    if (n >= 1000000) f"${n / 1000000.0}%.1fM"
    else if (n >= 1000) f"${n / 1000.0}%.1fk"
    else n.toString

  private def fmtBytes(bytes: Long): String = {
    val gb = bytes / (1024.0 * 1024.0 * 1024.0)
    if (gb >= 1024.0) f"${gb / 1024.0}%,.2f TB"
    else f"$gb%,.2f GB"
  }

  /** Sorts descending and folds everything past `cap` into an "Other" bucket, so a pie/donut
    * never exceeds the categorical palette's validated slot count — same convention as
    * [[guild.snowflakeusage.dashboard.UsageDashboard.capToOther]].
    */
  private def capToOther(items: Seq[(String, Double)], cap: Int = 5): Seq[(String, Double)] = {
    val sorted = items.sortBy(-_._2)
    if (sorted.length <= cap + 1) sorted
    else sorted.take(cap) :+ ("Other" -> sorted.drop(cap).map(_._2).sum)
  }
}
