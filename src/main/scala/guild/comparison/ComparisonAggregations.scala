package guild.comparison

import guild.reportkit.PlatformSummary


case class CapabilityRow(feature: String, snowflake: String, databricks: String, note: String)

/** The migration-load projection: takes Snowflake's *actual* current compute-seconds workload and
  * prices it at Databricks' own *observed* $-per-compute-second rate (`databricksObservedRatePerSecond`
  * = `databricks.computeCostUsd / databricks.totalComputeSeconds`), rather than an assumed or
  * industry-average multiplier. Grounded entirely in both platforms' own real numbers — see the
  * caveats rendered alongside it in `ComparisonDashboard` for what this does NOT account for
  * (query-engine efficiency, concurrency/scaling behavior, storage/egress cost, and the fact that
  * both `computeCostUsd` figures are themselves blended rates across many warehouse
  * sizes/SKUs, not a single like-for-like unit).
  */
case class Projection(
  snowflakeComputeSeconds: Double,
  snowflakeComputeCostUsd: Double,
  databricksObservedRatePerSecond: Double,
  projectedDatabricksCostUsd: Double,
  // `projectedDatabricksCostUsd` scaled by Databricks' own observed real/list discount ratio
  // (`PlatformSummary.realBilledDiscountRatio`), when one is available. This is a real extrapolation,
  // not a second real number — the ratio is observed across the account's WHOLE bill (all SKU
  // types), never confirmed to apply uniformly to compute specifically. `None` when no real total
  // was available to derive a ratio from.
  projectedDatabricksCostUsdRealAdjusted: Option[Double],
  realBilledDiscountRatioApplied: Option[Double]
)

case class ComparisonResult(
  snowflake: PlatformSummary,
  databricks: PlatformSummary,
  lookbackMismatchWarning: Option[String],
  projection: Projection,
  capabilityMatrix: Seq[CapabilityRow]
)

object ComparisonAggregations {

  /** Beyond this many days apart, every volume metric in the comparison is apples-to-oranges —
    * not a hard failure (the two reports are allowed to run on their own schedules), just a loud,
    * visible caveat on the rendered report rather than a silently-misleading side-by-side.
    */
  private val LOOKBACK_MISMATCH_THRESHOLD_DAYS = 2

  def compute(sf: PlatformSummary, dbx: PlatformSummary): ComparisonResult = {
    val lookbackMismatchWarning =
      if (math.abs(sf.lookbackDays - dbx.lookbackDays) > LOOKBACK_MISMATCH_THRESHOLD_DAYS)
        Some(
          s"Snowflake's summary covers ${sf.lookbackDays} days; Databricks' covers ${dbx.lookbackDays} days. " +
            "Every volume metric below (queries, cost, active users, ...) is comparing different-length " +
            "windows until both reports are regenerated with the same --lookback-days."
        )
      else None

    val databricksRatePerSecond = if (dbx.totalComputeSeconds > 0) dbx.computeCostUsd / dbx.totalComputeSeconds else 0.0
    val projectedDatabricksCostUsd = sf.totalComputeSeconds * databricksRatePerSecond
    val projection = Projection(
      snowflakeComputeSeconds = sf.totalComputeSeconds,
      snowflakeComputeCostUsd = sf.computeCostUsd,
      databricksObservedRatePerSecond = databricksRatePerSecond,
      projectedDatabricksCostUsd = projectedDatabricksCostUsd,
      projectedDatabricksCostUsdRealAdjusted = dbx.realBilledDiscountRatio.map(_ * projectedDatabricksCostUsd),
      realBilledDiscountRatioApplied = dbx.realBilledDiscountRatio
    )

    ComparisonResult(sf, dbx, lookbackMismatchWarning, projection, CAPABILITY_MATRIX)
  }

  /** Hand-authored, not pulled from either report — this is architectural/product knowledge, not
    * a metric either system tracks. Same "hand-maintained reference data alongside pulled data"
    * precedent as `guild.snowflakeusage.aggregate.Aggregations.KNOWN_NETWORK_POLICIES`.
    */
  val CAPABILITY_MATRIX: Seq[CapabilityRow] = Seq(
    CapabilityRow(
      "Scheduled orchestration",
      "Tasks — runs SQL or a single stored-procedure call",
      "Jobs (Lakeflow) — notebook, JAR, Python wheel, SQL, dbt, DLT pipeline, and more per task",
      "Databricks Jobs support a materially wider range of task types than Snowflake Tasks."
    ),
    CapabilityRow(
      "Continuous/incremental ingestion",
      "Snowpipe",
      "Auto Loader / Lakeflow Connect",
      "Both are the native \"land files as they arrive\" mechanism for their platform."
    ),
    CapabilityRow(
      "Declarative materialized transforms",
      "Materialized Views",
      "Delta Live Tables (DLT) pipelines",
      "DLT is a broader declarative-pipeline framework, not a narrow materialized-view feature — the overlap is partial, not 1:1."
    ),
    CapabilityRow(
      "Change-data-capture streams",
      "Streams",
      "Delta Change Data Feed (CDF)",
      "Not pulled into either report today — listed for completeness, not measured here."
    ),
    CapabilityRow(
      "Cross-account data sharing",
      "Secure Data Sharing",
      "Delta Sharing",
      "Both let another account query your data without copying it."
    ),
    CapabilityRow(
      "Cold/archive storage tiering",
      "Storage Lifecycle Policies (COOL/COLD tiers)",
      "No direct equivalent surfaced",
      "This account has 0 lifecycle policies configured today either way — see the Snowflake report's Storage section."
    ),
    CapabilityRow(
      "Object/lineage inventory",
      "ACCOUNT_USAGE object + ACCESS_HISTORY views",
      "Unity Catalog information_schema + system.access.table_lineage",
      "Unity Catalog's lineage is column/table-level and can span workspaces; ACCESS_HISTORY is scoped to one Snowflake account."
    ),
    CapabilityRow(
      "Service-identity naming",
      "Plain usernames, already human-readable",
      "UUID service-principal application ids, resolved via a separate SCIM lookup where a match exists",
      "Databricks service identities need an extra resolution step (built into this tool's report) that Snowflake's usernames never require."
    ),
    // The rows below are genuinely Databricks-native — not "Snowflake has a worse version," but
    // capabilities with no equivalent measured in the Snowflake report at all (Snowflake's own
    // ML/AI offerings — Snowpark ML, Cortex — exist but aren't pulled into that tool, so "not
    // measured" is the honest framing, not "doesn't exist"). Real usage numbers for the
    // Databricks side of each of these live in that report's own "AI/ML Platform Usage" section.
    CapabilityRow(
      "ML experiment tracking & model registry",
      "Snowpark ML Model Registry (separate offering; not measured in this report)",
      "MLflow — natively integrated into every workspace",
      "Real, heavy usage on this workspace (see the Databricks report) — not just a capability that exists on paper."
    ),
    CapabilityRow(
      "Real-time ML/AI model serving",
      "Cortex-hosted / Snowpark Container Services (different architecture; not measured)",
      "Model Serving — managed endpoints for custom models, agents, foundation models, and external models",
      "This workspace's Model Serving usage is dominated by custom AI agent endpoints, not generic LLM chat serving."
    ),
    CapabilityRow(
      "Governed multi-destination LLM gateway",
      "Cortex AI functions (built-in SQL functions calling Snowflake-hosted models; no separate external-provider routing layer)",
      "AI Gateway — unified routing, rate limiting, and spend tracking",
      "Different architecture, not just a maturity gap — Cortex calls Snowflake's own hosted models; AI Gateway fronts (on this workspace, mostly Databricks-hosted) foundation models with centralized governance."
    ),
    CapabilityRow(
      "Automated table-level data quality monitoring",
      "No native product equivalent (typically dbt tests / custom SQL checks)",
      "Lakehouse Monitoring — automated freshness/completeness checks with downstream-impact analysis",
      "A real Databricks-native capability gap on the Snowflake side, not just an unmeasured one — real monitors are actively running on this workspace."
    ),
    CapabilityRow(
      "Automated sensitive-data classification",
      "Native classification feature (auto-tags PII-like columns) — not measured in this report",
      "Unity Catalog automated data classification",
      "Rough parity, not a Databricks-only capability — both platforms have a native answer here, unlike the rows above."
    )
  )
}
