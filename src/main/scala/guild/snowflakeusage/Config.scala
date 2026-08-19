package guild.snowflakeusage

import org.rogach.scallop._


/** CLI + environment-variable configuration for the usage report.
  *
  * Every setting can come from a flag or its matching env var; flags win when both are set.
  * SSO users only need `--account`/`SNOWFLAKE_ACCOUNT`, `--user`/`SNOWFLAKE_USER`, and
  * `--warehouse`/`SNOWFLAKE_WAREHOUSE` — `--authenticator` defaults to `externalbrowser`.
  */
class Config(arguments: Seq[String]) extends ScallopConf(arguments) {

  val account: ScallopOption[String] = opt[String](
    default = sys.env.get("SNOWFLAKE_ACCOUNT"),
    descr = "Snowflake account identifier, e.g. xy12345.us-east-1"
  )

  val user: ScallopOption[String] = opt[String](
    default = sys.env.get("SNOWFLAKE_USER"),
    descr = "Snowflake username"
  )

  val role: ScallopOption[String] = opt[String](
    default = Some(sys.env.getOrElse("SNOWFLAKE_ROLE", "ACCOUNTADMIN")),
    descr = "Snowflake role to run queries as"
  )

  val warehouse: ScallopOption[String] = opt[String](
    default = sys.env.get("SNOWFLAKE_WAREHOUSE"),
    descr = "Snowflake warehouse to run queries with"
  )

  val authenticator: ScallopOption[String] = opt[String](
    default = Some(sys.env.getOrElse("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")),
    descr = "JDBC authenticator: externalbrowser (SSO, default), or snowflake (username/password)"
  )

  val password: ScallopOption[String] = opt[String](
    default = sys.env.get("SNOWFLAKE_PASSWORD"),
    descr = "Password; only needed when --authenticator=snowflake"
  )

  val lookbackDays: ScallopOption[Int] = opt[Int](
    default = Some(sys.env.get("LOOKBACK_DAYS").map(_.toInt).getOrElse(90)),
    descr = "Days of ACCOUNT_USAGE history to pull"
  )

  /** Decoupled from `--lookback-days`: billing data (`ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY`)
    * is one row per (day, usage type) — cheap to pull a full year of. `query_history` and the
    * other activity pulls are one row per query — a year of those would be hundreds of millions
    * of rows, well past what this row-pull architecture (see [[guild.snowflakeusage.SnowflakeJdbc]])
    * can handle. Defaults to `--lookback-days` so existing behavior doesn't change unless asked.
    */
  val costLookbackDays: ScallopOption[Int] = opt[Int](
    default = Some(sys.env.get("COST_LOOKBACK_DAYS").map(_.toInt).getOrElse(lookbackDays())),
    descr = "Days of ORGANIZATION_USAGE billing history to pull — independent of --lookback-days, so cost/bill data can span a full year cheaply"
  )

  val cacheDir: ScallopOption[String] = opt[String](
    default = Some(sys.env.getOrElse("CACHE_DIR", "data/raw")),
    descr = "Directory to cache pulled data as Parquet, so the report can be rebuilt without re-querying"
  )

  val skipFetch: ScallopOption[Boolean] = opt[Boolean](
    default = Some(false),
    descr = "Skip Snowflake entirely and rebuild the report from --cache-dir"
  )

  val outputPath: ScallopOption[String] = opt[String](
    default = Some(sys.env.getOrElse("OUTPUT_PATH", "output/snowflake-usage-report.html")),
    descr = "Path to write the generated HTML report"
  )

  /** `ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY` spans every account in the org, keyed by
    * ACCOUNT_LOCATOR (not the full `--account` identifier) — defaults to the part of `--account`
    * after the last hyphen (e.g. `guild-aa40032` -> `AA40032`), which holds for
    * org-account-hyphenated identifiers but isn't guaranteed for every Snowflake org, so it's
    * overridable.
    */
  val accountLocator: ScallopOption[String] = opt[String](
    default = sys.env
      .get("SNOWFLAKE_ACCOUNT_LOCATOR")
      .orElse(account.toOption.map(a => a.split("-").last.toUpperCase)),
    descr = "Account locator for ORGANIZATION_USAGE cost queries (default: derived from --account)"
  )

  val creditPriceUsd: ScallopOption[Double] = opt[Double](
    default = sys.env.get("CREDIT_PRICE_USD").map(_.toDouble),
    descr = "Fallback: USD per credit, used to estimate cost when ORGANIZATION_USAGE isn't accessible"
  )

  val storagePricePerTbUsd: ScallopOption[Double] = opt[Double](
    default = sys.env.get("STORAGE_PRICE_PER_TB_USD").map(_.toDouble),
    descr = "Fallback: USD per TB/month storage, used alongside --credit-price-usd for the cost estimate"
  )

  verify()

  if (!skipFetch()) {
    require(
      account.isDefined,
      "Missing Snowflake account: pass --account or set SNOWFLAKE_ACCOUNT"
    )
    require(user.isDefined, "Missing Snowflake user: pass --user or set SNOWFLAKE_USER")
    require(
      warehouse.isDefined,
      "Missing Snowflake warehouse: pass --warehouse or set SNOWFLAKE_WAREHOUSE"
    )
    require(
      authenticator() != "snowflake" || password.isDefined,
      "Missing password: pass --password or set SNOWFLAKE_PASSWORD when --authenticator=snowflake"
    )
  }
}
