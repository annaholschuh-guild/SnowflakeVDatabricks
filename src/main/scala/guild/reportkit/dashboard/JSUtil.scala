package guild.reportkit.dashboard


/** Generic JavaScript and HTML string utilities for hand-rolled dashboard generation. */
object JSUtil {

  def jsDoubles(values: Seq[Double]): String =
    "[" + values.map(v => f"$v%.6f").mkString(",") + "]"

  def jsStrings(values: Seq[String]): String =
    "[" + values.map(s => "\"" + escapeJs(s) + "\"").mkString(",") + "]"

  def jsInts(values: Seq[Long]): String =
    "[" + values.mkString(",") + "]"

  def escapeJs(s: String): String =
    Option(s).getOrElse("").replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ").replace("\r", "")

  /** Shortens long identifiers (qualified table names, etc.) for chart axis labels, keeping the
    * tail — the most specific/distinguishing part (e.g. the table name itself in
    * `DB.SCHEMA.TABLE_NAME`) — rather than the head. Pair with the untruncated string as a
    * chart's hover label so nothing is actually lost, just visually deferred.
    */
  def truncateLabel(s: String, maxLen: Int = 40): String =
    if (s.length <= maxLen) s else "…" + s.takeRight(maxLen - 1)

  def escapeHtml(s: String): String =
    Option(s)
      .getOrElse("")
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
}
