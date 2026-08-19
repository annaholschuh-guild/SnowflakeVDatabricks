package guild.reportkit.dashboard

import guild.reportkit.dashboard.JSUtil._


/** Plotly chart-generation utilities for the usage report.
  *
  * Deliberately no dual-axis charts (two y-scales on one plot) — per data-viz guidance that's
  * the single most common chart mistake. Where two differently-scaled measures need to be
  * shown together (e.g. query volume and average duration), render them as separate
  * single-axis small multiples instead.
  */
object PlotlyUtil {

  val PLOTLY_VERSION = "2.27.0"

  /** Fixed-order categorical palette (validated for CVD-safe adjacent pairs up to 6 series —
    * see dataviz palette validation). Never cycle past 6 series; fold extras into "Other".
    */
  val CATEGORICAL: Seq[String] =
    Seq("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300")

  val SEQUENTIAL_BLUE = "#256abf"
  val ACCENT_ORANGE = "#eb6834"


  def getScriptTagHTML(version: String = PLOTLY_VERSION, useLocal: Boolean = true): String = {
    val cdnTag = s"""<script src="https://cdn.plot.ly/plotly-$version.min.js"></script>"""
    if (!useLocal) return cdnTag
    val stream = getClass.getClassLoader.getResourceAsStream("plotly.min.js")
    if (stream == null) return cdnTag
    try {
      val js = scala.io.Source.fromInputStream(stream).mkString
      s"<script>$js</script>"
    } finally stream.close()
  }


  /** A scatter plot for showing where entities sit along two continuous measures at once (a bar
    * chart can only show one). `directLabels` should be on for small point counts (e.g. one
    * point per warehouse) and off once it would get cluttered (e.g. one point per table) — hover
    * still carries the label either way via `text`.
    */
  def plotlyScatter(
    elementId: String,
    labels: Seq[String],
    x: Seq[Double],
    y: Seq[Double],
    xTitle: String,
    yTitle: String,
    color: String = SEQUENTIAL_BLUE,
    directLabels: Boolean = false,
    height: Int = 380
  ): String = {
    val mode = if (directLabels) "markers+text" else "markers"
    val textPosition = if (directLabels) "textposition: 'top center', textfont: { size: 10 }," else ""
    s"""Plotly.newPlot('$elementId', [{
       |  x: ${jsDoubles(x)},
       |  y: ${jsDoubles(y)},
       |  text: ${jsStrings(labels)},
       |  mode: '$mode', type: 'scatter',
       |  $textPosition
       |  marker: { color: '$color', size: 9, opacity: 0.75 },
       |  hovertemplate: '%{text}<br>${escapeJs(xTitle)}: %{x:,.2f}<br>${escapeJs(yTitle)}: %{y:,.2f}<extra></extra>'
       |}], { height: $height, margin: {l:65,r:20,t:10,b:50},
       |      xaxis: { title: '${escapeJs(xTitle)}' }, yaxis: { title: '${escapeJs(yTitle)}' },
       |      showlegend: false });""".stripMargin
  }


  /** A single-series line chart (e.g. daily query volume, daily active users). */
  def plotlyLine(
    elementId: String,
    x: Seq[String],
    y: Seq[Double],
    color: String,
    yTitle: String,
    height: Int = 260
  ): String =
    s"""Plotly.newPlot('$elementId', [{
       |  x: ${jsStrings(x)},
       |  y: ${jsDoubles(y)},
       |  type: 'scatter', mode: 'lines', line: { color: '$color', width: 2 },
       |  hovertemplate: '%{x}<br>$yTitle: %{y:.2f}<extra></extra>'
       |}], { height: $height, margin: {l:55,r:20,t:10,b:40},
       |      xaxis: { title: 'date' }, yaxis: { title: '${escapeJs(yTitle)}', rangemode: 'tozero' },
       |      showlegend: false });""".stripMargin


  /** A bar + line combo on two y-axes: daily cost as bars (left axis), cumulative running total
    * as a line (right axis, `y2`). Both are USD, but the running total is inherently many times
    * larger than any single day's cost, so sharing one axis squashes the bars flat — this is a
    * deliberate exception to the "no dual-axis" default: a period-value-vs-its-own-cumulative-
    * total pairing (not two unrelated metrics scaled to manufacture a false correlation, which is
    * what that rule actually guards against). Right-axis gridlines are hidden so only one
    * gridline set shows, avoiding the usual dual-axis confusion about which grid belongs to which line.
    */
  def plotlyBarLineCombo(
    elementId: String,
    barX: Seq[String],
    barY: Seq[Double],
    barName: String,
    barColor: String,
    lineX: Seq[String],
    lineY: Seq[Double],
    lineName: String,
    lineColor: String,
    yTitle: String,
    height: Int = 320
  ): String =
    s"""Plotly.newPlot('$elementId', [
       |  { name: '${escapeJs(barName)}', x: ${jsStrings(barX)}, y: ${jsDoubles(barY)}, type: 'bar',
       |    marker: { color: '$barColor' },
       |    hovertemplate: '%{x}<br>' + '${escapeJs(barName)}' + ': %{y:$$,.2f}<extra></extra>' },
       |  { name: '${escapeJs(lineName)}', x: ${jsStrings(lineX)}, y: ${jsDoubles(lineY)}, yaxis: 'y2',
       |    type: 'scatter', mode: 'lines+markers', line: { color: '$lineColor', width: 4 },
       |    marker: { color: '$lineColor', size: 7 },
       |    hovertemplate: '%{x}<br>' + '${escapeJs(lineName)}' + ': %{y:$$,.2f}<extra></extra>' }
       |], { height: $height, margin: {l:65,r:65,t:10,b:40},
       |     xaxis: { title: 'date' },
       |     yaxis: { title: '${escapeJs(barName)} (${escapeJs(yTitle)})', rangemode: 'tozero' },
       |     yaxis2: { title: '${escapeJs(lineName)} (${escapeJs(yTitle)})', overlaying: 'y', side: 'right',
       |               rangemode: 'tozero', showgrid: false },
       |     legend: { orientation: 'h', y: -0.2 } });""".stripMargin


  /** Neutral gray for an "Other" rollup series — never a 7th categorical hue, so it reads as
    * "everything else" rather than an equally-weighted distinct entity.
    */
  private val OTHER_COLOR = "#a8a190"

  /** A stacked bar chart with a fixed-order categorical palette and a legend — the stack height
    * also reads as the daily total at a glance. A series literally named "Other" always renders
    * in neutral gray rather than cycling to a 7th categorical color. A 1px white border between
    * segments keeps adjacent same-day bars visually separated rather than blending into one block.
    */
  def plotlyStackedBar(
    elementId: String,
    series: Seq[(String, Seq[(String, Double)])],
    yTitle: String,
    height: Int = 320
  ): String = {
    val colorByName: Map[String, String] = series
      .map(_._1)
      .filterNot(_ == "Other")
      .zipWithIndex
      .map { case (name, i) => name -> CATEGORICAL(i % CATEGORICAL.length) }
      .toMap

    val traces = series
      .map { case (name, points) =>
        val color = if (name == "Other") OTHER_COLOR else colorByName(name)
        s"""{
           |  name: '${escapeJs(name)}',
           |  x: ${jsStrings(points.map(_._1))},
           |  y: ${jsDoubles(points.map(_._2))},
           |  type: 'bar',
           |  marker: { color: '$color', line: { color: '#ffffff', width: 1 } },
           |  hovertemplate: '%{x}<br>' + '${escapeJs(name)}' + ': %{y:.2f}<extra></extra>'
           |}""".stripMargin
      }
      .mkString(",\n")

    s"""Plotly.newPlot('$elementId', [
       |$traces
       |], { height: $height, margin: {l:60,r:20,t:10,b:40}, barmode: 'stack',
       |     xaxis: { title: 'date' }, yaxis: { title: '${escapeJs(yTitle)}', rangemode: 'tozero' },
       |     legend: { orientation: 'h', y: -0.2 } });""".stripMargin
  }


  /** A ranked horizontal bar chart (top-N by some measure), single uniform color.
    *
    * `labels` go on the y-axis as-is — callers truncate long names (qualified table names,
    * etc.) before passing them in. `fullLabels`, when given, supplies the untruncated names for
    * the hover tooltip via `customdata` so nothing is lost, just visually deferred.
    *
    * `height` auto-scales with item count by default (enough vertical room per bar to stay
    * legible) — override only when a fixed height is actually wanted.
    */
  def plotlyHBar(
    elementId: String,
    labels: Seq[String],
    values: Seq[Double],
    color: String,
    xTitle: String,
    valueFormat: String = "%{x:.2f}",
    fullLabels: Option[Seq[String]] = None,
    height: Option[Int] = None,
    marginL: Int = 220
  ): String = {
    val chartHeight = height.getOrElse(math.max(260, labels.length * 26 + 80))
    val customdataJs = fullLabels.map(fl => s"customdata: ${jsStrings(fl)},").getOrElse("")
    val hoverLabelToken = if (fullLabels.isDefined) "%{customdata}" else "%{y}"

    s"""Plotly.newPlot('$elementId', [{
       |  x: ${jsDoubles(values)},
       |  y: ${jsStrings(labels)},
       |  $customdataJs
       |  type: 'bar', orientation: 'h',
       |  marker: { color: '$color' },
       |  hovertemplate: '$hoverLabelToken<br>$valueFormat<extra></extra>'
       |}], { height: $chartHeight, margin: {l:$marginL,r:20,t:10,b:40},
       |      xaxis: { title: '${escapeJs(xTitle)}' },
       |      yaxis: { automargin: true, categoryorder: 'total ascending' },
       |      showlegend: false });""".stripMargin
  }


  /** A box-and-whisker plot with one box per day. `seriesByDay` is (day -> sampled values);
    * x is the day repeated once per sample so Plotly buckets points sharing an x into one box.
    *
    * Log-scale y-axis and no overlaid raw points: query/task duration is heavily right-skewed
    * (most queries near-instant, a long tail of slow ones) — on a linear axis the box collapses
    * to an invisible sliver near zero and only the outlier points are visible. `boxpoints: false`
    * hides those raw points so the box/whiskers themselves are what's actually visible.
    */
  /** A box plot from precomputed per-day quartiles (server-side `APPROX_PERCENTILE`), not raw
    * per-point samples — Plotly's `box` trace natively supports this via `q1`/`median`/`q3`/
    * `lowerfence`/`upperfence` arrays instead of a raw sample array. Two real differences from a
    * raw-sample box plot: whiskers are the true min/max supplied here (not the classical 1.5xIQR
    * convention — there are no raw samples to compute that from), and individual outlier points
    * can't be rendered (Plotly's outlier-point logic only works from a raw sample array) —
    * caption both explicitly; a separate top-N slow-queries table covers spot-checking specific
    * queries instead.
    */
  def plotlyBoxPerDayPrecomputed(
    elementId: String,
    dailyStats: Seq[(String, Double, Double, Double, Double, Double)], // (day, min, q1, median, q3, max)
    color: String,
    yTitle: String,
    height: Int = 320
  ): String = {
    val days = dailyStats.map(_._1)
    s"""Plotly.newPlot('$elementId', [{
       |  x: ${jsStrings(days)},
       |  lowerfence: ${jsDoubles(dailyStats.map(_._2))},
       |  q1: ${jsDoubles(dailyStats.map(_._3))},
       |  median: ${jsDoubles(dailyStats.map(_._4))},
       |  q3: ${jsDoubles(dailyStats.map(_._5))},
       |  upperfence: ${jsDoubles(dailyStats.map(_._6))},
       |  type: 'box', marker: { color: '$color' },
       |  hovertemplate: '%{x}<br>${escapeJs(yTitle)} median: %{median:.3f}<extra></extra>'
       |}], { height: $height, margin: {l:65,r:20,t:10,b:60},
       |      xaxis: { title: 'date' },
       |      yaxis: { title: '${escapeJs(yTitle)} (log scale)', type: 'log' },
       |      showlegend: false });""".stripMargin
  }


  /** A donut chart. `labels`/`values` should already be capped to [[CATEGORICAL]]'s 6 colors
    * (fold smaller slices into an "Other" bucket before calling) — never cycles past 6.
    */
  def plotlyDonut(elementId: String, labels: Seq[String], values: Seq[Double], height: Int = 320): String = {
    val colors = labels.indices.map(i => CATEGORICAL(i % CATEGORICAL.length))
    s"""Plotly.newPlot('$elementId', [{
       |  labels: ${jsStrings(labels)},
       |  values: ${jsDoubles(values)},
       |  type: 'pie', hole: 0.4,
       |  marker: { colors: ${jsStrings(colors)} },
       |  hovertemplate: '%{label}<br>$$%{value:,.2f} (%{percent})<extra></extra>'
       |}], { height: $height, margin: {l:20,r:20,t:10,b:10} });""".stripMargin
  }
}
