package guild.reportkit.dashboard


/** Generic HTML chart-generation utilities for hand-rolled dashboard generation. */
object HTMLUtil {

  object COLORS {
    val GUILD_ORANGE = "#E7651C"
    val GUILD_IVORY = "#F8F2E9"
    val GUILD_SAND = "#D7D1BD"
    val GUILD_CHARCOAL = "#170B01"
  }

  def div(cls: String)(content: => String): String =
    if (cls.isEmpty) s"<div>$content</div>" else s"""<div class="$cls">$content</div>"""

  def divId(id: String)(content: => String = ""): String = s"""<div id="$id">$content</div>"""

  def h(n: Int)(content: String): String = s"<h$n>$content</h$n>"

  def header(content: => String): String = s"<header>$content</header>"

  def footer(content: => String): String = s"<footer>$content</footer>"

  def script(js: String): String = s"<script>\n$js\n</script>"

  def wrap(content: => String): String = div("wrap")(content)


  def htmlPage(title: String, hasPlotly: Boolean = true, style: String = REPORT_STYLE)(body: => String): String =
    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |<meta charset="UTF-8">
       |<title>$title</title>
       |${if (hasPlotly) PlotlyUtil.getScriptTagHTML() else ""}
       |$style
       |</head>
       |<body>
       |<div id="sidebar">
       |  <nav id="sidebar-toc"><div class="toc-title">Contents</div></nav>
       |</div>
       |$body
       |$sidebarJS
       |$collapsibleSectionJS
       |$sortableTableJS
       |</body>
       |</html>
    """.stripMargin


  def collapsibleSection(title: String, divId: String, startCollapsed: Boolean = false)(contents: => String): String = {
    val headerCls = if (startCollapsed) "collapsible is-collapsed" else "collapsible"
    val bodyStyle = if (startCollapsed) """ style="display:none"""" else ""
    s"""<section>
       |  <h2 class="$headerCls" onclick="toggleSection('$divId',this)">$title</h2>
       |  <div id="$divId" class="section-body"$bodyStyle>
       |  $contents
       |  </div>
       |</section>""".stripMargin
  }


  /** A monospace, pre-formatted block for showing raw text verbatim (e.g. the SQL a query
    * actually ran) — wraps rather than horizontally scrolling so it survives print/PDF export.
    */
  def codeBlock(code: String): String =
    s"""<pre class="rpt-code"><code>${JSUtil.escapeHtml(code.trim)}</code></pre>"""


  def grid(size: Int)(contents: => String): String =
    s"""<div class="grid g$size">$contents</div>"""


  def card(title: String, divId: String, headingSize: Int = 3)(contents: => String = ""): String =
    s"""<div class="card"><h$headingSize>$title</h$headingSize><div id="$divId">$contents</div></div>"""


  /** `extraClass`, when non-empty, is appended to the row's class (e.g. `"stats-square"` to
    * make that specific row's tiles square) without affecting any other `stats()` row on the
    * page — `.stat` sizing is otherwise shared by every stat tile.
    */
  def stats(columns: Int = 4, extraClass: String = "")(contents: => String): String = {
    val cls = if (extraClass.nonEmpty) s"stats $extraClass" else "stats"
    s"""<div class="$cls" style="grid-template-columns:repeat($columns,1fr)">$contents</div>"""
  }


  /** `tooltip`, when non-empty, becomes a native `title` attribute (hover-to-read explanation
    * — e.g. what "Fail-safe" or "Retained for Clone" actually means) and adds a dotted
    * underline affordance so it's discoverable without hunting for it.
    *
    * `sub`, when non-empty, is a small tagline under the label (e.g. the actual calendar date
    * range under a "Day Lookback Window" tile's count).
    */
  def stat(title: String, tooltip: String = "", sub: String = "")(contents: => String): String = {
    val titleAttr = if (tooltip.nonEmpty) s""" title="${JSUtil.escapeHtml(tooltip)}"""" else ""
    val labelCls = if (tooltip.nonEmpty) "l l-hint" else "l"
    val subHtml = if (sub.nonEmpty) s"""<div class="sub-tag">${JSUtil.escapeHtml(sub)}</div>""" else ""
    s"""<div class="stat"$titleAttr><div class="v">$contents</div><div class="$labelCls">$title</div>$subHtml</div>"""
  }


  /** A plain data table for row-level detail that doesn't reduce cleanly to a two-column chart
    * (e.g. slow queries, dormant users, dead-weight tables).
    */
  def table(headers: Seq[String], rows: Seq[Seq[String]]): String = {
    val head = headers.map(h => s"<th>${JSUtil.escapeHtml(h)}</th>").mkString
    val body = rows
      .map(row => "<tr>" + row.map(c => s"<td>${JSUtil.escapeHtml(c)}</td>").mkString + "</tr>")
      .mkString("\n")
    s"""<table class="rpt-table"><thead><tr>$head</tr></thead><tbody>$body</tbody></table>"""
  }


  /** A client-side sortable + filterable table — click a header to sort (numeric columns sort
    * on `data-v`, the raw value, not the formatted display text like "1.2 TB"), type in the
    * filter box to show only matching rows. No backend involved (this is a static report) — the
    * shared JS lives once in [[sortableTableJS]], reused by every table built this way.
    *
    * `rows` is (display, raw-sort-value) per cell — raw is the plain number as a string for
    * numeric columns (so e.g. TB-scaled display text still sorts correctly), or same as display
    * for text columns.
    */
  def sortableFilterableTable(
    tableId: String,
    headers: Seq[String],
    numericCols: Set[Int],
    rows: Seq[Seq[(String, String)]]
  ): String = {
    val head = headers.zipWithIndex
      .map { case (hdr, i) =>
        val kind = if (numericCols.contains(i)) "number" else "text"
        s"""<th onclick="sortRptTable('$tableId',$i,'$kind')">${JSUtil.escapeHtml(hdr)}</th>"""
      }
      .mkString
    val body = rows
      .map(row =>
        "<tr>" + row
          .map { case (display, raw) => s"""<td data-v="${JSUtil.escapeHtml(raw)}">${JSUtil.escapeHtml(display)}</td>""" }
          .mkString + "</tr>"
      )
      .mkString("\n")
    s"""<div class="rpt-filter-bar"><input type="text" placeholder="Filter…" oninput="filterRptTable('$tableId',this.value)"></div>
       |<div class="rpt-scroll-table">
       |<table id="$tableId" class="rpt-table rpt-sortable"><thead><tr>$head</tr></thead><tbody>$body</tbody></table>
       |</div>""".stripMargin
  }


  val REPORT_STYLE =
    s"""
       |<style>
       |:root{--guild-charcoal:${COLORS.GUILD_CHARCOAL};--guild-orange:${COLORS.GUILD_ORANGE};--guild-ivory:${COLORS.GUILD_IVORY};--guild-sand:${COLORS.GUILD_SAND};--sidebar-w:190px}
       |*{box-sizing:border-box;margin:0;padding:0}
       |body{font-family:"Helvetica Neue",Arial,sans-serif;background:var(--guild-ivory);color:var(--guild-charcoal);margin-left:var(--sidebar-w)}
       |h1,h2,h3{font-family:Georgia,serif}
       |a{color:var(--guild-orange)}
       |header{background:var(--guild-charcoal);color:var(--guild-ivory);padding:20px 32px;display:flex;justify-content:space-between;align-items:flex-end}
       |header h1{font-size:1.65rem;font-weight:600}
       |header .sub{font-size:.95rem;opacity:.65;margin-top:3px}
       |header .meta{font-size:.85rem;opacity:.65;text-align:right}
       |.wrap{padding:26px 34px}
       |.stats{display:grid;gap:16px;margin-bottom:26px}
       |.stat{background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(23,11,1,.1);padding:18px;text-align:center}
       |.stat .v{font-size:2.1rem;font-weight:700;color:var(--guild-charcoal)}
       |.stat .l{font-size:.82rem;color:#8a8370;margin-top:4px;text-transform:uppercase;letter-spacing:.04em}
       |.stat .l-hint{cursor:help;border-bottom:1px dotted #8a8370;display:inline-block}
       |.stat .sub-tag{font-size:.72rem;color:#a8a190;margin-top:6px}
       |.stats-square .stat{aspect-ratio:1;display:flex;flex-direction:column;align-items:center;justify-content:center}
       |@media(max-width:900px){.stats-square{grid-template-columns:repeat(2,1fr)!important}}
       |section{margin-bottom:26px}
       |section h2{font-size:.95rem;font-weight:700;color:var(--guild-charcoal);text-transform:uppercase;letter-spacing:.06em;margin-bottom:14px;padding-bottom:7px;border-bottom:2px solid var(--guild-orange)}
       |.grid{display:grid;gap:16px;margin-bottom:16px}
       |.g1{grid-template-columns:1fr}
       |.g2{grid-template-columns:1fr 1fr}
       |.card{background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(23,11,1,.1);padding:16px;min-width:0}
       |.card h3{font-size:.88rem;font-weight:600;color:#8a8370;margin-bottom:10px;text-transform:uppercase;letter-spacing:.04em}
       |.note{color:#8a8370;padding:24px;text-align:center;font-size:.92rem}
       |footer{text-align:center;padding:20px;color:#8a8370;font-size:.85rem}
       |.collapsible{cursor:pointer;user-select:none;display:flex;align-items:center}
       |section h2.collapsible::before{content:'▾';font-size:1rem;line-height:1;transition:transform .2s;display:inline-block;margin-right:8px;flex-shrink:0}
       |section h2.collapsible.is-collapsed::before{transform:rotate(-90deg)}
       |.section-body{overflow:hidden}
       |#sidebar{position:fixed;left:0;top:0;height:100vh;width:var(--sidebar-w);background:var(--guild-charcoal);color:var(--guild-ivory);display:flex;flex-direction:column;z-index:100;box-shadow:2px 0 6px rgba(23,11,1,.18)}
       |#sidebar-toc{flex:1;overflow-y:auto;padding:14px 0 20px}
       |.toc-title{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.12em;color:rgba(248,242,233,.35);padding:0 14px 10px}
       |.toc-link{display:block;padding:6px 14px 6px 12px;font-size:.88rem;color:rgba(248,242,233,.6);text-decoration:none;border-left:2px solid transparent;line-height:1.35;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
       |.toc-link:hover{color:var(--guild-ivory);background:rgba(255,255,255,.07)}
       |.toc-link.toc-active{color:var(--guild-orange);border-left-color:var(--guild-orange)}
       |@media(max-width:900px){.g2{grid-template-columns:1fr}}
       |.rpt-table{width:100%;border-collapse:collapse;font-size:.85rem}
       |.rpt-table th{text-align:left;font-size:.72rem;text-transform:uppercase;letter-spacing:.05em;color:#8a8370;padding:6px 10px;border-bottom:2px solid var(--guild-sand)}
       |.rpt-table td{padding:6px 10px;border-bottom:1px solid rgba(215,209,189,.5);font-variant-numeric:tabular-nums}
       |.rpt-table tr:hover td{background:rgba(215,209,189,.25)}
       |.rpt-note{font-size:.85rem;color:#8a8370;margin:8px 0 0}
       |.rpt-code{background:var(--guild-charcoal);color:var(--guild-ivory);padding:14px 16px;border-radius:6px;font-size:.8rem;line-height:1.5;overflow-x:auto;font-family:"SF Mono",Monaco,Consolas,monospace;white-space:pre}
       |.rpt-code + h3{margin-top:16px}
       |.rpt-filter-bar{margin-bottom:8px}
       |.rpt-filter-bar input{width:100%;max-width:320px;padding:6px 10px;border:1px solid var(--guild-sand);border-radius:6px;font-size:.85rem;font-family:inherit}
       |.rpt-sortable th{cursor:pointer;user-select:none}
       |.rpt-sortable th.sorted-asc::after{content:" ▲"}
       |.rpt-sortable th.sorted-desc::after{content:" ▼"}
       |.rpt-scroll-table{max-height:480px;overflow-y:auto;border:1px solid var(--guild-sand);border-radius:6px}
       |.rpt-scroll-table table{margin:0;border-collapse:separate;border-spacing:0}
       |.rpt-scroll-table thead th{position:sticky;top:0;background:var(--guild-ivory);z-index:1}
       |.rpt-bill-scroll{max-height:480px;overflow:auto;border:1px solid var(--guild-sand);border-radius:6px}
       |.rpt-bill-scroll table{margin:0;border-collapse:separate;border-spacing:0;width:auto}
       |.rpt-bill-scroll th,.rpt-bill-scroll td{white-space:nowrap}
       |.rpt-bill-scroll th:not(:nth-child(1)):not(:nth-child(2)),.rpt-bill-scroll td:not(:nth-child(1)):not(:nth-child(2)){min-width:95px}
       |.rpt-bill-scroll thead th{position:sticky;top:0;background:var(--guild-ivory);z-index:2}
       |.rpt-bill-scroll td:nth-child(1),.rpt-bill-scroll th:nth-child(1){position:sticky;left:0;background:var(--guild-ivory);z-index:1;min-width:200px;max-width:200px;white-space:normal;word-break:break-word}
       |.rpt-bill-scroll td:nth-child(2),.rpt-bill-scroll th:nth-child(2){position:sticky;left:200px;background:var(--guild-ivory);z-index:1;min-width:130px;max-width:130px}
       |.rpt-bill-scroll thead th:nth-child(1),.rpt-bill-scroll thead th:nth-child(2){z-index:3}
       |.rpt-bill-total-col{font-weight:700}
       |.rpt-bill-other{cursor:help;border-bottom:1px dotted #8a8370}
       |@media print{
       |  .rpt-code{white-space:pre-wrap;word-break:break-word}
       |  #sidebar{display:none}
       |  body{margin-left:0}
       |  section{page-break-inside:avoid}
       |  .card{box-shadow:none;border:1px solid var(--guild-sand)}
       |  .section-body{display:block!important}
       |  section h2.collapsible::before{display:none}
       |}
       |</style>
    """.stripMargin


  val sidebarJS =
    s"""<script>
       |(function() {
       |  const nav = document.getElementById('sidebar-toc');
       |  document.querySelectorAll('section').forEach(function(sec, i) {
       |    if (!sec.id) sec.id = 'sec-' + i;
       |    const h2 = sec.querySelector('h2');
       |    const label = h2 ? h2.textContent.trim() : sec.id;
       |    const a = document.createElement('a');
       |    a.href = '#' + sec.id;
       |    a.className = 'toc-link';
       |    a.textContent = label;
       |    a.addEventListener('click', function(e) {
       |      e.preventDefault();
       |      document.getElementById(sec.id).scrollIntoView({ behavior: 'smooth', block: 'start' });
       |    });
       |    nav.appendChild(a);
       |  });
       |  const io = new IntersectionObserver(function(entries) {
       |    entries.forEach(function(e) {
       |      const a = nav.querySelector('[href="#' + e.target.id + '"]');
       |      if (a) a.classList.toggle('toc-active', e.isIntersecting);
       |    });
       |  }, { rootMargin: '-60px 0px -55% 0px', threshold: 0 });
       |  document.querySelectorAll('section').forEach(function(s) { io.observe(s); });
       |})();
       |</script>""".stripMargin


  val collapsibleSectionJS =
    s"""<script>
       |function toggleSection(bodyId, header) {
       |  const body = document.getElementById(bodyId);
       |  const hidden = body.style.display === 'none';
       |  body.style.display = hidden ? '' : 'none';
       |  header.classList.toggle('is-collapsed', !hidden);
       |}
       |</script>""".stripMargin


  /** Shared by every table built with [[sortableFilterableTable]] — sorts on each cell's
    * `data-v` (the raw value) rather than its formatted display text, so e.g. a "1.2 TB" column
    * still sorts numerically instead of alphabetically.
    */
  val sortableTableJS =
    s"""<script>
       |function sortRptTable(tableId, colIdx, kind) {
       |  const table = document.getElementById(tableId);
       |  const tbody = table.tBodies[0];
       |  const rows = Array.from(tbody.rows);
       |  const ths = table.tHead.rows[0].cells;
       |  const asc = ths[colIdx].getAttribute('data-dir') !== 'asc';
       |  Array.from(ths).forEach(function(c) { c.removeAttribute('data-dir'); c.classList.remove('sorted-asc', 'sorted-desc'); });
       |  ths[colIdx].setAttribute('data-dir', asc ? 'asc' : 'desc');
       |  ths[colIdx].classList.add(asc ? 'sorted-asc' : 'sorted-desc');
       |  rows.sort(function(a, b) {
       |    let av = a.cells[colIdx].getAttribute('data-v');
       |    let bv = b.cells[colIdx].getAttribute('data-v');
       |    if (kind === 'number') {
       |      av = parseFloat(av) || 0;
       |      bv = parseFloat(bv) || 0;
       |      return asc ? av - bv : bv - av;
       |    }
       |    return asc ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
       |  });
       |  rows.forEach(function(r) { tbody.appendChild(r); });
       |}
       |function filterRptTable(tableId, query) {
       |  const table = document.getElementById(tableId);
       |  const q = query.trim().toLowerCase();
       |  Array.from(table.tBodies[0].rows).forEach(function(row) {
       |    row.style.display = !q || row.textContent.toLowerCase().indexOf(q) !== -1 ? '' : 'none';
       |  });
       |}
       |</script>""".stripMargin
}
