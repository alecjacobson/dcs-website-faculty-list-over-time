#!/usr/bin/env python3
"""
visualize_html.py - Generate a standalone interactive HTML report.

Outputs plots/faculty_report.html with:
  - Sortable/filterable faculty table
  - Interactive arrivals & departures bar chart (hover shows names)
  - Faculty count over time (Plotly)
  - Note: replaces "cumulative arrivals" (inherently monotonic) with active count

Usage:
    python3 visualize_html.py
    python3 visualize_html.py --output plots/faculty_report.html
"""

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

PARSED_DIR   = Path("data/parsed")
TIMELINE_CSV = Path("data/faculty_timeline.csv")

SLUG_URLS = {
    "index_section95":     "http://web.cs.toronto.edu/dcs/index.php?section=95",
    "faculty_htm":         "http://web.cs.toronto.edu/people/faculty.htm",
    "contact_faculty_dir": "http://web.cs.toronto.edu/contact-us/faculty-directory",
    "people_faculty_dir":  "http://web.cs.toronto.edu/people/faculty-directory",
}


def yyyymm_to_date(s: str) -> datetime:
    return datetime.strptime(str(s)[:6], "%Y%m")


def yyyymm_to_label(s: str) -> str:
    d = yyyymm_to_date(s)
    return d.strftime("%b %Y")


LIVE_URL = "https://web.cs.toronto.edu/people/faculty-directory"


def build_wayback_map() -> dict[str, str]:
    """Return YYYYMM → URL, derived from parsed filenames."""
    wayback = {}
    for jf in PARSED_DIR.glob("*.json"):
        # Wayback snapshot: 14-digit timestamp + slug
        m = re.match(r"(\d{14})_(\w+)\.json", jf.name)
        if m:
            full_ts, slug = m.group(1), m.group(2)
            base = SLUG_URLS.get(slug, "")
            if base:
                wayback[full_ts[:6]] = f"https://web.archive.org/web/{full_ts}/{base}"
            continue
        # Live snapshot: YYYYMM_live.json
        m = re.match(r"(\d{6})_live\.json", jf.name)
        if m:
            wayback[m.group(1)] = LIVE_URL
    return wayback


def stream_confidence(row) -> str:
    last_year = int(str(row["last_seen"])[:4])
    stream = row["stream"]
    if stream == "teaching":
        return "confirmed"
    if last_year >= 2021:
        return "confirmed"
    if last_year >= 2009:
        return "inferred"
    return "uncertain"


def load_data():
    df = pd.read_csv(TIMELINE_CSV)
    wayback = build_wayback_map()
    df["stream_confidence"] = df.apply(stream_confidence, axis=1)
    df["first_label"] = df["first_seen"].apply(lambda s: yyyymm_to_label(str(s)))
    df["last_label"]  = df["last_seen"].apply(lambda s: yyyymm_to_label(str(s)))
    df["first_year"]  = df["first_seen"].astype(str).str[:4].astype(int)
    df["last_year"]   = df["last_seen"].astype(str).str[:4].astype(int)
    df["first_wayback"] = df["first_seen"].apply(lambda s: wayback.get(str(s)[:6], ""))
    return df


_PUNCT_RE = re.compile(r"['\-\.\,]")
_SPACE_RE = re.compile(r"\s+")
_TITLE_RE = re.compile(r"^(dr|prof|professor|mr|ms|mrs)\s+", re.IGNORECASE)

def _normalise(name: str) -> str:
    name = _TITLE_RE.sub("", name.lower())
    name = _PUNCT_RE.sub("", name)
    return _SPACE_RE.sub(" ", name).strip()


def build_name_stream_lookup(timeline_df: pd.DataFrame) -> dict:
    """Map normalised canonical name → final stream from the timeline CSV."""
    return {_normalise(row["canonical_name"]): row["stream"]
            for _, row in timeline_df.iterrows()}


def resolve_stream(raw_name: str, lookup: dict, threshold: int = 85) -> str:
    norm = _normalise(raw_name)
    if norm in lookup:
        return lookup[norm]
    result = process.extractOne(norm, list(lookup.keys()), scorer=fuzz.token_sort_ratio)
    if result and result[1] >= threshold:
        return lookup[result[0]]
    return "unknown"


def load_monthly_counts(timeline_df: pd.DataFrame):
    lookup = build_name_stream_lookup(timeline_df)
    rows = []
    for jf in sorted(PARSED_DIR.glob("*.json")):
        data = json.loads(jf.read_text())
        ts = data["timestamp"]
        counts: dict[str, int] = defaultdict(int)
        for f in data["faculty"]:
            stream = resolve_stream(f.get("name", ""), lookup)
            counts[stream] += 1
        rows.append({
            "date_label": yyyymm_to_date(ts).strftime("%Y-%m"),
            "research": counts.get("research", 0),
            "teaching": counts.get("teaching", 0),
            "unknown":  counts.get("unknown", 0),
            "total": counts.get("research", 0) + counts.get("teaching", 0),
        })
    return sorted(rows, key=lambda r: r["date_label"])


def build_arrivals_departures(df):
    """Per-year arrivals and departures with name lists."""
    arrivals = defaultdict(lambda: {"research": [], "teaching": [], "unknown": []})
    departures = defaultdict(lambda: {"research": [], "teaching": [], "unknown": []})

    for _, row in df.iterrows():
        yr = row["first_year"]
        stream = row["stream"] if row["stream"] in ("research", "teaching") else "unknown"
        arrivals[yr][stream].append(row["canonical_name"])

    for _, row in df[~df["currently_listed"]].iterrows():
        yr = row["last_year"]
        stream = row["stream"] if row["stream"] in ("research", "teaching") else "unknown"
        departures[yr][stream].append(row["canonical_name"])

    years = sorted(set(list(arrivals.keys()) + list(departures.keys())))
    return years, arrivals, departures


def generate_html(df, monthly_counts, years, arrivals, departures, output_path: Path):
    # --- Table rows JSON ---
    table_rows = []
    for _, row in df.sort_values("canonical_name").iterrows():
        conf = row["stream_confidence"]
        conf_badge = {
            "confirmed": '<span class="badge badge-confirmed">confirmed</span>',
            "inferred":  '<span class="badge badge-inferred">inferred</span>',
            "uncertain": '<span class="badge badge-uncertain">uncertain</span>',
        }[conf]
        stream_str = row["stream"] if row["stream"] in ("research", "teaching") else "unknown"
        status = "current" if row["currently_listed"] else "departed"
        table_rows.append({
            "name":        row["canonical_name"],
            "stream":      stream_str,
            "conf":        conf,
            "conf_badge":  conf_badge,
            "first":       row["first_label"],
            "first_url":   row["first_wayback"],
            "first_sort":  str(row["first_seen"])[:6],
            "last":        row["last_label"],
            "last_sort":   str(row["last_seen"])[:6],
            "status":      status,
            "snapshots":   int(row["months_active"]),
        })

    # --- Chart data JSON ---
    # ≤2007 is the initial population — separate traces, no departures shown for that year
    after_years = sorted(y for y in set(list(arrivals.keys()) + list(departures.keys())) if y > 2007)
    after_labels = [str(y) for y in after_years]

    def hover_names(names, verb):
        if not names:
            return f"No {verb}s"
        return f"<b>{len(names)} {verb}{'s' if len(names) != 1 else ''}:</b><br>" + "<br>".join(sorted(names))

    def hover_initial(names, stream):
        if not names:
            return f"No {stream} faculty"
        return f"<b>{len(names)} {stream} faculty already listed:</b><br>" + "<br>".join(sorted(names))

    # Pre-2007 initial population (single-point traces at x="≤2007")
    init_r       = [len(arrivals[2007]["research"])]
    init_t       = [len(arrivals[2007]["teaching"])]
    init_r_hover = [hover_initial(arrivals[2007]["research"], "research")]
    init_t_hover = [hover_initial(arrivals[2007]["teaching"], "teaching")]

    # Regular arrivals/departures from 2008 onward
    arr_r  = [len(arrivals[y]["research"]) for y in after_years]
    arr_t  = [len(arrivals[y]["teaching"]) for y in after_years]
    dep_r  = [-len(departures[y]["research"]) for y in after_years]
    dep_t  = [-len(departures[y]["teaching"]) for y in after_years]

    arr_r_hover = [hover_names(arrivals[y]["research"], "arrival") for y in after_years]
    arr_t_hover = [hover_names(arrivals[y]["teaching"], "arrival") for y in after_years]
    dep_r_hover = [hover_names(departures[y]["research"], "departure") for y in after_years]
    dep_t_hover = [hover_names(departures[y]["teaching"], "departure") for y in after_years]

    # Monthly count chart
    mc_dates    = [r["date_label"] for r in monthly_counts]
    mc_research = [r["research"] for r in monthly_counts]
    mc_teaching = [r["teaching"] for r in monthly_counts]
    mc_unknown  = [r["unknown"] for r in monthly_counts]

    # Summary stats
    n_research = (df["stream"] == "research").sum()
    n_teaching = (df["stream"] == "teaching").sum()
    n_current  = df["currently_listed"].sum()
    latest_snap = monthly_counts[-1] if monthly_counts else {}

    table_rows_json   = json.dumps(table_rows)
    after_labels_json = json.dumps(after_labels)
    init_r_json       = json.dumps(init_r)
    init_t_json       = json.dumps(init_t)
    init_r_hover_json = json.dumps(init_r_hover)
    init_t_hover_json = json.dumps(init_t_hover)
    arr_r_json        = json.dumps(arr_r)
    arr_t_json        = json.dumps(arr_t)
    dep_r_json        = json.dumps(dep_r)
    dep_t_json        = json.dumps(dep_t)
    arr_r_hover_json  = json.dumps(arr_r_hover)
    arr_t_hover_json  = json.dumps(arr_t_hover)
    dep_r_hover_json  = json.dumps(dep_r_hover)
    dep_t_hover_json  = json.dumps(dep_t_hover)
    mc_dates_json     = json.dumps(mc_dates)
    mc_research_json  = json.dumps(mc_research)
    mc_teaching_json  = json.dumps(mc_teaching)
    mc_unknown_json   = json.dumps(mc_unknown)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>UofT CS Faculty Change Over Time</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #f8f9fa; color: #212529; line-height: 1.5;
  }}
  .container {{ max-width: 1200px; margin: 0 auto; padding: 24px 16px; }}
  h1 {{ font-size: 1.8rem; font-weight: 700; margin-bottom: 4px; }}
  .subtitle {{ color: #6c757d; margin-bottom: 12px; }}
  .methodology {{
    font-size: 0.875rem; color: #495057; line-height: 1.6;
    background: #f1f3f5; border-left: 3px solid #ced4da;
    padding: 10px 14px; border-radius: 0 6px 6px 0;
    margin-bottom: 28px;
  }}
  .methodology a {{ color: #0d6efd; text-decoration: none; }}
  .methodology a:hover {{ text-decoration: underline; }}
  h2 {{ font-size: 1.2rem; font-weight: 600; margin-bottom: 12px; color: #343a40; }}

  .stats-row {{
    display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 32px;
  }}
  .stat-card {{
    background: white; border-radius: 8px; padding: 16px 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,.1); min-width: 140px;
  }}
  .stat-card .num {{ font-size: 2rem; font-weight: 700; line-height: 1; }}
  .stat-card .label {{ font-size: 0.8rem; color: #6c757d; margin-top: 4px; }}
  .stat-card.blue .num {{ color: #1f77b4; }}
  .stat-card.orange .num {{ color: #ff7f0e; }}
  .stat-card.green .num {{ color: #2ca02c; }}
  .stat-card.gray .num {{ color: #555; }}

  .card {{
    background: white; border-radius: 8px; padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,.1); margin-bottom: 28px;
  }}

  .chart-note {{
    font-size: 0.8rem; color: #6c757d; margin-top: 8px; font-style: italic;
  }}

  /* Table controls */
  .table-controls {{
    display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 12px; align-items: center;
  }}
  .table-controls input {{
    border: 1px solid #ced4da; border-radius: 6px; padding: 6px 12px;
    font-size: 0.9rem; outline: none; width: 220px;
  }}
  .table-controls input:focus {{ border-color: #86b7fe; box-shadow: 0 0 0 3px rgba(13,110,253,.15); }}
  .filter-btn {{
    border: 1px solid #ced4da; background: white; border-radius: 6px;
    padding: 5px 12px; font-size: 0.85rem; cursor: pointer; color: #495057;
    transition: all .15s;
  }}
  .filter-btn:hover {{ background: #e9ecef; }}
  .filter-btn.active {{ background: #0d6efd; color: white; border-color: #0d6efd; }}
  .table-count {{ color: #6c757d; font-size: 0.85rem; margin-left: auto; }}

  /* Table */
  .table-wrap {{ overflow-x: auto; }}
  table {{
    width: 100%; border-collapse: collapse; font-size: 0.875rem;
  }}
  th {{
    background: #f8f9fa; border-bottom: 2px solid #dee2e6;
    padding: 8px 12px; text-align: left; white-space: nowrap;
    position: sticky; top: 0; z-index: 1;
  }}
  th.sortable {{ cursor: pointer; user-select: none; }}
  th.sortable:hover {{ background: #e9ecef; }}
  th .sort-icon {{ color: #adb5bd; margin-left: 4px; font-size: 0.75rem; }}
  th.sort-asc .sort-icon::after {{ content: "▲"; color: #0d6efd; }}
  th.sort-desc .sort-icon::after {{ content: "▼"; color: #0d6efd; }}
  th:not(.sort-asc):not(.sort-desc) .sort-icon::after {{ content: "⇅"; }}
  td {{ padding: 7px 12px; border-bottom: 1px solid #f0f0f0; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #f8f9fa; }}

  .stream-research {{ color: #1f77b4; font-weight: 500; }}
  .stream-teaching {{ color: #ff7f0e; font-weight: 500; }}
  .stream-unknown  {{ color: #aaa; }}

  .status-current  {{ color: #198754; font-weight: 500; }}
  .status-departed {{ color: #6c757d; }}

  .badge {{
    display: inline-block; font-size: 0.7rem; padding: 1px 6px;
    border-radius: 10px; font-weight: 500; white-space: nowrap;
  }}
  .badge-confirmed {{ background: #d1e7dd; color: #0a3622; }}
  .badge-inferred  {{ background: #fff3cd; color: #664d03; }}
  .badge-uncertain {{ background: #f8d7da; color: #58151c; }}

  .no-results {{ text-align: center; color: #6c757d; padding: 32px; font-style: italic; }}
  td a {{ color: #0d6efd; text-decoration: none; }}
  td a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<div class="container">

  <h1>UofT CS Faculty Change Over Time</h1>
  <p class="subtitle">Department of Computer Science, University of Toronto &mdash; 2007&ndash;present</p>

  <p class="methodology">
    Data was collected from archived snapshots of the UofT CS faculty directory via the
    <a href="https://web.archive.org" target="_blank">Wayback Machine</a>, covering
    roughly one snapshot per month from November 2007 to the present.
    &ldquo;Appearance&rdquo; and &ldquo;departure&rdquo; refer to when a person&rsquo;s
    name was first or last visible on the department website &mdash; <em>not</em>
    necessarily their actual hiring or departure dates.
    Stream classification (research vs. teaching) is drawn from section headers and
    title strings on the modern site (2021+); earlier snapshots are inferred from
    title keywords or, for pre-2009 data, may be uncertain.
    The &ldquo;&le;2007&rdquo; cohort represents the 69 faculty already listed when
    data collection began; their true start dates predate these records.
    Vibe coded with Claude Code.
    Source on <a href="https://github.com/alecjacobson/dcs-website-faculty-list-over-time" target="_blank">GitHub</a>.
  </p>

  <div class="stats-row">
    <div class="stat-card gray">
      <div class="num">{len(df)}</div>
      <div class="label">Total unique faculty</div>
    </div>
    <div class="stat-card blue">
      <div class="num">{n_research}</div>
      <div class="label">Research stream</div>
    </div>
    <div class="stat-card orange">
      <div class="num">{n_teaching}</div>
      <div class="label">Teaching stream</div>
    </div>
    <div class="stat-card green">
      <div class="num">{n_current}</div>
      <div class="label">Currently listed</div>
    </div>
    <div class="stat-card gray">
      <div class="num">{len(monthly_counts)}</div>
      <div class="label">Snapshots (2007&ndash;now)</div>
    </div>
  </div>

  <!-- Chart 1: Arrivals & Departures -->
  <div class="card">
    <h2>Annual Arrivals &amp; Departures</h2>
    <div id="chart-arrivals" style="height:420px;"></div>
    <p class="chart-note">
      Hover bars to see names. &ldquo;&le;2007&rdquo; shows the 69 faculty already listed when data collection began &mdash; their actual start dates predate our records.
      Departures show the last year a person appeared in the directory.
    </p>
  </div>

  <!-- Chart 2: Count over time -->
  <div class="card">
    <h2>Faculty Count Over Time</h2>
    <div id="chart-count" style="height:380px;"></div>
    <p class="chart-note">
      Monthly snapshots from the Wayback Machine. Gaps between eras reflect URL changes on the CS website.
      "Unknown" = snapshots from the 2007&ndash;2008 era that did not distinguish streams.
      The large drop in 2018&ndash;2019 reflects a website redesign that reorganised the directory structure.
    </p>
  </div>

  <!-- Table -->
  <div class="card">
    <h2>Faculty Directory</h2>
    <div class="table-controls">
      <input type="text" id="search" placeholder="Search name…" oninput="applyFilters()">
      <button class="filter-btn active" data-stream="all"    onclick="setStreamFilter(this)">All</button>
      <button class="filter-btn"        data-stream="research" onclick="setStreamFilter(this)">Research</button>
      <button class="filter-btn"        data-stream="teaching" onclick="setStreamFilter(this)">Teaching</button>
      <button class="filter-btn active" data-status="all"    onclick="setStatusFilter(this, 'status')">All status</button>
      <button class="filter-btn"        data-status="current"  onclick="setStatusFilter(this, 'status')">Current</button>
      <button class="filter-btn"        data-status="departed" onclick="setStatusFilter(this, 'status')">Departed</button>
      <span class="table-count" id="table-count"></span>
    </div>
    <div class="table-wrap">
      <table id="faculty-table">
        <thead>
          <tr>
            <th class="sortable" data-col="name">Name<span class="sort-icon"></span></th>
            <th class="sortable" data-col="stream">Stream<span class="sort-icon"></span></th>
            <th>Stream confidence</th>
            <th class="sortable" data-col="first_sort">First seen<span class="sort-icon"></span></th>
            <th class="sortable" data-col="last_sort">Last seen<span class="sort-icon"></span></th>
            <th class="sortable" data-col="status">Status<span class="sort-icon"></span></th>
            <th class="sortable" data-col="snapshots" title="Number of monthly snapshots this person appeared in">Snapshots<span class="sort-icon"></span></th>
          </tr>
        </thead>
        <tbody id="table-body"></tbody>
      </table>
      <div class="no-results" id="no-results" style="display:none;">No matching faculty.</div>
    </div>
  </div>

</div>

<script>
// ---- Data ----
const ROWS = {table_rows_json};
const AFTER_LABELS = {after_labels_json};
const INIT_R = {init_r_json};
const INIT_T = {init_t_json};
const INIT_R_HOVER = {init_r_hover_json};
const INIT_T_HOVER = {init_t_hover_json};
const ARR_R = {arr_r_json};
const ARR_T = {arr_t_json};
const DEP_R = {dep_r_json};
const DEP_T = {dep_t_json};
const ARR_R_HOVER = {arr_r_hover_json};
const ARR_T_HOVER = {arr_t_hover_json};
const DEP_R_HOVER = {dep_r_hover_json};
const DEP_T_HOVER = {dep_t_hover_json};
const MC_DATES    = {mc_dates_json};
const MC_RESEARCH = {mc_research_json};
const MC_TEACHING = {mc_teaching_json};
const MC_UNKNOWN  = {mc_unknown_json};

// ---- Arrivals/Departures chart ----
// ≤2007 initial population uses its own traces (gray, no departures)
// Regular years (2008+) show arrivals above and departures below the axis
Plotly.newPlot('chart-arrivals', [
  {{
    name: 'Pre-2007 research', x: ['\u22642007'], y: INIT_R,
    text: INIT_R_HOVER, hovertemplate: '<b>\u22642007 — Research (pre-data)</b><br>%{{text}}<extra></extra>',
    type: 'bar', marker: {{ color: '#1f77b4', opacity: 0.35, pattern: {{ shape: '/', size: 6 }} }},
    showlegend: true,
  }},
  {{
    name: 'Pre-2007 teaching', x: ['\u22642007'], y: INIT_T,
    text: INIT_T_HOVER, hovertemplate: '<b>\u22642007 — Teaching (pre-data)</b><br>%{{text}}<extra></extra>',
    type: 'bar', marker: {{ color: '#ff7f0e', opacity: 0.35, pattern: {{ shape: '/', size: 6 }} }},
    showlegend: true,
  }},
  {{
    name: 'Research arrivals', x: AFTER_LABELS, y: ARR_R,
    text: ARR_R_HOVER, hovertemplate: '<b>%{{x}} — Research arrivals</b><br>%{{text}}<extra></extra>',
    type: 'bar', marker: {{ color: '#1f77b4', opacity: 0.85 }},
  }},
  {{
    name: 'Teaching arrivals', x: AFTER_LABELS, y: ARR_T,
    text: ARR_T_HOVER, hovertemplate: '<b>%{{x}} — Teaching arrivals</b><br>%{{text}}<extra></extra>',
    type: 'bar', marker: {{ color: '#ff7f0e', opacity: 0.85 }},
  }},
  {{
    name: 'Research departures', x: AFTER_LABELS, y: DEP_R,
    text: DEP_R_HOVER, hovertemplate: '<b>%{{x}} — Research departures</b><br>%{{text}}<extra></extra>',
    type: 'bar', marker: {{ color: '#1f77b4', opacity: 0.4, line: {{ color: '#1f77b4', width: 1 }} }},
  }},
  {{
    name: 'Teaching departures', x: AFTER_LABELS, y: DEP_T,
    text: DEP_T_HOVER, hovertemplate: '<b>%{{x}} — Teaching departures</b><br>%{{text}}<extra></extra>',
    type: 'bar', marker: {{ color: '#ff7f0e', opacity: 0.4, line: {{ color: '#ff7f0e', width: 1 }} }},
  }},
], {{
  barmode: 'relative',
  xaxis: {{ title: 'Year', tickfont: {{ size: 12 }} }},
  yaxis: {{ title: 'Faculty count', zeroline: true, zerolinewidth: 2, zerolinecolor: '#333' }},
  legend: {{ orientation: 'h', y: 1.15 }},
  margin: {{ t: 10, r: 20, b: 40, l: 50 }},
  plot_bgcolor: 'white', paper_bgcolor: 'white',
  hoverlabel: {{ align: 'left', font: {{ size: 12 }} }},
}}, {{responsive: true, displayModeBar: false}});

// ---- Count over time chart ----
Plotly.newPlot('chart-count', [
  {{
    name: 'Research stream', x: MC_DATES, y: MC_RESEARCH,
    type: 'scatter', mode: 'lines+markers',
    line: {{ color: '#1f77b4', width: 2 }},
    marker: {{ size: 4 }},
    hovertemplate: '<b>%{{x}}</b><br>Research: %{{y}}<extra></extra>',
  }},
  {{
    name: 'Teaching stream', x: MC_DATES, y: MC_TEACHING,
    type: 'scatter', mode: 'lines+markers',
    line: {{ color: '#ff7f0e', width: 2 }},
    marker: {{ size: 4 }},
    hovertemplate: '<b>%{{x}}</b><br>Teaching: %{{y}}<extra></extra>',
  }},
  ...(MC_UNKNOWN.some(v => v > 0) ? [{{
    name: 'Unclassified', x: MC_DATES, y: MC_UNKNOWN,
    type: 'scatter', mode: 'lines',
    line: {{ color: '#aaa', width: 1, dash: 'dot' }},
    hovertemplate: '<b>%{{x}}</b><br>Unclassified: %{{y}}<extra></extra>',
  }}] : []),
], {{
  xaxis: {{ title: 'Date', tickfont: {{ size: 12 }} }},
  yaxis: {{ title: 'Faculty on directory page', rangemode: 'tozero' }},
  legend: {{ orientation: 'h', y: 1.12 }},
  margin: {{ t: 10, r: 20, b: 40, l: 50 }},
  plot_bgcolor: 'white', paper_bgcolor: 'white',
  hovermode: 'x unified',
}}, {{responsive: true, displayModeBar: false}});

// ---- Table ----
let sortCol = 'name', sortDir = 1;
let streamFilter = 'all', statusFilter = 'all';

function setStreamFilter(btn) {{
  document.querySelectorAll('[data-stream]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  streamFilter = btn.dataset.stream;
  applyFilters();
}}

function setStatusFilter(btn) {{
  document.querySelectorAll('[data-status]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  statusFilter = btn.dataset.status;
  applyFilters();
}}

function applyFilters() {{
  const q = document.getElementById('search').value.toLowerCase();
  const filtered = ROWS.filter(r => {{
    if (q && !r.name.toLowerCase().includes(q)) return false;
    if (streamFilter !== 'all' && r.stream !== streamFilter) return false;
    if (statusFilter !== 'all' && r.status !== statusFilter) return false;
    return true;
  }});
  renderTable(filtered);
}}

function renderTable(rows) {{
  const sorted = [...rows].sort((a, b) => {{
    let va = a[sortCol], vb = b[sortCol];
    if (typeof va === 'string') va = va.toLowerCase();
    if (typeof vb === 'string') vb = vb.toLowerCase();
    return va < vb ? -sortDir : va > vb ? sortDir : 0;
  }});

  const tbody = document.getElementById('table-body');
  tbody.innerHTML = sorted.map(r => `
    <tr>
      <td>${{r.name}}</td>
      <td class="stream-${{r.stream}}">${{r.stream}}</td>
      <td>${{r.conf_badge}}</td>
      <td>${{r.first_url ? `<a href="${{r.first_url}}" target="_blank">${{r.first}}</a>` : r.first}}</td>
      <td>${{r.last}}</td>
      <td class="status-${{r.status}}">${{r.status}}</td>
      <td>${{r.snapshots}}</td>
    </tr>
  `).join('');

  document.getElementById('no-results').style.display = sorted.length ? 'none' : 'block';
  document.getElementById('table-count').textContent = `${{sorted.length}} / ${{ROWS.length}} faculty`;
}}

// Column sort
document.querySelectorAll('th.sortable').forEach(th => {{
  th.addEventListener('click', () => {{
    const col = th.dataset.col;
    if (sortCol === col) sortDir *= -1;
    else {{ sortCol = col; sortDir = 1; }}
    document.querySelectorAll('th').forEach(t => t.classList.remove('sort-asc', 'sort-desc'));
    th.classList.add(sortDir === 1 ? 'sort-asc' : 'sort-desc');
    applyFilters();
  }});
}});

// Initial render
applyFilters();
</script>
</body>
</html>
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html)
    print(f"Written: {output_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="plots/faculty_report.html")
    args = parser.parse_args()

    if not TIMELINE_CSV.exists():
        print("Run match.py first to generate faculty_timeline.csv")
        return
    if not PARSED_DIR.exists() or not any(PARSED_DIR.glob("*.json")):
        print("Run parse.py first to generate parsed JSON files")
        return

    print("Loading data...")
    df = load_data()
    monthly_counts = load_monthly_counts(df)
    years, arrivals, departures = build_arrivals_departures(df)

    print(f"  {len(df)} faculty, {len(monthly_counts)} snapshots")
    generate_html(df, monthly_counts, years, arrivals, departures, Path(args.output))


if __name__ == "__main__":
    main()
