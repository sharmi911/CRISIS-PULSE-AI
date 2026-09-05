"""
CRISIS PULSE AI — Dark Cyber War-Room Command Center (Streamlit UI)
====================================================================
Enterprise-styled front end for pipeline.py. Business logic lives entirely
in pipeline.py; this file only renders it. Pages:

  Live Command Center   — KPIs + live crisis feed + agent pipeline
  Crisis Digital Twin   — stateful per-incident view with risk timeline
  Source Verification   — evidence / corroboration / contradiction detail
  Prediction Engine      — escalation forecasts across active incidents
  Knowledge Graph        — client / regulator / competitor relationships
  SLA Monitor            — P50/P95/P99 + processing waterfall
  Incident History       — table of all incidents this session
  Crisis Simulator       — canned scenarios, zero API keys required
  System Health          — component status

Run:
    pip install -r requirements.txt --break-system-packages
    export GROQ_API_KEY=your_key_here      # not required just to explore the Simulator
    export NEWSAPI_KEY=your_key_here
    streamlit run streamlit_app.py
"""

import os
import threading
from datetime import datetime

import streamlit as st

import pipeline
import event_store
import worker

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False

AUTO_START_WORKER = os.environ.get("AUTO_START_WORKER", "true").lower() != "false"

# ---------------------------------------------------------------------------
# Palette — Dark Cyber War-Room (Intelligence War Room refresh)
# ---------------------------------------------------------------------------
BG = "#070A0F"
BG2 = "#0B1018"
CARD = "#101722"
CARD_HI = "#151E2D"
BORDER = "#263449"
CYAN = "#00E5FF"
CRITICAL = "#FF3158"
HIGH = "#FF9F1C"
MEDIUM = "#FFD166"
SAFE = "#22C55E"
TEXT = "#F8FAFC"
SUBTEXT = "#94A3B8"
MUTED = "#64748B"

BAND_COLOR = {"CRITICAL": CRITICAL, "HIGH": HIGH, "MEDIUM": MEDIUM, "LOW": SAFE}

NAV_GROUPS = [
    ("COMMAND", [("Command Center", "\u2302"), ("Active Crises", "\u25c9")]),
    ("INTELLIGENCE", [("Crisis Digital Twin", "\u25c8"), ("Source Verification", "\u2315"),
                       ("Prediction Engine", "\u25c7"), ("Knowledge Graph", "\u2b21")]),
    ("OPERATIONS", [("Response Center", "\u26a1"), ("SLA Monitor", "\u25f7"), ("Incident History", "\u25a4")]),
    ("SIMULATION", [("Crisis Simulator", "\u25b7")]),
    ("SYSTEM", [("System Health", "\u2699")]),
]

if "page" not in st.session_state:
    st.session_state.page = "Command Center"
if "selected_incident" not in st.session_state:
    st.session_state.selected_incident = None
if "acknowledged" not in st.session_state:
    st.session_state.acknowledged = set()


# ---------------------------------------------------------------------------
# Event-driven ingestion — starts the shared worker + fallback poller ONCE
# per process via cache_resource. Reads happen straight from event_store, so
# this UI works identically whether the worker running is this in-process
# thread or a completely separate `python worker.py` process pointed at the
# same CRISISPULSE_DB file.
# ---------------------------------------------------------------------------
@st.cache_resource
def start_ingestion_once():
    event_store.init_db()
    if AUTO_START_WORKER:
        worker.start_background_ingestion(with_fallback_poller=True)
    return True


# ---------------------------------------------------------------------------
# Page config + global CSS
# ---------------------------------------------------------------------------
st.set_page_config(page_title="CRISIS PULSE AI", page_icon="\u25c9", layout="wide")

if HAS_AUTOREFRESH:
    st_autorefresh(interval=15_000, key="cp_refresh")

st.markdown(f"""
<style>
    .stApp {{ background: {BG}; }}
    section[data-testid="stSidebar"] {{ background: {BG2}; border-right: 1px solid {BORDER}; }}
    .cp-header {{ padding: 16px 22px; border-radius: 10px; background: {BG2};
        border: 1px solid {BORDER}; margin-bottom: 16px; display: flex;
        justify-content: space-between; align-items: center; flex-wrap: wrap; }}
    .cp-title {{ font-size: 22px; font-weight: 800; color: {TEXT}; letter-spacing: .02em; }}
    .cp-title span {{ color: {CYAN}; }}
    .cp-tagline {{ color: {SUBTEXT}; font-size: 12px; text-transform: uppercase; letter-spacing: .08em; margin-top: 2px; }}
    .cp-status {{ display: flex; gap: 10px; flex-wrap: wrap; }}
    .cp-pill {{ background: {CARD}; border: 1px solid {BORDER}; border-radius: 6px;
        padding: 5px 10px; font-size: 11px; color: {SUBTEXT}; font-weight: 600; }}
    .cp-pill b {{ color: {TEXT}; }}
    .cp-live {{ color: {SAFE}; }}
    .cp-live::before {{ content: "\\25cf "; }}

    .kpi-card {{ background: {CARD}; border: 1px solid {BORDER}; border-radius: 8px;
        padding: 14px 16px; }}
    .kpi-num {{ font-size: 26px; font-weight: 800; color: {TEXT}; font-family: 'Courier New', monospace; }}
    .kpi-label {{ color: {SUBTEXT}; font-size: 10px; letter-spacing: .06em; text-transform: uppercase; margin-top: 2px; }}

    .cp-card {{ background: {CARD}; border: 1px solid {BORDER}; border-radius: 10px;
        padding: 18px 20px; margin-bottom: 16px; }}
    .cp-card-elevated {{ background: {CARD_HI}; }}
    .section-title {{ color: {TEXT}; font-size: 13px; font-weight: 800; letter-spacing: .06em;
        text-transform: uppercase; margin-bottom: 10px; border-left: 3px solid {CYAN}; padding-left: 8px; }}

    .band-pill {{ display: inline-block; padding: 3px 10px; border-radius: 4px; font-weight: 800;
        font-size: 12px; letter-spacing: .04em; }}
    .tier1-pill {{ background: #2a1f0a; color: {HIGH}; border: 1px solid {HIGH}; border-radius: 4px;
        padding: 3px 9px; font-size: 11px; font-weight: 700; }}
    .verify-pill {{ padding: 3px 9px; border-radius: 4px; font-size: 11px; font-weight: 700; border: 1px solid; }}
    .v-confirmed {{ color: {SAFE}; border-color: {SAFE}; background: #0f2318; }}
    .v-partially_verified {{ color: {MEDIUM}; border-color: {MEDIUM}; background: #2a2308; }}
    .v-contradicted {{ color: {CRITICAL}; border-color: {CRITICAL}; background: #2a0e14; }}
    .v-single_source {{ color: {SUBTEXT}; border-color: {BORDER}; background: {BG2}; }}

    .cp-h {{ color: {TEXT}; font-size: 13px; font-weight: 700; margin: 12px 0 4px; text-transform: uppercase;
        letter-spacing: .04em; color: {SUBTEXT}; }}
    .cp-p {{ color: {TEXT}; font-size: 14px; margin: 0 0 4px; line-height: 1.5; }}
    .cp-meta {{ color: {SUBTEXT}; font-size: 12px; font-family: 'Courier New', monospace; }}

    .plan-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-top: 8px; }}
    .plan-cell {{ background: {BG2}; border: 1px solid {BORDER}; border-radius: 6px; padding: 8px 10px; }}
    .plan-when {{ color: {CYAN}; font-size: 10px; text-transform: uppercase; letter-spacing: .05em; font-weight: 700; }}
    .plan-what {{ color: {TEXT}; font-size: 12px; margin-top: 3px; }}

    .evidence-chip {{ display: inline-block; background: {BG2}; border: 1px solid {BORDER};
        border-radius: 4px; padding: 2px 8px; font-size: 10px; color: {SUBTEXT}; margin: 2px 4px 2px 0;
        font-family: 'Courier New', monospace; }}

    .pipeline-track {{ display: flex; align-items: stretch; gap: 4px; margin-top: 8px; overflow-x: auto; }}
    .pipeline-stage {{ flex: 1; min-width: 90px; background: {BG2}; border: 1px solid {SAFE};
        border-radius: 6px; padding: 8px 10px; text-align: center; }}
    .pipeline-name {{ color: {SUBTEXT}; font-size: 9px; text-transform: uppercase; letter-spacing: .04em; }}
    .pipeline-check {{ color: {SAFE}; font-size: 13px; margin: 2px 0; }}
    .pipeline-time {{ color: {TEXT}; font-size: 11px; font-family: 'Courier New', monospace; }}

    .predict-gauge-wrap {{ background: {BG2}; border-radius: 6px; height: 16px; margin-top: 8px; overflow: hidden;
        border: 1px solid {BORDER}; }}
    .predict-gauge-fill {{ height: 16px; background: linear-gradient(90deg, {SAFE}, {MEDIUM}, {HIGH}, {CRITICAL}); }}

    .twin-state-track {{ display: flex; align-items: center; margin: 10px 0; }}
    .twin-state {{ flex: 1; text-align: center; padding: 6px 4px; font-size: 10px; font-weight: 700;
        text-transform: uppercase; letter-spacing: .03em; color: {SUBTEXT}; border-bottom: 3px solid {BORDER}; }}
    .twin-state.done {{ color: {CYAN}; border-bottom: 3px solid {CYAN}; }}
    .twin-state.current {{ color: {TEXT}; border-bottom: 3px solid {CRITICAL}; }}

    .health-row {{ display: flex; justify-content: space-between; align-items: center;
        padding: 8px 0; border-bottom: 1px solid {BORDER}; }}
    .health-name {{ color: {TEXT}; font-size: 13px; }}
    .health-ok {{ color: {SAFE}; font-size: 12px; font-weight: 700; }}
    .health-bad {{ color: {CRITICAL}; font-size: 12px; font-weight: 700; }}

    div[data-testid="stDataFrame"] {{ border: 1px solid {BORDER}; border-radius: 8px; }}

    /* Sidebar nav */
    section[data-testid="stSidebar"] .stButton button {{
        background: transparent; border: none; color: {SUBTEXT}; text-align: left;
        font-size: 13px; padding: 6px 10px; border-radius: 6px; font-weight: 500; }}
    section[data-testid="stSidebar"] .stButton button:hover {{ background: {CARD}; color: {TEXT}; }}
    section[data-testid="stSidebar"] .stButton button[kind="primary"] {{
        background: {CARD_HI}; color: {CYAN}; border-left: 2px solid {CYAN}; font-weight: 700; }}
    .nav-group-label {{ color: {MUTED}; font-size: 10px; letter-spacing: .1em; font-weight: 700;
        margin: 14px 0 2px 10px; text-transform: uppercase; }}
    .sidebar-status {{ margin-top: 20px; padding: 10px; background: {CARD}; border: 1px solid {BORDER};
        border-radius: 8px; }}
    .sidebar-status .dot {{ color: {SAFE}; }}

    /* Hero incident */
    .hero-card {{ background: {CARD_HI}; border: 1px solid {BORDER}; border-radius: 12px;
        padding: 20px 24px; margin-bottom: 16px; }}
    .hero-label {{ color: {CYAN}; font-size: 11px; font-weight: 800; letter-spacing: .08em; }}
    .hero-id {{ color: {MUTED}; font-size: 12px; font-family: 'Courier New', monospace; float: right; }}
    .hero-title {{ color: {TEXT}; font-size: 20px; font-weight: 800; margin: 8px 0 4px; }}
    .hero-metrics {{ display: flex; gap: 22px; margin-top: 14px; flex-wrap: wrap; }}
    .hero-metric-label {{ color: {MUTED}; font-size: 10px; letter-spacing: .06em; text-transform: uppercase; }}
    .hero-metric-val {{ color: {TEXT}; font-size: 16px; font-weight: 800; font-family: 'Courier New', monospace; }}

    /* Radial gauge (conic-gradient) */
    .gauge-wrap {{ display: flex; align-items: center; justify-content: center; flex-direction: column; }}
    .gauge {{ width: 110px; height: 110px; border-radius: 50%; display: flex; align-items: center;
        justify-content: center; position: relative; }}
    .gauge-inner {{ width: 84px; height: 84px; border-radius: 50%; background: {CARD_HI};
        display: flex; flex-direction: column; align-items: center; justify-content: center; }}
    .gauge-num {{ font-size: 26px; font-weight: 800; color: {TEXT}; font-family: 'Courier New', monospace; }}
    .gauge-band {{ font-size: 9px; font-weight: 700; letter-spacing: .05em; }}
    .gauge-delta {{ font-size: 11px; margin-top: 6px; font-family: 'Courier New', monospace; }}

    /* Compact incident row */
    .compact-row {{ background: {CARD}; border: 1px solid {BORDER}; border-left: 3px solid;
        border-radius: 6px; padding: 8px 14px; margin-bottom: 6px; display: flex;
        justify-content: space-between; align-items: center; }}
    .compact-row-left {{ display: flex; align-items: center; gap: 10px; }}
    .compact-badge {{ font-size: 11px; font-weight: 800; font-family: 'Courier New', monospace; }}
    .compact-headline {{ color: {TEXT}; font-size: 13px; }}
    .compact-time {{ color: {MUTED}; font-size: 11px; font-family: 'Courier New', monospace; }}
    div[data-testid="column"] .stButton button {{ font-size: 12px; padding: 4px 10px; }}

    .driver-bar-wrap {{ background: {BG2}; border-radius: 4px; height: 10px; margin: 3px 0 8px; overflow: hidden; }}
    .driver-bar-fill {{ height: 10px; background: {CYAN}; }}
    .driver-label {{ display: flex; justify-content: space-between; font-size: 11px; color: {SUBTEXT}; }}
</style>
""", unsafe_allow_html=True)

start_ingestion_once()


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
def render_header(last_run, last_error, avg_resp, sla_pct, queue):
    live = "cp-live" if not last_error else ""
    status_text = "SYSTEM LIVE" if not last_error else f"WORKER ERROR: {last_error[:40]}"
    st.markdown(f"""
    <div class="cp-header">
        <div>
            <div class="cp-title">\u25c9 <span>CRISIS PULSE</span> AI</div>
            <div class="cp-tagline">Autonomous Real-Time Crisis Intelligence</div>
        </div>
        <div class="cp-status">
            <div class="cp-pill {live}">{status_text}</div>
            <div class="cp-pill">Agents <b>7/7</b></div>
            <div class="cp-pill">Avg Latency <b>{avg_resp}s</b></div>
            <div class="cp-pill">SLA <b>{sla_pct}%</b></div>
            <div class="cp-pill">Queue <b>{queue.get('pending',0)}p / {queue.get('processing',0)}w</b></div>
            <div class="cp-pill">Last Event <b>{last_run or "--:--:--"}</b></div>
            <div class="cp-pill">Now <b>{datetime.now().strftime("%H:%M:%S")}</b></div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_kpis(results, incidents):
    escalated = [r for r in results if r.get("escalated")]
    critical = sum(1 for i in incidents if i.get("band") == "CRITICAL")
    high = sum(1 for i in incidents if i.get("band") == "HIGH")
    tier1 = sum(1 for r in results if r.get("profile", {}).get("tier") == "TIER1")
    predicted_esc = sum(1 for r in escalated if (r.get("prediction") or {}).get("probability", 0) >= 70)
    timed = [r["total_seconds"] for r in results if r.get("total_seconds") is not None]
    p95 = round(sorted(timed)[int(0.95 * (len(timed) - 1))], 1) if timed else 0

    cols = st.columns(6)
    kpis = [
        ("ACTIVE CRISES", len(incidents)), ("CRITICAL", critical), ("HIGH RISK", high),
        ("TIER-1 MENTIONS", tier1), ("PREDICTED ESCALATIONS", predicted_esc), ("SLA P95", f"{p95}s"),
    ]
    for col, (label, num) in zip(cols, kpis):
        with col:
            st.markdown(f'<div class="kpi-card"><div class="kpi-num">{num}</div>'
                         f'<div class="kpi-label">{label}</div></div>', unsafe_allow_html=True)


def dedupe_by_incident(results):
    """Results are newest-first; keep only the first (newest) result per
    incident id. Multiple articles about the same incident should show as
    ONE row that's been updated, not N duplicate rows — this is also what
    was causing duplicate Streamlit widget keys."""
    seen = set()
    out = []
    for r in results:
        inc_id = (r.get("incident") or {}).get("id")
        if inc_id is None or inc_id in seen:
            continue
        seen.add(inc_id)
        out.append(r)
    return out


def render_kpis4(results, incidents):
    """The decluttered Command Center KPI row — exactly 4 cards, per design brief."""
    critical = sum(1 for i in incidents if i.get("band") == "CRITICAL")
    escalating = sum(1 for i in incidents if i.get("band") in ("CRITICAL", "HIGH")
                      and any(r.get("velocity", {}).get("trend") == "accelerating"
                              for r in results if (r.get("incident") or {}).get("id") == i["id"]))
    timed = [r["total_seconds"] for r in results if r.get("total_seconds") is not None]
    p95 = round(sorted(timed)[int(0.95 * (len(timed) - 1))], 1) if timed else 0

    cols = st.columns(4)
    kpis = [("ACTIVE CRISES", len(incidents)), ("CRITICAL", critical),
            ("ESCALATING", escalating), ("SLA P95", f"{p95}s")]
    for col, (label, num) in zip(cols, kpis):
        with col:
            st.markdown(f'<div class="kpi-card"><div class="kpi-num">{num}</div>'
                         f'<div class="kpi-label">{label}</div></div>', unsafe_allow_html=True)


def render_gauge(score, band, delta=None):
    color = BAND_COLOR.get(band, SUBTEXT)
    pct = max(0, min(100, score))
    delta_html = ""
    if delta is not None:
        d_color = CRITICAL if delta > 0 and band in ("HIGH", "CRITICAL") else (SAFE if delta < 0 else MUTED)
        sign = "+" if delta > 0 else ""
        delta_html = f'<div class="gauge-delta" style="color:{d_color};">{sign}{delta} from previous</div>'
    st.markdown(f"""
    <div class="gauge-wrap">
        <div class="gauge" style="background: conic-gradient({color} {pct * 3.6}deg, {BORDER} 0deg);">
            <div class="gauge-inner">
                <div class="gauge-num">{score}</div>
                <div class="gauge-band" style="color:{color};">{band}</div>
            </div>
        </div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def render_hero_incident(r):
    """The single most important incident — Command Center's focal point."""
    article, crisis, insight = r["article"], r["crisis"], r["insight"]
    incident, prediction = r["incident"], r.get("prediction") or {}
    band_color = BAND_COLOR.get(crisis["band"], SUBTEXT)
    history = incident.get("score_history", [])
    delta = None
    if len(history) >= 2:
        delta = history[-1]["score"] - history[-2]["score"]

    left, right = st.columns([2, 1])
    with left:
        st.markdown(f"""
        <div class="hero-card">
            <span class="hero-label">\u26a0 MOST IMPORTANT INCIDENT</span>
            <span class="hero-id">{incident['id']}</span>
            <div class="hero-title">{article['headline']}</div>
            <div class="cp-meta">{incident['client'].upper()} \u00b7 {(incident['topic'] or '').upper()}</div>
            <div class="hero-metrics">
                <div><div class="hero-metric-label">Confidence</div><div class="hero-metric-val">{incident['confidence']}%</div></div>
                <div><div class="hero-metric-label">Sources</div><div class="hero-metric-val">{incident['distinct_sources']} verified</div></div>
                <div><div class="hero-metric-label">State</div><div class="hero-metric-val" style="color:{CYAN};">{r.get('twin_state') or '--'}</div></div>
                <div><div class="hero-metric-label">Escalation Prob.</div><div class="hero-metric-val" style="color:{band_color};">{prediction.get('probability',0)}%</div></div>
            </div>
            <div class="cp-h">What Happened</div>
            <div class="cp-p">{insight['what_happened']}</div>
            <div class="cp-h">Why It Matters</div>
            <div class="cp-p">{insight['why_it_matters']}</div>
        </div>
        """, unsafe_allow_html=True)
        b1, b2 = st.columns(2)
        with b1:
            if st.button("\u25b8 OPEN INCIDENT", key=f"open_hero_{incident['id']}", use_container_width=True, type="primary"):
                st.session_state.selected_incident = incident["id"]
                st.session_state.page = "Incident Detail"
                st.rerun()
        with b2:
            if st.button("VIEW EVIDENCE", key=f"evidence_hero_{incident['id']}", use_container_width=True):
                st.session_state.selected_incident = incident["id"]
                st.session_state.page = "Incident Detail"
                st.rerun()
    with right:
        st.markdown('<div class="hero-card" style="height:100%; display:flex; align-items:center; justify-content:center;">', unsafe_allow_html=True)
        render_gauge(crisis["score"], crisis["band"], delta)
        st.markdown('</div>', unsafe_allow_html=True)


def render_compact_row(r, key_suffix="", idx=0):
    """One-line clickable incident row — used for the Live Incident Stream / Active Crises page."""
    article, crisis, incident, velocity = r["article"], r["crisis"], r["incident"], r["velocity"]
    band_color = BAND_COLOR.get(crisis["band"], SUBTEXT)
    trend_icon = {"accelerating": "\u2191 ACCELERATING", "steady": "\u2192 STABLE",
                  "cooling": "\u2193 COOLING", "quiet": "\u2192 STABLE"}.get(velocity.get("trend"), "")
    dot = {"CRITICAL": "\U0001f534", "HIGH": "\U0001f7e0", "MEDIUM": "\U0001f7e1", "LOW": "\U0001f7e2"}.get(crisis["band"], "\u26aa")

    col1, col2 = st.columns([5, 1])
    with col1:
        st.markdown(f"""
        <div class="compact-row" style="border-left-color:{band_color};">
            <div class="compact-row-left">
                <span>{dot}</span>
                <span class="compact-badge" style="color:{band_color};">{incident['id']} {crisis['band']} {crisis['score']}</span>
                <span class="cp-meta">{trend_icon}</span>
                <span class="compact-headline">{article['headline'][:80]}</span>
            </div>
            <span class="compact-time">{r['ingest_ts']}</span>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        row_key = f"row_{incident['id']}_{key_suffix}_{idx}"
        if st.button("Open \u2192", key=row_key, use_container_width=True):
            st.session_state.selected_incident = incident["id"]
            st.session_state.page = "Incident Detail"
            st.rerun()


def render_response_center(escalated):
    if not escalated:
        st.info("No incidents currently require a response.")
        return
    escalated = dedupe_by_incident(escalated)
    groups = {"CRITICAL ACTION REQUIRED": [], "HIGH PRIORITY": [], "MONITOR": []}
    for r in escalated:
        band = r["crisis"]["band"]
        if band == "CRITICAL":
            groups["CRITICAL ACTION REQUIRED"].append(r)
        elif band == "HIGH":
            groups["HIGH PRIORITY"].append(r)
        else:
            groups["MONITOR"].append(r)

    deadlines = {"CRITICAL ACTION REQUIRED": "5 minutes", "HIGH PRIORITY": "15 minutes", "MONITOR": "1 hour"}
    for group_name, items in groups.items():
        if not items:
            continue
        st.markdown(f'<div class="section-title">{group_name}</div>', unsafe_allow_html=True)
        for r in items:
            incident, insight = r["incident"], r.get("insight") or {}
            plan = insight.get("response_plan", {})
            band_color = BAND_COLOR.get(r["crisis"]["band"], SUBTEXT)
            acked = incident["id"] in st.session_state.acknowledged
            col1, col2 = st.columns([5, 1])
            with col1:
                status = "ACKNOWLEDGED" if acked else "PENDING"
                status_color = SAFE if acked else HIGH
                st.markdown(f"""
                <div class="cp-card" style="border-left: 3px solid {band_color};">
                    <span class="compact-badge" style="color:{band_color};">{incident['id']} {r['crisis']['band']}</span>
                    <span class="cp-meta" style="float:right;">Deadline: {deadlines[group_name]} \u00b7
                        <span style="color:{status_color}; font-weight:800;">{status}</span></span>
                    <div class="cp-p" style="margin-top:8px; font-weight:700;">{r['article']['headline']}</div>
                    <div class="cp-meta">Recommended: {plan.get('immediate', '--')}</div>
                </div>
                """, unsafe_allow_html=True)
            with col2:
                st.write("")
                if not acked:
                    if st.button("ACKNOWLEDGE", key=f"ack_{incident['id']}", use_container_width=True):
                        st.session_state.acknowledged.add(incident["id"])
                        st.rerun()
                else:
                    st.button("\u2713 DONE", key=f"acked_{incident['id']}", use_container_width=True, disabled=True)
    st.caption("Acknowledgement is session-local for this demo \u2014 it resets when the app restarts, "
               "it isn't written back to the event store.")


def render_incident_detail(incident_id, results):
    matching = [r for r in results if (r.get("incident") or {}).get("id") == incident_id]
    if not matching:
        st.warning("That incident isn't in the current result window.")
        if st.button("\u2190 Back to Active Crises"):
            st.session_state.page = "Active Crises"
            st.session_state.selected_incident = None
            st.rerun()
        return
    r = matching[0]  # newest snapshot of this incident
    article, crisis, insight = r["article"], r["crisis"], r.get("insight") or {}
    incident = r["incident"]
    verification = r.get("verification") or {}
    prediction = r.get("prediction") or {}
    band_color = BAND_COLOR.get(crisis["band"], SUBTEXT)

    if st.button("\u2190 Back"):
        st.session_state.page = "Active Crises"
        st.session_state.selected_incident = None
        st.rerun()

    st.markdown(f"""
    <div class="hero-card">
        <span class="hero-id">{incident['id']}</span>
        <div class="hero-title">{incident['client'].upper()} / {(incident['topic'] or '').upper()}</div>
        <span class="band-pill" style="background:{band_color}22; color:{band_color}; border:1px solid {band_color};">
            {crisis['band']} \u2014 {crisis['score']}/100</span>
    </div>
    """, unsafe_allow_html=True)

    tab_overview, tab_evidence, tab_risk, tab_prediction, tab_response, tab_timeline = st.tabs(
        ["OVERVIEW", "EVIDENCE", "RISK EVOLUTION", "PREDICTION", "RESPONSE", "TIMELINE"]
    )

    with tab_overview:
        c1, c2, c3, c4 = st.columns(4)
        for col, (label, val) in zip([c1, c2, c3, c4], [
            ("State", r.get("twin_state") or "--"), ("Risk", f"{crisis['score']}/100"),
            ("Confidence", f"{incident['confidence']}%"),
            ("Narrative Velocity", r["velocity"]["trend"].upper()),
        ]):
            with col:
                st.markdown(f'<div class="kpi-card"><div class="kpi-num" style="font-size:16px;">{val}</div>'
                             f'<div class="kpi-label">{label}</div></div>', unsafe_allow_html=True)
        st.markdown('<div class="cp-h" style="margin-top:16px;">What Happened</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="cp-p">{insight.get("what_happened","--")}</div>', unsafe_allow_html=True)
        st.markdown('<div class="cp-h">Why It Matters</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="cp-p">{insight.get("why_it_matters","--")}</div>', unsafe_allow_html=True)
        st.markdown('<div class="cp-h">Current Recommendation</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="cp-p">{insight.get("response_plan",{}).get("immediate","--")}</div>', unsafe_allow_html=True)

    with tab_evidence:
        v_color = {"confirmed": SAFE, "partially_verified": MEDIUM, "contradicted": CRITICAL,
                   "single_source": SUBTEXT}.get(verification.get("status"), SUBTEXT)
        st.markdown(f"""
        <div class="cp-card">
            <span class="verify-pill v-{verification.get('status','single_source')}">
                {verification.get('status','single_source').upper().replace('_',' ')}</span>
            <span class="cp-meta"> \u00b7 {incident['distinct_sources']} independent source(s)</span>
            <div class="cp-p" style="margin-top:8px; color:{v_color};">{verification.get('note','')}</div>
        </div>
        """, unsafe_allow_html=True)
        agents = verification.get("agents") or {}
        if agents:
            claim, source, entity, contradiction = (agents.get("claim", {}), agents.get("source", {}),
                                                      agents.get("entity", {}), agents.get("contradiction", {}))
            source_color = {"high": SAFE, "medium": MEDIUM, "low": CRITICAL}.get(source.get("tier"), SUBTEXT)
            entity_ok = entity.get("client_confirmed")
            contra_flag = contradiction.get("contradiction")
            a1, a2, a3, a4 = st.columns(4)
            with a1:
                st.markdown(f'<div class="kpi-card"><div class="kpi-label">CLAIM AGENT</div>'
                             f'<div class="cp-p" style="font-size:12px; margin-top:4px;">{claim.get("claim","--")}</div></div>', unsafe_allow_html=True)
            with a2:
                st.markdown(f'<div class="kpi-card"><div class="kpi-label">SOURCE AGENT</div>'
                             f'<div class="kpi-num" style="font-size:18px; color:{source_color};">{source.get("reliability_score","--")}%</div></div>', unsafe_allow_html=True)
            with a3:
                ok_label = "CONFIRMED" if entity_ok else "UNCERTAIN"
                st.markdown(f'<div class="kpi-card"><div class="kpi-label">ENTITY AGENT</div>'
                             f'<div class="kpi-num" style="font-size:14px; color:{SAFE if entity_ok else HIGH};">{ok_label}</div></div>', unsafe_allow_html=True)
            with a4:
                c_label = "CONFLICT" if contra_flag else "NONE"
                st.markdown(f'<div class="kpi-card"><div class="kpi-label">CONTRADICTION</div>'
                             f'<div class="kpi-num" style="font-size:14px; color:{CRITICAL if contra_flag else SAFE};">{c_label}</div></div>', unsafe_allow_html=True)
        st.markdown('<div class="cp-h" style="margin-top:12px;">Sources</div>', unsafe_allow_html=True)
        for a in incident.get("articles", []):
            rel = pipeline.source_reliability(a["source"])
            st.markdown(f'<div class="compact-row" style="border-left-color:{BORDER};">'
                        f'<span class="compact-headline">{a.get("claim", a["headline"])[:70]}</span>'
                        f'<span class="cp-meta">{a["source"]} \u00b7 reliability {rel}%</span></div>',
                        unsafe_allow_html=True)

    with tab_risk:
        history = incident.get("score_history", [])
        if len(history) >= 2:
            st.line_chart({"score": [h["score"] for h in history]}, height=220)
            prev, curr = history[-2]["score"], history[-1]["score"]
            st.markdown('<div class="cp-h">Why Did Risk Change?</div>', unsafe_allow_html=True)
            for k, v in crisis["breakdown"].items():
                if v:
                    st.markdown(f'<div class="driver-label"><span>{k.replace("_"," ").title()}</span>'
                                f'<span>+{v}</span></div><div class="driver-bar-wrap">'
                                f'<div class="driver-bar-fill" style="width:{min(100,v*2.5)}%;"></div></div>',
                                unsafe_allow_html=True)
            c1, c2, c3 = st.columns(3)
            c1.metric("Previous", prev)
            c2.metric("Current", curr)
            c3.metric("Delta", f"{curr - prev:+d}")
        else:
            st.caption("Only one data point so far \u2014 the risk-evolution chart needs a second article on this incident.")
            for k, v in crisis["breakdown"].items():
                if v:
                    st.markdown(f'<div class="driver-label"><span>{k.replace("_"," ").title()}</span>'
                                f'<span>+{v}</span></div><div class="driver-bar-wrap">'
                                f'<div class="driver-bar-fill" style="width:{min(100,v*2.5)}%;"></div></div>',
                                unsafe_allow_html=True)

    with tab_prediction:
        st.markdown(f"""
        <div class="hero-card" style="text-align:center;">
            <div class="hero-metric-val" style="font-size:40px;">{prediction.get('probability',0)}%</div>
            <div class="hero-metric-label">ESCALATION PROBABILITY</div>
            <div class="cp-p" style="margin-top:10px;">Predicted state: <b style="color:{BAND_COLOR.get(prediction.get('predicted_band'),SUBTEXT)};">
                {prediction.get('predicted_band','--')}</b> \u00b7 ETA <b>{prediction.get('eta','--')}</b></div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown('<div class="cp-h">Prediction Drivers (from the deterministic scoring model)</div>', unsafe_allow_html=True)
        total_pts = sum(v for v in crisis["breakdown"].values() if v) or 1
        for k, v in sorted(crisis["breakdown"].items(), key=lambda kv: -kv[1]):
            if v:
                pct = round(100 * v / total_pts)
                st.markdown(f'<div class="driver-label"><span>{k.replace("_"," ").title()}</span>'
                            f'<span>{pct}%</span></div><div class="driver-bar-wrap">'
                            f'<div class="driver-bar-fill" style="width:{pct}%;"></div></div>', unsafe_allow_html=True)

    with tab_response:
        plan = insight.get("response_plan", {})
        st.markdown(f"""
        <div class="plan-grid" style="grid-template-columns: repeat(2, 1fr);">
            <div class="plan-cell"><div class="plan-when">0-5 min</div><div class="plan-what">{plan.get('immediate','--')}</div></div>
            <div class="plan-cell"><div class="plan-when">5-15 min</div><div class="plan-what">{plan.get('next_15min','--')}</div></div>
            <div class="plan-cell"><div class="plan-when">Next hour</div><div class="plan-what">{plan.get('next_1hr','--')}</div></div>
            <div class="plan-cell"><div class="plan-when">Executive</div><div class="plan-what">{plan.get('executive','--')}</div></div>
        </div>
        """, unsafe_allow_html=True)

    with tab_timeline:
        render_agent_pipeline(r.get("stages") or [])
        total = r.get("total_seconds")
        if total is not None:
            sla_ok = r.get("sla_met")
            color = SAFE if sla_ok else CRITICAL
            label = "SLA PASSED" if sla_ok else "SLA BREACHED"
            st.markdown(f'<div class="cp-meta" style="text-align:center; margin-top:10px; color:{color}; '
                        f'font-weight:800;">{total}s / 120s TARGET \u2014 {label}</div>', unsafe_allow_html=True)


def render_agent_pipeline(stages):
    if not stages:
        return
    parts = []
    for label, secs in stages:
        parts.append(f'<div class="pipeline-stage"><div class="pipeline-name">{label}</div>'
                      f'<div class="pipeline-check">\u2713</div><div class="pipeline-time">{secs}s</div></div>')
    st.markdown(f'<div class="pipeline-track">{"".join(parts)}</div>', unsafe_allow_html=True)


def render_crisis_card(r, expanded_pipeline=True):
    article, triage, crisis, insight = r["article"], r["triage"], r["crisis"], r["insight"]
    incident, profile, velocity = r["incident"], r["profile"], r["velocity"]
    verification = r.get("verification") or {"status": "single_source", "note": ""}
    prediction = r.get("prediction") or {"probability": 0, "predicted_band": crisis["band"], "eta": "--"}
    plan = insight.get("response_plan", {}) if insight else {}
    band_color = BAND_COLOR.get(crisis["band"], SUBTEXT)

    tier_html = '<span class="tier1-pill">TIER-1</span>' if profile.get("tier") == "TIER1" else ""
    evidence_html = "".join(
        f'<span class="evidence-chip">+{v} {k.replace("_"," ")}</span>'
        for k, v in crisis["breakdown"].items() if v
    )
    verify_labels = {"confirmed": "\u2713 VERIFIED", "partially_verified": "\u26a0 PARTIAL",
                      "contradicted": "\u26a0 CONTRADICTION", "single_source": "\u25cc UNCONFIRMED"}
    v_label = verify_labels.get(verification["status"], verification["status"])

    st.markdown(f"""
    <div class="cp-card">
        <span class="band-pill" style="background:{band_color}22; color:{band_color}; border:1px solid {band_color};">
            {crisis['band']} \u2014 {crisis['score']}/100</span>
        <span class="cp-pill" style="display:inline-block;">CONF {incident['confidence']}% \u00b7 {incident['distinct_sources']} SRC</span>
        <span class="verify-pill v-{verification['status']}">{v_label}</span>
        {tier_html}
        <span class="cp-meta" style="float:right;">{incident['id']} \u00b7 {r['ingest_ts']}</span>
        <div class="cp-p" style="font-size:16px; font-weight:700; margin-top:12px;">{article['headline']}</div>
        <div class="cp-meta">{article['source']} (reliability {pipeline.source_reliability(article['source'])}%) \u00b7
            {triage['topic'].upper()} \u00b7 {triage['sentiment'].upper()} \u00b7 velocity {velocity['trend']}</div>
        {f'<div class="cp-meta" style="font-style:italic; margin-top:4px;">{verification.get("note","")}</div>' if verification.get('note') else ''}
        <div style="margin-top:8px;">{evidence_html}</div>
        <div class="cp-h">What Happened</div>
        <div class="cp-p">{insight['what_happened']}</div>
        <div class="cp-h">Why It Matters</div>
        <div class="cp-p">{insight['why_it_matters']}</div>
        <div class="cp-h">Escalation Forecast</div>
        <div class="cp-p">{prediction['probability']}% chance of reaching
            <b style="color:{BAND_COLOR.get(prediction['predicted_band'], SUBTEXT)};">{prediction['predicted_band']}</b>
            within <b>{prediction['eta']}</b></div>
        <div class="predict-gauge-wrap"><div class="predict-gauge-fill" style="width:{prediction['probability']}%;"></div></div>
        <div class="cp-h">AI Response Plan</div>
        <div class="plan-grid">
            <div class="plan-cell"><div class="plan-when">0-5 min</div><div class="plan-what">{plan.get('immediate','--')}</div></div>
            <div class="plan-cell"><div class="plan-when">5-15 min</div><div class="plan-what">{plan.get('next_15min','--')}</div></div>
            <div class="plan-cell"><div class="plan-when">Next hour</div><div class="plan-what">{plan.get('next_1hr','--')}</div></div>
            <div class="plan-cell"><div class="plan-when">Executive</div><div class="plan-what">{plan.get('executive','--')}</div></div>
        </div>
        <div class="cp-h">AI Agent Pipeline</div>
    """, unsafe_allow_html=True)

    if expanded_pipeline:
        render_agent_pipeline(r.get("stages") or [])

    total = r.get("total_seconds")
    if total is not None:
        sla_ok = r.get("sla_met")
        color = SAFE if sla_ok else CRITICAL
        label = "SLA PASSED" if sla_ok else "SLA BREACHED"
        st.markdown(f'<div class="cp-meta" style="text-align:center; margin-top:8px; color:{color}; '
                     f'font-weight:800;">{total}s / 120s TARGET \u2014 {label}</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_digital_twin(incident, verification, velocity):
    state_now = pipeline.incident_state(incident, verification, velocity)
    idx = pipeline.TWIN_STATES.index(state_now) if state_now in pipeline.TWIN_STATES else 0
    cells = []
    for i, s in enumerate(pipeline.TWIN_STATES):
        cls = "done" if i < idx else ("current" if i == idx else "")
        cells.append(f'<div class="twin-state {cls}">{s}</div>')
    st.markdown(f"""
    <div class="cp-card cp-card-elevated">
        <span class="cp-meta" style="font-size:14px; font-weight:800; color:{TEXT};">{incident['id']}</span>
        <span class="cp-meta"> \u00b7 {incident['client'].upper()} / {(incident['topic'] or '').upper()}</span>
        <div class="twin-state-track">{''.join(cells)}</div>
        <div class="cp-meta">Peak risk {incident['max_score']}/100 ({incident['band']}) \u00b7
            {incident['article_count']} article(s) \u00b7 confidence {incident['confidence']}% \u00b7
            {incident['distinct_sources']} source(s)</div>
    </div>
    """, unsafe_allow_html=True)
    history = incident.get("score_history", [])
    if len(history) >= 2:
        chart_data = {"score": [h["score"] for h in history]}
        st.line_chart(chart_data, height=180)
    elif history:
        st.caption(f"Single data point so far: {history[0]['score']}/100. Chart appears once a second article lands.")


def render_knowledge_graph(profile, client_name, incidents):
    regulator = profile.get("regulator")
    competitors = profile.get("competitors", [])
    dot_lines = [f'"{client_name}" [color="{CYAN}", fontcolor="{CYAN}", style=filled, fillcolor="#0d2530"]']
    if regulator:
        dot_lines.append(f'"{regulator}" [color="{HIGH}", fontcolor="{HIGH}"]')
        dot_lines.append(f'"{regulator}" -> "{client_name}" [label="regulates", color="{SUBTEXT}", fontcolor="{SUBTEXT}"]')
    for c in competitors:
        dot_lines.append(f'"{c}" [color="{SUBTEXT}", fontcolor="{SUBTEXT}"]')
        dot_lines.append(f'"{client_name}" -> "{c}" [label="competes with", color="{SUBTEXT}", fontcolor="{SUBTEXT}"]')
    for inc in incidents:
        if inc["client"] == client_name:
            node = f'"{inc["id"]}"'
            color = BAND_COLOR.get(inc["band"], SUBTEXT)
            dot_lines.append(f'{node} [shape=box, color="{color}", fontcolor="{color}"]')
            dot_lines.append(f'"{client_name}" -> {node} [label="affected by", color="{SUBTEXT}", fontcolor="{SUBTEXT}"]')
    dot = "digraph G { bgcolor=\"" + BG2 + "\"; node [fontname=\"Helvetica\"]; " + " ".join(dot_lines) + " }"
    st.graphviz_chart(dot, use_container_width=True)


def render_sla_monitor(results, tier1_mentions):
    timed = sorted(r["total_seconds"] for r in results if r.get("total_seconds") is not None)
    breaches = sum(1 for r in results if r.get("sla_met") is False)

    def pctl(p):
        if not timed:
            return 0
        return round(timed[min(len(timed) - 1, int(p * (len(timed) - 1)))], 1)

    avg = round(sum(timed) / len(timed), 1) if timed else 0
    cols = st.columns(7)
    kpis = [("PROCESSED", len(results)), ("TIER-1", tier1_mentions), ("AVG", f"{avg}s"),
            ("P50", f"{pctl(0.5)}s"), ("P95", f"{pctl(0.95)}s"), ("P99", f"{pctl(0.99)}s"), ("BREACHES", breaches)]
    for col, (label, num) in zip(cols, kpis):
        with col:
            st.markdown(f'<div class="kpi-card"><div class="kpi-num">{num}</div>'
                         f'<div class="kpi-label">{label}</div></div>', unsafe_allow_html=True)

    st.write("")
    status_color = SAFE if breaches == 0 else HIGH
    status_text = "\u2713 SLA HEALTHY" if breaches == 0 else f"\u26a0 {breaches} BREACH(ES)"
    st.markdown(f'<div class="cp-card" style="text-align:center; color:{status_color}; '
                f'font-weight:800; font-size:16px;">{status_text}</div>', unsafe_allow_html=True)

    escalated_with_stages = [r for r in results if r.get("stages")]
    if escalated_with_stages:
        st.markdown('<div class="section-title">Processing Waterfall (most recent alert)</div>', unsafe_allow_html=True)
        render_agent_pipeline(escalated_with_stages[0]["stages"])

    if results:
        st.markdown('<div class="section-title" style="margin-top:16px;">Recent Articles</div>', unsafe_allow_html=True)
        table_rows = [{
            "Time": r["ingest_ts"], "Headline": r["article"]["headline"][:70],
            "Escalated": "Yes" if r["escalated"] else "No",
            "Total (s)": r["total_seconds"],
            "SLA": "PASS" if r["sla_met"] is True else ("BREACH" if r["sla_met"] is False else "--"),
        } for r in results]
        st.dataframe(table_rows, use_container_width=True, hide_index=True)


def render_incident_history(incidents):
    if not incidents:
        st.info("No incidents recorded yet this session.")
        return
    rows = [{
        "Incident": i["id"], "Client": i["client"], "Topic": i["topic"], "Band": i["band"],
        "Peak Score": i["max_score"], "Confidence": f"{i['confidence']}%",
        "Articles": i["article_count"], "Sources": i["distinct_sources"],
        "First seen": datetime.fromtimestamp(i["first_seen"]).strftime("%H:%M:%S"),
        "Last updated": datetime.fromtimestamp(i["last_updated"]).strftime("%H:%M:%S"),
    } for i in incidents]
    st.dataframe(rows, use_container_width=True, hide_index=True)
    st.caption("Each row is ONE evolving crisis \u2014 articles about the same client + topic within a "
               "3-hour window are collapsed into a single incident instead of separate alerts.")


def render_system_health(last_error, queue):
    components = [
        ("Webhook Ingress (event_store.enqueue_event)", True),
        ("Event Queue (SQLite, WAL)", True),
        ("Ingestion Worker (claim + process)", last_error is None),
        ("NewsAPI Fallback Poller", True),
        ("Groq Triage Agent", True),
        ("Verification Agent", True),
        ("Prediction Engine", True),
        ("Crisis Digital Twin Store", True),
        ("Insight Agent", True),
        ("Alert Engine (Email)", bool(os.environ.get("EMAIL_FROM"))),
    ]
    for name, ok in components:
        status_cls = "health-ok" if ok else "health-bad"
        status_text = "\u2713 ONLINE" if ok else "\u26a0 NOT CONFIGURED"
        st.markdown(f'<div class="health-row"><span class="health-name">{name}</span>'
                     f'<span class="{status_cls}">{status_text}</span></div>', unsafe_allow_html=True)
    st.write("")
    qcols = st.columns(4)
    for col, (label, key) in zip(qcols, [("PENDING", "pending"), ("PROCESSING", "processing"),
                                          ("DONE", "done"), ("ERRORS", "error")]):
        with col:
            st.markdown(f'<div class="kpi-card"><div class="kpi-num">{queue.get(key,0)}</div>'
                         f'<div class="kpi-label">{label}</div></div>', unsafe_allow_html=True)
    st.caption("Alert Engine shows NOT CONFIGURED until EMAIL_FROM/EMAIL_PASSWORD/EMAIL_TO are set \u2014 "
               "alerts still print to console either way. Push a live event: "
               "`curl -X POST http://localhost:5000/webhook/article -d '{\"headline\":\"...\"}'` "
               "(via app.py) or run `python webhook_server.py` standalone on :8000.")
    errs = event_store.recent_errors(limit=5)
    if errs:
        st.markdown('<div class="section-title" style="margin-top:16px;">Recent Processing Errors</div>', unsafe_allow_html=True)
        st.dataframe(errs, use_container_width=True, hide_index=True)


def render_simulator():
    st.markdown('<div class="section-title">Crisis Simulation Lab</div>', unsafe_allow_html=True)
    st.caption("Replays a scripted event sequence through the real scoring, clustering, verification and "
               "prediction logic \u2014 no NewsAPI or GROQ_API_KEY required. Use this if live news doesn't "
               "cooperate during a demo.")

    scenario_key = st.selectbox(
        "Scenario", options=list(pipeline.SIMULATION_SCENARIOS.keys()),
        format_func=lambda k: pipeline.SIMULATION_SCENARIOS[k]["label"],
    )
    if st.button("\u25b6 RUN SIMULATION", type="primary"):
        placeholder = st.empty()
        events_so_far = []

        def on_event(r):
            events_so_far.append(r)
            with placeholder.container():
                st.markdown(f'<div class="cp-meta">T+{len(events_so_far)*3}s \u2014 event {len(events_so_far)} '
                             f'of {len(pipeline.SIMULATION_SCENARIOS[scenario_key]["events"])} injected</div>',
                             unsafe_allow_html=True)
                for r in reversed(events_so_far):
                    render_crisis_card(r, expanded_pipeline=False)

        with st.spinner("Simulation running..."):
            pipeline.run_simulation(scenario_key, on_event=on_event)
        st.success(f"\u2713 CRISIS RESPONSE SLA PASSED \u2014 {len(events_so_far)} events processed, "
                   f"final state: {events_so_far[-1]['twin_state']}")


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Sidebar navigation — grouped, compact buttons (not a flat radio list)
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(f'<div style="color:{CYAN}; font-weight:800; font-size:15px; padding:8px 0 0;">\u25c9 CRISIS PULSE AI</div>'
                f'<div style="color:{MUTED}; font-size:10px; letter-spacing:.08em; margin-bottom:6px;">REAL-TIME INTELLIGENCE</div>',
                unsafe_allow_html=True)
    for group_label, items in NAV_GROUPS:
        st.markdown(f'<div class="nav-group-label">{group_label}</div>', unsafe_allow_html=True)
        for label, icon in items:
            is_active = st.session_state.page == label
            if st.button(f"{icon}  {label}", key=f"nav_{label}", use_container_width=True,
                         type="primary" if is_active else "secondary"):
                st.session_state.page = label
                st.session_state.selected_incident = None
                st.rerun()
    st.markdown(f"""
    <div class="sidebar-status">
        <div style="color:{MUTED}; font-size:9px; letter-spacing:.08em;">SYSTEM STATUS</div>
        <div style="color:{TEXT}; font-size:12px; margin-top:4px;"><span class="dot">\u25cf</span> ALL SYSTEMS OPERATIONAL</div>
        <div style="color:{MUTED}; font-size:11px; margin-top:2px;">7/7 AI AGENTS ONLINE</div>
    </div>
    """, unsafe_allow_html=True)

page = st.session_state.page

# ---------------------------------------------------------------------------
# Shared data for this run
# ---------------------------------------------------------------------------
if not os.environ.get("GROQ_API_KEY") or not os.environ.get("NEWSAPI_KEY"):
    st.warning("GROQ_API_KEY / NEWSAPI_KEY not set \u2014 live LLM processing is disabled. "
               "The Crisis Simulator page works without them. You can still push events via "
               "`python webhook_server.py` or app.py's /webhook/article \u2014 they'll queue but "
               "fail at the triage step without GROQ_API_KEY.")

results = event_store.get_recent_results(limit=60)
queue = event_store.queue_counts()
recent_errs = event_store.recent_errors(limit=1)
last_error = recent_errs[0]["error"] if recent_errs else None
last_run = results[0]["ingest_ts"] if results else None

escalated = [r for r in results if r.get("escalated")]
timed = [r["total_seconds"] for r in results if r.get("total_seconds") is not None]
avg_resp = round(sum(timed) / len(timed), 1) if timed else 0
sla_total = sum(1 for r in results if r.get("sla_met") is not None)
sla_met = sum(1 for r in results if r.get("sla_met") is True)
sla_pct = round(100 * sla_met / sla_total) if sla_total else 100
tier1_mentions = sum(1 for r in results if r.get("profile", {}).get("tier") == "TIER1")
incidents = event_store.get_latest_incidents()

render_header(last_run, last_error, avg_resp, sla_pct, queue)

# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------
if page == "Command Center":
    render_kpis4(results, incidents)
    st.write("")
    if escalated:
        # "most important" = highest current crisis score among escalated results
        hero = max(escalated, key=lambda r: r["crisis"]["score"])
        render_hero_incident(hero)
        st.write("")
        st.markdown('<div class="section-title">Live Incident Stream</div>', unsafe_allow_html=True)
        for idx, r in enumerate(dedupe_by_incident(escalated)[:6]):
            render_compact_row(r, key_suffix="cc", idx=idx)
    else:
        st.info("No high-priority events yet. Low-priority articles are triaged silently to avoid alert "
                 "fatigue \u2014 try the Crisis Simulator page for a guaranteed live walkthrough.")

elif page == "Active Crises":
    st.markdown('<div class="section-title">Active Crises</div>', unsafe_allow_html=True)
    if not escalated:
        st.info("No active crises right now.")
    for idx, r in enumerate(dedupe_by_incident(escalated)[:20]):
        render_compact_row(r, key_suffix="ac", idx=idx)

elif page == "Incident Detail":
    inc_id = st.session_state.selected_incident
    if not inc_id:
        st.info("Open an incident from Command Center or Active Crises to see its detail view.")
    else:
        render_incident_detail(inc_id, results)

elif page == "Crisis Digital Twin":
    st.markdown('<div class="section-title">Crisis Digital Twin</div>', unsafe_allow_html=True)
    st.caption("Each incident is a living object \u2014 new articles update the SAME twin instead of "
               "spawning a new alert.")
    if not incidents:
        st.info("No active incidents yet.")
    for inc in incidents[:6]:
        matching = next((r for r in results if (r.get("incident") or {}).get("id") == inc["id"]), None)
        verification = matching.get("verification") if matching else None
        velocity = matching.get("velocity") if matching else {"trend": "quiet"}
        render_digital_twin(inc, verification, velocity)

elif page == "Source Verification":
    st.markdown('<div class="section-title">Source Verification</div>', unsafe_allow_html=True)
    st.caption("Four independent checks per article: Claim (what's actually being asserted), "
               "Source (reliability score), Entity (is the client actually named), and Contradiction "
               "(does it conflict with prior reporting on this incident). The Contradiction check only "
               "runs from the second article on an incident onward.")
    if not escalated:
        st.info("No verified events yet.")
    for r in escalated[:8]:
        v = r.get("verification") or {}
        agents = v.get("agents") or {}
        inc = r["incident"]
        color = {"confirmed": SAFE, "partially_verified": MEDIUM, "contradicted": CRITICAL,
                 "single_source": SUBTEXT}.get(v.get("status"), SUBTEXT)
        st.markdown(f"""
        <div class="cp-card">
            <span class="cp-meta" style="font-weight:700; color:{TEXT};">{inc['id']} \u2014 {r['article']['headline']}</span><br>
            <span class="verify-pill v-{v.get('status','single_source')}" style="margin-top:6px; display:inline-block;">
                {v.get('status','single_source').upper().replace('_',' ')}</span>
            <span class="cp-meta"> \u00b7 Evidence confidence {inc['confidence']}% from {inc['distinct_sources']} independent source(s)</span>
            <div class="cp-p" style="margin-top:6px; color:{color};">{v.get('note','')}</div>
        </div>
        """, unsafe_allow_html=True)
        if agents:
            claim, source, entity, contradiction = (agents.get("claim", {}), agents.get("source", {}),
                                                      agents.get("entity", {}), agents.get("contradiction", {}))
            a1, a2, a3, a4 = st.columns(4)
            source_color = {"high": SAFE, "medium": MEDIUM, "low": CRITICAL}.get(source.get("tier"), SUBTEXT)
            entity_ok = entity.get("client_confirmed")
            contra_flag = contradiction.get("contradiction")
            with a1:
                st.markdown(f'<div class="kpi-card"><div class="kpi-label">CLAIM AGENT</div>'
                             f'<div class="cp-p" style="font-size:12px; margin-top:4px;">{claim.get("claim","--")}</div>'
                             f'<div class="cp-meta">specificity: {claim.get("specificity","--")}</div></div>', unsafe_allow_html=True)
            with a2:
                st.markdown(f'<div class="kpi-card"><div class="kpi-label">SOURCE AGENT</div>'
                             f'<div class="kpi-num" style="font-size:18px; color:{source_color};">{source.get("reliability_score","--")}%</div>'
                             f'<div class="cp-meta">tier: {source.get("tier","--")}</div></div>', unsafe_allow_html=True)
            with a3:
                st.markdown(f'<div class="kpi-card"><div class="kpi-label">ENTITY AGENT</div>'
                             f'<div class="kpi-num" style="font-size:16px; color:{SAFE if entity_ok else HIGH};">'
                             f'{"\u2713 CONFIRMED" if entity_ok else "\u26a0 UNCERTAIN"}</div>'
                             f'<div class="cp-meta">regulator mentioned: {entity.get("regulator_mentioned", False)}</div></div>', unsafe_allow_html=True)
            with a4:
                st.markdown(f'<div class="kpi-card"><div class="kpi-label">CONTRADICTION AGENT</div>'
                             f'<div class="kpi-num" style="font-size:16px; color:{CRITICAL if contra_flag else SAFE};">'
                             f'{"\u26a0 CONFLICT" if contra_flag else "\u2713 NONE"}</div>'
                             f'<div class="cp-meta">{contradiction.get("explanation","")[:40]}</div></div>', unsafe_allow_html=True)
        st.write("")

elif page == "Prediction Engine":
    st.markdown('<div class="section-title">Prediction Engine</div>', unsafe_allow_html=True)
    st.caption("Deterministic, explainable forecast \u2014 combines narrative velocity, whether the score "
               "is still climbing, source confidence, and client tier. Not a black box.")
    if not escalated:
        st.info("No predictions yet.")
    for r in escalated[:8]:
        p = r.get("prediction") or {}
        band_color = BAND_COLOR.get(p.get("predicted_band"), SUBTEXT)
        st.markdown(f"""
        <div class="cp-card">
            <span class="cp-meta" style="font-weight:700; color:{TEXT};">{r['incident']['id']} \u2014 {r['article']['headline']}</span>
            <div class="cp-p" style="margin-top:8px;">{p.get('probability',0)}% chance of reaching
                <b style="color:{band_color};">{p.get('predicted_band','--')}</b> within <b>{p.get('eta','--')}</b></div>
            <div class="predict-gauge-wrap"><div class="predict-gauge-fill" style="width:{p.get('probability',0)}%;"></div></div>
        </div>
        """, unsafe_allow_html=True)

elif page == "Knowledge Graph":
    st.markdown('<div class="section-title">Crisis Knowledge Graph</div>', unsafe_allow_html=True)
    st.caption(f"Relationships for {pipeline.CLIENT_KEYWORD} \u2014 regulator, competitors, and active incidents.")
    profile = pipeline.get_client_profile(pipeline.CLIENT_KEYWORD)
    render_knowledge_graph(profile, pipeline.CLIENT_KEYWORD, incidents)

elif page == "Response Center":
    st.markdown('<div class="section-title">Response Center</div>', unsafe_allow_html=True)
    render_response_center(escalated)

elif page == "SLA Monitor":
    st.markdown('<div class="section-title">SLA Monitor \u2014 Target &lt;120s</div>', unsafe_allow_html=True)
    render_sla_monitor(results, tier1_mentions)

elif page == "Incident History":
    st.markdown('<div class="section-title">Incident History</div>', unsafe_allow_html=True)
    if incidents:
        bands_available = sorted({i["band"] for i in incidents})
        band_filter = st.multiselect("Filter by risk band", bands_available, default=bands_available)
        client_filter = st.text_input("Filter by client (contains)", "")
        filtered = [i for i in incidents if i["band"] in band_filter
                    and client_filter.lower() in i["client"].lower()]
        render_incident_history(filtered)
    else:
        render_incident_history(incidents)

elif page == "Crisis Simulator":
    render_simulator()

elif page == "System Health":
    st.markdown('<div class="section-title">System Health</div>', unsafe_allow_html=True)
    render_system_health(last_error, queue)

if not HAS_AUTOREFRESH:
    st.caption("Install `streamlit-autorefresh` for automatic live updates.")
