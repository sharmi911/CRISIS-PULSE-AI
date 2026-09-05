"""
Web dashboard for CrisisPulse AI — event-driven ingestion version
================================================================================
Ingestion is now push-based: articles arrive via POST /webhook/article (or
the NewsAPI fallback poller, for the one source that has no webhook option)
and are queued in event_store.py. A background worker claims and processes
them within ~1 second, not on a fixed 2-minute cycle. This page just reads
the results.

Run:
    pip install -r requirements.txt --break-system-packages
    export GROQ_API_KEY=your_key_here
    export NEWSAPI_KEY=your_key_here
    export WEBHOOK_SECRET=your_secret        # optional — protects POST /webhook/article
    export EMAIL_FROM=you@gmail.com          # optional, for real email alerts
    export EMAIL_PASSWORD=your_app_password  # optional — Gmail App Password
    export EMAIL_TO=recipient@example.com    # optional
    python app.py

Then open: http://localhost:5000
Push a live event for a demo:
    curl -X POST http://localhost:5000/webhook/article \\
         -H "Content-Type: application/json" \\
         -d '{"headline": "RBI opens audit into Paytm", "source": "Reuters"}'
"""

import os
import threading
from flask import Flask, render_template_string
import event_store
import pipeline
import worker
import webhook_server

app = Flask(__name__)

AUTO_START_WORKER = os.environ.get("AUTO_START_WORKER", "true").lower() != "false"
_ingestion_started = False
_ingestion_lock = threading.Lock()


def ensure_ingestion_started():
    """Idempotent — safe to call from every request. Starts the in-process
    worker + fallback poller once. If you're running `python worker.py` as a
    separate process instead, set AUTO_START_WORKER=false here to avoid
    double-claiming (harmless either way since claims are atomic, but no
    reason to run two workers for a single-process demo)."""
    global _ingestion_started
    if _ingestion_started or not AUTO_START_WORKER:
        return
    with _ingestion_lock:
        if not _ingestion_started:
            worker.start_background_ingestion(with_fallback_poller=True)
            _ingestion_started = True


# Reuse the same webhook handler as webhook_server.py — one route definition,
# usable from either process.
app.add_url_rule("/webhook/article", view_func=webhook_server.handle_webhook_article, methods=["POST"])


PAGE_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Crisis Response Pipeline</title>
    <meta charset="utf-8">
    <meta http-equiv="refresh" content="15">
    <style>
        body { font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background: #0f1115; color: #e8e8e8; margin: 0; padding: 24px; }
        h1 { font-size: 22px; margin-bottom: 4px; }
        .sub { color: #9aa0a6; margin-bottom: 24px; font-size: 14px; }
        .card { background: #1a1d24; border: 1px solid #2a2e37; border-radius: 10px; padding: 18px 20px; margin-bottom: 16px; }
        .headline { font-size: 16px; font-weight: 600; margin-bottom: 6px; }
        .source { color: #9aa0a6; font-size: 13px; margin-bottom: 10px; }
        .badge { display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; margin-right: 6px; }
        .risk-high { background: #4a1e1e; color: #ff6b6b; }
        .risk-medium { background: #4a3a1e; color: #ffb86b; }
        .risk-low { background: #1e3a24; color: #6bff8f; }
        .alert-box { background: #2a1414; border: 1px solid #5a2323; border-radius: 8px; padding: 12px 16px; margin-top: 12px; }
        .alert-title { color: #ff6b6b; font-weight: 700; margin-bottom: 8px; }
        .row { font-size: 14px; margin-bottom: 4px; }
        .label { color: #9aa0a6; display: inline-block; width: 140px; }
        .sla { margin-top: 10px; font-size: 13px; color: #9aa0a6; }
        .met { color: #6bff8f; }
        .breach { color: #ff6b6b; }
        .no-alert { color: #6c7280; font-size: 13px; margin-top: 8px; }
        .empty { color: #9aa0a6; padding: 40px; text-align: center; }
        .status { color: #6c7280; font-size: 12px; margin-bottom: 20px; }
        .tier1 { background: #3a2a10; color: #ffcb6b; }
        .score-wrap { display: flex; align-items: center; gap: 10px; margin: 10px 0; }
        .score-num { font-size: 22px; font-weight: 800; }
        .band-CRITICAL { color: #ff5555; }
        .band-HIGH { color: #ff9b5c; }
        .band-MEDIUM { color: #ffd75c; }
        .band-LOW { color: #6bff8f; }
        .evidence { display: flex; flex-wrap: wrap; gap: 6px; margin: 6px 0 10px; }
        .chip { background: #22262f; border: 1px solid #2f3440; border-radius: 6px; padding: 2px 8px; font-size: 11px; color: #b7bcc6; }
        .velocity-accelerating { color: #ff8f5c; }
        .velocity-cooling { color: #6bff8f; }
        .velocity-steady, .velocity-quiet { color: #9aa0a6; }
    </style>
</head>
<body>
    <h1>🚨 CrisisPulse AI — Detect. Predict. Respond.</h1>
    <div class="sub">Event-driven ingestion for "{{ keyword }}" — webhook push + NewsAPI fallback + Groq</div>
    <div class="status">Queue: {{ queue.pending }} pending · {{ queue.processing }} processing · {{ queue.done }} done
        {% if queue.error %}· {{ queue.error }} error(s){% endif %}
        · page auto-refreshes every 15s · POST /webhook/article to push a live event</div>

    {% if stats.processed %}
    <div class="card" style="display:flex; gap:28px; flex-wrap:wrap;">
        <div><div class="label" style="width:auto;">Articles processed</div><div class="score-num" style="font-size:18px;">{{ stats.processed }}</div></div>
        <div><div class="label" style="width:auto;">Tier-1 articles</div><div class="score-num" style="font-size:18px;">{{ stats.tier1 }}</div></div>
        <div><div class="label" style="width:auto;">Avg response</div><div class="score-num" style="font-size:18px;">{{ stats.avg }}s</div></div>
        <div><div class="label" style="width:auto;">95th percentile</div><div class="score-num" style="font-size:18px;">{{ stats.p95 }}s</div></div>
        <div><div class="label" style="width:auto;">SLA breaches</div><div class="score-num band-{{ 'CRITICAL' if stats.breaches else 'LOW' }}" style="font-size:18px;">{{ stats.breaches }}</div></div>
    </div>
    {% endif %}

    {% if not results %}
        <div class="empty">Waiting for the first poll to complete (runs every 2 minutes)...</div>
    {% endif %}

    {% for r in results %}
    <div class="card">
        <div class="headline">{{ r.article.headline }}</div>
        <div class="source">
            {{ r.article.source }} · ingested {{ r.ingest_ts }}
            {% if r.profile and r.profile.tier == "TIER1" %}<span class="badge tier1">TIER-1</span>{% endif %}
        </div>

        {% if r.triage %}
        <span class="badge risk-{{ r.triage.risk_level }}">{{ r.triage.risk_level|upper }} RISK</span>
        <span class="badge" style="background:#2a2e37;color:#c8ccd4;">{{ r.triage.topic }}</span>
        <span class="badge" style="background:#2a2e37;color:#c8ccd4;">{{ r.triage.sentiment }}</span>
        {% if r.velocity %}<span class="badge velocity-{{ r.velocity.trend }}">↝ {{ r.velocity.trend }}</span>{% endif %}
        {% endif %}

        {% if r.crisis %}
        <div class="score-wrap">
            <span class="score-num band-{{ r.crisis.band }}">{{ r.crisis.score }}/100</span>
            <span class="band-{{ r.crisis.band }}">{{ r.crisis.band }}</span>
        </div>
        <div class="evidence">
            {% for k, v in r.crisis.breakdown.items() %}
                {% if v %}<span class="chip">+{{ v }} {{ k.replace('_', ' ') }}</span>{% endif %}
            {% endfor %}
        </div>
        {% endif %}

        {% if r.escalated and r.insight %}
        <div class="alert-box">
            <div class="alert-title">🚨 ALERT — email sent</div>
            <div class="row"><span class="label">What happened:</span>{{ r.insight.what_happened }}</div>
            <div class="row"><span class="label">Why it matters:</span>{{ r.insight.why_it_matters }}</div>
            {% if r.verification %}
            <div class="row"><span class="label">Verification:</span>{{ r.verification.status }} — {{ r.verification.note }}</div>
            {% endif %}
            {% if r.prediction %}
            <div class="row"><span class="label">Escalation prediction:</span>{{ r.prediction.probability }}% -> {{ r.prediction.predicted_band }} (ETA {{ r.prediction.eta }})</div>
            {% endif %}
            {% if r.insight.response_plan %}
            <div class="row"><span class="label">0-5 min:</span>{{ r.insight.response_plan.immediate }}</div>
            <div class="row"><span class="label">5-15 min:</span>{{ r.insight.response_plan.next_15min }}</div>
            <div class="row"><span class="label">Next hour:</span>{{ r.insight.response_plan.next_1hr }}</div>
            <div class="row"><span class="label">Executive:</span>{{ r.insight.response_plan.executive }}</div>
            {% endif %}
        </div>
        {% else %}
        <div class="no-alert">Low priority — no alert triggered.</div>
        {% endif %}

        {% if r.total_seconds is not none %}
        <div class="sla">
            ⏱ Ingest-to-alert: {{ r.total_seconds }}s (SLA target: &lt;120s)
            {% if r.sla_met %}<span class="met">✅ MET</span>{% elif r.sla_met == false %}<span class="breach">❌ BREACHED</span>{% endif %}
        </div>
        {% endif %}
    </div>
    {% endfor %}
</body>
</html>
"""


def compute_stats(results):
    timed = [r["total_seconds"] for r in results if r.get("total_seconds") is not None]
    tier1 = sum(1 for r in results if r.get("profile", {}).get("tier") == "TIER1")
    breaches = sum(1 for r in results if r.get("sla_met") is False)
    if timed:
        timed_sorted = sorted(timed)
        avg = round(sum(timed_sorted) / len(timed_sorted), 1)
        p95 = round(timed_sorted[int(0.95 * (len(timed_sorted) - 1))], 1)
    else:
        avg = p95 = 0
    return {"processed": len(results), "tier1": tier1, "avg": avg, "p95": p95, "breaches": breaches}


@app.route("/")
def dashboard():
    ensure_ingestion_started()
    results = event_store.get_recent_results(limit=30)
    stats = compute_stats(results)
    queue = event_store.queue_counts()
    return render_template_string(
        PAGE_TEMPLATE, results=results, keyword=pipeline.CLIENT_KEYWORD, queue=queue, stats=stats
    )


if __name__ == "__main__":
    if not os.environ.get("GROQ_API_KEY"):
        raise SystemExit("Set GROQ_API_KEY env var first (get one free at console.groq.com).")
    if not os.environ.get("NEWSAPI_KEY"):
        raise SystemExit("Set NEWSAPI_KEY env var first (get one free at newsapi.org).")
    if not (os.environ.get("EMAIL_FROM") and os.environ.get("EMAIL_PASSWORD") and os.environ.get("EMAIL_TO")):
        print("NOTE: EMAIL_FROM / EMAIL_PASSWORD / EMAIL_TO not fully set — alerts will print only, no real email sent.\n")
    if not os.environ.get("WEBHOOK_SECRET"):
        print("NOTE: WEBHOOK_SECRET not set — POST /webhook/article is unauthenticated on this instance.\n")

    event_store.init_db()
    ensure_ingestion_started()

    print(f"Event-driven ingestion running (worker + NewsAPI fallback poller). Dashboard: http://localhost:5000")
    print(f"Push a live event: curl -X POST http://localhost:5000/webhook/article "
          f"-H 'Content-Type: application/json' -d '{{\"headline\": \"...\", \"source\": \"...\"}}'\n")
    app.run(host="0.0.0.0", port=5000, debug=False)
