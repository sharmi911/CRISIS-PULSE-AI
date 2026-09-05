"""
2-Minute Crisis Response Pipeline — Hackathon Demo
====================================================
Simulates: streaming ingestion -> real-time triage -> 2-min insight brief -> alert
Every stage prints a timestamp so you can SHOW the sub-2-minute latency live.

Run:
    pip install groq requests --break-system-packages
    export GROQ_API_KEY=your_key_here          # https://console.groq.com -> API Keys (free)
    export NEWSAPI_KEY=your_newsapi_key_here   # https://newsapi.org -> Get API Key
    python pipeline.py

Ingestion is now REAL: pulls live articles from NewsAPI for CLIENT_KEYWORD.
This replaces batch/manual pulling with an actual streaming-style API call —
the exact pain point ("batching, polling every 15-30 min") from the problem statement.

Triage + insight generation run on Groq's free, fast LLM inference API.
"""

import os
import re
import time
import json
import smtplib
import requests
from email.mime.text import MIMEText
from datetime import datetime
from groq import Groq

# Optional: load a .env file if present (falls back silently to exported env vars)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

_groq_client = None


def get_groq_client():
    """Lazy singleton — only requires GROQ_API_KEY when an actual LLM call is made,
    so the Crisis Simulator (which makes no LLM calls) works with zero API keys set."""
    global _groq_client
    if _groq_client is None:
        _groq_client = Groq()  # reads GROQ_API_KEY from env
    return _groq_client


GROQ_MODEL = "openai/gpt-oss-20b"  # current Groq production model; low reasoning_effort keeps it fast
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY")
CLIENT_KEYWORD = "Paytm"  # swap for whichever client/brand you're tracking in the demo

# ---------------------------------------------------------------------------
# Client Intelligence Profiles
# ---------------------------------------------------------------------------
# Lets the system tell "just another PayU article" apart from "Tier-1 client +
# regulator-related event". Add entries for whichever brands you demo with;
# anything not listed falls back to DEFAULT_PROFILE.
CLIENT_PROFILES = {
    "paytm": {"tier": "TIER1", "industry": "FinTech", "regulator": "RBI",
              "competitors": ["PhonePe", "Google Pay", "Amazon Pay"]},
    "payu": {"tier": "TIER1", "industry": "FinTech", "regulator": "RBI",
             "competitors": ["PhonePe", "Razorpay", "Cashfree"]},
}
DEFAULT_PROFILE = {"tier": "TIER2", "industry": "Unknown", "regulator": None, "competitors": []}


def get_client_profile(keyword):
    return CLIENT_PROFILES.get(keyword.strip().lower(), DEFAULT_PROFILE)


# ---------------------------------------------------------------------------
# Narrative Velocity — how fast the story is spreading, tracked per client
# ---------------------------------------------------------------------------
# In-memory log of ingestion timestamps per client keyword. Deliberately simple
# (no DB) since this is a demo process; app.py's poll loop shares this module
# so the log accumulates across polls within one run.
_MENTION_LOG = {}  # keyword -> list[float] (unix timestamps)
_VELOCITY_WINDOW_SEC = 15 * 60  # compare last 15 min vs the 15 min before that


def record_mention(keyword, ts=None):
    ts = ts if ts is not None else time.time()
    _MENTION_LOG.setdefault(keyword, []).append(ts)


def compute_velocity(keyword):
    """Returns mentions in the current window, the prior window, and a trend label."""
    now = time.time()
    log = _MENTION_LOG.get(keyword, [])
    recent = [t for t in log if now - t <= _VELOCITY_WINDOW_SEC]
    prior = [t for t in log if _VELOCITY_WINDOW_SEC < now - t <= 2 * _VELOCITY_WINDOW_SEC]
    recent_n, prior_n = len(recent), len(prior)

    if prior_n == 0 and recent_n == 0:
        trend = "quiet"
    elif prior_n == 0:
        trend = "accelerating" if recent_n >= 2 else "steady"
    else:
        change = (recent_n - prior_n) / prior_n
        if change >= 0.5:
            trend = "accelerating"
        elif change <= -0.5:
            trend = "cooling"
        else:
            trend = "steady"

    return {"recent_count": recent_n, "prior_count": prior_n, "trend": trend}


# ---------------------------------------------------------------------------
# Crisis Score — deterministic, explainable 0-100 score computed from the
# triage agent's output (not another LLM call, so it's instant and auditable)
# ---------------------------------------------------------------------------
def compute_crisis_score(triage, profile, velocity):
    breakdown = {}

    sentiment_pts = {"negative": 40, "neutral": 15, "positive": 0}.get(triage.get("sentiment"), 15)
    breakdown["sentiment"] = sentiment_pts

    risk_pts = {"high": 30, "medium": 15, "low": 5}.get(triage.get("risk_level"), 5)
    breakdown["risk_level"] = risk_pts

    topic_pts = {"regulatory": 20, "financial": 10}.get(triage.get("topic"), 0)
    breakdown["topic"] = topic_pts

    competitor_pts = {"major": 10, "minor": 5, "none": 0}.get(triage.get("competitor_impact"), 0)
    breakdown["competitor_impact"] = competitor_pts

    tier_pts = 10 if profile.get("tier") == "TIER1" else 0
    breakdown["tier1_bonus"] = tier_pts

    velocity_pts = {"accelerating": 10, "steady": 3, "cooling": 0, "quiet": 0}.get(velocity.get("trend"), 0)
    breakdown["narrative_velocity"] = velocity_pts

    score = min(100, sum(breakdown.values()))

    if score >= 80:
        band = "CRITICAL"
    elif score >= 60:
        band = "HIGH"
    elif score >= 35:
        band = "MEDIUM"
    else:
        band = "LOW"

    return {"score": score, "band": band, "breakdown": breakdown}


# ---------------------------------------------------------------------------
# Source Reliability + Confidence — stops one shaky article from triggering a
# false escalation. Reliability is a static reputation table (swap in a real
# source-scoring service later); confidence rewards independent corroboration.
# ---------------------------------------------------------------------------
SOURCE_RELIABILITY = {
    "reuters": 96, "associated press": 96, "bloomberg": 94, "the wall street journal": 93,
    "financial times": 93, "bbc news": 92, "the economic times": 85, "livemint": 84,
    "business standard": 84, "the hindu": 86, "moneycontrol": 80, "cnbc": 88,
    "the times of india": 78, "hindustan times": 78, "ndtv": 79, "reuters india": 95,
}
DEFAULT_SOURCE_RELIABILITY = 45  # unrecognized outlet / blog


def source_reliability(source_name):
    return SOURCE_RELIABILITY.get((source_name or "").strip().lower(), DEFAULT_SOURCE_RELIABILITY)


def compute_confidence(sources):
    """sources: iterable of distinct source names backing one incident."""
    distinct = sorted(set(sources))
    if not distinct:
        return {"confidence": 0, "distinct_sources": 0}
    avg_reliability = sum(source_reliability(s) for s in distinct) / len(distinct)
    # Corroboration bonus: each extra independent source adds confidence,
    # with diminishing returns, capped so a single source never exceeds ~70.
    corroboration_bonus = min(25, (len(distinct) - 1) * 8)
    confidence = min(99, round(avg_reliability * 0.75 + corroboration_bonus))
    return {"confidence": confidence, "distinct_sources": len(distinct)}


# ---------------------------------------------------------------------------
# Crisis Digital Twin — one evolving incident per (client, topic) instead of
# treating every article as its own isolated alert. New escalated articles
# within the window update the SAME incident rather than spawning a new one,
# which is what keeps the war room from drowning in duplicate notifications.
# ---------------------------------------------------------------------------
_INCIDENTS = {}       # (client, topic) -> incident dict
_INCIDENT_WINDOW_SEC = 3 * 60 * 60   # articles within 3h of the last update join the same incident
_incident_counter = [1000]


def update_incident(article, triage, crisis, claim=None):
    key = (article["client"], triage.get("topic"))
    now = time.time()
    incident = _INCIDENTS.get(key)

    if incident is None or now - incident["last_updated"] > _INCIDENT_WINDOW_SEC:
        _incident_counter[0] += 1
        incident = {
            "id": f"CR-{_incident_counter[0]}",
            "client": article["client"],
            "topic": triage.get("topic"),
            "first_seen": now,
            "articles": [],
        }
        _INCIDENTS[key] = incident

    incident["last_updated"] = now
    incident["articles"].append({"headline": article["headline"], "source": article["source"],
                                  "claim": claim or article["headline"]})
    incident["article_count"] = len(incident["articles"])
    incident.setdefault("score_history", []).append({"ts": now, "score": crisis["score"], "band": crisis["band"]})
    if crisis["score"] >= incident.get("max_score", 0):
        incident["max_score"] = crisis["score"]
        incident["band"] = crisis["band"]
    incident.update(compute_confidence(a["source"] for a in incident["articles"]))
    return incident


def get_incidents():
    """Public accessor for the UI — newest-updated first."""
    return sorted(_INCIDENTS.values(), key=lambda i: i["last_updated"], reverse=True)


# ---------------------------------------------------------------------------
# Digital Twin state machine — DISCOVERY -> TRIAGED -> VERIFIED -> ESCALATING
# -> CRITICAL. Purely a display label derived from data already computed
# above; no separate state store needed.
# ---------------------------------------------------------------------------
TWIN_STATES = ["DISCOVERY", "TRIAGED", "VERIFIED", "ESCALATING", "CRITICAL"]


def incident_state(incident, verification, velocity):
    if incident["band"] == "CRITICAL":
        return "CRITICAL"
    if velocity.get("trend") == "accelerating" and incident["article_count"] > 1:
        return "ESCALATING"
    if verification and verification.get("status") == "confirmed":
        return "VERIFIED"
    if incident["article_count"] >= 1:
        return "TRIAGED"
    return "DISCOVERY"


# ---------------------------------------------------------------------------
# Multi-Agent Verification — four agents with distinct responsibilities,
# instead of one LLM call doing everything:
#
#   Claim Agent         (LLM)          extracts the article's core factual
#                                       claim + how specific it is
#   Source Agent        (deterministic) scores this source's reliability
#   Entity Agent        (deterministic) checks the client/regulator are
#                                       actually named in the article
#   Contradiction Agent (LLM, only from the 2nd article on) compares the new
#                                       claim against every prior claim on
#                                       this incident for direct conflicts
#
# Only the Claim and Contradiction agents cost an LLM call, and Contradiction
# only runs from the second article onward — so a first-time article costs
# exactly one extra call versus the old single-call design, not four.
# ---------------------------------------------------------------------------
CLAIM_SYSTEM = """You are a claim-extraction agent. Given a news article, extract the single core \
factual claim it makes, as one short sentence. Output ONLY a JSON object, no markdown, no extra text:
{"claim": "short factual claim in one sentence", "specificity": "specific"}
Valid specificity values: specific (names a concrete action, number, or decision), \
vague (general, speculative, or opinion-based)."""

CONTRADICTION_SYSTEM = """You are a contradiction-detection agent. Compare a NEW claim against a list \
of PRIOR claims about the same incident. Output ONLY a JSON object, no markdown, no extra text:
{"contradiction": false, "explanation": "one short sentence"}
Set contradiction true ONLY if the new claim factually conflicts with a prior claim — adding detail, \
narrowing scope, or reporting a later development is NOT a contradiction."""


def run_claim_agent(article):
    try:
        return call_agent(CLAIM_SYSTEM, f"Headline: {article['headline']}\nBody: {article.get('body','')}")
    except Exception:
        return {"claim": article["headline"], "specificity": "vague"}


def run_source_agent(article):
    score = source_reliability(article["source"])
    tier = "high" if score >= 80 else "medium" if score >= 55 else "low"
    return {"reliability_score": score, "tier": tier}


def run_entity_agent(article, profile):
    text = f"{article['headline']} {article.get('body') or ''}".lower()
    client_confirmed = article["client"].lower() in text
    regulator = (profile or {}).get("regulator")
    regulator_mentioned = bool(regulator) and regulator.lower() in text
    return {"client_confirmed": client_confirmed, "regulator_mentioned": regulator_mentioned}


def run_contradiction_agent(new_claim, prior_claims):
    if not prior_claims:
        return {"contradiction": False, "explanation": "No prior claims to compare against yet."}
    prior_text = "\n".join(f"- {c}" for c in prior_claims)
    try:
        return call_agent(
            CONTRADICTION_SYSTEM,
            f"NEW claim: {new_claim}\n\nPRIOR claims on this incident:\n{prior_text}",
        )
    except Exception:
        return {"contradiction": False, "explanation": "Contradiction check failed to parse — treat with caution."}


def aggregate_verification(claim, source, entity, contradiction, article_count):
    if contradiction.get("contradiction"):
        return "contradicted", contradiction.get("explanation", "Conflicts with prior reporting.")
    if article_count <= 1:
        return "single_source", "Only one source so far — treat as unverified."
    if source["tier"] == "low" and claim.get("specificity") == "specific":
        return "partially_verified", "A specific claim from a lower-reliability source — awaiting corroboration."
    if not entity.get("client_confirmed"):
        return "partially_verified", "Article doesn't clearly name the client — entity match is uncertain."
    return "confirmed", f"Corroborated by {article_count} article(s); no contradictions detected across sources."


def run_multi_agent_verification(article, incident, profile, claim):
    """Orchestrates Source, Entity, and Contradiction agents against an
    already-computed claim and an already-updated incident (so incident
    ["articles"][:-1] correctly excludes the current article). Returns a
    result that's backward compatible with the old single-agent shape
    (top-level "status"/"note") plus a new "agents" breakdown for the UI."""
    source = run_source_agent(article)
    entity = run_entity_agent(article, profile)
    prior_claims = [a.get("claim", a["headline"]) for a in incident["articles"][:-1]]
    contradiction = run_contradiction_agent(claim.get("claim", article["headline"]), prior_claims)
    status, note = aggregate_verification(claim, source, entity, contradiction, incident["article_count"])
    return {
        "status": status,
        "note": note,
        "agents": {"claim": claim, "source": source, "entity": entity, "contradiction": contradiction},
    }



# ---------------------------------------------------------------------------
# Predictive Crisis Escalation — deterministic, explainable estimate of
# whether this incident is likely to get WORSE, not just how bad it is now.
# ---------------------------------------------------------------------------
def compute_escalation_prediction(crisis, incident, velocity, profile, verification, prev_incident_max):
    probability = 0

    velocity_pts = {"accelerating": 40, "steady": 15, "cooling": 0, "quiet": 0}.get(velocity.get("trend"), 0)
    probability += velocity_pts

    if crisis["score"] > prev_incident_max:
        probability += 30  # this article pushed the incident to a new high
    elif crisis["score"] == prev_incident_max:
        probability += 10

    probability += round(incident["confidence"] / 100 * 20)

    if profile.get("tier") == "TIER1":
        probability += 10

    if verification.get("status") == "contradicted":
        probability -= 25  # unverified/conflicting claims shouldn't drive prediction up
    elif verification.get("status") == "confirmed" and incident["article_count"] > 1:
        probability += 5  # independent corroboration strengthens the prediction

    probability = max(0, min(99, probability))

    bands = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    current_idx = bands.index(crisis["band"])
    if probability >= 70 and current_idx < len(bands) - 1:
        predicted_band = bands[current_idx + 1]
    else:
        predicted_band = crisis["band"]

    if probability >= 75:
        eta = "8-15 min"
    elif probability >= 50:
        eta = "15-40 min"
    elif probability >= 25:
        eta = "40-90 min"
    else:
        eta = "not expected soon"

    return {"probability": probability, "predicted_band": predicted_band, "eta": eta}

# Email alert config (Gmail SMTP) — all optional; if unset, alerts just print to console
EMAIL_FROM = os.environ.get("EMAIL_FROM")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")  # Gmail App Password, not your normal password
EMAIL_TO = os.environ.get("EMAIL_TO")


def fetch_live_articles(keyword, page_size=5):
    """Real 3rd-party API call — pulls the latest live articles mentioning the client."""
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": keyword,
        "sortBy": "publishedAt",
        "pageSize": page_size,
        "language": "en",
        "apiKey": NEWSAPI_KEY,
    }
    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code != 200:
        print(f"\nNewsAPI error {resp.status_code}: {resp.text}\n")
    resp.raise_for_status()
    data = resp.json()
    articles = []
    for i, a in enumerate(data.get("articles", [])):
        articles.append({
            "id": f"art_{i:03d}",
            "client": keyword,
            "headline": a.get("title") or "",
            "body": (a.get("description") or "") + " " + (a.get("content") or ""),
            "source": (a.get("source") or {}).get("name", "Unknown"),
        })
    return articles

TRIAGE_SYSTEM = """You are a media-intelligence triage agent. Given a news article about a client, \
output ONLY a single JSON object and nothing else — no explanation, no markdown fences, no extra text before or after. \
Use exactly these keys:
{"client_mentioned": true, "risk_level": "high", "topic": "regulatory", "sentiment": "negative", "competitor_impact": "none"}
Valid values — risk_level: high/medium/low. topic: regulatory/sponsorship/financial/other. sentiment: negative/neutral/positive. competitor_impact: none/minor/major."""

INSIGHT_SYSTEM = """You are a crisis-insight agent for a corporate war-room. Given a high-risk article, \
output ONLY a single JSON object and nothing else — no explanation, no markdown fences, no extra text before or after. \
Use exactly these keys, each value a short single sentence, except response_plan which is an object:
{"what_happened": "...", "why_it_matters": "...", "risk_score": "7/10 - regulatory scrutiny", \
"response_plan": {"immediate": "action in next 5 min", "next_15min": "action in next 15 min", \
"next_1hr": "action in next hour", "executive": "who to notify and what to tell them"}}"""


def now_ms():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def call_agent(system_prompt, user_content):
    resp = get_groq_client().chat.completions.create(
        model=GROQ_MODEL,
        max_tokens=800,
        temperature=0.2,
        reasoning_effort="low",
        reasoning_format="hidden",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
    )
    text = resp.choices[0].message.content.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Fallback: pull out the first {...} block in case the model added extra text
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        print(f"\n--- RAW MODEL OUTPUT (could not parse as JSON) ---\n{text}\n---\n")
        raise


def send_email_alert(article, triage, insight, crisis, profile, incident, verification, prediction):
    """Sends a real email via Gmail SMTP. Requires EMAIL_FROM, EMAIL_PASSWORD, EMAIL_TO env vars."""
    if not (EMAIL_FROM and EMAIL_PASSWORD and EMAIL_TO):
        print("[EMAIL]   Skipped — EMAIL_FROM / EMAIL_PASSWORD / EMAIL_TO not set.")
        return False

    subject = f"🚨 {incident['id']}: {article['client']} — {crisis['band']} ({crisis['score']}/100)"
    evidence = "\n".join(f"  + {k.replace('_', ' ')}: {v}" for k, v in crisis["breakdown"].items() if v)
    plan = insight.get("response_plan", {})
    body = (
        f"Incident: {incident['id']} ({incident['article_count']} article(s), "
        f"confidence {incident['confidence']}% from {incident['distinct_sources']} source(s))\n"
        f"Verification: {verification['status']} — {verification.get('note', '')}\n"
        f"Escalation prediction: {prediction['probability']}% -> {prediction['predicted_band']} (ETA {prediction['eta']})\n"
        f"Client: {article['client']} ({profile['tier']}, {profile['industry']})\n"
        f"Headline: {article['headline']}\n"
        f"Source: {article['source']} (reliability {source_reliability(article['source'])}%)\n"
        f"Risk level: {triage['risk_level']} | Topic: {triage['topic']}\n\n"
        f"Crisis Score: {crisis['score']}/100 — {crisis['band']}\n"
        f"Evidence:\n{evidence}\n\n"
        f"What happened: {insight['what_happened']}\n"
        f"Why it matters: {insight['why_it_matters']}\n\n"
        f"Response plan:\n"
        f"  0-5 min:   {plan.get('immediate', '')}\n"
        f"  5-15 min:  {plan.get('next_15min', '')}\n"
        f"  Next hour: {plan.get('next_1hr', '')}\n"
        f"  Executive: {plan.get('executive', '')}\n"
    )
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())
        print(f"[EMAIL]   Sent to {EMAIL_TO}")
        return True
    except Exception as e:
        print(f"[EMAIL]   Failed to send: {e}")
        return False


def send_alert(article, triage, insight, crisis, profile, incident, verification, prediction):
    print("\n" + "=" * 60)
    print(f"🚨 ALERT — {now_ms()}  [{incident['id']}]")
    print(f"Client: {article['client']} ({profile['tier']}) | Crisis Score: {crisis['score']}/100 ({crisis['band']}) "
          f"| Confidence: {incident['confidence']}% ({incident['distinct_sources']} sources)")
    print(f"Verification: {verification['status']} | Escalation: {prediction['probability']}% -> {prediction['predicted_band']} (ETA {prediction['eta']})")
    print(f"Evidence: {crisis['breakdown']}")
    print(f"What happened:     {insight['what_happened']}")
    print(f"Why it matters:    {insight['why_it_matters']}")
    print(f"Response plan:     {insight.get('response_plan', {})}")
    print("=" * 60)
    send_email_alert(article, triage, insight, crisis, profile, incident, verification, prediction)


def process_article(article, received_at=None):
    # received_at is set by the event-driven worker to the moment the webhook
    # accepted the article (or the fallback poller enqueued it). Anchoring
    # ingest_time there — instead of "now" — means total_seconds/stages
    # honestly reflect the FULL event-driven latency, including any time the
    # event spent waiting in the queue before a worker picked it up.
    ingest_time = received_at if received_at is not None else time.time()
    print(f"\n[INGEST]  {now_ms()}  '{article['headline']}' from {article['source']}")

    profile = get_client_profile(article["client"])
    record_mention(article["client"], ingest_time)
    velocity = compute_velocity(article["client"])

    result = {
        "article": article,
        "ingest_ts": now_ms(),
        "profile": profile,
        "velocity": velocity,
        "triage": None,
        "crisis": None,
        "incident": None,
        "verification": None,
        "prediction": None,
        "twin_state": None,
        "insight": None,
        "escalated": False,
        "total_seconds": None,
        "sla_met": None,
        "stages": [],  # [(label, seconds_from_ingest), ...] for the SLA race UI
    }
    result["stages"].append(("Ingest", 0.0))

    triage = call_agent(
        TRIAGE_SYSTEM,
        f"Client: {article['client']}\nHeadline: {article['headline']}\nBody: {article['body']}",
    )
    triage_time = time.time()
    print(f"[TRIAGE]  {now_ms()}  ({triage_time - ingest_time:.2f}s)  {triage}")
    result["triage"] = triage
    result["stages"].append(("Triage", round(triage_time - ingest_time, 2)))

    crisis = compute_crisis_score(triage, profile, velocity)
    result["crisis"] = crisis
    print(f"[SCORE]   {now_ms()}  Crisis Score {crisis['score']}/100 ({crisis['band']})  "
          f"tier={profile['tier']} velocity={velocity['trend']}  breakdown={crisis['breakdown']}")
    result["stages"].append(("Score", round(time.time() - ingest_time, 2)))

    # Tier-1 clients escalate at a lower bar — a "medium" story about a Tier-1
    # regulator-adjacent client still belongs in front of the war room.
    score_threshold = 45 if profile["tier"] == "TIER1" else 60
    escalate = (
        crisis["score"] >= score_threshold
        or triage.get("risk_level") == "high"
        or triage.get("topic") == "regulatory"
    )

    if not escalate:
        total = time.time() - ingest_time
        print(f"[SKIP]    Low priority — no alert. Total: {total:.2f}s")
        result["total_seconds"] = round(total, 2)
        return result

    result["escalated"] = True
    key = (article["client"], triage.get("topic"))
    prev_incident_max = _INCIDENTS.get(key, {}).get("max_score", 0)

    claim = run_claim_agent(article)
    claim_time = time.time()
    result["stages"].append(("Claim", round(claim_time - ingest_time, 2)))

    incident = update_incident(article, triage, crisis, claim=claim.get("claim"))
    result["incident"] = incident
    twin_time = time.time()
    result["stages"].append(("Crisis Twin", round(twin_time - ingest_time, 2)))

    verification = run_multi_agent_verification(article, incident, profile, claim)
    result["verification"] = verification
    verify_time = time.time()
    print(f"[VERIFY]  {now_ms()}  ({verify_time - twin_time:.2f}s)  {verification}")
    result["stages"].append(("Verify", round(verify_time - ingest_time, 2)))

    prediction = compute_escalation_prediction(crisis, incident, velocity, profile, verification, prev_incident_max)
    result["prediction"] = prediction
    result["twin_state"] = incident_state(incident, verification, velocity)
    print(f"[PREDICT] {now_ms()}  Escalation probability {prediction['probability']}% -> {prediction['predicted_band']} "
          f"(ETA {prediction['eta']})")

    insight = call_agent(
        INSIGHT_SYSTEM,
        f"Headline: {article['headline']}\nBody: {article['body']}\nTriage: {triage}\n"
        f"Crisis Score: {crisis['score']}/100 ({crisis['band']})\nClient tier: {profile['tier']}\n"
        f"Incident: {incident['id']}, {incident['article_count']} article(s) so far\n"
        f"Verification: {verification['status']}\nEscalation probability: {prediction['probability']}%",
    )
    insight_time = time.time()
    print(f"[INSIGHT] {now_ms()}  ({insight_time - verify_time:.2f}s)  brief generated")
    result["insight"] = insight
    result["stages"].append(("Insight", round(insight_time - ingest_time, 2)))

    send_alert(article, triage, insight, crisis, profile, incident, verification, prediction)
    total = time.time() - ingest_time
    result["total_seconds"] = round(total, 2)
    result["sla_met"] = total < 120
    result["stages"].append(("Alert", round(total, 2)))
    print(f"\n⏱  INGEST-TO-ALERT: {total:.2f} seconds  (SLA target: <120s) {'✅ MET' if total < 120 else '❌ BREACHED'}")
    return result


# ---------------------------------------------------------------------------
# Crisis Simulator — canned, deterministic scenarios for live demos. Real
# NewsAPI results are unpredictable (a judge's demo slot might land on a
# quiet news day), so this replays a scripted sequence of articles through
# the SAME pipeline functions above — no mock scoring logic, just no live
# API calls for triage/insight/verification, so a demo never depends on
# network conditions or LLM latency variance.
# ---------------------------------------------------------------------------
SIMULATION_SCENARIOS = {
    "rbi_investigation": {
        "label": "RBI Regulatory Investigation",
        "client": "Paytm",
        "events": [
            {"delay": 0, "headline": "RBI seeks clarification from Paytm over compliance gaps",
             "source": "The Economic Times",
             "triage": {"risk_level": "medium", "topic": "regulatory", "sentiment": "negative", "competitor_impact": "none"},
             "insight": {"what_happened": "RBI has requested clarification from Paytm regarding compliance gaps.",
                         "why_it_matters": "Early regulatory attention often precedes formal investigations.",
                         "risk_score": "5/10", "response_plan": {
                             "immediate": "Confirm details with legal/compliance team",
                             "next_15min": "Draft internal holding statement",
                             "next_1hr": "Monitor for follow-up coverage",
                             "executive": "Brief compliance head"}}},
            {"delay": 3, "headline": "RBI formally initiates audit of Paytm's payment operations",
             "source": "Livemint",
             "triage": {"risk_level": "high", "topic": "regulatory", "sentiment": "negative", "competitor_impact": "minor"},
             "insight": {"what_happened": "RBI has formally opened an audit into Paytm's payment operations.",
                         "why_it_matters": "A formal audit signals escalated regulatory risk and investor concern.",
                         "risk_score": "8/10", "response_plan": {
                             "immediate": "Notify legal + crisis team", "next_15min": "Prepare holding statement",
                             "next_1hr": "Monitor customer and investor sentiment",
                             "executive": "Notify CMO and CEO office"}}},
            {"delay": 3, "headline": "Paytm audit: RBI widens scope to include KYC processes",
             "source": "Business Standard",
             "triage": {"risk_level": "high", "topic": "regulatory", "sentiment": "negative", "competitor_impact": "minor"},
             "insight": {"what_happened": "The RBI audit scope has widened to include KYC compliance processes.",
                         "why_it_matters": "Broader scope increases the probability of penalties or restrictions.",
                         "risk_score": "9/10", "response_plan": {
                             "immediate": "Escalate to executive crisis committee", "next_15min": "Finalize public statement",
                             "next_1hr": "Coordinate with legal on regulator communication",
                             "executive": "CEO briefing + board notification"}}},
            {"delay": 3, "headline": "Competitors PhonePe, Google Pay see user sign-up spike amid Paytm audit news",
             "source": "Moneycontrol",
             "triage": {"risk_level": "high", "topic": "regulatory", "sentiment": "negative", "competitor_impact": "major"},
             "insight": {"what_happened": "Competing wallets are reporting increased sign-ups following the audit news.",
                         "why_it_matters": "Active customer churn to competitors is now underway, not just reputational risk.",
                         "risk_score": "9/10", "response_plan": {
                             "immediate": "Activate customer retention messaging", "next_15min": "Publish verified public statement",
                             "next_1hr": "Track competitor narrative and churn indicators",
                             "executive": "Full executive war-room activation"}}},
        ],
    },
    "data_breach": {
        "label": "Customer Data Breach",
        "client": "Paytm",
        "events": [
            {"delay": 0, "headline": "Security researcher claims Paytm user data exposed on forum",
             "source": "Unknown Blog",
             "triage": {"risk_level": "medium", "topic": "other", "sentiment": "negative", "competitor_impact": "none"},
             "insight": {"what_happened": "An unverified claim alleges Paytm user data was posted on a hacking forum.",
                         "why_it_matters": "Unverified breach claims can still cause reputational damage before confirmation.",
                         "risk_score": "5/10", "response_plan": {
                             "immediate": "Security team to verify the claim", "next_15min": "Check forum post authenticity",
                             "next_1hr": "Prepare holding statement pending verification",
                             "executive": "Notify CISO"}}},
            {"delay": 3, "headline": "Paytm confirms investigating potential data exposure incident",
             "source": "Reuters",
             "triage": {"risk_level": "high", "topic": "other", "sentiment": "negative", "competitor_impact": "none"},
             "insight": {"what_happened": "Paytm has confirmed it is investigating a potential data exposure incident.",
                         "why_it_matters": "Company confirmation elevates this from rumor to a real security incident.",
                         "risk_score": "8/10", "response_plan": {
                             "immediate": "Notify legal, security, and comms leads", "next_15min": "Prepare customer notification draft",
                             "next_1hr": "Engage third-party forensics if needed",
                             "executive": "CISO + CEO briefing"}}},
        ],
    },
}


def run_simulation(scenario_key, on_event=None):
    """Replays a canned scenario through the real scoring/clustering/verification
    pipeline (no LLM calls). Yields each result dict as it's produced; call
    on_event(result) if provided for live UI updates."""
    scenario = SIMULATION_SCENARIOS[scenario_key]
    client_kw = scenario["client"]
    out = []
    for ev in scenario["events"]:
        time.sleep(ev["delay"])
        ingest_time = time.time()
        article = {"client": client_kw, "headline": ev["headline"], "body": "", "source": ev["source"]}
        profile = get_client_profile(client_kw)
        record_mention(client_kw, ingest_time)
        velocity = compute_velocity(client_kw)
        triage = ev["triage"]
        crisis = compute_crisis_score(triage, profile, velocity)

        key = (client_kw, triage.get("topic"))
        prev_max = _INCIDENTS.get(key, {}).get("max_score", 0)
        incident = update_incident(article, triage, crisis, claim=article["headline"])
        verification = verify_against_incident_simulated(article, incident, profile)
        prediction = compute_escalation_prediction(crisis, incident, velocity, profile, verification, prev_max)
        state = incident_state(incident, verification, velocity)

        result = {
            "article": article, "ingest_ts": now_ms(), "profile": profile, "velocity": velocity,
            "triage": triage, "crisis": crisis, "incident": incident, "verification": verification,
            "prediction": prediction, "twin_state": state, "insight": ev["insight"], "escalated": True,
            "total_seconds": round(time.time() - ingest_time, 2) + 1.2,  # simulated processing latency
            "sla_met": True,
            "stages": [("Ingest", 0.0), ("Triage", 0.3), ("Score", 0.4), ("Crisis Twin", 0.6),
                       ("Verify", 0.9), ("Insight", 1.2), ("Alert", 1.2)],
        }
        out.append(result)
        if on_event:
            on_event(result)
    return out


def verify_against_incident_simulated(article, incident, profile):
    """Same shape as run_multi_agent_verification, but zero LLM calls — the
    Source and Entity agents are already deterministic in the real pipeline,
    so they're reused as-is here. Claim/Contradiction use simple stand-ins
    since there's no LLM call in the simulator by design."""
    source = run_source_agent(article)
    entity = run_entity_agent(article, profile)
    claim = {"claim": article["headline"], "specificity": "specific"}
    contradiction = {"contradiction": False, "explanation": "Scripted scenario — no conflicting reports."}
    status, note = aggregate_verification(claim, source, entity, contradiction, incident["article_count"])
    return {"status": status, "note": note,
            "agents": {"claim": claim, "source": source, "entity": entity, "contradiction": contradiction}}


if __name__ == "__main__":
    if not os.environ.get("GROQ_API_KEY"):
        raise SystemExit("Set GROQ_API_KEY env var first (get one free at console.groq.com).")
    if not NEWSAPI_KEY:
        raise SystemExit("Set NEWSAPI_KEY env var first (get one free at newsapi.org).")

    print(f"Streaming ingestion started — pulling live coverage for '{CLIENT_KEYWORD}'...\n")
    articles = fetch_live_articles(CLIENT_KEYWORD)

    if not articles:
        print("No live articles found for this keyword right now — try a bigger brand name.")
    else:
        for art in articles:
            if not art["headline"]:
                continue
            process_article(art)
            time.sleep(1)  # small pause between articles for demo pacing
