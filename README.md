# 2-Minute Crisis Response Pipeline

A real-time media intelligence pipeline that replaces the slow "batch report every
5-6 hours" model with a streaming pipeline: live news ingestion -> AI triage ->
AI-generated crisis brief -> alert, all within a 2-minute target.

## What it does

```
NewsAPI (live) -> Triage agent (Groq LLM) -> Insight agent (Groq LLM) -> Alert
   <30s               <20s                      <90s              instant
```

1. **Ingestion** — pulls real, live articles from NewsAPI for a client/brand keyword
   (this is the 3rd-party API — replaces manual/batch news pulling).
2. **Triage agent** — a fast LLM call (Groq, free tier) classifies each article: is
   the client mentioned, what's the risk level, sentiment, topic.
3. **Insight agent** — for high-risk articles only, a second Groq call generates
   a short structured brief: what happened / why it matters / risk score / action.
4. **Alert** — prints a formatted alert (simulating a WhatsApp/Slack push) with a
   live stopwatch showing total ingest-to-alert time, proving the sub-2-minute SLA.

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt --break-system-packages
   ```

2. Get your API keys:
   - **Groq API key** (free, instant) — https://console.groq.com -> API Keys
   - **NewsAPI key** (free tier, instant) — https://newsapi.org -> "Get API Key"

3. Set your keys — either:
   - Copy `.env.example` to `.env` and fill in your keys, **or**
   - Export them directly in your terminal:
     ```
     export GROQ_API_KEY=your_key_here
     export NEWSAPI_KEY=your_key_here
     ```

4. (Optional) Open `pipeline.py` and change `CLIENT_KEYWORD = "Paytm"` near the top
   to whatever brand/company you want to track live in your demo. Pick something
   likely to have recent news coverage so the demo isn't empty.

## Run it

**Auto-polling web dashboard (recommended):**
```
python app.py
```
Then open **http://localhost:5000**. It automatically checks for fresh news
every 2 minutes in the background — no manual reload needed to get new data
(the page itself auto-refreshes every 30s just to redraw with whatever the
background poll has found).

**One-shot terminal run (for a quick manual test):**
```
python pipeline.py
```

### Real email alerts (optional)

To get an actual email sent when a high-risk article is detected:

1. Use a Gmail account. Turn on 2-Step Verification if it isn't already:
   https://myaccount.google.com/security
2. Generate an "App Password" (16 characters): https://myaccount.google.com/apppasswords
3. Set these before running `app.py`:
   ```
   export EMAIL_FROM=you@gmail.com
   export EMAIL_PASSWORD=your_16_char_app_password
   export EMAIL_TO=recipient@example.com
   ```
   (On Windows PowerShell, use `$env:EMAIL_FROM="..."` etc.)

If these aren't set, the pipeline still runs and shows alerts on the
dashboard — it just skips sending a real email and prints a note instead.

You'll see a live, timestamped trace for each article: ingest -> triage -> (insight
+ alert, for high-risk items only) -> total elapsed time with a pass/fail against
the 120-second SLA.

## Demo tips

- Run it live on stage — the printed timestamps are your proof of the sub-2-minute
  claim, no slides needed for that part.
- If NewsAPI returns nothing relevant for your keyword, try a bigger/more newsworthy
  brand name — smaller companies may not have recent coverage.
- One-line pitch: "Media crises take 5-6 hours to surface today because everything
  runs in overnight batches. Our agent pipeline triages and briefs breaking news in
  under 2 minutes and pushes it straight to the war room — before the story trends."

## Project structure

```
.
├── pipeline.py       # core pipeline: ingestion, triage, insight, alert
├── app.py            # Flask web dashboard (localhost:5000)
├── requirements.txt  # Python dependencies
├── .env.example      # template for your API keys
└── README.md         # this file
```
