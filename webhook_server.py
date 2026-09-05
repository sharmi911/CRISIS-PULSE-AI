"""
webhook_server.py — Real-time, push-based ingestion endpoint.
=================================================================
Run standalone:

    export WEBHOOK_SECRET=your_secret     # optional but recommended
    python webhook_server.py              # listens on :8000

POST /webhook/article
    Headers: X-Webhook-Secret: <WEBHOOK_SECRET>   (if WEBHOOK_SECRET is set)
    Body:    {"headline": "...", "body": "...", "source": "...", "client": "Paytm"}

Returns 202 immediately — the article is queued (event_store.enqueue_event),
not processed inline. THIS is what "no polling" actually means in practice:
ingestion latency is however long the HTTP round trip takes, typically well
under a second, instead of "up to 120 seconds until the next poll."

Wire a real push source to this endpoint for genuine event-driven ingestion:
a Zapier "new article matches search" trigger, an RSS-to-webhook relay, a
Twitter/X streaming rule forwarder, or just curl for a live demo:

    curl -X POST http://localhost:8000/webhook/article \\
         -H "Content-Type: application/json" \\
         -H "X-Webhook-Secret: $WEBHOOK_SECRET" \\
         -d '{"headline": "RBI opens audit into Paytm", "source": "Reuters"}'

The same /webhook/article route is also mounted directly on app.py's Flask
app, so you can run one process instead of two if you don't need a separate
ingress service.
"""

import os

from flask import Flask, request, jsonify

import event_store
import pipeline

app = Flask(__name__)
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")


def check_secret():
    if not WEBHOOK_SECRET:
        return True
    return request.headers.get("X-Webhook-Secret") == WEBHOOK_SECRET


def handle_webhook_article():
    if not check_secret():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    headline = (data.get("headline") or "").strip()
    if not headline:
        return jsonify({"error": "headline is required"}), 400
    client = data.get("client") or pipeline.CLIENT_KEYWORD
    event_id = event_store.enqueue_event(client, headline, data.get("body", ""), data.get("source", "Webhook"))
    return jsonify({"status": "queued", "event_id": event_id}), 202


@app.route("/webhook/article", methods=["POST"])
def webhook_article():
    return handle_webhook_article()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "queue_depth": event_store.queue_depth(), **event_store.queue_counts()})


if __name__ == "__main__":
    event_store.init_db()
    port = int(os.environ.get("WEBHOOK_PORT", "8000"))
    print(f"Webhook ingress listening on :{port} \u2014 POST /webhook/article")
    if not WEBHOOK_SECRET:
        print("WARNING: WEBHOOK_SECRET is not set \u2014 the endpoint is unauthenticated. "
              "Set WEBHOOK_SECRET before exposing this publicly.")
    app.run(host="0.0.0.0", port=port)
