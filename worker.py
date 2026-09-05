"""
worker.py — Event-driven ingestion worker.
============================================
Run standalone for the real multi-process architecture:

    python worker.py

...or it's auto-started as an in-process thread by app.py / streamlit_app.py
(see AUTO_START_WORKER in each) so a single command still works for a quick
demo. Multiple workers — in-process threads, or entirely separate `python
worker.py` processes on the same machine — can run concurrently; event_store's
atomic claim means they never double-process the same article, so this is
horizontally scalable the same way a real queue-consumer fleet would be.

Two responsibilities, run as two threads from __main__:

  run_worker_loop()     — claims events within ~1s of being queued (NOT the
                           old 120s poll) and runs them through
                           pipeline.process_article(). This is the actual
                           event-driven consumption path.

  run_fallback_poller() — the ONLY thing that still polls, because NewsAPI's
                           free tier has no webhook/push option. It ONLY
                           enqueues — it never calls process_article directly
                           — so a NewsAPI-sourced article and a webhook-pushed
                           article go through the exact same code path from
                           here on.
"""

import os
import time
import threading

import event_store
import pipeline

CLAIM_POLL_SECONDS = float(os.environ.get("CLAIM_POLL_SECONDS", "1"))
FALLBACK_POLL_SECONDS = int(os.environ.get("FALLBACK_POLL_SECONDS", "120"))


def process_one(event):
    article = {"client": event["client"], "headline": event["headline"],
               "body": event["body"], "source": event["source"]}
    try:
        result = pipeline.process_article(article, received_at=event["received_at"])
        event_store.mark_done(event["id"], result)
    except Exception as e:
        print(f"[WORKER] error processing event {event['id']}: {e}")
        event_store.mark_error(event["id"], e)


def run_worker_loop(stop_event=None):
    event_store.init_db()
    print(f"[WORKER] Started \u2014 claiming events within ~{CLAIM_POLL_SECONDS}s of arrival "
          f"(event-driven, not a fixed 120s poll).")
    while stop_event is None or not stop_event.is_set():
        event = event_store.claim_next_event()
        if event:
            queue_wait = time.time() - event["received_at"]
            print(f"[WORKER] Claimed event {event['id']} \u2014 waited {queue_wait:.2f}s in queue")
            process_one(event)
        else:
            time.sleep(CLAIM_POLL_SECONDS)


def run_fallback_poller(stop_event=None):
    """The one remaining polling loop in the system, and only for the one
    source (NewsAPI's free tier) that structurally requires it."""
    event_store.init_db()
    print(f"[FALLBACK POLLER] Enqueuing NewsAPI articles for '{pipeline.CLIENT_KEYWORD}' "
          f"every {FALLBACK_POLL_SECONDS}s (NewsAPI has no webhook option \u2014 "
          f"everything else in this system is push-based).")
    while stop_event is None or not stop_event.is_set():
        try:
            articles = pipeline.fetch_live_articles(pipeline.CLIENT_KEYWORD)
            for art in articles:
                event_store.enqueue_event(art["client"], art["headline"], art["body"], art["source"])
        except Exception as e:
            print(f"[FALLBACK POLLER] error: {e}")
        time.sleep(FALLBACK_POLL_SECONDS)


def start_background_ingestion(with_fallback_poller=True):
    """Convenience for app.py / streamlit_app.py: starts worker + (optionally)
    the fallback poller as daemon threads inside the CALLING process. Safe to
    call even if a separate `python worker.py` is also running elsewhere —
    event_store's atomic claim means there's no double-processing."""
    event_store.init_db()
    threading.Thread(target=run_worker_loop, daemon=True).start()
    if with_fallback_poller:
        threading.Thread(target=run_fallback_poller, daemon=True).start()


if __name__ == "__main__":
    threading.Thread(target=run_fallback_poller, daemon=True).start()
    run_worker_loop()
