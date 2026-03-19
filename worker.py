"""
worker.py — NMC Redis Queue Worker
Runs as a separate process on Render.
Picks up NMC jobs from Redis queue and processes them one at a time.
Start command: python worker.py
"""

import os
import logging
import asyncio
import sys

from pathlib import Path
from redis import Redis
from rq import Worker, Queue, Connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s"
)
log = logging.getLogger(__name__)

# ── Redis connection ──────────────────────────────────────────────────────────
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
NMC_QUEUE_NAME = "nextstep:nmc:jobs"

def main():
    log.info("[Worker] NMC worker starting — queue: %s", NMC_QUEUE_NAME)
    log.info("[Worker] Redis: %s", REDIS_URL[:30] + "...")

    redis_conn = Redis.from_url(REDIS_URL)

    # Test connection
    try:
        redis_conn.ping()
        log.info("[Worker] Redis connection OK")
    except Exception as e:
        log.error("[Worker] Redis connection FAILED: %s", e)
        sys.exit(1)

    with Connection(redis_conn):
        worker = Worker(
            queues=[NMC_QUEUE_NAME],
            connection=redis_conn,
            log_job_description=True,
        )
        log.info("[Worker] Ready — waiting for jobs...")
        worker.work(with_scheduler=True)

if __name__ == "__main__":
    main()
