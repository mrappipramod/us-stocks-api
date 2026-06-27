#!/usr/bin/env python3
"""
Scanner Scheduler
=================
Runs nasdaq_scanner.py on a schedule. Three ways to use it:

  OPTION A — Run this file directly (keeps running, uses schedule library)
      pip install schedule
      python schedule_scanner.py

  OPTION B — Cron (Linux/Mac, no extra dependencies)
      crontab -e
      # Run at 6:30 AM Mon-Fri (before US market open)
      30 6 * * 1-5 /usr/bin/python3 /path/to/nasdaq_scanner.py >> /path/to/scanner.log 2>&1

  OPTION C — GitHub Actions (free, runs in cloud, no server needed)
      See the generated .github/workflows/scanner.yml
"""

import time
import logging
import subprocess
import sys
from datetime import datetime

log = logging.getLogger("scheduler")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

try:
    import schedule
except ImportError:
    print("Install schedule: pip install schedule")
    sys.exit(1)

# ── SCHEDULE CONFIG ────────────────────────────────────────────────────────────
# Adjust these to your preference

SCANNER_CMD = [sys.executable, "nasdaq_scanner.py"]

JOBS = [
    # Pre-market scan — full universe, save top picks
    {"time": "06:00", "days": "weekdays", "args": ["--concurrency", "4"], "label": "Pre-market full scan"},
    # Mid-day rescan — top 200 by market cap (fast)
    {"time": "12:00", "days": "weekdays", "args": ["--limit", "200", "--concurrency", "5"], "label": "Mid-day scan"},
    # End-of-day scan — full universe again
    {"time": "16:30", "days": "weekdays", "args": ["--concurrency", "4"], "label": "End-of-day scan"},
]

def run_job(label: str, extra_args: list):
    log.info(f"▶ Starting: {label}")
    cmd = SCANNER_CMD + extra_args
    try:
        result = subprocess.run(cmd, capture_output=False, text=True)
        if result.returncode == 0:
            log.info(f"✓ Finished: {label}")
        else:
            log.error(f"✕ Failed: {label} (exit {result.returncode})")
    except Exception as e:
        log.error(f"✕ Error running {label}: {e}")

def is_weekday():
    return datetime.now().weekday() < 5  # Mon=0, Fri=4

def setup_schedule():
    for job in JOBS:
        label = job["label"]
        args  = job.get("args", [])
        t     = job["time"]
        days  = job.get("days", "daily")

        if days == "weekdays":
            # schedule runs every day but we gate inside the job
            schedule.every().day.at(t).do(
                lambda l=label, a=args: run_job(l, a) if is_weekday() else None
            )
        else:
            schedule.every().day.at(t).do(lambda l=label, a=args: run_job(l, a))

        log.info(f"Scheduled '{label}' at {t} ({days})")

def main():
    log.info("Scanner scheduler starting…")
    setup_schedule()
    log.info("Press Ctrl+C to stop\n")
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
