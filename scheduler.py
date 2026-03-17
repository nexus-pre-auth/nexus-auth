"""
CodeMed Automation Scheduler
=============================
Standalone process that drives the three recurring jobs that make
the denial recovery business run without human intervention:

  Daily  06:00  —  Detect + fix new denials for every connected clinic
  Weekly Mon 08:00  —  Send recovery digest email to each clinic
  Monthly 1st 07:00 —  Rollup revenue shares, generate invoice records

Run as a separate process (or Docker container):
    python scheduler.py

Uses APScheduler with a PostgreSQL job store so:
  • Jobs survive restarts
  • Multiple instances won't double-fire the same job
  • Job history is queryable from the database

Environment variables:
    DATABASE_URL          — PostgreSQL connection string (required)
    SCHEDULER_TIMEZONE    — e.g. "America/New_York" (default: UTC)
    NOTIFICATION_FROM     — sender email for clinic digests
    SMTP_HOST / SMTP_PORT — SMTP server config (or set SENDGRID_API_KEY)
"""

import logging
import os
import signal
import sys
import time
from datetime import date, datetime

import psycopg2
import psycopg2.extras
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger

from denial_recovery import DenialRecoveryEngine
from revenue_tracking import RevenueTracker

APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:5000")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
TIMEZONE     = os.getenv("SCHEDULER_TIMEZONE", "UTC")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _db() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


def _get_ready_clinics() -> list[dict]:
    """Return all clinics with an active, synced WebPT connection."""
    conn = _db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (clinic_id)
                    clinic_id,
                    id AS connection_id,
                    metadata->>'contact_email' AS contact_email,
                    metadata->>'clinic_name'   AS clinic_name
                FROM webpt_connections
                WHERE status = 'ready'
                ORDER BY clinic_id, created_at DESC
                """
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Job: daily denial scan
# ---------------------------------------------------------------------------

def job_daily_denial_scan() -> None:
    """
    Detect and fix new CO-16/50/97 denials for every connected clinic.
    Runs at 06:00 daily — catches overnight claim status changes from payers.
    """
    clinics = _get_ready_clinics()
    logger.info("Daily scan starting — %d clinic(s) to process", len(clinics))

    total_fixed   = 0
    total_value   = 0.0
    failed_clinics: list[str] = []

    for clinic in clinics:
        clinic_id = clinic["clinic_id"]
        conn = _db()
        try:
            engine = DenialRecoveryEngine(conn)
            result = engine.batch_process(clinic_id)
            total_fixed += result["fixed"]
            total_value += result.get("estimated_recovery", 0)
            logger.info(
                "Clinic %s — %d fixed, $%.2f estimated",
                clinic_id, result["fixed"], result.get("estimated_recovery", 0),
            )
        except Exception as exc:
            logger.error("Daily scan failed for clinic %s: %s", clinic_id, exc)
            failed_clinics.append(clinic_id)
        finally:
            conn.close()

    logger.info(
        "Daily scan complete — %d clinic(s), %d denials fixed, $%.2f estimated. "
        "Failed: %s",
        len(clinics), total_fixed, total_value,
        failed_clinics or "none",
    )


# ---------------------------------------------------------------------------
# Job: send approval request emails
# ---------------------------------------------------------------------------

def job_send_approval_requests() -> None:
    """
    For every clinic, send an approval-request email for any pending_approval
    denials that do not yet have an active (unexpired, unused) token.
    Runs daily at 06:30 — 30 minutes after the denial scan so fixes are ready.
    """
    from notifications import send_approval_request

    clinics = _get_ready_clinics()
    logger.info("Approval request job starting — %d clinic(s)", len(clinics))

    sent_total = 0
    for clinic in clinics:
        clinic_id     = clinic["clinic_id"]
        contact_email = clinic.get("contact_email")
        if not contact_email:
            continue

        conn = _db()
        try:
            engine  = DenialRecoveryEngine(conn)
            denials = engine.get_pending_approval_denials(clinic_id)
            if not denials:
                continue

            # Generate a token for each denial and bundle into one email
            enriched = []
            for d in denials:
                token = engine.generate_approval_token(str(d["id"]))
                enriched.append({
                    "denial_id":          str(d["id"]),
                    "denial_code":        d["denial_code"],
                    "fix_notes":          d.get("fix_notes") or "",
                    "estimated_recovery": float(d.get("estimated_recovery") or 0),
                    "token":              token,
                })
        finally:
            conn.close()

        try:
            send_approval_request(
                clinic_id=clinic_id,
                clinic_name=clinic.get("clinic_name") or clinic_id,
                to_email=contact_email,
                denials=enriched,
                base_url=APP_BASE_URL,
            )
            sent_total += len(enriched)
            logger.info(
                "Approval request sent to clinic %s — %d denial(s)", clinic_id, len(enriched)
            )
        except Exception as exc:
            logger.error("Approval email failed for clinic %s: %s", clinic_id, exc)

    logger.info("Approval request job complete — %d denial(s) emailed", sent_total)


# ---------------------------------------------------------------------------
# Job: submit approved denials
# ---------------------------------------------------------------------------

def job_submit_approved_denials() -> None:
    """
    Transition all 'approved' denials to 'submitted' and record the attempt.
    Runs daily at 07:00 — after approval emails have had time to be actioned
    (same-day approvals from overnight email sends).
    """
    clinics = _get_ready_clinics()
    logger.info("Submission job starting — %d clinic(s)", len(clinics))

    total_submitted = 0
    for clinic in clinics:
        clinic_id = clinic["clinic_id"]
        conn = _db()
        try:
            engine  = DenialRecoveryEngine(conn)
            denials = engine.get_approved_denials(clinic_id)
            for d in denials:
                try:
                    engine.submit_denial(str(d["id"]))
                    total_submitted += 1
                except Exception as exc:
                    logger.warning(
                        "Could not submit denial %s for clinic %s: %s",
                        d["id"], clinic_id, exc,
                    )
        except Exception as exc:
            logger.error("Submission job failed for clinic %s: %s", clinic_id, exc)
        finally:
            conn.close()

    logger.info("Submission job complete — %d denial(s) submitted", total_submitted)


# ---------------------------------------------------------------------------
# Job: weekly clinic digest
# ---------------------------------------------------------------------------

def job_weekly_digest() -> None:
    """
    Send each clinic a summary of what was recovered in the past week.
    Runs Monday 08:00 — clinics see results at the start of their work week.
    """
    from notifications import send_weekly_digest

    clinics = _get_ready_clinics()
    logger.info("Weekly digest starting — %d clinic(s)", len(clinics))

    sent = 0
    for clinic in clinics:
        clinic_id    = clinic["clinic_id"]
        contact_email = clinic.get("contact_email")
        if not contact_email:
            logger.debug("Clinic %s has no contact email — skipping digest", clinic_id)
            continue

        conn = _db()
        try:
            tracker = RevenueTracker(conn)
            summary = tracker.get_clinic_summary(clinic_id)
        finally:
            conn.close()

        try:
            send_weekly_digest(
                clinic_id=clinic_id,
                clinic_name=clinic.get("clinic_name") or clinic_id,
                to_email=contact_email,
                summary=summary,
            )
            sent += 1
        except Exception as exc:
            logger.error("Digest send failed for clinic %s: %s", clinic_id, exc)

    logger.info("Weekly digest complete — %d/%d sent", sent, len(clinics))


# ---------------------------------------------------------------------------
# Job: monthly revenue rollup
# ---------------------------------------------------------------------------

def job_monthly_rollup() -> None:
    """
    Aggregate paid denials into revenue_shares for the previous month.
    Runs on the 1st of each month at 07:00 — invoices the closed period.
    """
    today  = date.today()
    # Rollup the month that just closed
    if today.month == 1:
        period = date(today.year - 1, 12, 1)
    else:
        period = date(today.year, today.month - 1, 1)

    clinics = _get_ready_clinics()
    logger.info(
        "Monthly rollup for %s — %d clinic(s)", period.strftime("%Y-%m"), len(clinics)
    )

    for clinic in clinics:
        clinic_id = clinic["clinic_id"]
        conn = _db()
        try:
            tracker = RevenueTracker(conn)
            result  = tracker.rollup_monthly(clinic_id, period)
            logger.info(
                "Rollup %s %s — recovered $%.2f, fee $%.2f",
                clinic_id, period, result["total_recovered"], result["your_fee"],
            )
        except Exception as exc:
            logger.error("Monthly rollup failed for clinic %s: %s", clinic_id, exc)
        finally:
            conn.close()

    logger.info("Monthly rollup complete for %s", period.strftime("%Y-%m"))


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def build_scheduler() -> BlockingScheduler:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    jobstores = {
        "default": SQLAlchemyJobStore(url=DATABASE_URL),
    }
    executors = {
        "default": ThreadPoolExecutor(max_workers=4),
    }

    scheduler = BlockingScheduler(
        jobstores=jobstores,
        executors=executors,
        timezone=TIMEZONE,
    )

    scheduler.add_job(
        job_daily_denial_scan,
        CronTrigger(hour=6, minute=0, timezone=TIMEZONE),
        id="daily_denial_scan",
        name="Daily denial detection + fix",
        replace_existing=True,
        misfire_grace_time=3600,   # Run up to 1h late if scheduler was down
    )

    scheduler.add_job(
        job_send_approval_requests,
        CronTrigger(hour=6, minute=30, timezone=TIMEZONE),
        id="daily_approval_requests",
        name="Daily approval request emails",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    scheduler.add_job(
        job_submit_approved_denials,
        CronTrigger(hour=7, minute=0, timezone=TIMEZONE),
        id="daily_submit_approved",
        name="Daily submission of approved denials",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    scheduler.add_job(
        job_weekly_digest,
        CronTrigger(day_of_week="mon", hour=8, minute=0, timezone=TIMEZONE),
        id="weekly_clinic_digest",
        name="Weekly clinic recovery digest",
        replace_existing=True,
        misfire_grace_time=7200,
    )

    scheduler.add_job(
        job_monthly_rollup,
        CronTrigger(day=1, hour=7, minute=0, timezone=TIMEZONE),
        id="monthly_revenue_rollup",
        name="Monthly revenue share rollup",
        replace_existing=True,
        misfire_grace_time=86400,  # Run up to 24h late (e.g. after server restart)
    )

    return scheduler


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    scheduler = build_scheduler()

    # Graceful shutdown on SIGTERM / SIGINT
    def _shutdown(signum, frame):
        logger.info("Scheduler shutting down (signal %d)...", signum)
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info(
        "Scheduler started — timezone=%s, jobs=%d",
        TIMEZONE,
        len(scheduler.get_jobs()),
    )
    for job in scheduler.get_jobs():
        logger.info("  %-30s  next run: %s", job.name, job.next_run_time)

    scheduler.start()


if __name__ == "__main__":
    main()
