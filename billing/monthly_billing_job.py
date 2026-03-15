"""
CodeMed AI — Monthly Billing Cron Job
Run on the 1st of each month (e.g. via cron or a scheduler like APScheduler).

  cron: 0 8 1 * *  python -m billing.monthly_billing_job

Generates invoices for all active, Stripe-connected clinics for the
previous calendar month.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

from billing.customers import create_customer
from billing.invoices import create_monthly_invoice
from billing.stripe_config import configure_stripe

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


def run_monthly_billing() -> list[dict]:
    """Invoice all active clinics for last month. Returns result list."""
    configure_stripe()

    clinics = _get_active_clinics()
    logger.info("Monthly billing starting — %d active clinics", len(clinics))

    now = datetime.now()
    if now.month == 1:
        last_month, last_year = 12, now.year - 1
    else:
        last_month, last_year = now.month - 1, now.year

    results = []
    for clinic in clinics:
        clinic_id, name, email, stripe_customer_id = clinic

        if not stripe_customer_id:
            logger.info("Creating Stripe customer for clinic %s (%s)", clinic_id, name)
            try:
                create_customer(clinic_id, name, email)
            except Exception as exc:
                logger.error("Could not create customer for %s: %s", clinic_id, exc)
                results.append({"clinic_id": clinic_id, "error": str(exc)})
                continue

        try:
            result = create_monthly_invoice(clinic_id, last_year, last_month)
            logger.info("Invoice result for %s: %s", clinic_id, result.get("status") or result.get("invoice_id"))
        except Exception as exc:
            logger.error("Invoice failed for clinic %s: %s", clinic_id, exc)
            result = {"error": str(exc)}

        results.append({"clinic_id": clinic_id, **result})

    paid = sum(1 for r in results if "invoice_id" in r)
    skipped = sum(1 for r in results if r.get("status") == "no_fees")
    failed = sum(1 for r in results if "error" in r)
    logger.info("Monthly billing done — invoiced=%d skipped=%d failed=%d", paid, skipped, failed)
    return results


def _get_active_clinics() -> list[tuple]:
    """Return (id, name, email, stripe_customer_id) for all billable clinics."""
    try:
        import psycopg2
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, email, stripe_customer_id
                FROM   clinics
                WHERE  integration_disabled = false
                ORDER  BY name
                """
            )
            rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logger.error("Could not fetch clinics: %s", exc)
        return []


if __name__ == "__main__":
    results = run_monthly_billing()
    print(f"\nProcessed {len(results)} clinics.")
