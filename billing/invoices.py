"""
CodeMed AI — Monthly Invoice Generation
Creates per-recovery-line Stripe invoice items then finalises and sends
the invoice automatically.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import stripe

from billing.customers import get_customer

logger = logging.getLogger(__name__)

# Contingency fee rate (15 %)
DEFAULT_FEE_RATE: float = 0.15


def create_monthly_invoice(
    clinic_id: str,
    year: int,
    month: int,
    fee_rate: float = DEFAULT_FEE_RATE,
) -> dict[str, Any]:
    """
    Build a Stripe invoice for all recovered denials in a given month.

    Each recovered denial becomes one InvoiceItem so the clinic can see
    exactly what they're paying for.

    Returns a dict with invoice_id, hosted URL, PDF URL, and totals.
    Returns {'status': 'no_fees'} when nothing was recovered.
    """
    line_items = _fetch_recovery_line_items(clinic_id, year, month, fee_rate)
    if not line_items:
        return {"status": "no_fees", "clinic_id": clinic_id, "period": f"{year}-{month:02d}"}

    total_fee = sum(li["fee_cents"] for li in line_items) / 100

    customer = get_customer(clinic_id)
    if not customer:
        return {"error": "Stripe customer not found for clinic"}

    # Create one InvoiceItem per denial
    for li in line_items:
        stripe.InvoiceItem.create(
            customer=customer.id,
            amount=li["fee_cents"],
            currency="usd",
            description=(
                f"Recovery fee: {li['claim_id']} "
                f"({li['denial_code']}) — "
                f"${li['recovered_amount']:.2f} recovered"
            ),
            metadata={
                "denial_id": li["denial_id"],
                "claim_id": li["claim_id"],
                "denial_code": li["denial_code"],
            },
        )

    # 30-day due date
    due_ts = int(datetime.now(tz=timezone.utc).timestamp()) + 30 * 86400

    invoice = stripe.Invoice.create(
        customer=customer.id,
        auto_advance=True,
        collection_method="charge_automatically",
        due_date=due_ts,
        metadata={
            "clinic_id": clinic_id,
            "period": f"{year}-{month:02d}",
            "total_recovered": str(sum(li["recovered_amount"] for li in line_items)),
            "total_fee": str(total_fee),
        },
    )

    stripe.Invoice.send_invoice(invoice.id)
    logger.info(
        "Invoice %s sent to clinic %s — $%.2f due", invoice.id, clinic_id, total_fee
    )

    return {
        "invoice_id": invoice.id,
        "invoice_url": invoice.hosted_invoice_url,
        "pdf_url": invoice.invoice_pdf,
        "amount_due": (invoice.amount_due or 0) / 100,
        "due_date": invoice.due_date,
        "line_items": len(line_items),
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_recovery_line_items(
    clinic_id: str,
    year: int,
    month: int,
    fee_rate: float,
) -> list[dict[str, Any]]:
    """
    Pull paid denials from the DB for the given period.
    Replace with your actual query against the revenue_shares table.
    """
    try:
        import psycopg2, os
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id            AS denial_id,
                    claim_id,
                    denial_code,
                    recovered_amount
                FROM denials
                WHERE clinic_id        = %s
                  AND status           = 'recovered'
                  AND EXTRACT(YEAR  FROM recovered_at) = %s
                  AND EXTRACT(MONTH FROM recovered_at) = %s
                """,
                (clinic_id, year, month),
            )
            rows = cur.fetchall()
        conn.close()
    except Exception as exc:
        logger.error("DB query failed while fetching line items: %s", exc)
        return []

    items = []
    for denial_id, claim_id, denial_code, recovered_amount in rows:
        fee = float(recovered_amount) * fee_rate
        items.append(
            {
                "denial_id": str(denial_id),
                "claim_id": claim_id,
                "denial_code": denial_code,
                "recovered_amount": float(recovered_amount),
                "fee_cents": int(fee * 100),
            }
        )
    return items
