"""
CodeMed AI — Stripe Webhook Handler
Verifies the Stripe-Signature header and dispatches to event-specific handlers.

Mount this with FastAPI (see billing/routes.py) or Flask.
"""
from __future__ import annotations

import logging
from typing import Any

import stripe

from billing.stripe_config import STRIPE_WEBHOOK_SECRET

logger = logging.getLogger(__name__)


def handle_webhook_event(payload: bytes, sig_header: str) -> dict[str, Any]:
    """
    Verify and dispatch a Stripe webhook event.

    Args:
        payload:    Raw request body bytes.
        sig_header: Value of the ``Stripe-Signature`` HTTP header.

    Returns:
        dict with ``status`` key.  Raises ValueError or
        stripe.error.SignatureVerificationError on bad input.
    """
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except ValueError as exc:
        logger.warning("Invalid webhook payload: %s", exc)
        raise
    except stripe.error.SignatureVerificationError as exc:
        logger.warning("Invalid webhook signature: %s", exc)
        raise

    event_type: str = event["type"]
    data: dict = event["data"]["object"]

    logger.info("Stripe webhook received: %s (id=%s)", event_type, event["id"])

    handlers = {
        "invoice.payment_succeeded": _handle_payment_succeeded,
        "invoice.payment_failed": _handle_payment_failed,
        "customer.subscription.deleted": _handle_subscription_deleted,
        "setup_intent.succeeded": _handle_setup_intent_succeeded,
    }

    handler = handlers.get(event_type)
    if handler:
        handler(data)
    else:
        logger.debug("Unhandled Stripe event type: %s", event_type)

    return {"status": "success", "event_type": event_type}


# ---------------------------------------------------------------------------
# Event handlers
# ---------------------------------------------------------------------------

def _handle_payment_succeeded(invoice: dict) -> None:
    clinic_id = invoice.get("metadata", {}).get("clinic_id")
    period = invoice.get("metadata", {}).get("period")
    amount = (invoice.get("amount_paid") or 0) / 100

    logger.info(
        "Payment succeeded — clinic=%s period=%s amount=$%.2f invoice=%s",
        clinic_id, period, amount, invoice["id"],
    )

    try:
        import psycopg2, os
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE revenue_shares
                SET    status             = 'paid',
                       paid_at            = NOW(),
                       stripe_invoice_id  = %s,
                       paid_amount        = %s
                WHERE  clinic_id = %s
                  AND  period    = %s
                """,
                (invoice["id"], amount, clinic_id, period),
            )
        conn.close()
    except Exception as exc:
        logger.error("Failed to update revenue_shares after payment: %s", exc)


def _handle_payment_failed(invoice: dict) -> None:
    clinic_id = invoice.get("metadata", {}).get("clinic_id")
    amount = (invoice.get("amount_due") or 0) / 100

    logger.warning(
        "Payment FAILED — clinic=%s amount=$%.2f invoice=%s",
        clinic_id, amount, invoice["id"],
    )
    # TODO: trigger retry / notification workflow
    # send_payment_failed_notification(clinic_id, invoice)


def _handle_subscription_deleted(subscription: dict) -> None:
    clinic_id = subscription.get("metadata", {}).get("clinic_id")
    logger.info("Subscription cancelled for clinic=%s", clinic_id)
    # TODO: deprovision access


def _handle_setup_intent_succeeded(setup_intent: dict) -> None:
    clinic_id = setup_intent.get("metadata", {}).get("clinic_id")
    logger.info(
        "SetupIntent succeeded for clinic=%s — payment method %s ready",
        clinic_id, setup_intent.get("payment_method"),
    )
