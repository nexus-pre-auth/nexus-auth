"""
CodeMed AI — Stripe Customer Management
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Optional

import stripe

logger = logging.getLogger(__name__)


@contextmanager
def _get_db():
    """
    Database connection context manager.
    Replace with your actual DB helper (e.g. psycopg2 pool, SQLAlchemy session).
    """
    try:
        import psycopg2
        import os
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        try:
            yield conn
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover
        logger.error("DB connection failed: %s", exc)
        raise


def create_customer(
    clinic_id: str,
    clinic_name: str,
    clinic_email: str,
) -> stripe.Customer:
    """Create a Stripe customer for a clinic and persist the ID."""
    customer = stripe.Customer.create(
        name=clinic_name,
        email=clinic_email,
        metadata={
            "clinic_id": clinic_id,
            "source": "codemed_onboarding",
        },
    )
    logger.info("Created Stripe customer %s for clinic %s", customer.id, clinic_id)

    with _get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE clinics SET stripe_customer_id = %s WHERE id = %s",
                (customer.id, clinic_id),
            )
            conn.commit()

    return customer


def get_customer(clinic_id: str) -> Optional[stripe.Customer]:
    """Retrieve the Stripe customer object for a clinic, or None."""
    with _get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stripe_customer_id FROM clinics WHERE id = %s",
                (clinic_id,),
            )
            row = cur.fetchone()

    if row and row[0]:
        return stripe.Customer.retrieve(row[0])
    return None
