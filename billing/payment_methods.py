"""
CodeMed AI — Payment Method Setup
Handles SetupIntents (card and ACH) and attaching payment methods to customers.
"""
from __future__ import annotations

import logging
from typing import Any

import stripe

from billing.customers import get_customer

logger = logging.getLogger(__name__)


def create_setup_intent(clinic_id: str) -> dict[str, Any]:
    """
    Create a Stripe SetupIntent so the clinic can save a card or bank account.
    Returns the client_secret the frontend needs to render the payment element.
    """
    customer = get_customer(clinic_id)
    if not customer:
        return {"error": "Customer not found"}

    intent = stripe.SetupIntent.create(
        customer=customer.id,
        payment_method_types=["card", "us_bank_account"],
        metadata={"clinic_id": clinic_id},
    )
    logger.info("SetupIntent %s created for clinic %s", intent.id, clinic_id)

    return {
        "client_secret": intent.client_secret,
        "customer_id": customer.id,
    }


def attach_payment_method(
    clinic_id: str,
    payment_method_id: str,
) -> dict[str, Any]:
    """
    Attach a confirmed payment method to the clinic's Stripe customer
    and set it as the default for invoices.
    """
    customer = get_customer(clinic_id)
    if not customer:
        return {"error": "Customer not found"}

    payment_method = stripe.PaymentMethod.attach(
        payment_method_id,
        customer=customer.id,
    )

    stripe.Customer.modify(
        customer.id,
        invoice_settings={"default_payment_method": payment_method_id},
    )
    logger.info(
        "Attached payment method %s to clinic %s", payment_method_id, clinic_id
    )

    return {"status": "success", "payment_method_id": payment_method.id}
