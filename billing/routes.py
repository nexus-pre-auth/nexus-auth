"""
CodeMed AI — Stripe FastAPI Routes
Mounts under /v1/billing on the main app.

Usage in codemed/api.py:
    from billing.routes import billing_router
    app.include_router(billing_router, prefix="/v1/billing")
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

import stripe

from billing.customers import create_customer, get_customer
from billing.invoices import create_monthly_invoice
from billing.payment_methods import attach_payment_method, create_setup_intent
from billing.webhooks import handle_webhook_event

logger = logging.getLogger(__name__)

billing_router = APIRouter(tags=["Billing"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class CreateCustomerRequest(BaseModel):
    clinic_id: str
    clinic_name: str
    clinic_email: str


class AttachPaymentMethodRequest(BaseModel):
    payment_method_id: str


class CreateInvoiceRequest(BaseModel):
    year: int | None = None
    month: int | None = None
    fee_rate: float = 0.15


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@billing_router.post("/customers")
def create_stripe_customer(req: CreateCustomerRequest) -> dict[str, Any]:
    """Register a clinic as a Stripe customer."""
    try:
        customer = create_customer(req.clinic_id, req.clinic_name, req.clinic_email)
        return {"customer_id": customer.id, "status": "created"}
    except stripe.error.StripeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@billing_router.post("/setup-intent/{clinic_id}")
def setup_intent(clinic_id: str) -> dict[str, Any]:
    """Return a SetupIntent client_secret for the frontend payment element."""
    result = create_setup_intent(clinic_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@billing_router.post("/attach-payment-method/{clinic_id}")
def attach_method(clinic_id: str, req: AttachPaymentMethodRequest) -> dict[str, Any]:
    """Attach a confirmed payment method and make it the invoice default."""
    result = attach_payment_method(clinic_id, req.payment_method_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@billing_router.post("/invoices/{clinic_id}")
def create_invoice(clinic_id: str, req: CreateInvoiceRequest) -> dict[str, Any]:
    """Generate and send the monthly invoice for a clinic."""
    now = datetime.now()
    year = req.year or now.year
    month = req.month or now.month

    try:
        result = create_monthly_invoice(clinic_id, year, month, req.fee_rate)
    except stripe.error.StripeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return result


@billing_router.get("/customer/{clinic_id}")
def get_stripe_customer(clinic_id: str) -> dict[str, Any]:
    """Retrieve the Stripe customer record for a clinic."""
    customer = get_customer(clinic_id)
    if not customer:
        raise HTTPException(status_code=404, detail="No Stripe customer for this clinic")
    return {
        "customer_id": customer.id,
        "name": customer.name,
        "email": customer.email,
        "default_payment_method": (
            customer.invoice_settings.default_payment_method
            if customer.invoice_settings
            else None
        ),
    }


@billing_router.post("/webhook")
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(..., alias="Stripe-Signature"),
) -> dict[str, Any]:
    """
    Stripe webhook endpoint.
    Register URL in Stripe dashboard: https://api.codemed.com/v1/billing/webhook
    """
    payload = await request.body()
    try:
        return handle_webhook_event(payload, stripe_signature)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")
