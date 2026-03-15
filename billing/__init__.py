"""
CodeMed AI — Billing Package
Stripe integration for clinic payment processing and invoice automation.
"""
from billing.stripe_config import configure_stripe, STRIPE_PUBLISHABLE_KEY
from billing.customers import create_customer, get_customer
from billing.payment_methods import create_setup_intent, attach_payment_method
from billing.invoices import create_monthly_invoice
from billing.webhooks import handle_webhook_event

__all__ = [
    "configure_stripe",
    "STRIPE_PUBLISHABLE_KEY",
    "create_customer",
    "get_customer",
    "create_setup_intent",
    "attach_payment_method",
    "create_monthly_invoice",
    "handle_webhook_event",
]
