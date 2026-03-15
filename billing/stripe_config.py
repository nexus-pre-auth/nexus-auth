"""
CodeMed AI — Stripe Configuration
"""
import os
import logging

import stripe

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Keys (loaded from environment — never hard-code)
# ---------------------------------------------------------------------------
STRIPE_SECRET_KEY: str = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_PUBLISHABLE_KEY: str = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")
STRIPE_WEBHOOK_SECRET: str = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

# ---------------------------------------------------------------------------
# Business metadata
# ---------------------------------------------------------------------------
BUSINESS_NAME: str = os.environ.get("BUSINESS_NAME", "CodeMed Inc.")
BUSINESS_EMAIL: str = os.environ.get("BUSINESS_EMAIL", "billing@codemed.com")


def configure_stripe() -> None:
    """Set Stripe API key. Call once at application startup."""
    if not STRIPE_SECRET_KEY:
        logger.warning(
            "STRIPE_SECRET_KEY is not set — Stripe billing will be unavailable."
        )
        return
    stripe.api_key = STRIPE_SECRET_KEY
    logger.info("Stripe configured (key prefix: %s...)", STRIPE_SECRET_KEY[:7])
