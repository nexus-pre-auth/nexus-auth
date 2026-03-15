"""
WebPT webhook registration.

Registers a webhook with the WebPT API so that real-time claim events
are pushed to our callback URL after the historical sync is complete.

Supported event types:
  - claim.created
  - claim.updated
  - claim.submitted
"""

import logging
import os
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

WEBPT_API_BASE    = os.getenv("WEBPT_API_BASE", "https://api.webpt.com/v1")
WEBHOOK_CALLBACK  = os.getenv("WEBPT_WEBHOOK_CALLBACK_URL", "")
WEBHOOK_SECRET    = os.getenv("WEBPT_WEBHOOK_SECRET", "")

WEBHOOK_EVENTS = [
    "claim.created",
    "claim.updated",
    "claim.submitted",
]


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15), reraise=True)
def register_webhook(access_token: str, practice_id: Optional[str] = None) -> str:
    """
    Register a webhook with WebPT and return the webhook ID.
    Raises requests.HTTPError on failure.
    """
    payload = {
        "url": WEBHOOK_CALLBACK,
        "events": WEBHOOK_EVENTS,
        "secret": WEBHOOK_SECRET,
    }
    if practice_id:
        payload["practice_id"] = practice_id

    resp = requests.post(
        f"{WEBPT_API_BASE}/webhooks",
        json=payload,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    webhook_id = str(data.get("id") or data.get("webhook_id", ""))
    logger.info("Registered WebPT webhook %s for events %s", webhook_id, WEBHOOK_EVENTS)
    return webhook_id


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15), reraise=True)
def delete_webhook(access_token: str, webhook_id: str) -> None:
    """Deregister a webhook (called on disconnect)."""
    resp = requests.delete(
        f"{WEBPT_API_BASE}/webhooks/{webhook_id}",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    resp.raise_for_status()
    logger.info("Deleted WebPT webhook %s", webhook_id)
