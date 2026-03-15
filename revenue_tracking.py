"""
Revenue Tracking — 20 % fee on every recovered dollar.

Responsibilities:
  • record_payment()        — called when a payer pays a recovered claim
  • rollup_monthly()        — aggregate a clinic's revenue into revenue_shares
  • get_clinic_summary()    — current pipeline value for a clinic
  • get_monthly_report()    — invoice-ready breakdown for a given month
"""

import logging
from datetime import date, datetime
from typing import Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

FEE_PERCENTAGE = 0.20


class RevenueTracker:
    def __init__(self, db_conn: psycopg2.extensions.connection):
        self._conn = db_conn

    # ------------------------------------------------------------------
    # Record a payment from the payer
    # ------------------------------------------------------------------

    def record_payment(self, denial_id: str, paid_amount: float) -> dict:
        """
        Mark a denial as paid and split the revenue 80/20.
        Returns the split breakdown.
        """
        your_fee   = round(paid_amount * FEE_PERCENTAGE, 2)
        clinic_net = round(paid_amount - your_fee, 2)

        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE recoverable_denials
                SET status     = 'paid',
                    paid_at    = NOW(),
                    paid_amount = %s,
                    your_fee   = %s,
                    clinic_net = %s
                WHERE id = %s
                RETURNING clinic_id
                """,
                (paid_amount, your_fee, clinic_net, denial_id),
            )
            row = cur.fetchone()
        self._conn.commit()

        if row is None:
            raise ValueError(f"Denial {denial_id} not found")

        clinic_id = row[0]
        self.rollup_monthly(clinic_id, date.today().replace(day=1))

        logger.info(
            "Payment recorded for denial %s — paid $%.2f, fee $%.2f, clinic $%.2f",
            denial_id, paid_amount, your_fee, clinic_net,
        )
        return {
            "denial_id": denial_id,
            "paid_amount": paid_amount,
            "your_fee": your_fee,
            "clinic_net": clinic_net,
        }

    # ------------------------------------------------------------------
    # Monthly rollup
    # ------------------------------------------------------------------

    def rollup_monthly(self, clinic_id: str, period_month: date) -> dict:
        """
        Aggregate all paid denials for a clinic + month into revenue_shares.
        Safe to call multiple times (upsert).
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(paid_amount), 0),
                    COALESCE(SUM(your_fee), 0),
                    COALESCE(SUM(clinic_net), 0),
                    COUNT(*)
                FROM recoverable_denials
                WHERE clinic_id = %s
                  AND status = 'paid'
                  AND DATE_TRUNC('month', paid_at) = DATE_TRUNC('month', %s::date)
                """,
                (clinic_id, period_month),
            )
            total_recovered, your_fee, clinic_payout, count = cur.fetchone()

            cur.execute(
                """
                INSERT INTO revenue_shares
                    (clinic_id, period_month, total_recovered, your_fee, clinic_payout, denial_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (clinic_id, period_month) DO UPDATE SET
                    total_recovered = EXCLUDED.total_recovered,
                    your_fee        = EXCLUDED.your_fee,
                    clinic_payout   = EXCLUDED.clinic_payout,
                    denial_count    = EXCLUDED.denial_count,
                    updated_at      = NOW()
                RETURNING id
                """,
                (clinic_id, period_month, total_recovered, your_fee, clinic_payout, count),
            )
        self._conn.commit()

        return {
            "clinic_id": clinic_id,
            "period_month": period_month.isoformat(),
            "total_recovered": float(total_recovered),
            "your_fee": float(your_fee),
            "clinic_payout": float(clinic_payout),
            "denial_count": int(count),
        }

    # ------------------------------------------------------------------
    # Clinic summary (dashboard / pipeline view)
    # ------------------------------------------------------------------

    def get_clinic_summary(self, clinic_id: str) -> dict:
        """
        Return the full pipeline value for a clinic across all statuses.
        This is what you show on the clinic's dashboard.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)                                            AS total_denials,
                    COALESCE(SUM(billed_amount), 0)                    AS total_billed,
                    COALESCE(SUM(estimated_recovery), 0)               AS total_potential,

                    -- by denial code
                    COUNT(*) FILTER (WHERE denial_code = 'CO-16')      AS co16_count,
                    COALESCE(SUM(estimated_recovery) FILTER
                        (WHERE denial_code = 'CO-16'), 0)              AS co16_value,

                    COUNT(*) FILTER (WHERE denial_code = 'CO-50')      AS co50_count,
                    COALESCE(SUM(estimated_recovery) FILTER
                        (WHERE denial_code = 'CO-50'), 0)              AS co50_value,

                    COUNT(*) FILTER (WHERE denial_code = 'CO-97')      AS co97_count,
                    COALESCE(SUM(estimated_recovery) FILTER
                        (WHERE denial_code = 'CO-97'), 0)              AS co97_value,

                    -- by status
                    COUNT(*) FILTER (WHERE status = 'detected')        AS detected_count,
                    COUNT(*) FILTER (WHERE status = 'fixed')           AS fixed_count,
                    COUNT(*) FILTER (WHERE status = 'submitted')       AS submitted_count,
                    COUNT(*) FILTER (WHERE status = 'paid')            AS paid_count,

                    -- realised revenue
                    COALESCE(SUM(paid_amount) FILTER
                        (WHERE status = 'paid'), 0)                    AS total_recovered,
                    COALESCE(SUM(your_fee) FILTER
                        (WHERE status = 'paid'), 0)                    AS fees_earned,
                    COALESCE(SUM(clinic_net) FILTER
                        (WHERE status = 'paid'), 0)                    AS clinic_net
                FROM recoverable_denials
                WHERE clinic_id = %s
                """,
                (clinic_id,),
            )
            row = cur.fetchone()

        (
            total_denials, total_billed, total_potential,
            co16_count, co16_value, co50_count, co50_value, co97_count, co97_value,
            detected, fixed, submitted, paid,
            total_recovered, fees_earned, clinic_net,
        ) = row

        return {
            "clinic_id": clinic_id,
            "summary": {
                "total_denials":    int(total_denials),
                "total_billed":     float(total_billed),
                "total_potential":  float(total_potential),   # if all estimated recoveries land
                "total_recovered":  float(total_recovered),   # actual cash collected
                "fees_earned":      float(fees_earned),
                "clinic_net":       float(clinic_net),
            },
            "by_code": {
                "CO-16": {"count": int(co16_count), "value": float(co16_value)},
                "CO-50": {"count": int(co50_count), "value": float(co50_value)},
                "CO-97": {"count": int(co97_count), "value": float(co97_value)},
            },
            "pipeline": {
                "detected":  int(detected),
                "fixed":     int(fixed),
                "submitted": int(submitted),
                "paid":      int(paid),
            },
        }

    # ------------------------------------------------------------------
    # Monthly invoice report
    # ------------------------------------------------------------------

    def get_monthly_report(self, clinic_id: str, year: int, month: int) -> dict:
        """Invoice-ready breakdown for a specific month."""
        period = date(year, month, 1)

        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Summary row
            cur.execute(
                """
                SELECT * FROM revenue_shares
                WHERE clinic_id = %s AND period_month = %s
                """,
                (clinic_id, period),
            )
            share = cur.fetchone()

            # Line items
            cur.execute(
                """
                SELECT webpt_claim_id, denial_code, billed_amount,
                       paid_amount, your_fee, clinic_net, paid_at
                FROM recoverable_denials
                WHERE clinic_id = %s
                  AND status = 'paid'
                  AND DATE_TRUNC('month', paid_at) = %s
                ORDER BY paid_at
                """,
                (clinic_id, period),
            )
            line_items = [dict(r) for r in cur.fetchall()]

        return {
            "clinic_id": clinic_id,
            "period": period.isoformat(),
            "summary": dict(share) if share else {
                "total_recovered": 0, "your_fee": 0, "clinic_payout": 0, "denial_count": 0
            },
            "line_items": line_items,
        }
