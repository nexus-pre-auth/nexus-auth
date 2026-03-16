"""
2026 CMS Regulatory Mandates
============================
Structured reference data for CMS rules effective in 2026.

Used by knowledge-base/seeder.py to populate knowledge_documents so these
mandates are automatically cited in CO-50 medical necessity appeal letters.

Sources:
  CMS Final Rule 2026 (MA/Part D)
  CMS-1832-F (MIPS/HIPAA Security Rule)
  Q4 2024 FFS data release
"""

# CPT codes most directly affected by 2026 MA payment and FFS changes.
# Broad PT/rehab set so mandates surface in CO-50 lookups for these claims.
_PT_CPT_CODES = [
    "97110", "97112", "97116", "97140", "97150",
    "97530", "97535", "97542", "97750", "97755",
    "97760", "97761", "97763",
]

# All MA payer codes in the system — mandate applies to all of them.
_MA_PAYER_CODES = ["CMS", "UHC", "AETNA", "BCBS"]


# ---------------------------------------------------------------------------
# 2026 mandate records
# Each entry maps to one row in knowledge_documents.
# ---------------------------------------------------------------------------

MANDATES_2026 = [
    {
        "key": "ma_payment_increase_2026",
        "title": "2026 Medicare Advantage Payment Rate — 5.06% Average Increase (CMS Final Rule)",
        "document_type": "coverage_determination",
        "policy_number": "CMS-MAPD-2026",
        "effective_date": "2026-01-01",
        "content_text": (
            "CMS issued its 2026 Medicare Advantage Final Rule establishing an average 5.06% "
            "payment increase for Medicare Advantage plans. This increase reflects the revised "
            "effective growth rate of 9.04% (up from the 5.93% initial estimate), based on "
            "Q4 2024 fee-for-service (FFS) data. Plans receiving these higher benchmark payments "
            "are expected to maintain or expand covered benefits for enrollees. Denial of "
            "medically necessary services covered under the enrollee's plan benefits is "
            "inconsistent with the plan's fiduciary obligations under the Final Rule."
        ),
        "content_summary": (
            "MA plans receive 5.06% avg payment increase for 2026 per CMS Final Rule. "
            "Higher benchmark payments support maintaining covered benefits."
        ),
        "cpt_codes": _PT_CPT_CODES,
        "specialties": ["physical_therapy", "occupational_therapy", "rehabilitation"],
        "payer_codes": _MA_PAYER_CODES,
        "confidence_score": 0.97,
        "metadata": {
            "value": "5.06%",
            "effective_growth_rate": "9.04%",
            "baseline_egr": "5.93%",
            "source": "CMS Final Rule 2026",
            "data_basis": "Q4 2024 FFS data",
        },
    },
    {
        "key": "medical_education_phase_out_2026",
        "title": "2026 FFS Normalization — Medical Education Cost Removal (100% Phase-Out Complete)",
        "document_type": "coverage_determination",
        "policy_number": "CMS-FFS-MEDED-2026",
        "effective_date": "2026-01-01",
        "content_text": (
            "The 2024–2026 phase-out of indirect medical education (IME) costs from FFS "
            "calculations reaches 100% removal in 2026. This normalization lowers the FFS "
            "benchmark used to calculate MA benchmarks, which CMS has offset through the "
            "5.06% rate increase. For purposes of medical necessity review, the removal of "
            "IME costs from FFS calculations does not alter coverage criteria for physical "
            "therapy and rehabilitation services under LCDs and NCDs — those criteria remain "
            "governed by the applicable local and national coverage determinations."
        ),
        "content_summary": (
            "IME costs fully removed from FFS calculations in 2026. Coverage criteria for "
            "PT/rehab services remain governed by applicable LCDs and NCDs."
        ),
        "cpt_codes": _PT_CPT_CODES,
        "specialties": ["physical_therapy", "occupational_therapy", "rehabilitation"],
        "payer_codes": _MA_PAYER_CODES,
        "confidence_score": 0.93,
        "metadata": {
            "phase_in_years": "2024-2026",
            "adjustment": "100% removal",
            "source": "CMS Final Rule 2026",
        },
    },
    {
        "key": "mips_hipaa_attestation_2026",
        "title": "2026 MIPS Requirement — HIPAA Security Rule Attestation (CMS-1832-F)",
        "document_type": "billing_guidelines",
        "policy_number": "CMS-1832-F",
        "effective_date": "2026-01-01",
        "content_text": (
            "Under CMS rule CMS-1832-F, MIPS-eligible clinicians are required to attest to "
            "completion of a formal HIPAA Security Rule risk analysis as part of their 2026 "
            "MIPS reporting. Failure to complete and document a formal risk analysis before "
            "the attestation deadline may result in MIPS payment adjustments. This requirement "
            "applies to all clinicians submitting claims under MIPS, including physical "
            "therapists who qualify as MIPS-eligible clinicians. Practices should consult "
            "their compliance officer or a qualified security professional to conduct and "
            "document the risk analysis."
        ),
        "content_summary": (
            "MIPS-eligible clinicians must attest to a formal HIPAA Security Rule risk "
            "analysis in 2026 under CMS-1832-F. Non-compliance risks payment adjustment."
        ),
        "cpt_codes": _PT_CPT_CODES,
        "specialties": ["physical_therapy", "occupational_therapy", "rehabilitation"],
        "payer_codes": ["CMS"],
        "confidence_score": 0.95,
        "metadata": {
            "rule": "CMS-1832-F",
            "program": "MIPS",
            "deadline": "2026",
            "action_required": "Formal HIPAA Security Rule risk analysis",
        },
    },
    {
        "key": "part_d_redesign_2026",
        "title": "2026 Part D Benefit Redesign — Final Instructions Issued",
        "document_type": "coverage_determination",
        "policy_number": "CMS-PARTD-2026",
        "effective_date": "2026-01-01",
        "content_text": (
            "CMS has issued final instructions for the continued 2026 Part D benefit redesign "
            "following the Inflation Reduction Act provisions. The redesign restructures "
            "cost-sharing thresholds and manufacturer discount obligations under the Part D "
            "drug benefit. These changes do not directly affect medical (Part B) coverage "
            "criteria for physical therapy or rehabilitation services, but may affect "
            "bundled care arrangements and integrated MA-PD plans' overall cost structures."
        ),
        "content_summary": (
            "Part D benefit redesign continues in 2026 per IRA provisions. Does not alter "
            "Part B coverage criteria for PT/rehab services."
        ),
        "cpt_codes": [],   # Not directly CPT-relevant — broad regulatory context only
        "specialties": ["physical_therapy", "occupational_therapy"],
        "payer_codes": _MA_PAYER_CODES,
        "confidence_score": 0.80,
        "metadata": {
            "status": "Final instructions issued",
            "basis": "Inflation Reduction Act",
            "source": "CMS MAPD Final Rule 2026",
        },
    },
]
