"""
CodeMed AI — Automated Prior Authorization Appeals Generator
=============================================================
Generates formal, citation-backed prior authorization appeal letters using
specific CMS LCD/NCD IDs, clinical evidence, and patient data.

The appeals generator:
  1. Accepts a denial scenario (patient info, denied codes, denial reason)
  2. Looks up the relevant LCD/NCD policy from the knowledge layer (or uses
     built-in CMS policy references for offline mode)
  3. Constructs a structured appeal letter with:
     - Patient and claim identifiers
     - Specific LCD/NCD citations (policy title + ID + effective date)
     - Medical necessity arguments based on the policy criteria
     - MEAT evidence excerpts supporting the diagnosis
     - Regulatory citations (42 CFR, Social Security Act)
  4. Returns a formal letter in plain text (suitable for PDF rendering)

Target: Appeal letters generated in seconds with specific LCD/NCD IDs,
achieving defensible documentation for the 94% audit defensibility target.
"""

from __future__ import annotations

import logging
import textwrap
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Built-in LCD/NCD policy index
# For online mode, these are pulled from the knowledge layer DB.
# Offline mode uses this curated subset of high-volume policies.
# ---------------------------------------------------------------------------

POLICY_INDEX: dict[str, dict] = {
    # Cardiac
    "L33822": {
        "title": "Cardiac Event Monitors",
        "type": "LCD",
        "mac": "Noridian Healthcare Solutions",
        "effective_date": "2016-10-01",
        "icd10_covered": ["I48.0", "I48.11", "I48.19", "I49.9", "R00.0", "R00.1"],
        "cpt_covered": ["93224", "93225", "93226", "93268", "93270", "93271", "93272"],
        "criteria": (
            "Coverage is provided when the beneficiary has symptoms suggestive of "
            "cardiac arrhythmia (palpitations, presyncope, syncope, chest pain, or "
            "shortness of breath) occurring infrequently (>24 hours between episodes), "
            "OR monitoring is required to regulate antiarrhythmic medication, OR "
            "monitoring is required for stroke/TIA patients to detect atrial fibrillation."
        ),
        "url": "https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid=33822",
    },
    "L34869": {
        "title": "Implantable Cardiac Monitors",
        "type": "LCD",
        "mac": "CGS Administrators",
        "effective_date": "2019-01-01",
        "icd10_covered": ["I48.0", "I63.9", "R55", "R00.1"],
        "cpt_covered": ["33285", "33286", "93285", "93291"],
        "criteria": (
            "Implantable cardiac monitoring is covered for unexplained syncope, "
            "cryptogenic stroke evaluation, or atrial fibrillation detection in "
            "patients with confirmed stroke when non-invasive methods are inconclusive."
        ),
        "url": "https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid=34869",
    },
    # Spine / orthopedics
    "L33836": {
        "title": "Lumbar Spinal Fusion",
        "type": "LCD",
        "mac": "First Coast Service Options",
        "effective_date": "2016-10-01",
        "icd10_covered": ["M51.16", "M51.17", "M47.816", "M47.817", "M43.16"],
        "cpt_covered": ["22612", "22630", "22633", "22558"],
        "criteria": (
            "Lumbar fusion is medically necessary when the patient has failed at least "
            "6 weeks of conservative management (physical therapy, medications), AND "
            "imaging confirms structural pathology consistent with the clinical diagnosis, "
            "AND the patient has significant functional impairment."
        ),
        "url": "https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid=33836",
    },
    # CPAP / sleep
    "L33718": {
        "title": "Continuous Positive Airway Pressure (CPAP) Therapy",
        "type": "LCD",
        "mac": "Palmetto GBA",
        "effective_date": "2015-10-01",
        "icd10_covered": ["G47.33", "G47.30"],
        "cpt_covered": [],
        "hcpcs_covered": ["E0601", "E0470", "A7027", "A7028", "A7029"],
        "criteria": (
            "CPAP is covered for beneficiaries with a diagnosis of obstructive sleep "
            "apnea documented by a sleep test (PSG or HST) with AHI ≥15, OR AHI ≥5 with "
            "documented symptoms of excessive daytime sleepiness, impaired cognition, "
            "mood disorders, insomnia, or hypertension/cardiovascular disease/stroke. "
            "Coverage continues if compliance is documented (≥4 hours/night for 70% of "
            "nights in any consecutive 30-day period)."
        ),
        "url": "https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?lcdid=33718",
    },
    # NCD: Dialysis
    "110.3": {
        "title": "End Stage Renal Disease (ESRD) — Dialysis Services",
        "type": "NCD",
        "effective_date": "2008-04-07",
        "icd10_covered": ["N18.6", "Z99.2"],
        "cpt_covered": ["90935", "90937", "90945", "90947"],
        "criteria": (
            "Medicare covers dialysis services for beneficiaries with ESRD who have "
            "received a regular course of dialysis (minimum 3 times per week) or "
            "who are being prepared for a kidney transplant. Coverage includes "
            "hemodialysis, peritoneal dialysis, and continuous ambulatory peritoneal dialysis."
        ),
        "url": "https://www.cms.gov/medicare-coverage-database/view/ncd.aspx?ncdid=110.3",
    },
    # NCD: Bone density
    "150.3": {
        "title": "Bone Mass Measurements",
        "type": "NCD",
        "effective_date": "1998-07-01",
        "icd10_covered": ["M81.0", "M85.80", "Z87.310"],
        "cpt_covered": ["76977", "77080", "77081", "77083"],
        "criteria": (
            "Bone mass measurement is covered once every 24 months (or more frequently "
            "if medically necessary) for: estrogen-deficient women at clinical risk for "
            "osteoporosis, individuals with vertebral abnormalities, recipients of long-term "
            "glucocorticoid therapy, primary hyperparathyroidism, or individuals being "
            "monitored to assess the response to or efficacy of approved osteoporosis drug therapy."
        ),
        "url": "https://www.cms.gov/medicare-coverage-database/view/ncd.aspx?ncdid=150.3",
    },
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class DenialScenario:
    """Input to the appeals generator describing a prior auth denial."""
    # Patient info
    patient_name: str
    patient_dob: str
    patient_id: str
    insurance_member_id: str

    # Claim info
    provider_name: str
    provider_npi: str
    service_date: str
    claim_number: str

    # Denied items
    denied_cpt_codes: list[str]
    diagnosis_codes: list[str]          # Supporting ICD-10 codes

    # Denial details
    denial_reason: str                  # Payer's denial reason
    denial_date: str
    payer_name: str

    # Clinical support
    clinical_summary: str               # Brief clinical summary
    meat_evidence: list[str] = field(default_factory=list)  # MEAT evidence quotes
    policy_ids: list[str] = field(default_factory=list)     # Specific LCD/NCD IDs


@dataclass
class AppealLetter:
    """Generated prior authorization appeal letter."""
    letter_text: str
    policy_citations: list[dict]    # List of {policy_id, title, url}
    regulatory_citations: list[str]
    generated_at: str
    word_count: int

    def to_dict(self) -> dict:
        return {
            "generated_at": self.generated_at,
            "word_count": self.word_count,
            "policy_citations": self.policy_citations,
            "regulatory_citations": self.regulatory_citations,
            "letter_text": self.letter_text,
        }


# ---------------------------------------------------------------------------
# Appeals Generator
# ---------------------------------------------------------------------------

class AppealsGenerator:
    """
    Generates formal prior authorization appeal letters with LCD/NCD citations.

    Usage:
        generator = AppealsGenerator()
        scenario = DenialScenario(
            patient_name="Jane Doe",
            ...
        )
        letter = generator.generate(scenario)
        print(letter.letter_text)
    """

    def __init__(self, policy_index: dict | None = None, db_conn=None):
        self._policy_index = policy_index or POLICY_INDEX
        self._db_conn = db_conn
        logger.info(
            "AppealsGenerator initialised: %d policies in index",
            len(self._policy_index),
        )

    # ── Public API ────────────────────────────────────────────────────────

    def generate(
        self,
        scenario: DenialScenario,
        include_meat: bool = True,
    ) -> AppealLetter:
        """
        Generate a formal prior authorization appeal letter.

        Args:
            scenario:     DenialScenario describing the denied claim
            include_meat: Whether to include MEAT evidence quotes (default True)

        Returns:
            AppealLetter with full letter text and citation metadata
        """
        # Resolve applicable policies
        policies = self._resolve_policies(scenario)

        # Build citation lists
        policy_citations = [
            {
                "policy_id": pid,
                "title": pol["title"],
                "type": pol["type"],
                "url": pol.get("url", ""),
                "effective_date": pol.get("effective_date", ""),
            }
            for pid, pol in policies.items()
        ]

        regulatory_citations = self._get_regulatory_citations(scenario, policies)

        # Generate the letter
        letter_text = self._compose_letter(
            scenario=scenario,
            policies=policies,
            regulatory_citations=regulatory_citations,
            include_meat=include_meat,
        )

        letter = AppealLetter(
            letter_text=letter_text,
            policy_citations=policy_citations,
            regulatory_citations=regulatory_citations,
            generated_at=datetime.utcnow().isoformat(),
            word_count=len(letter_text.split()),
        )

        logger.info(
            "Appeal letter generated: %d words, %d policy citations",
            letter.word_count,
            len(policy_citations),
        )
        return letter

    def find_applicable_policies(
        self,
        cpt_codes: list[str],
        icd10_codes: list[str],
    ) -> dict[str, dict]:
        """
        Find CMS LCD/NCD policies applicable to given codes.

        Returns dict of {policy_id: policy_dict}
        """
        applicable: dict[str, dict] = {}

        for policy_id, policy in self._policy_index.items():
            cpt_match = any(
                c in policy.get("cpt_covered", []) for c in cpt_codes
            )
            icd_match = any(
                c in policy.get("icd10_covered", []) for c in icd10_codes
            )
            hcpcs_match = any(
                c in policy.get("hcpcs_covered", []) for c in cpt_codes
            )

            if cpt_match or icd_match or hcpcs_match:
                applicable[policy_id] = policy

        return applicable

    # ── Private ───────────────────────────────────────────────────────────

    def _resolve_policies(self, scenario: DenialScenario) -> dict[str, dict]:
        """Find all applicable policies for this denial scenario."""
        # Specific policy IDs provided by caller
        policies: dict[str, dict] = {}
        for pid in scenario.policy_ids:
            if pid in self._policy_index:
                policies[pid] = self._policy_index[pid]

        # Auto-detect applicable policies if none specified
        if not policies:
            policies = self.find_applicable_policies(
                cpt_codes=scenario.denied_cpt_codes,
                icd10_codes=scenario.diagnosis_codes,
            )

        # If DB is available, enrich with DB policies
        if self._db_conn and not policies:
            policies.update(self._fetch_policies_from_db(
                scenario.denied_cpt_codes,
                scenario.diagnosis_codes,
            ))

        return policies

    def _fetch_policies_from_db(
        self,
        cpt_codes: list[str],
        icd10_codes: list[str],
    ) -> dict[str, dict]:
        """Fetch applicable policies from the knowledge layer DB."""
        try:
            policies: dict[str, dict] = {}
            with self._db_conn.cursor() as cur:
                # Search knowledge_documents for relevant LCDs/NCDs
                cur.execute(
                    """
                    SELECT lcd_id, ncd_id, title, source_url, effective_date,
                           cpt_codes, icd10_codes, content_summary
                    FROM knowledge_documents
                    WHERE document_type IN ('lcd', 'ncd')
                      AND is_active = TRUE
                      AND (
                          cpt_codes && %s::text[]
                          OR icd10_codes && %s::text[]
                      )
                    LIMIT 5
                    """,
                    (cpt_codes, icd10_codes),
                )
                rows = cur.fetchall()
                for row in rows:
                    lcd_id, ncd_id, title, url, eff_date, cpts, icd10s, summary = row
                    pid = lcd_id or ncd_id or "UNKNOWN"
                    policies[pid] = {
                        "title": title,
                        "type": "LCD" if lcd_id else "NCD",
                        "effective_date": str(eff_date) if eff_date else "",
                        "url": url,
                        "cpt_covered": cpts or [],
                        "icd10_covered": icd10s or [],
                        "criteria": summary or "",
                    }
            return policies
        except Exception as exc:
            logger.warning("DB policy fetch failed: %s", exc)
            return {}

    def _get_regulatory_citations(
        self,
        scenario: DenialScenario,
        policies: dict[str, dict],
    ) -> list[str]:
        """Generate applicable regulatory citations."""
        citations = [
            "Section 1862(a)(1)(A) of the Social Security Act "
            "(coverage of medically reasonable and necessary services)",
            "42 CFR § 411.15(k)(1) — Particular services excluded from coverage",
            "CMS Medicare Benefit Policy Manual, Chapter 15 — Covered Medical and Other "
            "Health Services",
            "CMS Medicare Claims Processing Manual, Chapter 30 — Financial Liability "
            "Protections",
        ]

        # Add policy-specific regulatory references
        for pid, policy in policies.items():
            if policy.get("type") == "LCD":
                citations.append(
                    f"CMS Local Coverage Determination {pid}: "
                    f"{policy['title']} (effective {policy.get('effective_date', 'N/A')})"
                )
            elif policy.get("type") == "NCD":
                citations.append(
                    f"CMS National Coverage Determination {pid}: "
                    f"{policy['title']} (effective {policy.get('effective_date', 'N/A')})"
                )

        return citations

    def _compose_letter(
        self,
        scenario: DenialScenario,
        policies: dict[str, dict],
        regulatory_citations: list[str],
        include_meat: bool,
    ) -> str:
        """Compose the full appeal letter text."""
        today = date.today().strftime("%B %d, %Y")

        lines: list[str] = []

        # Header
        lines += [
            today,
            "",
            f"{scenario.payer_name}",
            "Medical Director / Prior Authorization Appeals Department",
            "",
            f"RE: FORMAL APPEAL — Prior Authorization Denial",
            f"    Patient: {scenario.patient_name} (DOB: {scenario.patient_dob})",
            f"    Member ID: {scenario.insurance_member_id}",
            f"    Claim Number: {scenario.claim_number}",
            f"    Denied CPT Code(s): {', '.join(scenario.denied_cpt_codes)}",
            f"    Date(s) of Service: {scenario.service_date}",
            f"    Denial Date: {scenario.denial_date}",
            "",
            "Dear Medical Director,",
            "",
        ]

        # Opening paragraph
        lines += [
            textwrap.fill(
                f"On behalf of {scenario.provider_name} (NPI: {scenario.provider_npi}), "
                f"we respectfully submit this formal appeal of the above-referenced prior "
                f"authorization denial for patient {scenario.patient_name}. The denial "
                f"was issued on {scenario.denial_date} with the stated reason: "
                f'"{scenario.denial_reason}." We submit that the requested services are '
                f"medically necessary and meet all applicable Medicare coverage criteria, "
                f"as detailed below.",
                width=80,
            ),
            "",
        ]

        # Clinical summary
        lines += [
            "I. CLINICAL SUMMARY",
            "-" * 40,
            textwrap.fill(scenario.clinical_summary, width=80),
            "",
        ]

        # Diagnosis codes
        lines += [
            "II. SUPPORTING DIAGNOSIS CODES",
            "-" * 40,
        ]
        for code in scenario.diagnosis_codes:
            lines.append(f"    • {code}")
        lines.append("")

        # Policy citations
        if policies:
            lines += [
                "III. APPLICABLE CMS COVERAGE POLICIES",
                "-" * 40,
            ]
            for policy_id, policy in policies.items():
                policy_type = policy.get("type", "LCD")
                title = policy.get("title", "")
                eff_date = policy.get("effective_date", "")
                url = policy.get("url", "")

                lines += [
                    f"    {policy_type} {policy_id}: {title}",
                    f"    Effective Date: {eff_date}",
                    f"    Source: {url}",
                    "",
                    "    Coverage Criteria Met:",
                ]
                criteria = policy.get("criteria", "")
                if criteria:
                    wrapped = textwrap.fill(criteria, width=76, initial_indent="    ",
                                           subsequent_indent="    ")
                    lines.append(wrapped)

                # Cross-reference covered codes
                covered_cpt = policy.get("cpt_covered", [])
                denied_covered = [c for c in scenario.denied_cpt_codes if c in covered_cpt]
                if denied_covered:
                    lines.append(f"\n    This policy explicitly covers CPT code(s): "
                                 f"{', '.join(denied_covered)}")
                lines.append("")
        else:
            lines += [
                "III. MEDICAL NECESSITY",
                "-" * 40,
                textwrap.fill(
                    "The requested services are medically necessary pursuant to "
                    "Section 1862(a)(1)(A) of the Social Security Act, which requires "
                    "coverage of services that are reasonable and necessary for the "
                    "diagnosis or treatment of illness or injury.",
                    width=80,
                ),
                "",
            ]

        # MEAT evidence
        if include_meat and scenario.meat_evidence:
            lines += [
                "IV. DOCUMENTATION SUPPORTING MEDICAL NECESSITY (MEAT EVIDENCE)",
                "-" * 40,
                "The following excerpts from the medical record demonstrate that the "
                "patient's condition was actively Monitored, Evaluated, Assessed, "
                "and Treated as required for defensible ICD-10 coding:",
                "",
            ]
            for i, quote in enumerate(scenario.meat_evidence[:5], 1):
                wrapped = textwrap.fill(
                    f"{i}. {quote}",
                    width=80, initial_indent="    ", subsequent_indent="       ",
                )
                lines.append(wrapped)
            lines.append("")

        # Regulatory citations
        lines += [
            "V. REGULATORY CITATIONS",
            "-" * 40,
        ]
        for citation in regulatory_citations:
            lines.append(textwrap.fill(
                f"  • {citation}", width=80, subsequent_indent="    "
            ))
        lines.append("")

        # Closing
        lines += [
            "VI. REQUESTED ACTION",
            "-" * 40,
            textwrap.fill(
                f"We respectfully request that {scenario.payer_name} overturn the denial "
                f"and approve prior authorization for CPT code(s) "
                f"{', '.join(scenario.denied_cpt_codes)} for patient "
                f"{scenario.patient_name}. The clinical evidence, applicable CMS coverage "
                f"policies, and regulatory requirements presented above clearly demonstrate "
                f"that the requested services are medically necessary and covered under "
                f"the patient's benefit plan.",
                width=80,
            ),
            "",
            textwrap.fill(
                "If you require additional clinical documentation, please contact our "
                "office within 5 business days of receiving this appeal. We are prepared "
                "to provide complete medical records, operative notes, or direct peer-to-peer "
                "consultation with the reviewing medical director.",
                width=80,
            ),
            "",
            "Sincerely,",
            "",
            scenario.provider_name,
            f"NPI: {scenario.provider_npi}",
            "",
            "_" * 40,
            "Signature",
            "",
            f"Date: {today}",
            "",
            "— Generated by CodeMed AI Appeals Engine —",
        ]

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI test harness
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    generator = AppealsGenerator()

    scenario = DenialScenario(
        patient_name="Robert Johnson",
        patient_dob="1955-03-14",
        patient_id="PT-2024-08871",
        insurance_member_id="1EG4-TE5-MK72",
        provider_name="Cardiology Associates of Springfield",
        provider_npi="1234567890",
        service_date="2024-11-15",
        claim_number="CLM-2024-449821",
        denied_cpt_codes=["93224", "93268"],
        diagnosis_codes=["I48.0", "R00.1"],
        denial_reason="Medical necessity not established for extended cardiac monitoring",
        denial_date="2024-11-28",
        payer_name="UnitedHealthcare",
        clinical_summary=(
            "Patient is a 69-year-old male with a 3-month history of intermittent "
            "palpitations and one near-syncopal episode. Standard 12-lead ECG and "
            "24-hour Holter monitor were non-diagnostic. Clinical suspicion remains "
            "high for paroxysmal atrial fibrillation given risk factor profile including "
            "hypertension, obesity, and sleep apnea. Extended cardiac event monitoring "
            "is requested to capture arrhythmic events during symptomatic episodes."
        ),
        meat_evidence=[
            "MONITORING: Repeat ECG performed 2024-11-15; sinus rhythm, no acute changes.",
            "EVALUATION: Palpitations occur approximately every 5-7 days, lasting 15-30 "
            "minutes. Holter (2024-10-20) showed 1,247 PACs but no sustained arrhythmia.",
            "ASSESSMENT: Paroxysmal atrial fibrillation, likely — consistent with symptom "
            "frequency and negative 24-hour monitor. ICD-10: I48.0.",
            "TREATMENT: Metoprolol succinate 25mg daily started for rate control pending "
            "definitive rhythm diagnosis. Anticoagulation deferred pending monitoring results.",
        ],
        policy_ids=["L33822"],
    )

    letter = generator.generate(scenario)
    print(letter.letter_text)
    print(f"\n--- Generated: {letter.generated_at} | Words: {letter.word_count} ---")
    print(f"Policy citations: {json.dumps(letter.policy_citations, indent=2)}")
