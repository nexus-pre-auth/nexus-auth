"""
CodeMed AI — V28 HCC Hierarchy Enforcement Engine
==================================================
Implements CMS-HCC Model V28 disease hierarchy enforcement.

The V28 model groups ICD-10 codes into Hierarchical Condition Categories (HCCs).
When a patient qualifies for multiple HCCs in the same hierarchy group, only the
highest-severity (highest RAF weight) HCC is coded — lower ones are "trumped" and
suppressed.

This prevents "code stacking" (over-coding lower-acuity conditions when a
higher-severity condition is already present) and ensures defensible RAF scores.

Key capabilities:
  - Map ICD-10 codes → HCC numbers (V28 crosswalk)
  - Detect hierarchy conflicts within a condition group
  - Return the winning (highest-acuity) HCC per group
  - Provide audit trail of which codes were suppressed and why
  - Calculate pre/post RAF score impact

RAF Score basis: 2024 CMS-HCC V28 relative factor weights (community, full-benefit dual)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# V28 HCC Crosswalk: ICD-10 → HCC
# Subset of the official CMS V28 crosswalk covering the most clinically
# significant HCCs. Full file: https://www.cms.gov/medicare/health-plans/
# medicareadvtgspecratestats/risk-adjustors/2024-model-software
# ---------------------------------------------------------------------------

# Format: "ICD10_CODE": (hcc_number, description, hierarchy_group, raf_weight)
# hierarchy_group=None means the HCC is not in a hierarchy (standalone)

V28_ICD10_TO_HCC: dict[str, tuple[int, str, Optional[str], float]] = {
    # ── Diabetes ────────────────────────────────────────────────────────────
    "E10.10": (17,  "Type 1 Diabetes with Ketoacidosis",          "DIABETES", 0.302),
    "E10.11": (17,  "Type 1 Diabetes with Ketoacidosis w/Coma",   "DIABETES", 0.302),
    "E11.10": (17,  "Type 2 Diabetes with Ketoacidosis",          "DIABETES", 0.302),
    "E10.641":(17,  "Type 1 Diabetes with Hypoglycemia w/Coma",   "DIABETES", 0.302),
    "E11.641":(17,  "Type 2 Diabetes with Hypoglycemia w/Coma",   "DIABETES", 0.302),
    "E10.40": (18,  "Type 1 Diabetes with Diabetic Neuropathy",   "DIABETES", 0.179),
    "E10.51": (18,  "Type 1 Diabetes with Peripheral Angiopathy", "DIABETES", 0.179),
    "E11.40": (18,  "Type 2 Diabetes with Diabetic Neuropathy",   "DIABETES", 0.179),
    "E11.51": (18,  "Type 2 Diabetes with Peripheral Angiopathy", "DIABETES", 0.179),
    "E10.65": (18,  "Type 1 Diabetes with Hyperglycemia",         "DIABETES", 0.179),
    "E11.65": (18,  "Type 2 Diabetes with Hyperglycemia",         "DIABETES", 0.179),
    "E10.9":  (19,  "Type 1 Diabetes without Complications",      "DIABETES", 0.118),
    "E11.9":  (19,  "Type 2 Diabetes without Complications",      "DIABETES", 0.118),
    "E13.9":  (19,  "Other specified Diabetes without Compl.",    "DIABETES", 0.118),

    # ── Vascular Disease ─────────────────────────────────────────────────
    "I70.201":(108, "Atherosclerosis of native arteries, right leg","VASCULAR", 0.288),
    "I70.211":(108, "Atherosclerosis of native arteries w/rest pain","VASCULAR",0.288),
    "I70.231":(108, "Atherosclerosis w/ulceration right leg",      "VASCULAR", 0.288),
    "I73.9":  (109, "Peripheral vascular disease, unspecified",    "VASCULAR", 0.193),
    "I70.209":(109, "Atherosclerosis of native arteries, unspec.", "VASCULAR", 0.193),

    # ── Chronic Kidney Disease ───────────────────────────────────────────
    "N18.6":  (329, "End Stage Renal Disease",                     "CKD",     0.493),
    "Z99.2":  (329, "Dependence on renal dialysis",                "CKD",     0.493),
    "N18.5":  (330, "Chronic Kidney Disease, Stage 5",             "CKD",     0.289),
    "N18.4":  (331, "Chronic Kidney Disease, Stage 4",             "CKD",     0.200),
    "N18.32": (332, "Chronic Kidney Disease, Stage 3b",            "CKD",     0.137),
    "N18.31": (333, "Chronic Kidney Disease, Stage 3a",            "CKD",     0.074),
    "N18.2":  (334, "Chronic Kidney Disease, Stage 2",             "CKD",     0.000),
    "N18.1":  (334, "Chronic Kidney Disease, Stage 1",             "CKD",     0.000),

    # ── Heart Failure ───────────────────────────────────────────────────
    "I50.21": (224, "Acute Systolic Heart Failure",                "HEART_FAILURE", 0.368),
    "I50.23": (224, "Acute on Chronic Systolic Heart Failure",     "HEART_FAILURE", 0.368),
    "I50.31": (224, "Acute Diastolic Heart Failure",               "HEART_FAILURE", 0.368),
    "I50.33": (224, "Acute on Chronic Diastolic Heart Failure",    "HEART_FAILURE", 0.368),
    "I50.20": (225, "Unspecified Systolic Heart Failure",          "HEART_FAILURE", 0.259),
    "I50.22": (225, "Chronic Systolic Heart Failure",              "HEART_FAILURE", 0.259),
    "I50.30": (225, "Unspecified Diastolic Heart Failure",         "HEART_FAILURE", 0.259),
    "I50.32": (225, "Chronic Diastolic Heart Failure",             "HEART_FAILURE", 0.259),
    "I50.9":  (226, "Heart Failure, unspecified",                  "HEART_FAILURE", 0.201),

    # ── COPD / Asthma ───────────────────────────────────────────────────
    "J44.1":  (280, "COPD with acute exacerbation",                "COPD",    0.346),
    "J44.0":  (281, "COPD with acute lower respiratory infection", "COPD",    0.259),
    "J44.9":  (282, "COPD, unspecified",                           "COPD",    0.193),
    "J45.51": (282, "Severe persistent asthma with acute exac.",   "COPD",    0.193),

    # ── Liver Disease ────────────────────────────────────────────────────
    "K72.10": (27,  "Chronic Hepatic Failure without Coma",        "LIVER",   0.487),
    "K72.11": (27,  "Chronic Hepatic Failure with Coma",           "LIVER",   0.487),
    "K76.7":  (27,  "Hepatorenal Syndrome",                        "LIVER",   0.487),
    "K74.60": (28,  "Unspecified Cirrhosis of liver",              "LIVER",   0.352),
    "K74.69": (28,  "Other Cirrhosis of liver",                    "LIVER",   0.352),
    "K70.30": (28,  "Alcoholic Cirrhosis of liver without ascites","LIVER",   0.352),
    "K73.2":  (29,  "Chronic active hepatitis, NEC",               "LIVER",   0.221),
    "K74.0":  (29,  "Hepatic fibrosis",                            "LIVER",   0.221),
    "K73.9":  (29,  "Chronic hepatitis, unspecified",              "LIVER",   0.221),

    # ── Dementia ─────────────────────────────────────────────────────────
    "G30.9":  (52,  "Alzheimer's disease, unspecified",            "DEMENTIA",0.346),
    "G30.0":  (52,  "Alzheimer's disease with early onset",        "DEMENTIA",0.346),
    "F01.51": (52,  "Vascular dementia with behavioral disturb.",  "DEMENTIA",0.346),
    "F02.81": (52,  "Dementia in other diseases, w/behav. dist.",  "DEMENTIA",0.346),
    "F03.91": (53,  "Unspecified dementia with behavioral dist.",  "DEMENTIA",0.211),
    "G31.09": (53,  "Other frontotemporal dementia",               "DEMENTIA",0.211),
    "F03.90": (54,  "Unspecified dementia without behavioral",     "DEMENTIA",0.138),
    "G30.1":  (54,  "Alzheimer's disease with late onset",         "DEMENTIA",0.138),

    # ── Cancer ───────────────────────────────────────────────────────────
    "C61":    (12,  "Malignant neoplasm of prostate",              "CANCER",  0.147),
    "C50.911":(12,  "Malignant neoplasm of breast, unspec.",       "CANCER",  0.147),
    "C18.9":  (12,  "Malignant neoplasm of colon, unspecified",    "CANCER",  0.147),
    "C34.10": (11,  "Malignant neoplasm of upper lobe, bronchus",  "CANCER",  0.359),
    "C34.90": (11,  "Malignant neoplasm of bronchus and lung",     "CANCER",  0.359),
    "C25.9":  (11,  "Malignant neoplasm of pancreas, unspec.",     "CANCER",  0.359),
    "C16.9":  (11,  "Malignant neoplasm of stomach, unspec.",      "CANCER",  0.359),

    # ── Stroke / TIA ─────────────────────────────────────────────────────
    "I63.50": (167, "Cerebral infarction due to unspec. occlusion","STROKE",  0.353),
    "I63.9":  (167, "Cerebral infarction, unspecified",            "STROKE",  0.353),
    "I69.30": (168, "Sequelae of cerebral infarction",             "STROKE",  0.217),
    "I69.391":(168, "Other sequelae of cerebral infarction",       "STROKE",  0.217),

    # ── Major Depression ─────────────────────────────────────────────────
    "F32.2":  (155, "Major depressive disorder, single, severe",   "DEPRESSION",0.289),
    "F33.2":  (155, "Major depressive disorder, recurrent, severe","DEPRESSION",0.289),
    "F32.1":  (156, "Major depressive disorder, single, moderate", "DEPRESSION",0.193),
    "F33.1":  (156, "Major depressive disorder, recurrent, moderate","DEPRESSION",0.193),
    "F32.9":  (157, "Major depressive disorder, unspecified",      "DEPRESSION",0.099),
    "F33.9":  (157, "Recurrent depressive disorder, unspecified",  "DEPRESSION",0.099),
}

# ---------------------------------------------------------------------------
# HCC hierarchy ranks within each group (lower number = higher severity / wins)
# ---------------------------------------------------------------------------

HCC_GROUP_RANKS: dict[str, list[int]] = {
    "DIABETES":     [17, 18, 19],           # 17 trumps 18 trumps 19
    "VASCULAR":     [108, 109],
    "CKD":          [329, 330, 331, 332, 333, 334],
    "HEART_FAILURE":[224, 225, 226],
    "COPD":         [280, 281, 282],
    "LIVER":        [27, 28, 29],
    "DEMENTIA":     [52, 53, 54],
    "CANCER":       [11, 12],
    "STROKE":       [167, 168],
    "DEPRESSION":   [155, 156, 157],
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class HCCCode:
    """A single HCC with its source ICD-10 and metadata."""
    hcc_number: int
    hcc_description: str
    hierarchy_group: Optional[str]
    raf_weight: float
    source_icd10: str
    source_icd10_description: str
    status: str = "active"          # "active" | "trumped"
    trumped_by_hcc: Optional[int] = None


@dataclass
class HCCResult:
    """
    Output of the HCC hierarchy enforcement engine for a patient's code set.
    """
    # All HCCs found (before hierarchy enforcement)
    all_hccs: list[HCCCode] = field(default_factory=list)

    # Active HCCs after hierarchy enforcement (these should be coded)
    active_hccs: list[HCCCode] = field(default_factory=list)

    # HCCs suppressed because a higher-severity code was present
    suppressed_hccs: list[HCCCode] = field(default_factory=list)

    # ICD-10 codes with no HCC mapping
    unmapped_codes: list[str] = field(default_factory=list)

    # RAF score estimates
    raf_before_enforcement: float = 0.0   # Sum of all HCC weights (pre-hierarchy)
    raf_after_enforcement: float = 0.0    # Sum of active HCC weights only

    # Audit trail
    hierarchy_conflicts: list[dict] = field(default_factory=list)

    @property
    def raf_delta(self) -> float:
        """Difference in RAF score after hierarchy enforcement."""
        return self.raf_after_enforcement - self.raf_before_enforcement

    def summary(self) -> dict:
        return {
            "total_icd10_codes": len(self.all_hccs) + len(self.unmapped_codes),
            "hccs_found": len(self.all_hccs),
            "hccs_active": len(self.active_hccs),
            "hccs_suppressed": len(self.suppressed_hccs),
            "raf_before": round(self.raf_before_enforcement, 4),
            "raf_after": round(self.raf_after_enforcement, 4),
            "raf_delta": round(self.raf_delta, 4),
            "hierarchy_conflicts": len(self.hierarchy_conflicts),
            "unmapped_codes": self.unmapped_codes,
        }


# ---------------------------------------------------------------------------
# HCC Engine
# ---------------------------------------------------------------------------

class HCCEngine:
    """
    CMS-HCC Model V28 hierarchy enforcement engine.

    Usage:
        engine = HCCEngine()
        result = engine.enforce_hierarchy(["E11.9", "E11.40", "N18.4", "N18.5"])
        print(result.summary())
    """

    def __init__(self, crosswalk: dict | None = None, group_ranks: dict | None = None):
        self._crosswalk = crosswalk or V28_ICD10_TO_HCC
        self._group_ranks = group_ranks or HCC_GROUP_RANKS
        logger.info(
            "HCCEngine V28 initialised: %d ICD-10 mappings, %d hierarchy groups",
            len(self._crosswalk),
            len(self._group_ranks),
        )

    # ── Public API ────────────────────────────────────────────────────────

    def map_icd10_to_hcc(self, icd10_code: str) -> Optional[HCCCode]:
        """
        Map a single ICD-10 code to an HCC.

        Returns HCCCode or None if no mapping exists.
        """
        code = icd10_code.strip().upper()
        entry = self._crosswalk.get(code)
        if not entry:
            return None
        hcc_num, hcc_desc, group, raf = entry
        return HCCCode(
            hcc_number=hcc_num,
            hcc_description=hcc_desc,
            hierarchy_group=group,
            raf_weight=raf,
            source_icd10=code,
            source_icd10_description=hcc_desc,
        )

    def enforce_hierarchy(self, icd10_codes: list[str]) -> HCCResult:
        """
        Apply V28 hierarchy enforcement to a list of ICD-10 codes.

        Steps:
          1. Map each ICD-10 → HCC
          2. Group HCCs by hierarchy_group
          3. Within each group, keep only the highest-severity HCC
          4. Mark lower-severity HCCs as "trumped"
          5. Calculate RAF score impact

        Args:
            icd10_codes: List of ICD-10 codes for a patient encounter

        Returns:
            HCCResult with active/suppressed HCCs and RAF estimates
        """
        result = HCCResult()

        # Step 1: Map all codes → HCCs
        hcc_by_group: dict[str, list[HCCCode]] = {}

        for code in icd10_codes:
            hcc = self.map_icd10_to_hcc(code)
            if hcc is None:
                result.unmapped_codes.append(code)
                logger.debug("No V28 HCC mapping for ICD-10: %s", code)
                continue

            result.all_hccs.append(hcc)
            result.raf_before_enforcement += hcc.raf_weight

            # Group by hierarchy
            group = hcc.hierarchy_group or f"STANDALONE_{hcc.hcc_number}"
            hcc_by_group.setdefault(group, []).append(hcc)

        # Step 2: Enforce hierarchy within each group
        for group, hccs in hcc_by_group.items():
            if group.startswith("STANDALONE_") or len(hccs) == 1:
                # No hierarchy conflict — all HCCs are active
                for hcc in hccs:
                    result.active_hccs.append(hcc)
                    result.raf_after_enforcement += hcc.raf_weight
                continue

            # Deduplicate by HCC number (keep highest raf_weight per HCC)
            unique_hccs: dict[int, HCCCode] = {}
            for hcc in hccs:
                if hcc.hcc_number not in unique_hccs:
                    unique_hccs[hcc.hcc_number] = hcc
                else:
                    # Keep the one with higher RAF weight
                    if hcc.raf_weight > unique_hccs[hcc.hcc_number].raf_weight:
                        unique_hccs[hcc.hcc_number] = hcc

            # Apply group rank order: first rank in HCC_GROUP_RANKS wins
            group_rank = self._group_ranks.get(group, [])
            winner_hcc: Optional[HCCCode] = None

            for rank_hcc in group_rank:
                if rank_hcc in unique_hccs:
                    winner_hcc = unique_hccs[rank_hcc]
                    break

            if winner_hcc is None:
                # No ranked match — use highest raf_weight
                winner_hcc = max(unique_hccs.values(), key=lambda h: h.raf_weight)

            # Mark winner as active, losers as trumped
            for hcc_num, hcc in unique_hccs.items():
                if hcc_num == winner_hcc.hcc_number:
                    result.active_hccs.append(hcc)
                    result.raf_after_enforcement += hcc.raf_weight
                else:
                    hcc.status = "trumped"
                    hcc.trumped_by_hcc = winner_hcc.hcc_number
                    result.suppressed_hccs.append(hcc)
                    result.hierarchy_conflicts.append({
                        "group": group,
                        "winner_hcc": winner_hcc.hcc_number,
                        "winner_description": winner_hcc.hcc_description,
                        "winner_icd10": winner_hcc.source_icd10,
                        "trumped_hcc": hcc_num,
                        "trumped_description": hcc.hcc_description,
                        "trumped_icd10": hcc.source_icd10,
                        "raf_impact": round(hcc.raf_weight, 4),
                    })
                    logger.info(
                        "Hierarchy enforcement: HCC %d (%s) trumps HCC %d (%s) in group %s",
                        winner_hcc.hcc_number, winner_hcc.source_icd10,
                        hcc_num, hcc.source_icd10,
                        group,
                    )

        return result

    def get_supported_icd10_codes(self) -> list[str]:
        """Return all ICD-10 codes in the V28 crosswalk."""
        return sorted(self._crosswalk.keys())

    def get_hcc_info(self, hcc_number: int) -> list[dict]:
        """Return all ICD-10 codes that map to a specific HCC number."""
        results = []
        for code, (hcc_num, desc, group, raf) in self._crosswalk.items():
            if hcc_num == hcc_number:
                results.append({
                    "icd10_code": code,
                    "hcc_number": hcc_num,
                    "description": desc,
                    "hierarchy_group": group,
                    "raf_weight": raf,
                })
        return results


# ---------------------------------------------------------------------------
# CLI test harness
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    engine = HCCEngine()

    test_sets = [
        {
            "name": "Diabetes hierarchy conflict (T2DM w/ neuropathy + T2DM w/o complications)",
            "codes": ["E11.40", "E11.9", "N18.4"],
        },
        {
            "name": "CKD hierarchy conflict (Stage 4 + Stage 3b)",
            "codes": ["N18.4", "N18.32", "N18.31"],
        },
        {
            "name": "Heart failure hierarchy + COPD",
            "codes": ["I50.22", "I50.9", "J44.1"],
        },
        {
            "name": "Multiple groups — no conflicts",
            "codes": ["E11.9", "I63.9", "G30.9"],
        },
        {
            "name": "Unknown codes only",
            "codes": ["Z99.99", "X99.9"],
        },
    ]

    for ts in test_sets:
        print(f"\n{'=' * 60}")
        print(f"Test: {ts['name']}")
        print(f"Input codes: {ts['codes']}")
        result = engine.enforce_hierarchy(ts["codes"])
        print(json.dumps(result.summary(), indent=2))
        if result.hierarchy_conflicts:
            print("Conflicts:")
            for c in result.hierarchy_conflicts:
                print(f"  HCC {c['winner_hcc']} ({c['winner_icd10']}) trumps "
                      f"HCC {c['trumped_hcc']} ({c['trumped_icd10']}) — "
                      f"RAF impact: -{c['raf_impact']}")
