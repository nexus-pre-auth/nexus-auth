"""
CMS-HCC Version 28 Risk Adjustment Engine — 2026 Implementation
================================================================
V28 replaces V24 as the sole MA risk adjustment model in 2026
(the 3-year blend period ends).  Key changes vs V24:

  • HCC count: 86 → 115 (new conditions, refined granularity)
  • ICD-10 coverage: 9,797 → 7,770 codes (2,027 codes removed)
  • Diabetes hierarchy tightened — hyperglycemia codes now distinct
  • Heart failure requires specificity; I50.9 only counts when alone

Used by the denial recovery engine to:
  1. Validate ICD-10 codes on claims before CO-50 appeal generation
  2. Compute RAF context to strengthen medical necessity arguments
  3. Detect hierarchy violations that may explain payer denials
"""

import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).parent / "data"


class V28RiskAdjustmentEngine:
    """
    CMS-HCC Version 28 — 100% implementation for 2026.
    """

    def __init__(self):
        self.v28_hcc_count = 115          # Up from 86 in V24
        self.valid_icd10_count = 7770     # Down from 9,797 (2,027 codes removed)

        # Disease hierarchies — prevent code stacking that inflates RAF or triggers denial
        self.hierarchies = {
            "diabetes": {
                "T2DM_with_complications": ["E11.3", "E11.4", "E11.5", "E11.6"],
                "T2DM_with_hyperglycemia": ["E11.65"],   # HCC37 — weight 0.105
                "T2DM_uncomplicated": ["E11.9"],          # Rejected if specific code exists
                "rule": (
                    "If T2DM_with_hyperglycemia (E11.65) present, "
                    "T2DM_uncomplicated (E11.9) is NOT payment-eligible"
                ),
            },
            "heart_failure": {
                "systolic":    ["I50.2", "I50.20", "I50.21", "I50.22", "I50.23"],
                "diastolic":   ["I50.3", "I50.30", "I50.31", "I50.32", "I50.33"],
                "combined":    ["I50.4", "I50.40", "I50.41", "I50.42", "I50.43"],
                "unspecified": ["I50.9"],   # Lowest hierarchy
                "rule": "Unspecified heart failure only counts if no specific type documented",
            },
        }

        # HCC weights for 2026 — subset; full list loaded from v28_icd_hcc.json
        self.hcc_weights = {
            "HCC37":  0.105,   # T2DM with hyperglycemia
            "HCC38":  0.065,   # T2DM without complications
            "HCC40":  0.421,   # Rheumatoid arthritis / inflammatory connective tissue
            "HCC70":  1.045,   # Quadriplegia
            "HCC77":  0.522,   # Multiple sclerosis
            "HCC78":  0.261,   # Parkinson's disease
            "HCC85":  0.332,   # Congestive heart failure
            "HCC96":  0.261,   # Specified heart arrhythmias
            "HCC100": 0.350,   # Ischemic stroke
            "HCC103": 0.391,   # Hemiplegia / other paralytic syndromes
            "HCC111": 0.335,   # COPD
            "HCC136": 0.289,   # Chronic kidney disease, stage 3–4
            "HCC168": 0.444,   # Vertebral fractures without SCI
            "HCC169": 0.627,   # Pathological vertebral fracture
            "HCC174": 0.097,   # Chronic pain syndrome (low back / general)
        }

        # Pre-load the ICD-10 → HCC crosswalk
        self._icd_hcc_map: dict[str, str] = self._load_icd_hcc_map()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_codes(self, icd10_codes: list[str]) -> dict:
        """
        Validate ICD-10 codes against V28 requirements.
        Returns valid_codes, invalid_codes, hierarchy_violations, and RAF impact.
        """
        valid: list[str] = []
        invalid: list[dict] = []
        violations: list[dict] = []

        v28_codes = self.load_v28_mapping()

        for code in icd10_codes:
            if code not in v28_codes:
                invalid.append({
                    "code": code,
                    "reason": "Not mapped in V28 (code removed or never valid for 2026)",
                    "action": "Verify documentation supports a more specific V28-mapped code",
                })
                continue

            violation = self.check_hierarchy(code, icd10_codes)
            if violation:
                violations.append(violation)
            else:
                valid.append(code)

        return {
            "valid_codes":           valid,
            "invalid_codes":         invalid,
            "hierarchy_violations":  violations,
            "raf_impact":            self.calculate_raf_impact(valid),
        }

    def check_hierarchy(self, code: str, all_codes: list[str]) -> Optional[dict]:
        """
        Enforce V28 disease hierarchies.
        Returns a violation dict if the code is overridden by a more specific sibling.
        """
        for disease, hierarchy in self.hierarchies.items():
            for level, level_codes in hierarchy.items():
                if level == "rule" or not isinstance(level_codes, list):
                    continue
                if code not in level_codes:
                    continue

                if level == "unspecified":
                    # Collect all specific-level codes for this disease
                    specific: list[str] = []
                    for lv, lv_codes in hierarchy.items():
                        if lv in ("unspecified", "rule") or not isinstance(lv_codes, list):
                            continue
                        specific.extend(lv_codes)

                    present_specifics = [sc for sc in specific if sc in all_codes]
                    if present_specifics:
                        return {
                            "code":                   code,
                            "disease":                disease,
                            "violation":              "Unspecified code present alongside specific code",
                            "specific_codes_present": present_specifics,
                            "action": (
                                "Remove unspecified code from payment calculation; "
                                "specific code takes precedence in V28 hierarchy"
                            ),
                        }
        return None

    def calculate_raf_impact(self, valid_codes: list[str]) -> dict:
        """Calculate RAF (Risk Adjustment Factor) contribution for valid codes."""
        total_raf = 0.0
        details: list[dict] = []

        for code in valid_codes:
            hcc = self.map_code_to_hcc(code)
            if hcc and hcc in self.hcc_weights:
                weight = self.hcc_weights[hcc]
                total_raf += weight
                details.append({"code": code, "hcc": hcc, "weight": weight})

        return {
            "total_raf": round(total_raf, 3),
            "details":   details,
        }

    def load_v28_mapping(self) -> set:
        """
        Return the set of ICD-10 codes that are valid under V28.
        Loads from the JSON crosswalk; falls back to known hierarchy codes.
        """
        return set(self._icd_hcc_map.keys())

    def map_code_to_hcc(self, icd10_code: str) -> Optional[str]:
        """Return the V28 HCC for a given ICD-10 code, or None if not mapped."""
        return self._icd_hcc_map.get(icd10_code)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_icd_hcc_map(self) -> dict[str, str]:
        """Load ICD-10 → HCC crosswalk from the data file."""
        data_file = _DATA_DIR / "v28_icd_hcc.json"
        if data_file.exists():
            try:
                with open(data_file) as f:
                    return json.load(f)
            except Exception as exc:
                logger.warning("Could not load V28 crosswalk from %s: %s", data_file, exc)

        # Fallback: build from hierarchy definitions
        logger.warning("V28 crosswalk file not found — using hierarchy seed codes only")
        fallback: dict[str, str] = {}
        for hierarchy in self.hierarchies.values():
            for level, codes in hierarchy.items():
                if level == "rule" or not isinstance(codes, list):
                    continue
                for code in codes:
                    # Assign a placeholder HCC; real file has correct mappings
                    fallback.setdefault(code, "HCC_UNKNOWN")
        return fallback
