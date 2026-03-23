"""
CodeMed AI — V28 HCC Hierarchy Enforcement Engine
==================================================
Implements CMS-HCC Model V28 disease hierarchy enforcement.

The V28 model (CY2024 onwards, 100% weight for non-PACE MA plans in CY2026)
groups ICD-10 codes into 115 payment HCCs. When a patient qualifies for multiple
HCCs within the same hierarchy group, only the highest-severity HCC is kept —
lower ones are "trumped" and suppressed to prevent code stacking.

Key capabilities:
  - Map ICD-10 codes → correct V28 HCC numbers
  - Detect and resolve hierarchy conflicts within condition groups
  - Apply disease interaction adjustments (DIABETES_HF, HF_KIDNEY, etc.)
  - Calculate RAF scores before/after enforcement (community non-dual segment)
  - Full audit trail of every suppression decision
  - load_from_cms_zip(): load authoritative crosswalk from CMS model ZIP files

RAF coefficients: CMS-HCC Model V28, community non-dual aged segment.
For other segments (ESRD, institutional, new enrollee) use load_from_cms_zip().

HCC numbering reference:
  All HCC numbers in this file match the CMS-HCC Model V28 published numbering.
  Do NOT confuse with V24 numbers (the two models renumber many categories).
  Source: https://www.cms.gov/medicare/health-plans/medicareadvtgspecratestats/risk-adjustors
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# V28 HCC Crosswalk: ICD-10 → (hcc_number, description, hierarchy_group, raf_weight)
#
# HCC numbers are the *official* V28 numbers as published by CMS.
# RAF weights are community non-dual aged segment coefficients from V28.
# hierarchy_group=None → standalone HCC (no within-group trumping applies).
#
# For the authoritative full ~7,770-code mapping, call
# HCCEngine.load_from_cms_zip() which downloads and parses the official CMS file.
# ---------------------------------------------------------------------------

V28_ICD10_TO_HCC: dict[str, tuple[int, str, Optional[str], float]] = {

    # ── HIV / AIDS  (HCC 1) ──────────────────────────────────────────────
    "B20":    (1, "Human immunodeficiency virus disease",      None,      0.335),
    "Z21":    (1, "Asymptomatic HIV infection status",         None,      0.335),

    # ── Septicemia / Sepsis / SIRS  (HCC 2) ─────────────────────────────
    "A41.9":  (2, "Sepsis, unspecified organism",              None,      0.514),
    "A41.51": (2, "Sepsis due to Escherichia coli",            None,      0.514),
    "R65.20": (2, "Severe sepsis without septic shock",        None,      0.514),
    "R65.21": (2, "Severe sepsis with septic shock",           None,      0.514),

    # ── Opportunistic Infections  (HCC 6) ───────────────────────────────
    "B37.1":  (6, "Pulmonary candidiasis",                     None,      0.441),
    "B45.1":  (6, "Pulmonary cryptococcosis",                  None,      0.441),
    "B58.3":  (6, "Pulmonary toxoplasmosis",                   None,      0.441),

    # ── Cancer — Metastatic, Lung/Liver/Brain  (HCC 17) ─────────────────
    "C78.00": (17, "Secondary malignant neoplasm of lung",     "CANCER",  0.650),
    "C78.7":  (17, "Secondary malignant neoplasm of liver",    "CANCER",  0.650),
    "C79.31": (17, "Secondary malignant neoplasm of brain",    "CANCER",  0.650),
    "C91.00": (17, "Acute lymphoblastic leukemia in remission","CANCER",  0.650),
    "C92.00": (17, "Acute myeloid leukemia, w/o remission",    "CANCER",  0.650),

    # ── Cancer — Metastatic to Bone / Other  (HCC 18) ───────────────────
    "C79.51": (18, "Secondary malignant neoplasm of bone",     "CANCER",  0.516),
    "C79.89": (18, "Secondary malignant neoplasm, other sites","CANCER",  0.516),
    "C80.0":  (18, "Disseminated malignant neoplasm, unspec.", "CANCER",  0.516),
    "C91.10": (18, "Chronic lymphocytic leukemia",             "CANCER",  0.516),

    # ── Myelodysplastic / Multiple Myeloma  (HCC 19) ────────────────────
    "C90.00": (19, "Multiple myeloma, not in remission",       "CANCER",  0.421),
    "D46.9":  (19, "Myelodysplastic syndrome, unspecified",    "CANCER",  0.421),
    "C88.4":  (19, "Extranodal marginal zone B-cell lymphoma", "CANCER",  0.421),

    # ── Lung and Other Severe Cancers  (HCC 20) ─────────────────────────
    "C34.10": (20, "Malignant neoplasm of upper lobe, bronchus","CANCER", 0.591),
    "C34.90": (20, "Malignant neoplasm of bronchus and lung",   "CANCER", 0.591),
    "C25.9":  (20, "Malignant neoplasm of pancreas, unspec.",   "CANCER", 0.591),
    "C22.0":  (20, "Hepatocellular carcinoma",                  "CANCER", 0.591),
    "C15.9":  (20, "Malignant neoplasm of esophagus",           "CANCER", 0.591),

    # ── Lymphoma and Other Cancers  (HCC 21) ────────────────────────────
    "C83.30": (21, "Diffuse large B-cell lymphoma, unspec.",   "CANCER",  0.420),
    "C85.90": (21, "Non-Hodgkin lymphoma, unspecified",        "CANCER",  0.420),
    "C81.90": (21, "Hodgkin lymphoma, unspecified",            "CANCER",  0.420),

    # ── Bladder, Colorectal, and Other Cancers  (HCC 22) ────────────────
    "C18.9":  (22, "Malignant neoplasm of colon, unspecified", "CANCER",  0.283),
    "C67.9":  (22, "Malignant neoplasm of bladder, unspec.",   "CANCER",  0.283),
    "C16.9":  (22, "Malignant neoplasm of stomach, unspec.",   "CANCER",  0.283),
    "C64.9":  (22, "Malignant neoplasm of kidney, unspec.",    "CANCER",  0.283),

    # ── Prostate, Breast, and Other Cancers  (HCC 23) ───────────────────
    "C61":    (23, "Malignant neoplasm of prostate",           "CANCER",  0.147),
    "C50.911":(23, "Malignant neoplasm of breast, unspec.",    "CANCER",  0.147),
    "C43.9":  (23, "Malignant melanoma of skin, unspecified",  "CANCER",  0.147),
    "C73":    (23, "Malignant neoplasm of thyroid gland",      "CANCER",  0.147),

    # Cancer hierarchy: 17 > 18 > 19 > 20 > 21 > 22 > 23

    # ── Pancreas Transplant Status  (HCC 35 — standalone) ───────────────
    "Z94.83": (35, "Pancreas transplant status",               None,      0.592),

    # ── Diabetes with Severe Acute Complications  (HCC 36) ──────────────
    "E10.10": (36, "T1DM with ketoacidosis, unspecified",      "DIABETES",0.302),
    "E10.11": (36, "T1DM with ketoacidosis with coma",         "DIABETES",0.302),
    "E11.10": (36, "T2DM with ketoacidosis, unspecified",      "DIABETES",0.302),
    "E11.11": (36, "T2DM with ketoacidosis with coma",         "DIABETES",0.302),
    "E10.641":(36, "T1DM with hypoglycemia with coma",         "DIABETES",0.302),
    "E11.641":(36, "T2DM with hypoglycemia with coma",         "DIABETES",0.302),
    "E13.10": (36, "Other DM with ketoacidosis, unspecified",  "DIABETES",0.302),

    # ── Diabetes with Chronic Complications  (HCC 37) ───────────────────
    "E10.40": (37, "T1DM with diabetic neuropathy, unspec.",   "DIABETES",0.179),
    "E10.51": (37, "T1DM with peripheral angiopathy w/o gangrene","DIABETES",0.179),
    "E10.52": (37, "T1DM with peripheral angiopathy w/ gangrene","DIABETES",0.179),
    "E10.65": (37, "T1DM with hyperglycemia",                  "DIABETES",0.179),
    "E11.40": (37, "T2DM with diabetic neuropathy, unspec.",   "DIABETES",0.179),
    "E11.51": (37, "T2DM with peripheral angiopathy w/o gangrene","DIABETES",0.179),
    "E11.52": (37, "T2DM with peripheral angiopathy w/ gangrene","DIABETES",0.179),
    "E11.65": (37, "T2DM with hyperglycemia",                  "DIABETES",0.179),
    "E11.21": (37, "T2DM with diabetic nephropathy",           "DIABETES",0.179),
    "E11.311":(37, "T2DM with unspecified diabetic retinopathy","DIABETES",0.179),
    "E13.40": (37, "Other DM with diabetic neuropathy, unspec.","DIABETES",0.179),

    # ── Diabetes with Glycemic / No Complications  (HCC 38) ─────────────
    "E10.9":  (38, "T1DM without complications",               "DIABETES",0.118),
    "E11.9":  (38, "T2DM without complications",               "DIABETES",0.118),
    "E13.9":  (38, "Other specified DM without complications", "DIABETES",0.118),
    "E11.69": (38, "T2DM with other specified complication",   "DIABETES",0.118),

    # ── Morbid Obesity  (HCC 48 — standalone) ───────────────────────────
    "E66.01": (48, "Morbid (severe) obesity due to excess calories", None, 0.273),
    "E66.09": (48, "Other obesity due to excess calories",      None,      0.273),

    # ── Liver Transplant  (HCC 62 — standalone) ─────────────────────────
    "Z94.4":  (62, "Liver transplant status",                  None,      0.618),

    # ── Chronic Liver Failure / End-Stage Liver  (HCC 63) ───────────────
    "K72.10": (63, "Chronic hepatic failure without coma",     "LIVER",   0.487),
    "K72.11": (63, "Chronic hepatic failure with coma",        "LIVER",   0.487),
    "K76.7":  (63, "Hepatorenal syndrome",                     "LIVER",   0.487),

    # ── Cirrhosis of Liver  (HCC 64) ────────────────────────────────────
    "K74.60": (64, "Unspecified cirrhosis of liver",           "LIVER",   0.352),
    "K74.69": (64, "Other cirrhosis of liver",                 "LIVER",   0.352),
    "K70.30": (64, "Alcoholic cirrhosis of liver without ascites","LIVER",0.352),
    "K70.31": (64, "Alcoholic cirrhosis of liver with ascites","LIVER",   0.352),

    # ── Chronic Hepatitis  (HCC 65) ─────────────────────────────────────
    "K73.2":  (65, "Chronic active hepatitis, NEC",            "LIVER",   0.221),
    "K73.9":  (65, "Chronic hepatitis, unspecified",           "LIVER",   0.221),
    "K74.0":  (65, "Hepatic fibrosis",                         "LIVER",   0.221),
    "B18.2":  (65, "Chronic viral hepatitis C",                "LIVER",   0.221),
    "B18.1":  (65, "Chronic viral hepatitis B without delta",  "LIVER",   0.221),

    # Liver hierarchy: 63 > 64 > 65

    # ── Intestine Transplant  (HCC 77 — standalone) ──────────────────────
    "Z94.82": (77, "Intestine transplant status",              None,      0.592),

    # ── Crohn's Disease  (HCC 80 — standalone) ───────────────────────────
    "K50.90": (80, "Crohn's disease of small intestine, unspec.",None,    0.302),
    "K50.00": (80, "Crohn's disease of small intestine w/o complications",None,0.302),
    "K50.10": (80, "Crohn's disease of large intestine, unspec.",None,    0.302),

    # ── Ulcerative Colitis  (HCC 81 — standalone) ────────────────────────
    "K51.90": (81, "Ulcerative colitis, unspecified, unspec. complications",None,0.225),
    "K51.00": (81, "Ulcerative pancolitis without complications",None,     0.225),

    # ── Rheumatoid Arthritis  (HCC 93 — standalone) ─────────────────────
    "M05.79": (93, "Rheumatoid arthritis with rheumatoid factor",None,     0.421),
    "M05.9":  (93, "Rheumatoid arthritis with RF, unspec.",    None,       0.421),
    "M06.00": (93, "Rheumatoid arthritis without RF, unspec.", None,       0.421),
    "M06.9":  (93, "Rheumatoid arthritis, unspecified",        None,       0.421),
    "M32.10": (93, "Systemic lupus erythematosus, unspec.",    None,       0.421),

    # ── Sickle Cell Anemia  (HCC 107 — standalone) ──────────────────────
    "D57.1":  (107,"Sickle-cell disease without crisis",       None,       0.398),
    "D57.00": (107,"Hb-SS disease with crisis, unspecified",   None,       0.398),
    "D56.1":  (107,"Beta thalassemia",                         None,       0.398),

    # ── Dementia — Severe  (HCC 125) ────────────────────────────────────
    "G30.9":  (125,"Alzheimer's disease, unspecified",         "DEMENTIA", 0.346),
    "G30.0":  (125,"Alzheimer's disease with early onset",     "DEMENTIA", 0.346),
    "F01.51": (125,"Vascular dementia with behavioral disturbance","DEMENTIA",0.346),
    "F02.81": (125,"Dementia in other diseases w/ behavioral disturbance","DEMENTIA",0.346),

    # ── Dementia — Moderate  (HCC 126) ──────────────────────────────────
    "F03.91": (126,"Unspecified dementia with behavioral disturbance","DEMENTIA",0.211),
    "G31.09": (126,"Other frontotemporal dementia",            "DEMENTIA", 0.211),
    "G23.1":  (126,"Progressive supranuclear ophthalmoplegia", "DEMENTIA", 0.211),

    # ── Dementia — Mild / Unspecified  (HCC 127) ────────────────────────
    "F03.90": (127,"Unspecified dementia without behavioral disturbance","DEMENTIA",0.138),
    "G30.1":  (127,"Alzheimer's disease with late onset",      "DEMENTIA", 0.138),
    "G31.84": (127,"Mild cognitive impairment of uncertain etiology","DEMENTIA",0.138),

    # Dementia hierarchy: 125 > 126 > 127

    # ── Schizophrenia  (HCC 151 — standalone) ───────────────────────────
    "F20.9":  (151,"Schizophrenia, unspecified",               None,       0.421),
    "F20.0":  (151,"Paranoid schizophrenia",                   None,       0.421),
    "F25.0":  (151,"Schizoaffective disorder, bipolar type",   None,       0.421),

    # ── Major Depressive Disorder  (HCC 155) ────────────────────────────
    "F32.2":  (155,"Major depressive disorder, single, severe","DEPRESSION",0.289),
    "F33.2":  (155,"Major depressive disorder, recurrent, severe","DEPRESSION",0.289),
    "F32.1":  (156,"Major depressive disorder, single, moderate","DEPRESSION",0.193),
    "F33.1":  (156,"Major depressive disorder, recurrent, moderate","DEPRESSION",0.193),
    "F32.9":  (157,"Major depressive disorder, unspecified",   "DEPRESSION",0.099),
    "F33.9":  (157,"Recurrent depressive disorder, unspecified","DEPRESSION",0.099),

    # Depression hierarchy: 155 > 156 > 157

    # ── Quadriplegia  (HCC 180 — standalone) ────────────────────────────
    "G82.50": (180,"Quadriplegia, unspecified",                None,       0.591),
    "G82.51": (180,"Quadriplegia, C1-C4 incomplete",           None,       0.591),
    "G82.54": (180,"Quadriplegia, C5-C7 complete",             None,       0.591),

    # ── Heart Transplant  (HCC 221 — standalone) ────────────────────────
    "Z94.1":  (221,"Heart transplant status",                  None,       0.752),
    "T86.20": (221,"Unspecified complication of heart transplant",None,    0.752),

    # ── End-Stage Heart Failure  (HCC 222) ──────────────────────────────
    # Stage D / refractory heart failure requiring advanced therapy
    "I50.84": (222,"End-stage heart failure",                  "HEART_FAILURE",0.514),

    # ── Acute on Chronic Heart Failure  (HCC 224) ───────────────────────
    "I50.23": (224,"Acute on chronic systolic heart failure",  "HEART_FAILURE",0.368),
    "I50.33": (224,"Acute on chronic diastolic heart failure", "HEART_FAILURE",0.368),
    "I50.43": (224,"Acute on chronic combined systolic/diastolic HF","HEART_FAILURE",0.368),
    "I50.13": (224,"Acute on chronic right-sided heart failure","HEART_FAILURE",0.368),
    "I50.21": (224,"Acute systolic (congestive) heart failure","HEART_FAILURE",0.368),
    "I50.31": (224,"Acute diastolic (congestive) heart failure","HEART_FAILURE",0.368),

    # ── Heart Failure — Other  (HCC 226) ────────────────────────────────
    "I50.20": (226,"Unspecified systolic heart failure",       "HEART_FAILURE",0.259),
    "I50.22": (226,"Chronic systolic (congestive) heart failure","HEART_FAILURE",0.259),
    "I50.30": (226,"Unspecified diastolic heart failure",      "HEART_FAILURE",0.259),
    "I50.32": (226,"Chronic diastolic (congestive) heart failure","HEART_FAILURE",0.259),
    "I50.40": (226,"Unspecified combined systolic/diastolic HF","HEART_FAILURE",0.259),
    "I50.42": (226,"Chronic combined systolic/diastolic HF",   "HEART_FAILURE",0.259),
    "I50.9":  (226,"Heart failure, unspecified",               "HEART_FAILURE",0.259),

    # Heart failure hierarchy: 222 > 224 > 226

    # ── Specified Heart Arrhythmias  (HCC 238 — standalone) ─────────────
    "I48.0":  (238,"Paroxysmal atrial fibrillation",           None,       0.302),
    "I48.11": (238,"Longstanding persistent atrial fibrillation",None,     0.302),
    "I48.19": (238,"Other persistent atrial fibrillation",     None,       0.302),
    "I48.20": (238,"Chronic atrial fibrillation, unspecified", None,       0.302),
    "I49.01": (238,"Ventricular fibrillation",                 None,       0.302),
    "I44.2":  (238,"Atrioventricular block, complete",         None,       0.302),
    "I45.3":  (238,"Trifascicular block",                      None,       0.302),

    # ── Vascular Disease  (HCC 107 group omitted — keeping standalone) ──
    # Peripheral Artery Disease — standalone in V28 context
    "I70.201":(245,"Atherosclerosis of native arteries of right leg",None, 0.288),
    "I70.211":(245,"Atherosclerosis of native arteries, rest pain",None,   0.288),
    "I70.231":(245,"Atherosclerosis of native arteries, ulceration right",None,0.288),
    "I70.209":(245,"Atherosclerosis of native arteries, unspec.",None,     0.193),
    "I73.9":  (245,"Peripheral vascular disease, unspecified",  None,      0.193),

    # ── Hemiplegia / Hemiparesis  (HCC 253 — standalone) ────────────────
    "G81.90": (253,"Hemiplegia, unspecified",                  None,       0.421),
    "G81.10": (253,"Spastic hemiplegia, unspecified side",     None,       0.421),
    "I69.351":(253,"Hemiplegia/hemiparesis following cerebral infarction",None,0.421),
    "I69.30": (253,"Sequelae of cerebral infarction, unspec.", None,       0.421),

    # ── Stroke / Cerebral Infarction  (HCC 167) ─────────────────────────
    "I63.50": (167,"Cerebral infarction due to unspecified occlusion",None,0.353),
    "I63.9":  (167,"Cerebral infarction, unspecified",         None,       0.353),
    "I63.00": (167,"Cerebral infarction due to thrombosis",    None,       0.353),

    # ── Severe Persistent Asthma  (HCC 279 — standalone) ────────────────
    "J45.51": (279,"Severe persistent asthma, uncomplicated",  None,       0.302),
    "J45.50": (279,"Severe persistent asthma, unspecified",    None,       0.302),
    "J45.52": (279,"Severe persistent asthma with status asthmaticus",None,0.302),

    # ── COPD / Chronic Lung  (HCC 280 — standalone) ─────────────────────
    "J44.1":  (280,"COPD with acute exacerbation",             None,       0.346),
    "J44.0":  (280,"COPD with acute lower respiratory infection",None,     0.346),
    "J44.9":  (280,"COPD, unspecified",                        None,       0.259),
    "J43.9":  (280,"Emphysema, unspecified",                   None,       0.259),
    "J84.10": (280,"Pulmonary fibrosis, unspecified",          None,       0.259),
    "J96.11": (280,"Chronic respiratory failure with hypoxia", None,       0.346),

    # ── Chronic Kidney Disease — Stage 5 / ESRD / Dialysis  (HCC 326) ───
    "N18.6":  (326,"End-stage renal disease",                  "CKD",      0.493),
    "N18.5":  (326,"Chronic kidney disease, stage 5",          "CKD",      0.493),
    "Z99.2":  (326,"Dependence on renal dialysis",             "CKD",      0.493),
    "Z94.0":  (326,"Kidney transplant status",                 "CKD",      0.493),

    # ── Chronic Kidney Disease — Stage 4  (HCC 327) ─────────────────────
    "N18.4":  (327,"Chronic kidney disease, stage 4",          "CKD",      0.289),

    # ── Chronic Kidney Disease — Stage 3B  (HCC 328) ────────────────────
    "N18.32": (328,"Chronic kidney disease, stage 3b",         "CKD",      0.200),

    # ── Chronic Kidney Disease — Stage 3 (not 3B)  (HCC 329) ────────────
    "N18.31": (329,"Chronic kidney disease, stage 3a",         "CKD",      0.137),
    "N18.30": (329,"Chronic kidney disease, stage 3 unspecified","CKD",    0.137),

    # CKD hierarchy: 326 > 327 > 328 > 329

    # ── Pressure Ulcer with Necrosis  (HCC 379 — standalone) ─────────────
    "L89.003":(379,"Pressure ulcer of unspecified elbow, stage 3",None,   0.591),
    "L89.004":(379,"Pressure ulcer of unspecified elbow, stage 4",None,   0.591),
    "L89.153":(379,"Pressure ulcer of sacral region, stage 3",None,       0.591),
    "L89.154":(379,"Pressure ulcer of sacral region, stage 4",None,       0.591),
    "L89.209":(379,"Pressure ulcer of unspecified hip, stage unspec.",None,0.591),
}

# ---------------------------------------------------------------------------
# V28 hierarchy rank tables
# Within each group, HCCs listed left-to-right from HIGHEST to LOWEST severity.
# The first (leftmost) HCC present in a patient's code set wins; all others
# in the same group are suppressed.
# ---------------------------------------------------------------------------

HCC_GROUP_RANKS: dict[str, list[int]] = {
    "CANCER":       [17, 18, 19, 20, 21, 22, 23],
    "DIABETES":     [36, 37, 38],
    "LIVER":        [63, 64, 65],
    "DEMENTIA":     [125, 126, 127],
    "DEPRESSION":   [155, 156, 157],
    "HEART_FAILURE":[222, 224, 226],
    "CKD":          [326, 327, 328, 329],
}

# ---------------------------------------------------------------------------
# V28 Disease Interaction Adjustments
# These are ADDITIVE RAF bonuses when a patient qualifies for BOTH sides of
# an interaction. Source: CMS V28 model coefficient tables.
#
# Format: interaction_name → (hcc_set_A, hcc_set_B, raf_bonus)
# Bonus applies when patient has ≥1 HCC from set_A AND ≥1 HCC from set_B.
# ---------------------------------------------------------------------------

V28_INTERACTIONS: list[tuple[str, frozenset[int], frozenset[int], float]] = [
    (
        "DIABETES_HF",
        frozenset({36, 37, 38}),            # any diabetes HCC
        frozenset({222, 224, 226}),          # any heart failure HCC
        0.156,
    ),
    (
        "HF_KIDNEY",
        frozenset({222, 224, 226}),          # any heart failure HCC
        frozenset({326, 327, 328, 329}),     # any CKD HCC
        0.134,
    ),
    (
        "DIABETES_KIDNEY",
        frozenset({36, 37, 38}),             # any diabetes HCC
        frozenset({326, 327, 328, 329}),     # any CKD HCC
        0.115,
    ),
    (
        "HF_COPD",
        frozenset({222, 224, 226}),          # any heart failure HCC
        frozenset({279, 280}),               # asthma or COPD
        0.099,
    ),
    (
        "CANCER_DIABETES",
        frozenset({17, 18, 19, 20, 21, 22, 23}),  # any cancer HCC
        frozenset({36, 37, 38}),             # any diabetes HCC
        0.072,
    ),
]


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
    all_hccs: list[HCCCode] = field(default_factory=list)
    active_hccs: list[HCCCode] = field(default_factory=list)
    suppressed_hccs: list[HCCCode] = field(default_factory=list)
    unmapped_codes: list[str] = field(default_factory=list)

    # RAF estimates (community non-dual aged, before interactions)
    raf_before_enforcement: float = 0.0
    raf_after_enforcement: float = 0.0

    # Disease interaction bonuses applied
    interaction_bonuses: list[dict] = field(default_factory=list)

    # Audit trail
    hierarchy_conflicts: list[dict] = field(default_factory=list)

    @property
    def raf_delta(self) -> float:
        return self.raf_after_enforcement - self.raf_before_enforcement

    @property
    def raf_total(self) -> float:
        """Final RAF including disease interaction adjustments."""
        bonus = sum(i["raf_bonus"] for i in self.interaction_bonuses)
        return round(self.raf_after_enforcement + bonus, 4)

    def summary(self) -> dict:
        return {
            "total_icd10_codes": len(self.all_hccs) + len(self.unmapped_codes),
            "hccs_found": len(self.all_hccs),
            "hccs_active": len(self.active_hccs),
            "hccs_suppressed": len(self.suppressed_hccs),
            "raf_before": round(self.raf_before_enforcement, 4),
            "raf_after": round(self.raf_after_enforcement, 4),
            "raf_delta": round(self.raf_delta, 4),
            "raf_total_with_interactions": self.raf_total,
            "interaction_bonuses": self.interaction_bonuses,
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
        result = engine.enforce_hierarchy(["E11.9", "E11.40", "N18.4"])
        print(result.summary())

    Load full CMS crosswalk (~7,770 codes) from official ZIP:
        engine = HCCEngine.load_from_cms_zip(
            "https://www.cms.gov/files/zip/2026-initial-icd-10-cm-mappings.zip"
        )
    """

    def __init__(
        self,
        crosswalk: dict | None = None,
        group_ranks: dict | None = None,
        interactions: list | None = None,
    ):
        self._crosswalk = crosswalk or V28_ICD10_TO_HCC
        self._group_ranks = group_ranks or HCC_GROUP_RANKS
        self._interactions = interactions or V28_INTERACTIONS
        logger.info(
            "HCCEngine V28 initialised: %d ICD-10 mappings, %d hierarchy groups, "
            "%d interaction terms",
            len(self._crosswalk),
            len(self._group_ranks),
            len(self._interactions),
        )

    # ── Class-method constructor from CMS ZIP ─────────────────────────────

    @classmethod
    def load_from_cms_zip(
        cls,
        mappings_url: str | None = None,
        software_url: str | None = None,
        local_mappings_path: str | None = None,
    ) -> "HCCEngine":
        """
        Build an HCCEngine whose crosswalk is loaded from the official CMS
        V28 ICD-10 mapping file (overrides the built-in ~95-code starter set
        with the full ~7,770-code official set).

        Args:
            mappings_url: URL of the CMS 2026 ICD-10 mappings ZIP.
                Defaults to the 2026 initial mappings ZIP.
            software_url: URL of the CMS model software ZIP (for coefficients).
                Optional — built-in community coefficients are used if omitted.
            local_mappings_path: Path to a locally downloaded mappings ZIP.
                Takes priority over mappings_url.

        Returns:
            HCCEngine instance with the full CMS crosswalk loaded.
        """
        from ingestion.scrapers.hcc_model_scraper import build_crosswalk_from_cms_data

        crosswalk = build_crosswalk_from_cms_data(
            mappings_url=mappings_url,
            software_url=software_url,
            local_path=local_mappings_path,
            fallback_crosswalk=V28_ICD10_TO_HCC,
        )
        logger.info(
            "HCCEngine loaded from CMS ZIP: %d ICD-10 mappings", len(crosswalk)
        )
        return cls(crosswalk=crosswalk)

    # ── Public API ────────────────────────────────────────────────────────

    def map_icd10_to_hcc(self, icd10_code: str) -> Optional[HCCCode]:
        """Map a single ICD-10 code to an HCCCode, or None if not in crosswalk."""
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
          3. Within each group keep only the highest-severity HCC (per group ranks)
          4. Mark lower-severity HCCs as "trumped"
          5. Apply disease interaction RAF bonuses
          6. Return full audit trail

        Args:
            icd10_codes: ICD-10 codes for a patient encounter

        Returns:
            HCCResult with active/suppressed HCCs, RAF estimates, and interactions
        """
        result = HCCResult()
        hcc_by_group: dict[str, list[HCCCode]] = {}

        # Step 1: Map all codes → HCCs
        for code in icd10_codes:
            hcc = self.map_icd10_to_hcc(code)
            if hcc is None:
                result.unmapped_codes.append(code)
                logger.debug("No V28 HCC mapping for ICD-10: %s", code)
                continue

            result.all_hccs.append(hcc)
            result.raf_before_enforcement += hcc.raf_weight

            group = hcc.hierarchy_group or f"STANDALONE_{hcc.hcc_number}"
            hcc_by_group.setdefault(group, []).append(hcc)

        # Step 2: Enforce hierarchy within each group
        for group, hccs in hcc_by_group.items():
            if group.startswith("STANDALONE_") or len(hccs) == 1:
                for hcc in hccs:
                    result.active_hccs.append(hcc)
                    result.raf_after_enforcement += hcc.raf_weight
                continue

            # Deduplicate: per HCC number, keep highest RAF
            unique_hccs: dict[int, HCCCode] = {}
            for hcc in hccs:
                if hcc.hcc_number not in unique_hccs or \
                   hcc.raf_weight > unique_hccs[hcc.hcc_number].raf_weight:
                    unique_hccs[hcc.hcc_number] = hcc

            # Find the winning HCC (first in group rank order)
            group_rank = self._group_ranks.get(group, [])
            winner_hcc: Optional[HCCCode] = None
            for rank_hcc in group_rank:
                if rank_hcc in unique_hccs:
                    winner_hcc = unique_hccs[rank_hcc]
                    break
            if winner_hcc is None:
                winner_hcc = max(unique_hccs.values(), key=lambda h: h.raf_weight)

            # Mark winner active; suppress others
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
                        "V28 hierarchy: HCC %d (%s) trumps HCC %d (%s) [group=%s]",
                        winner_hcc.hcc_number, winner_hcc.source_icd10,
                        hcc_num, hcc.source_icd10, group,
                    )

        # Step 3: Apply disease interaction bonuses
        active_hcc_nums = frozenset(h.hcc_number for h in result.active_hccs)
        for name, set_a, set_b, bonus in self._interactions:
            if active_hcc_nums & set_a and active_hcc_nums & set_b:
                result.interaction_bonuses.append({
                    "interaction": name,
                    "raf_bonus": bonus,
                    "triggered_by_a": sorted(active_hcc_nums & set_a),
                    "triggered_by_b": sorted(active_hcc_nums & set_b),
                })
                logger.info(
                    "V28 interaction %s triggered: +%.3f RAF", name, bonus
                )

        return result

    def get_supported_icd10_codes(self) -> list[str]:
        """Return all ICD-10 codes in the current crosswalk."""
        return sorted(self._crosswalk.keys())

    def get_hcc_info(self, hcc_number: int) -> list[dict]:
        """Return all ICD-10 codes that map to a specific HCC number."""
        return [
            {
                "icd10_code": code,
                "hcc_number": hcc_num,
                "description": desc,
                "hierarchy_group": group,
                "raf_weight": raf,
            }
            for code, (hcc_num, desc, group, raf) in self._crosswalk.items()
            if hcc_num == hcc_number
        ]


# ---------------------------------------------------------------------------
# CLI test harness
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    engine = HCCEngine()

    test_sets = [
        {
            "name": "Diabetes hierarchy (T2DM w/ neuropathy + T2DM w/o complications)",
            "codes": ["E11.40", "E11.9", "N18.4"],
        },
        {
            "name": "CKD hierarchy (Stage 4 + Stage 3b)",
            "codes": ["N18.4", "N18.32", "N18.31"],
        },
        {
            "name": "Heart failure hierarchy (HCC 224 acute vs 226 chronic)",
            "codes": ["I50.23", "I50.22", "I50.9"],
        },
        {
            "name": "DIABETES_HF + HF_KIDNEY interactions",
            "codes": ["E11.9", "I50.22", "N18.4"],
        },
        {
            "name": "Cancer hierarchy (metastatic liver > colon)",
            "codes": ["C78.7", "C18.9"],
        },
        {
            "name": "Dementia hierarchy (severe > mild)",
            "codes": ["G30.9", "F03.90"],
        },
        {
            "name": "Unknown codes only",
            "codes": ["Z99.99", "X99.9"],
        },
    ]

    for ts in test_sets:
        print(f"\n{'=' * 60}")
        print(f"Test: {ts['name']}")
        print(f"Input: {ts['codes']}")
        result = engine.enforce_hierarchy(ts["codes"])
        print(json.dumps(result.summary(), indent=2))
        if result.hierarchy_conflicts:
            for c in result.hierarchy_conflicts:
                print(
                    f"  Conflict [{c['group']}]: HCC {c['winner_hcc']} "
                    f"({c['winner_icd10']}) trumps HCC {c['trumped_hcc']} "
                    f"({c['trumped_icd10']}) — RAF -{c['raf_impact']}"
                )
