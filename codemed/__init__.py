"""
CodeMed AI — Medical Coding Intelligence Engine
================================================
Provides:
  - HCC V28 hierarchy enforcement (prevent code stacking, defensible RAF scores)
  - MEAT evidence extraction from clinical notes (audit defensibility)
  - Natural language coding queries (ICD-10 / CPT / HCPCS plain-English search)
  - Automated prior auth appeal letter generation with LCD/NCD citations

Session 3 artifact — builds on the Session 2 knowledge layer.
"""

from codemed.hcc_engine import HCCEngine, HCCResult
from codemed.meat_extractor import MEATExtractor, MEATResult
from codemed.nlq_engine import NLQEngine, CodeSearchResult
from codemed.appeals_generator import AppealsGenerator, AppealLetter

__all__ = [
    "HCCEngine",
    "HCCResult",
    "MEATExtractor",
    "MEATResult",
    "NLQEngine",
    "CodeSearchResult",
    "AppealsGenerator",
    "AppealLetter",
]

__version__ = "1.0.0"
