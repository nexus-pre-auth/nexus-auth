"""
CodeMed AI — Interactive Demo
==============================
Streamlit demo showcasing all four CodeMed AI engines.

Run:
  streamlit run demo/app.py --server.port 8501

Requires the CodeMed API running:
  uvicorn codemed.api:app --port 8001
  (or set CODEMED_API_URL env var)

Default API key: dev-key-codemed
(set CODEMED_API_KEY env var to override)
"""

import os
import json
import time

import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_BASE = os.environ.get("CODEMED_API_URL", "http://localhost:8001")
API_KEY  = os.environ.get("CODEMED_API_KEY", "dev-key-codemed")
HEADERS  = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="CodeMed AI",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def api_post(path: str, payload: dict) -> tuple[dict | None, str | None]:
    """POST to the CodeMed API. Returns (data, error)."""
    try:
        r = requests.post(f"{API_BASE}{path}", json=payload, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return r.json(), None
        return None, f"HTTP {r.status_code}: {r.text[:300]}"
    except requests.exceptions.ConnectionError:
        return None, f"Cannot reach API at {API_BASE}. Is `uvicorn codemed.api:app --port 8001` running?"
    except Exception as e:
        return None, str(e)


def api_get(path: str) -> tuple[dict | None, str | None]:
    """GET from the CodeMed API. Returns (data, error)."""
    try:
        r = requests.get(f"{API_BASE}{path}", headers=HEADERS, timeout=5)
        if r.status_code == 200:
            return r.json(), None
        return None, f"HTTP {r.status_code}: {r.text[:300]}"
    except requests.exceptions.ConnectionError:
        return None, f"Cannot reach API at {API_BASE}."
    except Exception as e:
        return None, str(e)


def raf_color(delta: float) -> str:
    if delta > 0:
        return "🟢"
    if delta == 0:
        return "⚪"
    return "🔴"


def score_badge(score: float) -> str:
    if score >= 94:
        return "🟢 Audit-Proof"
    if score >= 80:
        return "🟡 Needs Enhancement"
    return "🔴 High Risk"


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("## 🏥 CodeMed AI")
    st.markdown("*Medical Coding Intelligence*")
    st.divider()

    mode = st.radio(
        "Select Module",
        [
            "🏠 Overview",
            "⚕️ HCC Enforcement",
            "📋 MEAT Extractor",
            "🔍 Code Search",
            "⚖️ Appeals Generator",
        ],
        label_visibility="collapsed",
    )

    st.divider()
    # API health indicator
    health_data, health_err = api_get("/v1/health")
    if health_data:
        st.success("✅ API Online")
        engines = health_data.get("engines", {})
        for name, ok in engines.items():
            icon = "✅" if ok else "❌"
            st.caption(f"{icon} {name.upper()} engine")
    else:
        st.error("❌ API Offline")
        st.caption(f"`{API_BASE}`")
        st.caption("Run: `uvicorn codemed.api:app --port 8001`")

    st.divider()
    st.caption("API Key")
    st.code(API_KEY[:12] + "…", language=None)


# ===========================================================================
# OVERVIEW
# ===========================================================================

if mode == "🏠 Overview":
    st.title("CodeMed AI — Medical Coding Intelligence")
    st.markdown(
        "Built on **1,307 CMS LCD/NCD policies** · "
        "**V28 HCC hierarchy enforcement** · "
        "**94% audit defensibility target**"
    )
    st.divider()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("HCC Capture Rate", "89%", "+21pp vs baseline")
    c2.metric("Audit Defensibility", "94%", "+33pp vs baseline")
    c3.metric("Annual Revenue Lift", "+$29.4M", "per 50K-patient plan")
    c4.metric("Appeal Generation", "< 2 sec", "vs 15 min manual")

    st.divider()

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("🧠 Core Intelligence")
        st.markdown("""
| Feature | Description |
|---------|-------------|
| **1,307 CMS LCD/NCD policies** | Complete regulatory corpus with exact citations |
| **Natural Language Code Search** | ICD-10 / CPT / HCPCS via plain-English |
| **API-First HIPAA Architecture** | Secure REST endpoints, audit logs |
""")

    with col_r:
        st.subheader("🚀 2026 Value Drivers")
        st.markdown("""
| Feature | Description |
|---------|-------------|
| **V28 HCC Hierarchy** | Prevents code stacking, defensible RAF |
| **MEAT Evidence Extraction** | 94% audit defensibility score |
| **Automated Prior Auth Appeals** | Formal letters with LCD/NCD citations |
""")

    st.divider()
    st.info("👈 Select a module from the sidebar to start the demo")


# ===========================================================================
# HCC ENFORCEMENT
# ===========================================================================

elif mode == "⚕️ HCC Enforcement":
    st.title("⚕️ V28 HCC Hierarchy Enforcement")
    st.markdown(
        "Paste ICD-10 codes from a patient encounter. "
        "CodeMed enforces CMS V28 hierarchy rules — suppressing lower-severity "
        "HCCs when a higher-severity code is already present — and calculates "
        "the RAF score impact."
    )

    col_in, col_out = st.columns([1, 1])

    with col_in:
        st.subheader("Input")
        default_codes = "E11.40\nE11.9\nN18.4\nN18.32\nI50.22\nI50.9"
        codes_raw = st.text_area(
            "ICD-10 codes (one per line)",
            value=default_codes,
            height=160,
            help="Enter one ICD-10 code per line. Mixed severity codes trigger hierarchy enforcement.",
        )

        st.caption("**Try these examples:**")
        ex1, ex2, ex3 = st.columns(3)
        if ex1.button("Diabetes conflict", use_container_width=True):
            st.session_state["hcc_codes"] = "E11.40\nE11.9"
        if ex2.button("CKD cascade", use_container_width=True):
            st.session_state["hcc_codes"] = "N18.6\nN18.5\nN18.4\nN18.32"
        if ex3.button("Multi-group", use_container_width=True):
            st.session_state["hcc_codes"] = "E11.9\nI50.22\nJ44.1\nG30.9"

        if "hcc_codes" in st.session_state:
            codes_raw = st.session_state.pop("hcc_codes")

        run = st.button("⚕️ Enforce V28 Hierarchy", type="primary", use_container_width=True)

    with col_out:
        st.subheader("Results")
        if run:
            codes = [c.strip().upper() for c in codes_raw.splitlines() if c.strip()]
            if not codes:
                st.warning("Enter at least one ICD-10 code.")
            else:
                with st.spinner("Applying V28 rules…"):
                    data, err = api_post("/v1/hcc/enforce", {"icd10_codes": codes})

                if err:
                    st.error(err)
                elif data:
                    summary = data["summary"]

                    # RAF metrics
                    m1, m2, m3 = st.columns(3)
                    m1.metric("RAF Before", f"{data['raf_before']:.3f}")
                    m2.metric(
                        "RAF After (V28)",
                        f"{data['raf_after']:.3f}",
                        delta=f"{data['raf_delta']:+.3f}",
                        delta_color="normal",
                    )
                    m3.metric("Conflicts Resolved", summary["hierarchy_conflicts"])

                    st.divider()

                    # Active HCCs
                    if data["active_hccs"]:
                        st.markdown("**✅ Active HCCs (code these)**")
                        for h in data["active_hccs"]:
                            st.success(
                                f"HCC {h['hcc_number']} · {h['source_icd10']} · "
                                f"{h['hcc_description']} · RAF {h['raf_weight']:.3f}"
                            )

                    # Suppressed HCCs
                    if data["suppressed_hccs"]:
                        st.markdown("**⚠️ Suppressed HCCs (do NOT code)**")
                        for h in data["suppressed_hccs"]:
                            st.warning(
                                f"~~HCC {h['hcc_number']}~~ · {h['source_icd10']} · "
                                f"trumped by HCC {h['trumped_by_hcc']}"
                            )

                    # Unmapped codes
                    if data["unmapped_codes"]:
                        st.markdown("**ℹ️ No V28 HCC mapping**")
                        for c in data["unmapped_codes"]:
                            st.info(f"{c} — not in V28 crosswalk")

                    # Conflict detail
                    if data["hierarchy_conflicts"]:
                        with st.expander("📋 Conflict Detail"):
                            for c in data["hierarchy_conflicts"]:
                                st.markdown(
                                    f"**Group: {c['group']}**  \n"
                                    f"Winner: HCC {c['winner_hcc']} `{c['winner_icd10']}` "
                                    f"— {c['winner_description']}  \n"
                                    f"Suppressed: HCC {c['trumped_hcc']} `{c['trumped_icd10']}` "
                                    f"— RAF impact: **-{c['raf_impact']:.3f}**"
                                )
        else:
            st.info("Click **Enforce V28 Hierarchy** to see results.")


# ===========================================================================
# MEAT EXTRACTOR
# ===========================================================================

elif mode == "📋 MEAT Extractor":
    st.title("📋 MEAT Evidence Extractor")
    st.markdown(
        "Paste a clinical note. CodeMed scans for **M**onitoring, **E**valuation, "
        "**A**ssessment, and **T**reatment evidence and scores each coded diagnosis "
        "for audit defensibility. **Target: 94%.**"
    )

    col_in, col_out = st.columns([1, 1])

    with col_in:
        st.subheader("Input")
        default_note = """\
SUBJECTIVE:
Patient is a 68-year-old male with Type 2 Diabetes and CKD Stage 4.
Blood glucose logs reviewed — readings 140-190 mg/dL. Stable.

OBJECTIVE:
Vitals: BP 138/84, HR 72, Weight 198 lbs.
Labs: HbA1c 8.4%, serum creatinine 2.8 mg/dL, eGFR 24 mL/min.
Urinalysis: 2+ protein. BMP reviewed — electrolytes normal.

ASSESSMENT:
1. Type 2 Diabetes Mellitus, uncontrolled — A1c above goal 7.5%.
2. Chronic Kidney Disease, Stage 4 — eGFR stable vs last visit.

PLAN:
Metformin dose increased to 1000mg BID. Counseled on diabetic diet.
Lisinopril continued 10mg for renal protection. Nephrology referral placed.
Repeat renal labs in 3 months. Return to clinic in 4 weeks.\
"""

        clinical_note = st.text_area("Clinical Note", value=default_note, height=300)

        icd10_raw = st.text_input(
            "ICD-10 codes (comma-separated)",
            value="E11.9, N18.4, I10",
        )
        icd10_codes = [c.strip().upper() for c in icd10_raw.split(",") if c.strip()]

        run = st.button("📋 Extract MEAT Evidence", type="primary", use_container_width=True)

    with col_out:
        st.subheader("Results")
        if run:
            if not clinical_note.strip():
                st.warning("Paste a clinical note first.")
            elif not icd10_codes:
                st.warning("Enter at least one ICD-10 code.")
            else:
                with st.spinner("Extracting MEAT evidence…"):
                    data, err = api_post(
                        "/v1/meat/extract",
                        {"clinical_note": clinical_note, "icd10_codes": icd10_codes},
                    )

                if err:
                    st.error(err)
                elif data:
                    summary = data["summary"]
                    score = summary["overall_defensibility_score"]

                    # Overall score
                    badge = score_badge(score)
                    st.metric("Overall Defensibility Score", f"{score:.1f}%", badge)
                    st.progress(min(score / 100.0, 1.0))

                    st.divider()

                    # Per-diagnosis
                    for diag in summary.get("diagnoses", []):
                        ds = diag["defensibility_score"]
                        icon = "✅" if diag["is_supported"] else "⚠️"
                        label = f"{icon} {diag['icd10_code']} — {diag['description'][:50]}"
                        with st.expander(f"{label} · {ds:.0f}%"):
                            cats = diag.get("categories_found", [])
                            all_cats = ["monitoring", "evaluation", "assessment", "treatment"]
                            cat_row = st.columns(4)
                            for i, cat in enumerate(all_cats):
                                found = cat in cats
                                cat_row[i].markdown(
                                    f"{'✅' if found else '❌'} **{cat.upper()[0]}**  \n"
                                    f"{'Found' if found else 'Missing'}"
                                )

                            if diag.get("evidence"):
                                st.markdown("**Evidence Quotes:**")
                                for ev in diag["evidence"][:3]:
                                    st.caption(
                                        f"`{ev['category'].upper()}` — {ev['quote'][:120]}…"
                                    )
                            if diag.get("missing_categories"):
                                st.warning(
                                    "Add documentation for: "
                                    + ", ".join(diag["missing_categories"])
                                )
        else:
            st.info("Click **Extract MEAT Evidence** to analyse the note.")


# ===========================================================================
# CODE SEARCH
# ===========================================================================

elif mode == "🔍 Code Search":
    st.title("🔍 Natural Language Code Search")
    st.markdown(
        "Search ICD-10, CPT, and HCPCS codes using plain English. "
        "No more looking up code books — just describe what you need."
    )

    search_query = st.text_input(
        "Search query",
        value="cardiac monitoring atrial fibrillation",
        placeholder="e.g. knee replacement surgery, diabetes with kidney complications…",
    )

    c1, c2, c3 = st.columns([1, 1, 1])
    code_types = c1.multiselect(
        "Code types",
        ["ICD-10", "CPT", "HCPCS"],
        default=["ICD-10", "CPT", "HCPCS"],
    )
    max_results = c2.slider("Max results", 5, 30, 10)
    mode_sel = c3.radio("Mode", ["keyword", "semantic"], horizontal=True)

    run_search = st.button("🔍 Search Codes", type="primary")

    # Exact lookup
    st.divider()
    st.subheader("Exact Code Lookup")
    lookup_code_val = st.text_input("Enter a specific code", placeholder="e.g. E11.9, 99214, E0601")
    run_lookup = st.button("Look Up Code")

    if run_search and search_query:
        with st.spinner("Searching…"):
            data, err = api_post(
                "/v1/codes/search",
                {
                    "query": search_query,
                    "code_types": code_types or None,
                    "max_results": max_results,
                    "mode": mode_sel,
                },
            )

        if err:
            st.error(err)
        elif data:
            st.success(
                f"Found **{data['total_results']}** results in "
                f"**{data['latency_ms']}ms** (mode: {data['search_mode']})"
            )

            results = data["results"]
            if not results:
                st.info("No matching codes found. Try different search terms.")
            else:
                # Group by code type
                for ct in ["ICD-10", "CPT", "HCPCS"]:
                    group = [r for r in results if r["code_type"] == ct]
                    if group:
                        with st.expander(f"**{ct}** ({len(group)} results)", expanded=True):
                            for r in group:
                                c_code, c_desc, c_score, c_cat = st.columns([1, 3, 1, 1])
                                c_code.markdown(f"**`{r['code']}`**")
                                c_desc.markdown(r["description"][:80])
                                c_score.metric("Score", f"{r['relevance_score']:.0%}")
                                c_cat.caption(r["category"])

    if run_lookup and lookup_code_val.strip():
        code_clean = lookup_code_val.strip().upper()
        with st.spinner(f"Looking up {code_clean}…"):
            result, err = api_get(f"/v1/codes/lookup/{code_clean}")

        if err:
            st.error(err)
        elif result:
            if result.get("found"):
                st.success(
                    f"**{result['code']}** — {result['description']}  \n"
                    f"Type: `{result['code_type']}` · Category: {result['category']}"
                )
            else:
                st.warning(f"Code `{code_clean}` not found in the reference table.")


# ===========================================================================
# APPEALS GENERATOR
# ===========================================================================

elif mode == "⚖️ Appeals Generator":
    st.title("⚖️ Prior Auth Appeals Generator")
    st.markdown(
        "Fill in the denial details. CodeMed generates a formal, "
        "citation-backed appeal letter with specific CMS LCD/NCD IDs in seconds."
    )

    with st.form("appeals_form"):
        st.subheader("Patient & Claim Information")
        r1c1, r1c2, r1c3 = st.columns(3)
        patient_name      = r1c1.text_input("Patient Name",          "Robert Johnson")
        patient_dob       = r1c2.text_input("Date of Birth",         "1955-03-14")
        patient_id        = r1c3.text_input("Patient ID",            "PT-2024-08871")

        r2c1, r2c2, r2c3 = st.columns(3)
        member_id         = r2c1.text_input("Insurance Member ID",   "1EG4-TE5-MK72")
        claim_number      = r2c2.text_input("Claim Number",          "CLM-2024-449821")
        payer_name        = r2c3.text_input("Payer",                 "UnitedHealthcare")

        r3c1, r3c2 = st.columns(2)
        provider_name     = r3c1.text_input("Provider Name",         "Cardiology Associates")
        provider_npi      = r3c2.text_input("Provider NPI",          "1234567890")

        r4c1, r4c2, r4c3 = st.columns(3)
        service_date      = r4c1.text_input("Service Date",          "2024-11-15")
        denial_date       = r4c2.text_input("Denial Date",           "2024-11-28")
        policy_ids_raw    = r4c3.text_input(
            "LCD/NCD Policy IDs (optional)", "L33822",
            help="Specific CMS policy IDs to cite, e.g. L33822, 110.3",
        )

        st.subheader("Denial Details")
        cpt_raw      = st.text_input("Denied CPT Codes (comma-separated)", "93224, 93268")
        icd10_raw    = st.text_input("Diagnosis ICD-10 Codes",             "I48.0, R00.1")
        denial_reason = st.text_area(
            "Denial Reason (from EOB)",
            "Medical necessity not established for extended cardiac monitoring",
        )

        st.subheader("Clinical Summary")
        clinical_summary = st.text_area(
            "Brief clinical narrative",
            "Patient is a 69-year-old male with 3-month history of intermittent "
            "palpitations and one near-syncopal episode. Standard 12-lead ECG and "
            "24-hour Holter monitor were non-diagnostic. Extended cardiac event "
            "monitoring requested to capture paroxysmal atrial fibrillation.",
            height=100,
        )

        st.subheader("MEAT Evidence (optional — paste key quotes from chart)")
        meat_ev1 = st.text_input("Evidence 1", "MONITORING: Holter 2024-10-20 non-diagnostic; palpitations persist.")
        meat_ev2 = st.text_input("Evidence 2", "ASSESSMENT: Paroxysmal AFib (I48.0) — extended monitoring indicated.")
        meat_ev3 = st.text_input("Evidence 3", "TREATMENT: Metoprolol 25mg daily started; anticoagulation deferred.")
        meat_ev4 = st.text_input("Evidence 4", "")

        include_meat = st.checkbox("Include MEAT evidence in letter", value=True)

        submitted = st.form_submit_button(
            "⚖️ Generate Appeal Letter", type="primary", use_container_width=True
        )

    if submitted:
        cpt_codes  = [c.strip() for c in cpt_raw.split(",")   if c.strip()]
        icd10_codes = [c.strip() for c in icd10_raw.split(",") if c.strip()]
        policy_ids  = [p.strip() for p in policy_ids_raw.split(",") if p.strip()]
        meat_ev     = [e for e in [meat_ev1, meat_ev2, meat_ev3, meat_ev4] if e.strip()]

        payload = {
            "patient_name":       patient_name,
            "patient_dob":        patient_dob,
            "patient_id":         patient_id,
            "insurance_member_id": member_id,
            "provider_name":      provider_name,
            "provider_npi":       provider_npi,
            "service_date":       service_date,
            "claim_number":       claim_number,
            "denied_cpt_codes":   cpt_codes,
            "diagnosis_codes":    icd10_codes,
            "denial_reason":      denial_reason,
            "denial_date":        denial_date,
            "payer_name":         payer_name,
            "clinical_summary":   clinical_summary,
            "meat_evidence":      meat_ev,
            "policy_ids":         policy_ids,
            "include_meat":       include_meat,
        }

        with st.spinner("Generating appeal letter with CMS citations…"):
            data, err = api_post("/v1/appeals/generate", payload)

        if err:
            st.error(err)
        elif data:
            st.success(
                f"✅ Appeal letter generated · "
                f"{data['word_count']} words · "
                f"{len(data['policy_citations'])} policy citation(s)"
            )

            # Cited policies
            if data["policy_citations"]:
                with st.expander("📋 Policy Citations", expanded=True):
                    for p in data["policy_citations"]:
                        st.markdown(
                            f"**{p['type']} {p['policy_id']}**: {p['title']}  \n"
                            f"Effective: {p.get('effective_date', 'N/A')} · "
                            f"[View Policy]({p['url']})"
                        )

            # Regulatory citations
            if data["regulatory_citations"]:
                with st.expander("⚖️ Regulatory Citations"):
                    for c in data["regulatory_citations"]:
                        st.caption(f"• {c}")

            # Full letter
            st.subheader("Generated Appeal Letter")
            st.text_area(
                "Letter text (copy or download)",
                value=data["letter_text"],
                height=500,
                label_visibility="collapsed",
            )

            st.download_button(
                label="⬇️ Download Appeal Letter (.txt)",
                data=data["letter_text"],
                file_name=f"appeal_{claim_number.replace('-', '_')}.txt",
                mime="text/plain",
                use_container_width=True,
            )
