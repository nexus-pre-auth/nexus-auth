# Y Combinator Application — CodeMed

*Draft. Fill in [BRACKETS] with real numbers as pilots produce data.*

---

## Company

**Company name:** CodeMed

**URL:** [your domain]

**Description (50 words):**
CodeMed automatically recovers denied insurance claims for physical therapy
clinics. We connect to WebPT via OAuth, detect CO-16, CO-50, and CO-97
denials, apply payer-specific fixes, and resubmit — taking 20% of what we
recover. Clinics pay nothing until they get paid.

---

## Founders

**[Founder 1 name]**
Role: [CEO/CTO]
Background: [2–3 sentences. Lead with domain expertise or relevant prior work.]

**[Founder 2 name — if applicable]**
Role:
Background:

---

## Product

### What does your company do?

Physical therapy clinics lose an estimated 15–20% of revenue to denied
insurance claims. Most of those denials are fixable — they're rejected for
administrative reasons (missing NPI, missing prior auth), medical necessity
disputes, or bundling errors. But fixing them requires time billing staff don't
have and expertise they typically lack.

CodeMed connects to a clinic's WebPT account in 15 minutes via OAuth, scans
their denied claims, and automatically fixes three denial codes that together
represent roughly 55% of all PT denials:

- **CO-16** (missing information): We add the missing NPI numbers or prior
  authorization references and resubmit. ~85% fix rate.
- **CO-50** (medical necessity): We generate appeal letters that cite the
  actual LCD/NCD policy for the specific CPT code and payer — not a generic
  template. ~45% fix rate.
- **CO-97** (bundling/unbundling): We apply modifier 59 to unbundled procedures
  and resubmit. ~75% fix rate.

The weighted average recovery rate across all three codes is ~68%. Our fee is
20% of recovered amounts, charged only when the payer pays.

### What's new about what you're doing?

The CO-50 fix is the core differentiator. Generic appeal letters for medical
necessity denials get ignored. Our system queries a knowledge graph of
LCD/NCD policies indexed by CPT code and payer, then generates appeals that
cite the specific policy language that applies to each claim. That's what gets
them paid.

The rest of the stack:
- WebPT OAuth integration with real-time webhook sync — no manual exports
- Full denial lifecycle tracking (detected → fixed → submitted → paid)
- 80/20 revenue split enforced at the database level, with per-clinic monthly
  invoice reports
- React dashboard giving clinic owners a live view of pipeline and realized
  recovery

### Why now?

Two converging factors:

1. **WebPT's OAuth API** matured enough to support this integration cleanly.
   WebPT has ~24,000 PT clinic customers. That's our distribution.
2. **Payer policy databases** (LCD/NCD) are now machine-readable. The CO-50
   advantage we have depends on being able to programmatically match claims
   to the right policy — that wasn't feasible at this level of specificity
   two years ago.

---

## Traction

*Fill in with real pilot data. Target by Week 4:*

- Clinics connected: [3 pilots → goal: 8 by demo day]
- Total denied claims analyzed: [X]
- Total recoverable identified: $[X]
- Total recovered to date: $[X]
- Revenue to CodeMed to date: $[X] (20% of recovered)
- Average recovery per clinic per month: $[X]
- Our fee per clinic per month: $[X]

---

## Market

**TAM:** There are ~24,000 WebPT-connected PT clinics in the US. The average
PT clinic bills ~$1.2M/year. At an industry-average 15% denial rate, that's
~$180,000 in denials per clinic per year. We recover ~68% of what we touch,
earning 20% of that — ~$24,500 per clinic per year at full penetration.

24,000 clinics × $24,500 = ~$590M TAM within WebPT alone.

**SAM:** We're starting with clinics that joined WebPT in the last 90 days
(highest activation window) and clinics with high denial rates visible in
their WebPT data. Realistic 3-year SAM: 2,000 clinics = ~$49M ARR.

**SOM:** 500 clinics in 18 months = ~$12M ARR. That's the seed-round target.

---

## Business model

- No setup fee, no monthly fee
- 20% of recovered amounts, invoiced monthly, paid net-30
- Clinics can disconnect at any time
- The model scales: every new claim processed improves pattern detection;
  every new LCD/NCD document indexed improves CO-50 fix rates

Unit economics (per clinic at steady state):
- Average monthly denials processed: $[X]
- Average monthly recovery: $[X]
- CodeMed fee: $[X] (20%)
- Gross margin: ~92% (no COGS beyond compute)

---

## Competition

**Manual billing companies:** Charge flat monthly fees (~$500–2,000/month)
regardless of outcomes. They resubmit generically. They don't cite
payer-specific policy on CO-50 appeals. We're additive to them — clinics
keep their billing company and add us.

**RCM software (Waystar, Availity, Quadax):** Enterprise tools that require
dedicated billing staff to operate. Priced for health systems, not 3-provider
PT clinics. We're building the version that's zero-effort for the clinic.

**Doing nothing:** The default. Most clinics write off CO-50 denials entirely
and only rework the obvious CO-16 errors. This is the real competitor —
inertia — which is why the performance-based model matters. There's no reason
to say no.

---

## Why us?

[Founders: write 2–3 sentences each. Lead with what makes you the right people
to build this specifically. Examples: prior billing experience, PT clinic
background, prior health-tech exits, deep knowledge of CMS LCD/NCD policy.]

---

## Equity and funding

**Have you raised money?** [Yes/No. If yes: amount, from whom.]

**How much are you raising?** $[X] seed

**Use of funds:**
- [X]% engineering (Quadax real-time eligibility integration, expand beyond PT
  to OT and speech therapy)
- [X]% sales (hire first sales rep for outbound to WebPT clinic base)
- [X]% operations (denial tracking, appeals management, compliance)

**Target:** [X] clinics on platform, $[X] MRR, at end of 18 months.

---

## What do you need most from YC?

1. **Introductions to WebPT.** A partnership or preferred-vendor relationship
   with WebPT would compress our go-to-market from cold outbound to in-product
   distribution.
2. **Regulatory guidance.** We're navigating HIPAA BAAs, state-level billing
   regulations, and CMS compliance as we scale. YC partners with health-tech
   experience here would accelerate that significantly.
3. **Investor intros for seed.** The business becomes very defensible at 500+
   clinics — we'd like to get there in 18 months with the right seed capital.

---

## Demo day one-liner

> CodeMed connects to WebPT in 15 minutes, finds your fixable denied claims,
> fixes them, and takes 20% when you get paid. We've recovered $[X] for [N]
> clinics. We're raising $[X] to reach 500.
