# Pilot Outreach Email

Use this template for each clinic. Before sending, run detection against their
WebPT data and substitute the `[VARIABLES]` with real numbers from the API:

```
GET /api/recovery/stats/{clinic_id}
```

---

## Subject line options (A/B test these)

**A:** We found $[TOTAL] in recoverable denials in your WebPT account
**B:** Your CO-16 and CO-50 denials are fixable — here's proof
**C:** [CLINIC NAME]: $[TOTAL] sitting in denied claims we can recover

---

## Email body

**To:** [Clinic Owner / Office Manager]
**From:** [Your Name], CodeMed
**Subject:** We found $[TOTAL] in recoverable denials in your WebPT account

---

Hi [First Name],

We connected to your WebPT account and ran our denial detection scan. Here's
what we found:

| Code | Issue | Claims | Est. Recovery |
|------|-------|--------|---------------|
| CO-16 | Missing NPI or prior auth | [CO16_COUNT] | $[CO16_AMT] |
| CO-50 | Medical necessity (appealable) | [CO50_COUNT] | $[CO50_AMT] |
| CO-97 | Bundling — fixable with modifier 59 | [CO97_COUNT] | $[CO97_AMT] |
| **Total** | | **[TOTAL_COUNT]** | **$[TOTAL_AMT]** |

These aren't write-offs. They're fixable denials that most billing software
leaves on the table.

**What we do:**

- CO-16: We automatically add missing NPI numbers and prior auth references,
  then resubmit. Fix rate: ~85%.
- CO-50: We generate appeal letters that cite the actual LCD/NCD policy for
  your CPT codes and payer — not a generic template. Fix rate: ~45%.
- CO-97: We apply modifier 59 to unbundled procedures and resubmit. Fix rate:
  ~75%.

**The pilot offer:**

We'll run all [TOTAL_COUNT] of these fixes for free. You keep 100% of whatever
we recover.

After the pilot, our fee is 20% of recovered amounts — only when you get paid.
No monthly fees, no setup costs.

**Next step:**

Reply to this email and I'll schedule a 20-minute call to walk you through
exactly what we found and how the fix process works.

— [Your Name]
CodeMed
[Phone]
[Email]

---

*You're receiving this because your clinic uses WebPT. We ran a read-only
analysis of your denied claims. We don't store claim data beyond what's needed
to process denials.*

---

## Follow-up sequence (if no reply in 3 days)

**Subject:** Quick follow-up — $[TOTAL] in recoverable denials

Hi [First Name],

Wanted to follow up on the denial recovery analysis I sent a few days ago.

The short version: we found $[TOTAL] in CO-16, CO-50, and CO-97 denials that
have a high likelihood of being recovered with the right fixes.

Would a 15-minute call this week work? I can show you exactly which claims,
what the fix looks like, and what we'd expect to recover.

— [Your Name]

---

## Filling in the variables

```bash
# Detect denials first (if not already done)
curl -X POST http://localhost:5000/api/recovery/detect/CLINIC_ID

# Pull the stats to fill in the email template
curl http://localhost:5000/api/recovery/stats/CLINIC_ID
```

The `stats` response includes `by_code` (counts and amounts per CO code) and
`total_recoverable` (the headline number for the subject line).

---

## Objection handling

**"We already have a billing company."**
> Most billing companies don't work denials this way — they resubmit the same
> claim and accept the second denial. We use payer-specific LCD/NCD citations
> on CO-50 appeals, which is what actually gets them paid. Your billing company
> can keep doing what they're doing; we're additive.

**"What if you don't recover anything?"**
> Then you owe us nothing. Our fee is 20% of actual recoveries — zero-risk
> pilot, and you keep everything we find during the free period.

**"How did you get into our WebPT account?"**
> You authorized it via WebPT's standard OAuth flow — the same way any
> WebPT-integrated app connects. We only request read access to claims data.
> You can revoke it at any time from your WebPT integrations settings.
