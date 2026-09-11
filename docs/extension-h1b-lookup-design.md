# Chrome Extension — H1B Company Lookup Design

> Design note only — not yet implemented. Full spec to be done later.

---

## Purpose

When a user visits a job listing, the extension should show whether the company
sponsors H1B visas and their track record (petition count, approval rate, recent LCA volume).
This is the primary value of the extension — more useful than ATS detection.

---

## Core Decision: Domain-Only Matching

**Fuzzy name matching is not reliable enough.** Legal names on LCA filings diverge
from brand names in job descriptions (e.g. `Chase` → `JPMorgan Chase Bank, N.A.`).

Only domain-based lookups are trustworthy. Two signals the extension can extract:

| Signal | Example URL | Extracted |
|---|---|---|
| ATS platform + company slug | `boards.greenhouse.io/stripe` | platform=`greenhouse`, slug=`stripe` |
| ATS subdomain slug | `stripe.myworkdayjobs.com` | platform=`workday`, slug=`stripe` |
| Career subdomain | `careers.stripe.com` | domain=`stripe.com` |

No name matching at all. If neither signal resolves → return `{"found": false}` → extension shows nothing.

---

## API Endpoint (to be built)

```
GET /lookup-company
  ?platform=greenhouse&slug=stripe     # ATS slug lookup
  ?domain=stripe.com                   # direct domain lookup
```

### Slug → FEIN resolution (Cases 1 & 2)

```sql
SELECT f.employer_fein, f.public_domain,
       u.petition_count
FROM company_ats ca
JOIN fein_domain_map f ON ca.employer_fein = f.employer_fein
LEFT JOIN uscis_petition_counts u ON u.employer_fein = f.employer_fein
WHERE ca.platform = %s AND ca.slug = %s
```

Company must already be in `company_ats` (detected by our pipeline). Companies not
yet detected → `{"found": false}` → likely small companies that don't sponsor anyway.

### Domain → FEIN resolution (Case 3)

```sql
SELECT f.employer_fein, u.petition_count
FROM fein_domain_map f
LEFT JOIN uscis_petition_counts u ON u.employer_fein = f.employer_fein
WHERE regexp_replace(regexp_replace(LOWER(f.public_domain), '^https?://', ''), '^www\.', '')
    = regexp_replace(regexp_replace(LOWER(%s), '^https?://', ''), '^www\.', '')
```

---

## Response Schema

```json
{
  "found": true,
  "match_method": "slug",
  "company_name": "Stripe, Inc.",
  "petition_count": 847,
  "lca_count_last_year": 312,
  "approval_rate": 0.91,
  "sponsors_h1b": true,
  "not_tracked": false
}
```

`not_tracked: true` = company sponsors H1B (LCA data confirms) but we don't monitor
their job board (below petition threshold). Extension still shows H1B data.

---

## Coverage

| Page type | Signal available | Coverage |
|---|---|---|
| Greenhouse / Lever / Ashby / iCIMS | platform + slug | ✅ for companies in our DB |
| Workday / SuccessFactors / Taleo | ATS subdomain slug | ✅ for companies in our DB |
| Career subdomain (`careers.company.com`) | root domain | ✅ always |
| LinkedIn / Indeed | company name only | ❌ no reliable signal — show nothing |

LinkedIn/Indeed: honest silence is better than a wrong match.

---

## Not Doing

- Fuzzy name matching
- Wikidata lookup at query time (too slow, too expensive)
- `brand_fein_alias` table (manual curation — revisit if slug coverage is insufficient)
