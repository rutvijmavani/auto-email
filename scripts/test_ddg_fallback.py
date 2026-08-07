"""
scripts/test_ddg_fallback.py — Test DuckDuckGo Instant Answers + domain inference as website fallback.

Queries DDG for every company in h1b_ats_discovery that has been checked (last_checked IS NOT NULL)
but has no website_url, then falls back to domain inference for anything DDG misses.
Goal: measure combined reliability before integrating into the main pipeline.

Usage:
    python scripts/test_ddg_fallback.py [--limit N] [--delay SECS] [--output FILE]

Options:
    --limit N       Max companies to query (default: all)
    --delay SECS    Seconds between DDG requests (default: 1.0)
    --output FILE   Write CSV results to FILE
"""

import argparse
import csv
import os
import re
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests

from db.connection import get_conn
from logger import get_logger, init_logging

DDG_API = "https://api.duckduckgo.com/"

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# DDG lookup
# ---------------------------------------------------------------------------

def _ddg_extract_official_website(data: dict) -> str:
    """
    Pull official website from DDG response, in priority order:
      1. OfficialWebsite  — top-level field, most reliable
      2. OfficialDomain   — domain only; prepend https://
      3. Results[]        — entries labelled "Official site"
      4. Infobox content  — data_type == "official_website"
    """
    # 1. Direct field
    ow = data.get("OfficialWebsite") or ""
    if ow:
        return ow

    # 2. Domain-only field
    od = data.get("OfficialDomain") or ""
    if od:
        return "https://" + od

    # 3. Results[] "Official site" entries
    for r in data.get("Results") or []:
        text = r.get("Text") or ""
        url  = r.get("FirstURL") or ""
        if "official site" in text.lower() and url:
            return url

    # 4. Infobox
    for item in (data.get("Infobox") or {}).get("content") or []:
        if item.get("data_type") == "official_website":
            v = item.get("value") or ""
            if isinstance(v, str) and v.startswith("http"):
                return v

    return ""


def _ddg_lookup(query: str, session: requests.Session) -> dict:
    """Call DDG Instant Answers API and return extracted website + metadata."""
    try:
        r = session.get(
            DDG_API,
            params={
                "q":            query,
                "format":       "json",
                "no_redirect":  "1",
                "no_html":      "1",
                "skip_disambig": "1",
            },
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()

        website = _ddg_extract_official_website(data)
        return {
            "website":       website,
            "heading":       data.get("Heading") or "",
            "entity":        data.get("Entity") or "",
            "ddg_type":      data.get("Type") or "",
            "abstract_text": (data.get("AbstractText") or "")[:120],
        }
    except Exception as exc:
        return {"error": str(exc), "website": ""}


# ---------------------------------------------------------------------------
# Domain inference
# ---------------------------------------------------------------------------

# Legal type words to strip before building a slug
_SLUG_STRIP = re.compile(
    r"\b(inc|llc|ltd|corp|lp|plc|pvt|co|na|llp|corporation|incorporated|"
    r"limited|company|partnership|group|holdings|international|global|"
    r"services|solutions|technologies|systems|enterprises|ventures|"
    r"consulting|management|associates|partners|us|usa|america|americas)\b",
    re.IGNORECASE,
)

_NON_ALPHA = re.compile(r"[^a-z0-9]+")


def _infer_domains(canonical: str, legal: str) -> list[str]:
    """
    Generate candidate domains from the company name.
    Returns up to 3 guesses, most-likely first:
      1. slug of canonical name (stripped of noise words) + .com
      2. first-word of canonical + .com  (if > 4 chars and different from #1)
      3. same from legal name if canonical was empty
    """
    candidates = []

    def _slug(s: str) -> str:
        s = _SLUG_STRIP.sub(" ", s or "").strip()
        s = _NON_ALPHA.sub("", s.lower())
        return s

    def _first_word_slug(s: str) -> str:
        # first meaningful token after stripping noise
        parts = _SLUG_STRIP.sub(" ", s or "").split()
        parts = [p for p in parts if len(p) > 1]
        return parts[0].lower() if parts else ""

    base = canonical or legal
    full = _slug(base)
    first = _first_word_slug(base)

    if full and len(full) > 2:
        candidates.append(full + ".com")
    if first and len(first) > 3 and (first + ".com") not in candidates:
        candidates.append(first + ".com")

    # Try legal name too if canonical gave nothing useful
    if canonical and canonical != legal:
        lslugged = _slug(legal)
        if lslugged and len(lslugged) > 2 and (lslugged + ".com") not in candidates:
            candidates.append(lslugged + ".com")

    return candidates[:3]


def _probe_domain(domain: str, session: requests.Session) -> str | None:
    """HEAD request to https://domain — return URL if it resolves, else None."""
    try:
        r = session.head(
            "https://" + domain,
            timeout=6,
            allow_redirects=True,
        )
        if r.status_code < 400:
            return r.url
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",  type=int,   default=None)
    parser.add_argument("--delay",  type=float, default=1.0)
    parser.add_argument("--output", type=str,   default=None)
    args = parser.parse_args()

    init_logging()

    sql = """
        SELECT d.employer_fein, d.employer_name, h.canonical_name, h.kg_mid
        FROM   dol_h1b_employers d
        JOIN   h1b_ats_discovery h ON h.employer_fein = d.employer_fein
        WHERE  h.last_checked IS NOT NULL
          AND  h.website_url  IS NULL
        ORDER BY d.employer_name
    """
    if args.limit:
        sql += f" LIMIT {args.limit}"
    with get_conn() as conn:
        rows = conn.execute(sql).fetchall()

    if not rows:
        print("No companies without website found — nothing to test.")
        return

    print(f"Testing DDG + domain inference on {len(rows)} companies …\n")

    results  = []
    ddg_hit  = 0
    dom_hit  = 0
    total_found = 0

    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (compatible; h1b-pipeline-test/1.0)"

    for i, (fein, legal_name, canonical_name, kg_mid) in enumerate(rows, 1):
        query_str = canonical_name or legal_name

        # ── DDG ──────────────────────────────────────────────────────────
        ddg       = _ddg_lookup(query_str, session)
        ddg_url   = ddg.get("website") or ""
        ddg_found = bool(ddg_url)

        # ── Domain inference (only if DDG missed) ────────────────────────
        domain_guesses  = _infer_domains(canonical_name or "", legal_name)
        domain_hit_url  = ""
        domain_hit_name = ""

        if not ddg_found:
            for dom in domain_guesses:
                resolved = _probe_domain(dom, session)
                if resolved:
                    domain_hit_url  = resolved
                    domain_hit_name = dom
                    break

        # ── Tallies ───────────────────────────────────────────────────────
        if ddg_found:
            ddg_hit += 1
            total_found += 1
            source = "ddg"
            final_url = ddg_url
        elif domain_hit_url:
            dom_hit += 1
            total_found += 1
            source = "domain"
            final_url = domain_hit_url
        else:
            source = "miss"
            final_url = ""

        result = {
            "fein":           fein,
            "legal_name":     legal_name,
            "canonical":      canonical_name or "",
            "kg_mid":         kg_mid or "",
            "source":         source,
            "final_url":      final_url,
            "ddg_url":        ddg_url,
            "ddg_heading":    ddg.get("heading", ""),
            "ddg_entity":     ddg.get("entity", ""),
            "ddg_type":       ddg.get("ddg_type", ""),
            "abstract_text":  ddg.get("abstract_text", ""),
            "domain_guesses": " | ".join(domain_guesses),
            "domain_hit":     domain_hit_name,
            "error":          ddg.get("error", ""),
        }
        results.append(result)

        mark = {"ddg": "D", "domain": "I", "miss": "✗"}[source]
        print(
            f"[{i:>3}/{len(rows)}] [{mark}]  {query_str[:42]:<42}  →  {(final_url or '(miss)')[:60]}"
        )

        if i < len(rows):
            time.sleep(args.delay)

    # ── Summary ───────────────────────────────────────────────────────────
    n = len(rows)
    print(f"\n{'='*65}")
    print(f"Total tested       : {n}")
    print(f"[D] DDG hit        : {ddg_hit:>4}  ({100*ddg_hit/n:.1f}%)")
    print(f"[I] Inferred domain: {dom_hit:>4}  ({100*dom_hit/n:.1f}%)")
    print(f"    Combined        : {total_found:>4}  ({100*total_found/n:.1f}%)")
    print(f"    Still missing   : {n-total_found:>4}  ({100*(n-total_found)/n:.1f}%)")
    print(f"\nLegend: D=DuckDuckGo  I=domain inference  ✗=miss")

    if args.output:
        fields = [
            "fein", "legal_name", "canonical", "kg_mid", "source", "final_url",
            "ddg_url", "ddg_heading", "ddg_entity", "ddg_type", "abstract_text",
            "domain_guesses", "domain_hit", "error",
        ]
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(results)
        print(f"\nFull results → {args.output}")


if __name__ == "__main__":
    main()
