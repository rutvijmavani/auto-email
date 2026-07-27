"""
scripts/sync_dol_lca.py — Auto-sync DOL LCA quarterly disclosure files

Scrapes the DOL performance page for LCA Excel links, compares against
already-processed quarters in the DB, downloads and ingests new ones.

Two tables are scraped:
  1. "OFLC Programs and Disclosures" (summary attr) — current FY quarter
  2. "LCA Programs (H-1B, H-1B1, E-3)" (summary attr) — historical FY2025 backfill

Schedule on Oracle server (crontab):
    0 6 1 * * cd /home/opc/mail && python scripts/sync_dol_lca.py

DOL fiscal year quarters:
    Q1: Oct–Dec   Q2: Jan–Mar   Q3: Apr–Jun   Q4: Jul–Sep
    Files are typically published 1–3 months after quarter end.
"""

import os
import re
import smtplib
import subprocess
import sys
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import EMAIL, APP_PASSWORD
from db.connection import get_conn
from logger import get_logger, init_logging

log = get_logger(__name__)

DOL_PERFORMANCE_URL = "https://www.dol.gov/agencies/eta/foreign-labor/performance"
DOL_BASE_URL        = "https://www.dol.gov"

DOWNLOAD_DIR = Path(os.environ.get("DOL_LCA_CACHE_DIR", "/home/opc/mail/data/dol_lca"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Only backfill these fiscal years from the historical table.
# FY2026 comes from the current-quarter table automatically.
BACKFILL_YEARS = {2025}


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_processed_quarters() -> set[str]:
    """Return all quarter identifiers already ingested into dol_h1b_employers."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT DISTINCT unnest(quarters_processed) AS q FROM dol_h1b_employers"
        ).fetchall()
        return {r["q"] for r in rows}
    except Exception:
        return set()
    finally:
        conn.close()


def count_employers_for_quarter(quarter: str) -> int:
    """Count employers that have the given quarter in quarters_processed."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM dol_h1b_employers WHERE %s = ANY(quarters_processed)",
            (quarter,),
        ).fetchone()
        return row["n"] if row else 0
    except Exception:
        return 0
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Quarter helpers
# ─────────────────────────────────────────────────────────────────────────────

def _extract_quarter(text: str) -> str | None:
    """Extract a quarter identifier like 'FY2026_Q2' from a filename or URL."""
    m = re.search(r"FY(\d{4})_?Q(\d)", text, re.IGNORECASE)
    if m:
        return f"FY{m.group(1)}_Q{m.group(2)}"
    return None


def _resolve_url(href: str) -> str | None:
    """Resolve a relative DOL href to an absolute URL; reject non-DOL absolute hrefs."""
    if not href.startswith("http"):
        return f"{DOL_BASE_URL}{href}"
    dol_host = "dol.gov"
    from urllib.parse import urlparse
    if dol_host not in urlparse(href).netloc:
        log.warning("Rejected non-DOL URL in scrape result: %s", href)
        return None
    return href


# ─────────────────────────────────────────────────────────────────────────────
# Page scraping — two separate tables
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_page() -> BeautifulSoup | None:
    log.info("Fetching DOL performance page …")
    try:
        r = requests.get(DOL_PERFORMANCE_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as exc:
        log.error("Could not fetch DOL page: %s", exc)
        return None


def scrape_current_quarter(soup: BeautifulSoup) -> dict[str, str]:
    """
    Find the current-quarter LCA file from the "OFLC Programs and Disclosures" table.
    That table has one row per program; the LCA row's first .xlsx link is the current file.
    Returns {quarter_id: url}.
    """
    table = soup.find("table", summary=lambda s: s and "OFLC Programs and Disclosures" in s)
    if table is None:
        log.warning("Could not find 'OFLC Programs and Disclosures' table on page")
        return {}

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        row_text = cells[0].get_text(strip=True)
        if "LCA Programs" not in row_text:
            continue
        # LCA row found — grab first .xlsx link in the row
        for a in row.find_all("a", href=True):
            if a["href"].lower().endswith(".xlsx"):
                quarter = _extract_quarter(a["href"]) or _extract_quarter(a.get_text())
                if quarter:
                    url = _resolve_url(a["href"])
                    if url:
                        log.info("Current quarter: %s → %s", quarter, url)
                        return {quarter: url}

    log.warning("LCA Programs row not found in current-quarter table")
    return {}


def scrape_historical_quarters(soup: BeautifulSoup) -> dict[str, str]:
    """
    Find historical LCA quarterly files from the "LCA Programs (H-1B, H-1B1, E-3)" table.
    That table has one row per fiscal year; each row contains all quarterly .xlsx links.
    Only processes fiscal years listed in BACKFILL_YEARS.
    Returns {quarter_id: url}.
    """
    table = soup.find(
        "table",
        summary=lambda s: s and "LCA Programs" in s and "H-1B" in s,
    )
    if table is None:
        log.warning("Could not find historical 'LCA Programs' table on page")
        return {}

    found = {}
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        # First cell typically contains the fiscal year number
        year_text = cells[0].get_text(strip=True)
        m = re.search(r"\b(20\d{2})\b", year_text)
        if not m:
            continue
        year = int(m.group(1))
        if year not in BACKFILL_YEARS:
            continue
        # Collect all .xlsx links in this row
        for a in row.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith(".xlsx"):
                continue
            quarter = _extract_quarter(href) or _extract_quarter(a.get_text())
            if quarter:
                url = _resolve_url(href)
                if url:
                    found[quarter] = url
                    log.info("Historical: %s → %s", quarter, url)

    log.info("Found %d historical LCA files for years %s", len(found), BACKFILL_YEARS)
    return found


# ─────────────────────────────────────────────────────────────────────────────
# Download
# ─────────────────────────────────────────────────────────────────────────────

def download(url: str, quarter: str) -> Path | None:
    """Download a file to DOWNLOAD_DIR. Returns local path or None on failure."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = DOWNLOAD_DIR / f"LCA_{quarter}.xlsx"

    if dest.exists():
        log.info("Already downloaded: %s", dest)
        return dest

    log.info("Downloading %s …", url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=300, stream=True)
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
        size_mb = dest.stat().st_size / (1024 * 1024)
        log.info("Downloaded %.1f MB → %s", size_mb, dest)
        return dest
    except Exception as exc:
        log.error("Download failed for %s: %s", url, exc)
        if dest.exists():
            dest.unlink()
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Ingest
# ─────────────────────────────────────────────────────────────────────────────

_INGEST_TIMEOUT_S = 3600  # 1 hour; a quarterly file should process well within this

def ingest(file_path: Path, quarter: str) -> bool:
    """Run process_dol_lca.py on a downloaded file. Returns True on success."""
    script = Path(__file__).parent / "process_dol_lca.py"
    cmd    = [sys.executable, str(script), "--file", str(file_path), "--quarter", quarter]
    log.info("Running ingestion for %s …", quarter)
    try:
        result = subprocess.run(cmd, capture_output=False, timeout=_INGEST_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        log.error("Ingestion timed out after %ds for %s", _INGEST_TIMEOUT_S, quarter)
        return False
    if result.returncode != 0:
        log.error("Ingestion failed for %s (exit %d)", quarter, result.returncode)
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Email notification
# ─────────────────────────────────────────────────────────────────────────────

def send_notification(ingested: list[str], failed: list[str]) -> None:
    """Send an email summary of the sync run."""
    from_email   = EMAIL
    app_password = APP_PASSWORD
    to_email     = os.environ.get("NOTIFY_EMAIL", from_email)

    if not from_email or not app_password:
        log.warning("EMAIL / APP_PASSWORD not set — skipping notification")
        return

    lines = ["DOL LCA sync completed.\n"]
    if ingested:
        lines.append(f"Ingested quarters ({len(ingested)}):")
        for q in ingested:
            count = count_employers_for_quarter(q)
            lines.append(f"  {q}: {count:,} employers in DB")
        lines.append("")
    if failed:
        lines.append(f"Failed quarters ({len(failed)}): {', '.join(failed)}")
        lines.append("")
    if not ingested and not failed:
        lines.append("Nothing new — all quarters already processed.")

    subject = (
        f"DOL LCA ingested: {', '.join(ingested)}"
        if ingested
        else "DOL LCA sync — nothing new"
    )
    body = "\n".join(lines)

    try:
        msg            = EmailMessage()
        msg["From"]    = from_email
        msg["To"]      = to_email
        msg["Subject"] = subject
        msg.set_content(body)

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.starttls()
            server.login(from_email, app_password)
            server.send_message(msg)
        log.info("Notification sent to %s", to_email)
    except Exception as exc:
        log.warning("Could not send notification email: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    processed = get_processed_quarters()
    log.info("Already processed: %s", sorted(processed) or "none")

    soup = _fetch_page()
    if soup is None:
        log.error("Cannot proceed without DOL page — exiting")
        sys.exit(1)

    # Gather all available quarters from both tables
    available: dict[str, str] = {}
    available.update(scrape_current_quarter(soup))
    available.update(scrape_historical_quarters(soup))

    if not available:
        log.error("No LCA files found on DOL page — check page structure")
        sys.exit(1)

    # Filter to only unprocessed quarters
    new_quarters = {q: url for q, url in available.items() if q not in processed}
    if not new_quarters:
        log.info("All quarters already processed — nothing to do")
        send_notification([], [])
        return

    log.info("New quarters to process: %s", sorted(new_quarters))

    ingested: list[str] = []
    failed:   list[str] = []

    for quarter in sorted(new_quarters):
        url = new_quarters[quarter]

        local_file = download(url, quarter)
        if local_file is None:
            failed.append(quarter)
            continue

        if ingest(local_file, quarter):
            ingested.append(quarter)
            try:
                local_file.unlink()
                log.info("Deleted %s after successful ingestion", local_file.name)
            except Exception as exc:
                log.warning("Could not delete %s: %s", local_file, exc)
        else:
            failed.append(quarter)
            log.info("Keeping %s for retry on next run", local_file.name)

    log.info("Done. Ingested: %s | Failed: %s", ingested or "none", failed or "none")

    send_notification(ingested, failed)


if __name__ == "__main__":
    init_logging("sync_dol_lca")
    main()
