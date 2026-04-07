"""
shallow_crawler.py
==================
Requisito 1: Shallow (surface-level) crawling.

Scans the main page HTML for internal links whose href or anchor text
match date-related keywords (e.g. "important-dates", "deadlines",
"call-for-papers").  Downloads at most MAX_SUBPAGES sub-pages and
returns their date-relevant text merged and deduplicated.

Token-safe: the combined output never exceeds MAX_SMART_TEXT_CHARS.

CHANGES:
  FIX-C1 — Expanded _LINK_KEYWORDS: added 'notification', 'program', 'schedule',
             'registration', 'key-date', 'registration', 'brochure'.
  FIX-C2 — Added field_aware_crawl_needed(): crawl trigger is now field-aware —
             crawl fires even when date_hits >= 3 if key fields are missing.
  FIX-C3 — MAX_SUBPAGES raised from 2 to 3.
"""

import re
import logging
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from config import (
    HTTP_HEADERS, REQUEST_TIMEOUT, MAX_SMART_TEXT_CHARS,
)
from text_extractor import extract_date_text

log = logging.getLogger(__name__)

# ─── keywords that signal a date-relevant sub-page ──────────────────

# FIX-C1: Expanded keyword set to catch more sub-page patterns
_LINK_KEYWORDS = re.compile(
    r"important[.\-_\s]?dates?|deadlines?|call[.\-_\s]?for[.\-_\s]?papers?"
    r"|key[.\-_\s]?dates?|submission|cfp|plazos|fechas"
    r"|notification|program(?:me)?|schedule|registration|brochure"
    r"|dates[.\-_\s]?importantes|convocatoria",
    re.IGNORECASE,
)

MAX_SUBPAGES = 3  # FIX-C3: raised from 2


# ─── public API ─────────────────────────────────────────────────────

def find_date_links(html: str, base_url: str) -> list[str]:
    """
    Scan <a> tags in *html* for hrefs whose text or URL match
    date-related keywords.  Returns at most MAX_SUBPAGES resolved,
    deduplicated URLs that belong to the same domain.
    """
    soup = BeautifulSoup(html, "html.parser")
    base_domain = urlparse(base_url).netloc.lower()
    base_norm = base_url.rstrip("/").lower()

    seen: set[str] = {base_norm}
    found: list[str] = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if not href or href.startswith(("#", "mailto:", "javascript:")):
            continue

        anchor_text = a_tag.get_text(strip=True)
        # Match on anchor text OR href path
        if not (_LINK_KEYWORDS.search(anchor_text) or
                _LINK_KEYWORDS.search(href)):
            continue

        resolved = urljoin(base_url, href)
        # Only same-domain links
        if urlparse(resolved).netloc.lower() != base_domain:
            continue

        norm = resolved.rstrip("/").lower()
        if norm in seen:
            continue
        seen.add(norm)
        found.append(resolved)

        if len(found) >= MAX_SUBPAGES:
            break

    if found:
        log.info("  [ShallowCrawl] Found %d date-related sub-link(s): %s",
                 len(found), found)
    return found


def field_aware_crawl_needed(
    date_hits: int,
    date_text_len: int,
    regex_dates: dict,
) -> bool:
    """
    FIX-C2: Determine whether a shallow crawl is needed based on the actual
    fields extracted, not just raw text length.

    Crawl fires when ANY of the following conditions is true:
      1. Original P8 trigger: date_hits < 3 AND text short (< 1500 chars)
      2. Key deadline fields are ALL missing (submission + notification + registration)
         even if the page has plenty of text — dates are on a sub-page
      3. fecha_inicio or fecha_fin are missing AND at least one deadline IS set
         (page has partial dates, conference start/end is elsewhere)
    """
    # Condition 1: original sparse-content trigger (P8)
    if date_hits < 3 and date_text_len < 1500:
        return True

    # Condition 2: all key deadlines missing (dates are on a different sub-page)
    deadline_fields = ["envio_trabajo", "notificacion_aceptacion", "inscripcion"]
    all_deadlines_missing = all(regex_dates.get(f) is None for f in deadline_fields)
    if all_deadlines_missing:
        log.info(
            "  [FIX-C2] All deadline fields missing — triggering field-aware crawl."
        )
        return True

    # Condition 3: conference dates missing but deadlines present
    conf_dates_missing = (
        regex_dates.get("fecha_inicio") is None
        or regex_dates.get("fecha_fin") is None
    )
    some_deadline_found = any(regex_dates.get(f) for f in deadline_fields)
    if conf_dates_missing and some_deadline_found:
        log.info(
            "  [FIX-C2] Conference dates missing while deadlines found — triggering crawl."
        )
        return True

    return False


def fetch_supplementary_text(
    sub_urls: list[str],
    main_text: str,
    budget: int | None = None,
) -> str:
    """
    Download sub-pages, extract date-relevant text from each,
    deduplicate against *main_text*, and return the merged supplement
    truncated to *budget* characters (default MAX_SMART_TEXT_CHARS // 2).

    The caller is expected to append this to the main_text with a
    separator, so we only use half the character budget by default.
    """
    if not sub_urls:
        return ""

    budget = budget or MAX_SMART_TEXT_CHARS // 2
    parts: list[str] = []
    used = 0

    for url in sub_urls:
        try:
            resp = requests.get(url, headers=HTTP_HEADERS,
                                timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            sub_html = resp.text
        except Exception as exc:
            log.warning("  [ShallowCrawl] Failed to fetch %s: %s", url, exc)
            continue

        sub_text = extract_date_text(sub_html)
        if not sub_text.strip():
            continue

        # Dedup: skip if the sub-page text is already contained in main
        if sub_text.strip() in main_text:
            log.info("  [ShallowCrawl] Sub-page text already in main — skipped.")
            continue

        remaining = budget - used
        if remaining <= 100:
            break

        chunk = sub_text[:remaining]
        parts.append(chunk)
        used += len(chunk)
        log.info("  [ShallowCrawl] Added %d chars from %s", len(chunk), url)

    return "\n--- SUB-PAGE ---\n".join(parts)
