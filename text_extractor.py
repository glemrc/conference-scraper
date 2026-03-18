"""
text_extractor.py
=================
Layer 2: Smart text extraction — pulls only date-relevant sections
from the HTML instead of dumping the entire page to the LLM.

Strategy (in priority order):
  1. Find headings matching date-related keywords and grab their
     sibling content (covers ~60% of conference sites).
  2. Find <table>/<dl>/<ul> elements containing date patterns.
  3. Scan all text lines for date patterns and include ±3 lines
     of context around each match.
  4. Fallback: return the full cleaned text (capped).

Typical output is 500-2000 chars instead of 10 000 +.
"""

import re
import logging

from bs4 import BeautifulSoup, Tag

from config import MAX_TEXT_CHARS, MAX_SMART_TEXT_CHARS

log = logging.getLogger(__name__)

# ─── keywords that signal a date-relevant section ───────────────────

_SECTION_KEYWORDS = [
    "important date",
    "key date",
    "deadline",
    "call for paper",
    "submission",
    "notification",
    "registration",
    "camera.ready",
    "conference date",
    "congress date",
    "symposium date",
    "workshop date",
    "fechas importantes",
    "plazos",
    "envío",
    "inscripci",
    "aceptaci",
]

# Regex: matches common date-like patterns (does NOT validate)
_DATE_PATTERN = re.compile(
    r"""
    (?:                                            # ── Named month formats ──
        \d{1,2}\s+                                 # 15 June 2026
        (?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|
           Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)
        [,.\s]+\d{4}
    |
        (?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|
           Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)
        \s+\d{1,2}[,.\s]+\d{4}                     # June 15, 2026
    |                                              # ── Numeric formats ──
        \d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}           # 15/06/2026  or  06-15-26
    |
        \d{4}[/\-]\d{1,2}[/\-]\d{1,2}             # 2026-06-15
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)


# ─── internal helpers ───────────────────────────────────────────────

def _clean_soup(html: str) -> BeautifulSoup:
    """Parse HTML and strip non-content tags."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header",
                     "noscript", "aside", "iframe", "svg", "form"]):
        tag.decompose()
    return soup


def _matches_keyword(text: str) -> bool:
    """True if *text* contains any date-section keyword."""
    low = text.lower()
    return any(kw in low for kw in _SECTION_KEYWORDS)


def _collect_section(heading: Tag, max_chars: int = 2000) -> str:
    """Collect text following a heading until the next heading of equal or
    higher level.  Uses ``find_all_next`` so it crosses parent-div
    boundaries (common in conference sites where the heading and data
    live in sibling containers)."""
    parts: list[str] = [heading.get_text(strip=True)]
    heading_level = int(heading.name[1]) if heading.name[0] == "h" else 99

    # First try direct siblings (fastest)
    found_content = False
    for sib in heading.find_next_siblings():
        if isinstance(sib, Tag):
            if sib.name and sib.name[0] == "h" and sib.name[1:].isdigit():
                if int(sib.name[1]) <= heading_level:
                    break
            txt = sib.get_text(separator=" ", strip=True)
            if txt:
                parts.append(txt)
                found_content = True
        if sum(len(p) for p in parts) >= max_chars:
            break

    # If siblings yielded nothing useful, walk all subsequent elements
    # (handles cases where heading is in a wrapper div)
    if not found_content or not _DATE_PATTERN.search("\n".join(parts)):
        parts = [heading.get_text(strip=True)]
        for elem in heading.find_all_next():
            if not isinstance(elem, Tag):
                continue
            if elem.name and elem.name[0] == "h" and elem.name[1:].isdigit():
                if int(elem.name[1]) <= heading_level and elem != heading:
                    break
            # Skip container tags — only read leaf-level text
            if elem.string or (not list(elem.children) or
                               all(not isinstance(c, Tag) for c in elem.children)):
                txt = elem.get_text(separator=" ", strip=True)
                if txt and txt not in parts:
                    parts.append(txt)
            if sum(len(p) for p in parts) >= max_chars:
                break

    return "\n".join(parts)


def _has_dates(text: str) -> bool:
    """Return True if the text contains at least one date-like pattern."""
    return bool(_DATE_PATTERN.search(text))


def _extract_by_headings(soup: BeautifulSoup) -> str:
    """Strategy 1: grab content under date-related headings."""
    sections: list[str] = []
    for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        if _matches_keyword(heading.get_text()):
            section = _collect_section(heading)
            # Only accept if the section actually contains dates
            if section.strip() and _has_dates(section):
                sections.append(section)

    return "\n---\n".join(sections)


def _extract_by_tables(soup: BeautifulSoup) -> str:
    """Strategy 2: grab tables / definition lists that contain dates."""
    sections: list[str] = []
    for container in soup.find_all(["table", "dl", "ul", "ol"]):
        text = container.get_text(separator="\n", strip=True)
        if _DATE_PATTERN.search(text):
            sections.append(text)

    return "\n---\n".join(sections)


def _extract_by_context_window(full_text: str, window: int = 3) -> str:
    """Strategy 3: find lines with dates and include ±window context."""
    lines = full_text.split("\n")
    selected: set[int] = set()
    for i, line in enumerate(lines):
        if _DATE_PATTERN.search(line):
            for j in range(max(0, i - window), min(len(lines), i + window + 1)):
                selected.add(j)

    if not selected:
        return ""

    return "\n".join(lines[i] for i in sorted(selected))


# ─── public API ─────────────────────────────────────────────────────

def extract_full_text(html: str) -> str:
    """Fallback: clean and return full page text (capped)."""
    soup = _clean_soup(html)
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:MAX_TEXT_CHARS]


def extract_date_text(html: str) -> str:
    """
    Smart extraction: return only the date-relevant portions of the page.

    Tries three strategies in order and returns the first non-empty result.
    Falls back to full text if nothing is found.
    """
    soup = _clean_soup(html)

    # Strategy 1 — heading-based sections
    result = _extract_by_headings(soup)
    if result.strip():
        log.info("  [TextExtractor] Strategy 1 (headings): %d chars", len(result))
        return result[:MAX_SMART_TEXT_CHARS]

    # Strategy 2 — tables / definition lists with dates
    result = _extract_by_tables(soup)
    if result.strip():
        log.info("  [TextExtractor] Strategy 2 (tables): %d chars", len(result))
        return result[:MAX_SMART_TEXT_CHARS]

    # Strategy 3 — context windows around date patterns
    full_text = soup.get_text(separator="\n", strip=True)
    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
    result = _extract_by_context_window(full_text)
    if result.strip():
        log.info("  [TextExtractor] Strategy 3 (context window): %d chars", len(result))
        return result[:MAX_SMART_TEXT_CHARS]

    # Fallback — full cleaned text
    log.info("  [TextExtractor] Fallback (full text): %d chars", len(full_text))
    return full_text[:MAX_TEXT_CHARS]
