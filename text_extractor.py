"""
text_extractor.py
=================
Layer 2: Smart text extraction — pulls only date-relevant sections
from the HTML instead of dumping the entire page to the LLM.

F2 fix — div-based layouts:
  Added Strategy 4 (_extract_by_divs) that scans generic block-level
  containers (div, section, article) for date content.  This covers the
  large majority of modern conference sites that use CSS-grid or flexbox
  layouts instead of semantic <table>/<dl> elements.
  The strategy includes a containment deduplication pass to avoid returning
  the same text block multiple times when parent divs contain child divs.

F5 fix — priority-aware truncation:
  _prioritize_text() re-orders extracted sections so that the block that
  most strongly matches "Important Dates" keywords appears first.  This
  ensures the most valuable content is preserved when MAX_SMART_TEXT_CHARS
  is reached.  The character budget is consumed from highest-priority
  sections down, rather than cutting at a fixed offset.

Strategy order (in priority):
  1. Headings-based sections (h1–h6 with date keywords)
  2. Semantic containers: <table>, <dl>, <ul>, <ol>
  3. Generic block containers: <div>, <section>, <article>  ← F2 new
  4. Context-window around date patterns in full text
  5. Full cleaned text (hard fallback, capped)
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

# Higher-priority keywords: sections containing these are placed first.
_PRIORITY_KEYWORDS = [
    "important date",
    "key date",
    "fechas importantes",
    "deadline",
    "plazos",
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


def _matches_priority_keyword(text: str) -> bool:
    """True if *text* contains a high-priority date-section keyword."""
    low = text.lower()
    return any(kw in low for kw in _PRIORITY_KEYWORDS)


def _collect_section(heading: Tag, max_chars: int = 2000) -> str:
    """Collect text following a heading until the next heading of equal or
    higher level."""
    parts: list[str] = [heading.get_text(strip=True)]
    heading_level = int(heading.name[1]) if heading.name[0] == "h" else 99

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

    if not found_content or not _DATE_PATTERN.search("\n".join(parts)):
        parts = [heading.get_text(strip=True)]
        for elem in heading.find_all_next():
            if not isinstance(elem, Tag):
                continue
            if elem.name and elem.name[0] == "h" and elem.name[1:].isdigit():
                if int(elem.name[1]) <= heading_level and elem != heading:
                    break
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


def _deduplicate_sections(sections: list[str]) -> list[str]:
    """
    Remove sections that are fully contained within a longer section.
    Prevents returning the same text block multiple times when parent/child
    div structures are both captured.
    """
    sections_sorted = sorted(set(sections), key=len, reverse=True)
    deduped: list[str] = []
    for candidate in sections_sorted:
        candidate_stripped = candidate.strip()
        if not any(candidate_stripped in existing for existing in deduped):
            deduped.append(candidate_stripped)
    return deduped


def _prioritize_text(sections: list[str]) -> str:
    """
    F5 fix: re-order sections so high-priority content appears first,
    then build the output string respecting MAX_SMART_TEXT_CHARS.

    This ensures the "Important Dates" block is never truncated away in
    favour of lower-relevance content that happened to appear earlier.
    """
    priority: list[str] = []
    normal: list[str] = []
    for s in sections:
        if _matches_priority_keyword(s):
            priority.append(s)
        else:
            normal.append(s)

    ordered = priority + normal
    result_parts: list[str] = []
    used = 0
    for sec in ordered:
        if used + len(sec) + 5 > MAX_SMART_TEXT_CHARS:
            # Include a truncated fragment only if the section is priority
            remaining = MAX_SMART_TEXT_CHARS - used - 5
            if remaining > 200 and _matches_priority_keyword(sec):
                result_parts.append(sec[:remaining])
            break
        result_parts.append(sec)
        used += len(sec) + 5  # +5 for separator "---\n"

    return "\n---\n".join(result_parts)


# ─── extraction strategies ──────────────────────────────────────────

def _extract_by_headings(soup: BeautifulSoup) -> list[str]:
    """Strategy 1: grab content under date-related headings."""
    sections: list[str] = []
    for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        if _matches_keyword(heading.get_text()):
            section = _collect_section(heading)
            if section.strip() and _has_dates(section):
                sections.append(section)
    return sections


def _extract_by_tables(soup: BeautifulSoup) -> list[str]:
    """Strategy 2: grab semantic list/table containers that contain dates."""
    sections: list[str] = []
    for container in soup.find_all(["table", "dl", "ul", "ol"]):
        text = container.get_text(separator="\n", strip=True)
        if _DATE_PATTERN.search(text):
            sections.append(text)
    return sections


def _extract_by_divs(soup: BeautifulSoup) -> list[str]:
    """
    Strategy 3 (F2 fix): scan generic block containers (div, section,
    article) that both match a date-keyword AND contain at least one
    date pattern.

    Guards against noise:
      - Skips containers larger than 6 000 chars (likely a page wrapper).
      - Only accepts containers that satisfy BOTH the keyword check AND
        the date-pattern check, keeping precision high.
      - A containment deduplication pass removes child sections already
        covered by a parent that was also captured.
    """
    sections: list[str] = []
    for tag in soup.find_all(["div", "section", "article"]):
        text = tag.get_text(separator="\n", strip=True)
        # Skip overly large wrappers (page shell, main content area, etc.)
        if len(text) > 6_000:
            continue
        if _matches_keyword(text) and _has_dates(text):
            sections.append(text)
    return _deduplicate_sections(sections)


def _extract_by_context_window(full_text: str, window: int = 3) -> str:
    """Strategy 4: find lines with dates and include ±window context."""
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

    Pipeline (F2 + F5 updated):
      1. Headings-based sections
      2. Semantic containers (table, dl, ul, ol)
      3. Generic block containers (div, section, article)   ← F2 new
      4. Context windows around date patterns
      5. Full cleaned text fallback

    Sections from strategies 1–3 are merged, deduplicated, and re-ordered
    by priority (F5) before being capped at MAX_SMART_TEXT_CHARS.
    """
    soup = _clean_soup(html)

    # Collect from all structural strategies
    heading_sections = _extract_by_headings(soup)
    table_sections   = _extract_by_tables(soup)
    div_sections     = _extract_by_divs(soup)

    all_structural = _deduplicate_sections(
        heading_sections + table_sections + div_sections
    )

    if all_structural:
        result = _prioritize_text(all_structural)
        if result.strip():
            log.info(
                "  [TextExtractor] Structural strategies yielded %d chars "
                "(%d sections, priority-ordered).",
                len(result), len(all_structural),
            )
            return result

    # Strategy 4 — context window (when no structural match)
    full_text = soup.get_text(separator="\n", strip=True)
    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
    result = _extract_by_context_window(full_text)
    if result.strip():
        log.info("  [TextExtractor] Context-window strategy: %d chars", len(result))
        return result[:MAX_SMART_TEXT_CHARS]

    # Strategy 5 — full cleaned text
    log.info("  [TextExtractor] Fallback (full text): %d chars", len(full_text))
    return full_text[:MAX_TEXT_CHARS]
