# CHANGES:
# FIX-S1 — _clean_soup: remove <s>/<del>/<strike> to avoid strikethrough date confusion.
# FIX-S2 — _normalize_whitespace: replace en-dash/em-dash with hyphen for regex compat.
# FIX-S3 — Added more section keywords: 'notification', 'key date', PDF link detection.
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
    # FIX-S3: more coverage
    "author notification",
    "paper notification",
    "conference dates",
    "event date",
    "program date",
    "schedule",
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
_MONTH_RE = (
    r"Jan(?:uary)?\.?|Feb(?:ruary)?\.?|Mar(?:ch)?\.?|Apr(?:il)?\.?|May\.?|Jun(?:e)?\.?|"
    r"Jul(?:y)?\.?|Aug(?:ust)?\.?|Sep(?:tember)?\.?|Oct(?:ober)?\.?|Nov(?:ember)?\.?|Dec(?:ember)?\.?"
)

_DATE_PATTERN = re.compile(
    rf"""
    (?:                                            # ── Named month formats ──
        \d{{1,2}}(?:st|nd|rd|th)?\s+              # 15 June 2026
        (?:{_MONTH_RE})
        [,.\s]+\d{{4}}
    |
        (?:{_MONTH_RE})                            # June 15, 2026
        \s+\d{{1,2}}(?:st|nd|rd|th)?
        (?:\s*[-–—]\s*\d{{1,2}}(?:st|nd|rd|th)?)?  # optional range: June 15-17, 2026
        [,.\s]+\d{{4}}
    |
        \d{{1,2}}(?:st|nd|rd|th)?                  # 15-17 June 2026 (day range)
        \s*[-–—]\s*\d{{1,2}}(?:st|nd|rd|th)?
        \s+(?:{_MONTH_RE})
        [,.\s]+\d{{4}}
    |                                              # ── Numeric formats ──
        \d{{1,2}}[/\-]\d{{1,2}}[/\-]\d{{2,4}}     # 15/06/2026  or  06-15-26
    |
        \d{{4}}[/\-]\d{{1,2}}[/\-]\d{{1,2}}       # 2026-06-15
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)


# ─── internal helpers ───────────────────────────────────────────────

def _clean_soup(html: str) -> BeautifulSoup:
    """Parse HTML and strip non-content tags.

    FIX-S1: Also removes strikethrough tags (<s>, <del>, <strike>) so that
    "extended" / "replaced" dates inside those tags don't pollute the text.
    FIX-S2: Normalises Unicode dashes (\u2013 en-dash, \u2014 em-dash) to
    ASCII hyphen so downstream regex patterns match correctly.
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header",
                     "noscript", "aside", "iframe", "svg", "form"]):
        tag.decompose()
    # FIX-S1: Remove strikethrough / deleted text (old/crossed-out dates)
    for tag in soup(["s", "del", "strike"]):
        tag.decompose()
    return soup


def _normalize_dashes(text: str) -> str:
    """FIX-S2: Replace en-dash (\u2013) and em-dash (\u2014) with hyphen-minus.

    This ensures date patterns like '03 \u2013 04 April 2026' are matched by
    the standard regex that only looks for ASCII '-'.
    """
    return text.replace("\u2013", "-").replace("\u2014", "-")


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

    Priority sections are sorted shortest-first so that a concise
    "Important Dates" block is never pushed out by a large generic div
    that also happens to contain a priority keyword somewhere.
    """
    priority: list[str] = []
    normal: list[str] = []
    for s in sections:
        if _matches_priority_keyword(s):
            priority.append(s)
        else:
            normal.append(s)

    # Shortest priority sections first — specific date blocks beat large wrappers
    priority.sort(key=len)
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
    """Strategy 1: grab content under date-related headings.

    Two trigger conditions:
      a) heading text contains a date-section keyword (e.g. 'Important Dates')
      b) heading text itself IS a date or date range (e.g. '24-26 September 2025,
         Seoul') — common on Elementor/builder pages where the conference date
         is placed directly in an <h4> with no surrounding keywords.
    """
    sections: list[str] = []
    for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        heading_text = heading.get_text()
        if _matches_keyword(heading_text) or _has_dates(heading_text):
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
    text = _normalize_dashes(text)  # FIX-S2
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:MAX_TEXT_CHARS]


def detect_pdf_links(html: str, base_url: str = "") -> list[str]:
    """FIX-S3: Detect PDF links in the HTML for manual review flagging.

    Returns a list of resolved PDF URLs found on the page (e.g. brochures,
    programmes that may contain date information). Callers append these to
    the 'Notes' column so the user can download them for manual review.
    """
    from urllib.parse import urljoin
    soup = BeautifulSoup(html, "html.parser")
    pdf_urls: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf") or "pdf" in href.lower():
            resolved = urljoin(base_url, href) if base_url else href
            if resolved not in seen:
                seen.add(resolved)
                pdf_urls.append(resolved)
    return pdf_urls[:5]  # cap at 5 to keep Notes readable


# ─── Conference name extraction ─────────────────────────────────────

# Month names to filter out false positives in conference name extraction
_MONTH_WORDS = {
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
    "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
}

# Noise words that should not be treated as conference acronyms
_NOISE_WORDS = {
    "home", "welcome", "the", "call", "download", "about", "contact",
    "login", "register", "paper", "papers", "submit", "submission",
    "conference", "symposium", "workshop", "congress", "international",
    "deadline", "deadlines", "important", "search",
} | _MONTH_WORDS

# Patterns tried in order — first match wins.
_CONF_NAME_PATTERNS = [
    # "ICOAMP 2026" / "LACCEI 2026" — classic ACRONYM YEAR with space
    re.compile(r"\b([A-Z]{2,10}\s+\d{4})\b"),
    # "ICOAMP2026" / "ICITAI2026" / "CSOC2026" — ACRONYM glued to YEAR
    re.compile(r"\b([A-Z]{2,10})(\d{4})\b"),
    # "PEIS-2026" / "STAI-2026" — ACRONYM-YEAR with hyphen
    re.compile(r"\b([A-Z]{2,10})\s*[-–]\s*(\d{4})\b"),
    # "(ICoAMP)" in parentheses — mixed case inside parens
    re.compile(r"\(([A-Za-z]{3,12})\)"),
    # "CoMeSySo2026" / "WorldCist'26" — mixed-case acronym + year
    re.compile(r"\b([A-Za-z]{3,14}?)[\s'']?(\d{2,4})\b"),
]


def _format_conf_name(m: re.Match) -> str:
    """Normalize match groups into 'ACRONYM YEAR' format."""
    groups = m.groups()
    if len(groups) == 1:
        # Single group — could be "ICOAMP 2026" or "(ICoAMP)"
        text = groups[0].strip()
        if " " in text:
            return text
        # Parenthesized acronym without year
        return text.upper()
    acronym, year = groups[0], groups[1]
    # Normalize short year: '26' -> '2026'
    if len(year) == 2:
        year = "20" + year
    return f"{acronym.upper()} {year}"


def _extract_acronym_from_url(url: str) -> str | None:
    """Last-resort: extract acronym from URL domain or path segments like
    /conference/icitai2026, /peis2026, acdsa.org/2026/, or icet.org."""
    import re as _re
    from urllib.parse import urlparse

    parsed = urlparse(url)

    # Pattern 1: path segment with acronym+year glued (e.g. /icitai2026)
    m = _re.search(r"/([a-zA-Z]{2,12})(\d{4})(?:[/\?#]|$)", parsed.path)
    if m:
        acronym = m.group(1).upper()
        year = m.group(2)
        if acronym.lower() not in _NOISE_WORDS:
            return f"{acronym} {year}"

    # Pattern 2: path has /acronym/year/ separately (e.g. /acdsa.org/2026/)
    m = _re.search(r"/([a-zA-Z]{2,12})/(\d{4})(?:[/\?#]|$)", url)
    if m:
        acronym = m.group(1).upper()
        year = m.group(2)
        if acronym.lower() not in _NOISE_WORDS:
            return f"{acronym} {year}"

    # Pattern 3: domain-based (e.g. acdsa.org, icet.org, icacit.org.pe)
    domain = parsed.hostname or ""
    m = _re.match(r"(?:www\.)?([a-zA-Z]{2,12})\.", domain)
    if m:
        acronym = m.group(1).upper()
        if acronym.lower() not in _NOISE_WORDS and len(acronym) >= 3:
            # Try to find a year from the path
            year_m = _re.search(r"(\d{4})", parsed.path)
            if year_m:
                return f"{acronym} {year_m.group(1)}"
            return acronym

    return None


def extract_conference_name(html: str, url: str = "") -> str | None:
    """
    Extract conference name in 'ACRONYM YEAR' format (e.g. 'ICOAMP 2026')
    from the page <title>, <h1>, <h2>, and meta tags.

    Handles multiple real-world formats:
      - "ICOAMP 2026" (space-separated)
      - "ICITAI2026" (glued)
      - "PEIS-2026" (hyphenated)
      - "(ICoAMP)" (parenthesized in body text)
      - "CoMeSySo2026" / "WorldCist'26" (mixed-case + short year)

    Falls back to extracting acronym from URL path if nothing found in HTML.

    Returns the first match found, or None.
    """
    soup = BeautifulSoup(html, "html.parser")

    candidates: list[str] = []

    title_tag = soup.find("title")
    if title_tag:
        candidates.append(title_tag.get_text(strip=True))

    for tag_name in ("h1", "h2"):
        for tag in soup.find_all(tag_name):
            text = tag.get_text(strip=True)
            if text:
                candidates.append(text)

    # Also check <meta property="og:title"> and <meta name="title">
    for meta in soup.find_all("meta"):
        prop = meta.get("property", "") or meta.get("name", "")
        if prop.lower() in ("og:title", "title", "og:site_name"):
            content = meta.get("content", "").strip()
            if content:
                candidates.append(content)

    # Try each pattern in priority order across all candidates
    for pattern in _CONF_NAME_PATTERNS:
        for candidate in candidates:
            m = pattern.search(candidate)
            if m:
                name = _format_conf_name(m)
                # Filter out noise
                acronym = name.split()[0]
                if acronym.lower() in _NOISE_WORDS:
                    continue
                return name

    # Fallback: extract from URL
    if url:
        return _extract_acronym_from_url(url)

    return None


def extract_date_text(html: str) -> str:
    """
    Smart extraction: return only the date-relevant portions of the page.

    Pipeline (F2 + F5 + FIX-S updated):
      1. Headings-based sections
      2. Semantic containers (table, dl, ul, ol)
      3. Generic block containers (div, section, article)   ← F2 new
      4. Context windows around date patterns
      5. Full cleaned text fallback

    Sections from strategies 1–3 are merged, deduplicated, and re-ordered
    by priority (F5) before being capped at MAX_SMART_TEXT_CHARS.
    FIX-S1/S2: soup already has strikethrough tags removed and en-dashes
    normalized by _clean_soup() and _normalize_dashes().
    """
    soup = _clean_soup(html)  # FIX-S1: strips <s>/<del>/<strike> too

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
            result = _normalize_dashes(result)  # FIX-S2
            log.info(
                "  [TextExtractor] Structural strategies yielded %d chars "
                "(%d sections, priority-ordered).",
                len(result), len(all_structural),
            )
            return result

    # Strategy 4 — context window (when no structural match)
    full_text = soup.get_text(separator="\n", strip=True)
    full_text = _normalize_dashes(full_text)  # FIX-S2
    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
    result = _extract_by_context_window(full_text)
    if result.strip():
        log.info("  [TextExtractor] Context-window strategy: %d chars", len(result))
        return result[:MAX_SMART_TEXT_CHARS]

    # Strategy 5 — full cleaned text
    log.info("  [TextExtractor] Fallback (full text): %d chars", len(full_text))
    return full_text[:MAX_TEXT_CHARS]
