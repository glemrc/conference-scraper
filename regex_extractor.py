"""
regex_extractor.py
==================
Layer 3: Rule-based date extraction using regex patterns.

Tries to extract all five date fields from the page text using a set of
label-pattern + date-format combinations.  Returns a results dict plus a
confidence score (0.0 – 1.0) that the caller uses to decide whether to
skip the LLM entirely, call it for missing fields only, or call it for
the full extraction.

Confidence thresholds (from config.py):
  >= REGEX_CONFIDENCE_HIGH    → use regex results, skip LLM
  >= REGEX_CONFIDENCE_PARTIAL → call LLM for missing fields only
  <  REGEX_CONFIDENCE_PARTIAL → full LLM call
"""

import re
import logging
from dateutil import parser as dateutil_parser
from config import DATE_KEYS

log = logging.getLogger(__name__)

# ─── Month name helpers ──────────────────────────────────────────────

_MONTH_NAMES = (
    r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
)

# Individual date pattern components
_DATE_RE_PARTS = [
    # "15 June 2026" / "15 Jun 2026"
    rf"\d{{1,2}}\s+(?:{_MONTH_NAMES})[,.\s]+\d{{4}}",
    # "June 15, 2026" / "Jun 15 2026"
    rf"(?:{_MONTH_NAMES})\s+\d{{1,2}}[,.\s]+\d{{4}}",
    # "15/06/2026", "06-15-2026", etc.
    r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}",
    # "2026-06-15"
    r"\d{4}[/\-]\d{1,2}[/\-]\d{1,2}",
    # "15 June, 2026"
    r"\d{1,2}\s+(?:{_MONTH_NAMES})[,.\s]+\d{4}",

]

# Combined date pattern (matches any single date)
_DATE_RE = re.compile("|".join(f"(?:{p})" for p in _DATE_RE_PARTS), re.IGNORECASE)


# ─── Label patterns for each field ──────────────────────────────────
# Each list is tried in order; the first match wins.

_LABEL_PATTERNS: dict[str, list[str]] = {
    "fecha_inicio": [
        r"conference\s*(?:dates?|start|begins?)",
        r"(?:congress|symposium|workshop)\s*(?:dates?|start|begins?)",
        r"(?:event|meeting)\s*(?:dates?|start|begins?)",
        r"(?:oral|poster)\s*(?:presentations?|sessions?|workshops?)\s*(?:dates?|start|begins?)",
        r"(?:dates?\s*(?:of\s*(?:the\s*)?)?(?:conference|congress|symposium|event))",
    ],
    "fecha_fin": [
        # The end date is usually on the same line as the start,
        # expressed as a range (e.g. "5-7 February 2026").
        # We handle ranges in _try_date_range().
    ],
    "envio_trabajo": [
        r"(?:full[\s\-]?text\s+)?(?:paper\s+)?submission\s*(?:deadline)?",
        r"call\s+for\s+papers?\s*(?:deadline)?",
        r"abstract\s+submission\s*(?:deadline)?",
        r"manuscript\s+(?:submission|due)",
        r"paper\s+due",
        r"envío\s+de\s+(?:trabajo|artículo|ponencia|manuscrito)",
    ],
    "notificacion_aceptacion": [
        r"(?:notification|notice)\s+of\s+(?:acceptance|accept)",
        r"(?:author|paper)\s+(?:notification|acceptance)",
        r"acceptance\s+(?:notification|notice|decision)",
        r"review\s+(?:notification|results?|decision)",
        r"notificaci[oó]n\s+de\s+(?:aceptaci[oó]n|resultados)",
    ],
    "inscripcion": [
        r"(?:early[\s\-]?bird\s+)?registration\s*(?:deadline)?",
        r"(?:late\s+)?registration\s*(?:deadline)?",
        r"inscripci[oó]n",
    ],
}

# Date range patterns (e.g. "5-7 February 2026", "February 5-7, 2026",
# "October 21–23, 2026")
_RANGE_PATTERNS = [
    # "5-7 February 2026"
    re.compile(
        rf"(\d{{1,2}})\s*[\-–—]\s*(\d{{1,2}})\s+({_MONTH_NAMES})[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
    # "February 5-7, 2026"
    re.compile(
        rf"({_MONTH_NAMES})\s+(\d{{1,2}})\s*[\-–—]\s*(\d{{1,2}})[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
    # "5 February - 7 February 2026"  or  "5 Feb – 7 Feb 2026"
    re.compile(
        rf"(\d{{1,2}})\s+({_MONTH_NAMES})\s*[\-–—]\s*(\d{{1,2}})\s+({_MONTH_NAMES})[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
    # "February 5 - February 7, 2026"
    re.compile(
        rf"({_MONTH_NAMES})\s+(\d{{1,2}})\s*[\-–—]\s*({_MONTH_NAMES})\s+(\d{{1,2}})[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
]


# ─── Internal helpers ────────────────────────────────────────────────

def _normalize_date(raw: str) -> str | None:
    """Parse a raw date string and return YYYY-MM-DD or None."""
    raw = raw.strip().rstrip(".")
    if not raw or raw.lower() in ("none", "null", "n/a", "tbd", ""):
        return None
    try:
        dt = dateutil_parser.parse(raw, dayfirst=True)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        return None


def _find_date_near_label(text: str, label_re: str) -> str | None:
    """Find the first date that appears on or near a line matching label_re."""
    pattern = re.compile(label_re, re.IGNORECASE)
    lines = text.split("\n")

    for i, line in enumerate(lines):
        if not pattern.search(line):
            continue

        # Look for a date on the same line
        m = _DATE_RE.search(line[pattern.search(line).end():])
        if m:
            return _normalize_date(m.group(0))

        # Look on the next 2 lines (label and value are often on separate lines)
        for offset in range(1, 3):
            if i + offset < len(lines):
                m = _DATE_RE.search(lines[i + offset])
                if m:
                    return _normalize_date(m.group(0))

    return None


def _try_date_range(text: str) -> tuple[str | None, str | None]:
    """Try to extract conference start and end dates from a range expression."""
    for pat in _RANGE_PATTERNS:
        m = pat.search(text)
        if not m:
            continue

        groups = m.groups()
        if len(groups) == 4:
            # "5-7 February 2026"  → groups = (5, 7, February, 2026)
            # "February 5-7, 2026" → groups = (February, 5, 7, 2026)
            if groups[0].isdigit():
                day_start, day_end, month, year = groups
            else:
                month, day_start, day_end, year = groups
            start = _normalize_date(f"{day_start} {month} {year}")
            end = _normalize_date(f"{day_end} {month} {year}")
            return start, end
        elif len(groups) == 5:
            # "5 Feb – 7 Mar 2026"  → groups = (5, Feb, 7, Mar, 2026)
            # "Feb 5 – Mar 7, 2026" → groups = (Feb, 5, Mar, 7, 2026)
            if groups[0].isdigit():
                d1, m1, d2, m2, year = groups
            else:
                m1, d1, m2, d2, year = groups
            start = _normalize_date(f"{d1} {m1} {year}")
            end = _normalize_date(f"{d2} {m2} {year}")
            return start, end

    return None, None


# ─── Public API ──────────────────────────────────────────────────────

def extract_with_regex(text: str) -> tuple[dict, float]:
    """
    Attempt to extract conference dates using regex patterns.

    Returns
    -------
    results : dict
        Keys from DATE_KEYS, values are "YYYY-MM-DD" or None.
    confidence : float
        Fraction of fields that were extracted (0.0 – 1.0).
    """
    results: dict[str, str | None] = {k: None for k in DATE_KEYS}

    # ── conference dates (range) ──
    start, end = _try_date_range(text)
    if start:
        results["fecha_inicio"] = start
    if end:
        results["fecha_fin"] = end

    # ── conference start (if range didn't work) ──
    if not results["fecha_inicio"]:
        for lp in _LABEL_PATTERNS["fecha_inicio"]:
            val = _find_date_near_label(text, lp)
            if val:
                results["fecha_inicio"] = val
                break

    # ── other fields ──
    for key in ["envio_trabajo", "notificacion_aceptacion", "inscripcion"]:
        for lp in _LABEL_PATTERNS.get(key, []):
            val = _find_date_near_label(text, lp)
            if val:
                results[key] = val
                break

    # ── confidence ──
    found = sum(1 for v in results.values() if v is not None)
    confidence = found / len(DATE_KEYS)

    log.info(
        "  [RegexExtractor] Found %d/%d fields (confidence %.0f%%)",
        found, len(DATE_KEYS), confidence * 100,
    )
    return results, confidence
