"""
regex_extractor.py
==================
Layer 3: Rule-based date extraction using regex patterns.

F4 fixes:
  1. _LABEL_PATTERNS["fecha_fin"] was an empty list.  Added label patterns
     for sites that publish a standalone end-date label ("conference ends",
     "last day", etc.).

  2. _RANGE_PATTERNS extended with:
       - "to" keyword variants ("5 February to 7 February 2026")
       - Ordinal suffix variants ("5th–7th February 2026")
       - Two-line ranges: when start and end dates appear on consecutive
         lines joined by a shared year, _try_multiline_range() catches them.

  3. _find_date_near_label() lookahead increased from 2 to 5 lines.
     Many conference sites render each Important-Dates row as a <tr> or
     <div> pair where the label and value are separated by 3–4 newlines
     after text extraction.  The original 2-line limit missed these.
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

_DATE_RE_PARTS = [
    rf"\d{{1,2}}(?:st|nd|rd|th)?\s+(?:{_MONTH_NAMES})[,.\s]+\d{{4}}",
    rf"(?:{_MONTH_NAMES})\s+\d{{1,2}}(?:st|nd|rd|th)?[,.\s]+\d{{4}}",
    r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}",
    r"\d{4}[/\-]\d{1,2}[/\-]\d{1,2}",
]

_DATE_RE = re.compile("|".join(f"(?:{p})" for p in _DATE_RE_PARTS), re.IGNORECASE)


# ─── Label patterns ──────────────────────────────────────────────────

_LABEL_PATTERNS: dict[str, list[str]] = {
    "fecha_inicio": [
        r"conference\s*(?:dates?|start|begins?|opens?)",
        r"(?:congress|symposium|workshop)\s*(?:dates?|start|begins?)",
        r"(?:event|meeting)\s*(?:dates?|start|begins?)",
        r"(?:oral|poster)\s*(?:presentations?|sessions?)\s*(?:dates?|start)",
        r"dates?\s*of\s*(?:the\s*)?(?:conference|congress|symposium|event)",
    ],
    # F4 fix: was empty [].  End-date labels are rare but do appear on some
    # sites ("Conference ends:", "Last day of event:", etc.).
    "fecha_fin": [
        r"conference\s*(?:end|ends?|close|closes?|last\s+day)",
        r"(?:congress|symposium|workshop)\s*(?:end|ends?|closes?)",
        r"(?:event|meeting)\s*(?:end|ends?|closes?)",
        r"last\s+day\s+(?:of\s+(?:the\s+)?)?(?:conference|congress|event|symposium)",
        r"(?:conference|event)\s*(?:finish|finishes|concludes?)",
        r"end\s+(?:of\s+)?(?:conference|congress|event)",
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

# ─── Range patterns ─────────────────────────────────────────────────
# F4 fix: added "to" keyword variants and ordinal suffix stripping.
# Ordinals (1st, 2nd, 3rd, 4th…) are stripped via _strip_ordinal()
# before passing to _normalize_date().

_ORD_STRIP = re.compile(r"(\d+)(?:st|nd|rd|th)", re.IGNORECASE)

_RANGE_PATTERNS = [
    # "5-7 February 2026" / "5th–7th February 2026"
    re.compile(
        rf"(\d{{1,2}})(?:st|nd|rd|th)?\s*[\-–—]\s*(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_NAMES})[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
    # "February 5-7, 2026"
    re.compile(
        rf"({_MONTH_NAMES})\s+(\d{{1,2}})(?:st|nd|rd|th)?\s*[\-–—]\s*(\d{{1,2}})(?:st|nd|rd|th)?[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
    # "5 February - 7 February 2026" / "5 Feb – 7 Feb 2026"
    re.compile(
        rf"(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_NAMES})\s*[\-–—]\s*(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_NAMES})[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
    # "February 5 - February 7, 2026"
    re.compile(
        rf"({_MONTH_NAMES})\s+(\d{{1,2}})(?:st|nd|rd|th)?\s*[\-–—]\s*({_MONTH_NAMES})\s+(\d{{1,2}})(?:st|nd|rd|th)?[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
    # F4 new: "5 February to 7 February 2026"
    re.compile(
        rf"(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_NAMES})\s+to\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_NAMES})[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
    # F4 new: "February 5 to 7, 2026" (same month, "to" keyword)
    re.compile(
        rf"({_MONTH_NAMES})\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+to\s+(\d{{1,2}})(?:st|nd|rd|th)?[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
    # F4 new: "5 to 7 February 2026"
    re.compile(
        rf"(\d{{1,2}})(?:st|nd|rd|th)?\s+to\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_NAMES})[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
    # F4 new: "5 February through 7 February 2026"
    re.compile(
        rf"(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_NAMES})\s+through\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_NAMES})[,.\s]+(\d{{4}})",
        re.IGNORECASE,
    ),
]


# ─── Helpers ────────────────────────────────────────────────────────

def _strip_ordinal(s: str) -> str:
    """Remove ordinal suffixes: '5th' → '5', '21st' → '21'."""
    return _ORD_STRIP.sub(r"\1", s)


def _normalize_date(raw: str) -> str | None:
    """Parse a raw date string and return YYYY-MM-DD or None."""
    raw = _strip_ordinal(raw.strip().rstrip("."))
    if not raw or raw.lower() in ("none", "null", "n/a", "tbd", ""):
        return None
    try:
        dt = dateutil_parser.parse(raw, dayfirst=True)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        return None


def _find_date_near_label(text: str, label_re: str, lookahead: int = 5) -> str | None:
    """
    Find the first date that appears on or near a line matching label_re.

    F4 fix: lookahead increased from 2 to 5 lines (default).
    Many conference sites emit the label and the date value in separate
    <td> or <div> elements that become 3–4 lines apart after text
    extraction.  The original 2-line limit was too narrow for these cases.
    """
    pattern = re.compile(label_re, re.IGNORECASE)
    lines = text.split("\n")

    for i, line in enumerate(lines):
        m = pattern.search(line)
        if not m:
            continue

        # Look for a date on the same line, after the label
        date_m = _DATE_RE.search(line[m.end():])
        if date_m:
            return _normalize_date(date_m.group(0))

        # Look forward up to `lookahead` lines
        for offset in range(1, lookahead + 1):
            if i + offset >= len(lines):
                break
            candidate_line = lines[i + offset].strip()
            # Skip separator/blank lines but count them toward lookahead
            if not candidate_line:
                continue
            date_m = _DATE_RE.search(candidate_line)
            if date_m:
                return _normalize_date(date_m.group(0))

    return None


def _try_date_range(text: str) -> tuple[str | None, str | None]:
    """
    Try to extract conference start and end dates from an inline range
    expression using the patterns in _RANGE_PATTERNS.
    """
    for pat in _RANGE_PATTERNS:
        m = pat.search(text)
        if not m:
            continue

        groups = m.groups()
        if len(groups) == 4:
            if groups[0].isdigit():
                day_start, day_end, month, year = groups
            else:
                month, day_start, day_end, year = groups
            start = _normalize_date(f"{day_start} {month} {year}")
            end   = _normalize_date(f"{day_end}   {month} {year}")
            return start, end
        elif len(groups) == 5:
            if groups[0].isdigit():
                d1, m1, d2, m2, year = groups
            else:
                m1, d1, m2, d2, year = groups
            start = _normalize_date(f"{d1} {m1} {year}")
            end   = _normalize_date(f"{d2} {m2} {year}")
            return start, end

    return None, None


def _try_multiline_range(text: str) -> tuple[str | None, str | None]:
    """
    F4 new: detect conference start/end when dates appear on separate lines
    that are adjacent or close together, e.g.:

        Conference dates
        October 12, 2026
        October 14, 2026

    The heuristic looks for two standalone full dates within 4 lines of a
    conference-date label.  If the second date is later than the first, they
    are treated as start/end.
    """
    label_re = re.compile(
        r"conference\s*(?:dates?|period)|(?:congress|symposium|event)\s*dates?",
        re.IGNORECASE,
    )
    lines = text.split("\n")

    for i, line in enumerate(lines):
        if not label_re.search(line):
            continue

        # Collect up to 6 dates from the next 8 lines
        found_dates: list[str] = []
        for offset in range(1, 9):
            if i + offset >= len(lines):
                break
            m = _DATE_RE.search(lines[i + offset])
            if m:
                norm = _normalize_date(m.group(0))
                if norm and norm not in found_dates:
                    found_dates.append(norm)
            if len(found_dates) >= 2:
                break

        if len(found_dates) >= 2:
            start, end = found_dates[0], found_dates[1]
            if end >= start:
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

    # ── conference date range (single-line) ──
    start, end = _try_date_range(text)
    if start:
        results["fecha_inicio"] = start
    if end:
        results["fecha_fin"] = end

    # ── conference date range (multi-line, F4 new) ──
    if not results["fecha_inicio"] or not results["fecha_fin"]:
        ml_start, ml_end = _try_multiline_range(text)
        if ml_start and not results["fecha_inicio"]:
            results["fecha_inicio"] = ml_start
        if ml_end and not results["fecha_fin"]:
            results["fecha_fin"] = ml_end

    # ── conference start (label-based fallback) ──
    if not results["fecha_inicio"]:
        for lp in _LABEL_PATTERNS["fecha_inicio"]:
            val = _find_date_near_label(text, lp)
            if val:
                results["fecha_inicio"] = val
                break

    # ── conference end (label-based, F4 new) ──
    if not results["fecha_fin"]:
        for lp in _LABEL_PATTERNS["fecha_fin"]:
            val = _find_date_near_label(text, lp)
            if val:
                results["fecha_fin"] = val
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
