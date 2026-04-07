# CHANGES:
# P4 — _find_date_near_label: added stop_keywords param; scanning stops when a line matches another field's label.
# P4 — Added _cross_field_dedup() to null logically impossible duplicate dates between fields.
# P5 — _infer_end_date_from_context: changed guard from start_dt <= cand_dt to start_dt < cand_dt so same-date is discarded.
# P7 — _find_date_near_label: collects ALL date candidates near a label and returns the latest (most recent) to prefer deadline extensions.

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
from datetime import datetime as _dt
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
        r"(?:Notification|Notice)\s+of\s+(?:Acceptance|Accept)",
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


def _build_all_label_patterns_except(exclude_key: str) -> list[re.Pattern]:
    """
    P4 fix: build compiled regex patterns for ALL fields EXCEPT exclude_key.
    Used as stop_keywords so _find_date_near_label stops scanning when it
    crosses into another field's label zone.
    """
    patterns = []
    for key, label_list in _LABEL_PATTERNS.items():
        if key == exclude_key:
            continue
        for lp in label_list:
            patterns.append(re.compile(lp, re.IGNORECASE))
    return patterns


def _find_date_near_label(
    text: str,
    label_re: str,
    lookahead: int = 5,
    stop_keywords: list[re.Pattern] | None = None,
) -> str | None:
    """
    Find the date that appears on or near a line matching label_re.

    P4 fix: stop_keywords — if a lookahead line matches another field's label,
    stop scanning immediately to avoid proximity confusion.

    P7 fix: collects ALL date candidates within the lookahead window (for the
    same label occurrence) and returns the LATEST one, so deadline extensions
    that appear below the original are preferred.

    F4 fix: lookahead increased from 2 to 5 lines (default).
    """
    pattern = re.compile(label_re, re.IGNORECASE)
    lines = text.split("\n")
    stop_keywords = stop_keywords or []

    for i, line in enumerate(lines):
        m = pattern.search(line)
        if not m:
            continue

        candidates: list[str] = []

        # Look for a date on the same line, after the label
        date_m = _DATE_RE.search(line[m.end():])
        if date_m:
            norm = _normalize_date(date_m.group(0))
            if norm:
                candidates.append(norm)

        # Look forward up to `lookahead` lines
        for offset in range(1, lookahead + 1):
            if i + offset >= len(lines):
                break
            candidate_line = lines[i + offset].strip()
            # Skip separator/blank lines but count them toward lookahead
            if not candidate_line:
                continue
            # P4: stop if this line contains another field's label keyword
            if any(sk.search(candidate_line) for sk in stop_keywords):
                break
            date_m = _DATE_RE.search(candidate_line)
            if date_m:
                norm = _normalize_date(date_m.group(0))
                if norm:
                    candidates.append(norm)

        # P7: return the latest (most recent) date to prefer extensions
        if candidates:
            return max(candidates)

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


def _infer_end_date_from_context(text: str, start_date_str: str) -> str | None:
    """
    P4 fix: when fecha_inicio was found but fecha_fin was not captured by
    any range or label pattern, scan the lines near the start-date match
    for a second date that is >= start and within 30 days.

    P5 fix: changed guard from start_dt <= cand_dt to start_dt < cand_dt
    so that an inferred end date equal to fecha_inicio is discarded.

    Conservative guards:
      - Only considers dates within 30 days of start (avoids grabbing a
        submission deadline as end date).
      - Skips lines that match deadline-related keywords.
    """
    from datetime import timedelta

    try:
        start_dt = _dt.strptime(start_date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None

    max_end = start_dt + timedelta(days=30)
    deadline_keywords = re.compile(
        r"submission|deadline|notification|acceptance|registration|inscripci",
        re.IGNORECASE,
    )

    lines = text.split("\n")
    # Find the line where start_date appears
    start_line_idx = None
    for i, line in enumerate(lines):
        m = _DATE_RE.search(line)
        if m and _normalize_date(m.group(0)) == start_date_str:
            start_line_idx = i
            break

    if start_line_idx is None:
        return None

    # Scan next 8 lines for a candidate end date
    for offset in range(1, 9):
        idx = start_line_idx + offset
        if idx >= len(lines):
            break
        line = lines[idx]
        # Skip lines with deadline keywords
        if deadline_keywords.search(line):
            continue
        m = _DATE_RE.search(line)
        if not m:
            continue
        candidate = _normalize_date(m.group(0))
        if not candidate:
            continue
        try:
            cand_dt = _dt.strptime(candidate, "%Y-%m-%d")
        except ValueError:
            continue
        # P5 fix: strict inequality — inferred end must be AFTER start
        if start_dt < cand_dt <= max_end:
            return candidate

    return None


# ─── Cross-field deduplication (P4 fix) ──────────────────────────────

# Logically impossible pairs: if these two fields share the exact same date,
# the second in the tuple is considered less reliable and gets nulled.
_IMPOSSIBLE_DUPES = [
    ("envio_trabajo", "notificacion_aceptacion"),  # submission can't == acceptance
    ("notificacion_aceptacion", "inscripcion"),     # acceptance can't == registration
    ("inscripcion", "fecha_fin"),                   # registration can't == conf end
    ("inscripcion", "fecha_inicio"),                # registration can't == conf start
    ("envio_trabajo", "inscripcion"),               # submission can't == registration
]


def _cross_field_dedup(results: dict) -> dict:
    """
    P4 fix: after extracting all fields, check for logically impossible
    duplicate dates between different fields. Null the less reliable one.
    """
    for field_a, field_b in _IMPOSSIBLE_DUPES:
        val_a = results.get(field_a)
        val_b = results.get(field_b)
        if val_a and val_b and val_a == val_b:
            log.warning(
                "  [RegexExtractor] Duplicate date %s shared by %s and %s "
                "— nullifying %s (less reliable).",
                val_a, field_a, field_b, field_b,
            )
            results[field_b] = None
    return results


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
        stop_kw = _build_all_label_patterns_except("fecha_inicio")
        for lp in _LABEL_PATTERNS["fecha_inicio"]:
            val = _find_date_near_label(text, lp, stop_keywords=stop_kw)
            if val:
                results["fecha_inicio"] = val
                break

    # ── conference end (label-based, F4 new) ──
    if not results["fecha_fin"]:
        stop_kw = _build_all_label_patterns_except("fecha_fin")
        for lp in _LABEL_PATTERNS["fecha_fin"]:
            val = _find_date_near_label(text, lp, stop_keywords=stop_kw)
            if val:
                results["fecha_fin"] = val
                break

    # ── conference end (contextual inference, P4 new) ──
    if not results["fecha_fin"] and results["fecha_inicio"]:
        inferred = _infer_end_date_from_context(text, results["fecha_inicio"])
        if inferred:
            results["fecha_fin"] = inferred
            log.info("  [RegexExtractor] Inferred fecha_fin=%s from context.", inferred)

    # ── other fields (P4: pass stop_keywords to avoid proximity confusion) ──
    for key in ["envio_trabajo", "notificacion_aceptacion", "inscripcion"]:
        stop_kw = _build_all_label_patterns_except(key)
        for lp in _LABEL_PATTERNS.get(key, []):
            val = _find_date_near_label(text, lp, stop_keywords=stop_kw)
            if val:
                results[key] = val
                break

    # ── P4: cross-field deduplication ──
    results = _cross_field_dedup(results)

    # ── confidence ──
    found = sum(1 for v in results.values() if v is not None)
    confidence = found / len(DATE_KEYS)

    log.info(
        "  [RegexExtractor] Found %d/%d fields (confidence %.0f%%)",
        found, len(DATE_KEYS), confidence * 100,
    )
    return results, confidence
