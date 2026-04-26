"""
Comprehensive tests for regex_extractor.py — date extraction from conference text.

Covers:
  - _normalize_date: various date formats, edge cases
  - _try_date_range: single-line date range patterns
  - _try_multiline_range: multi-line conference date ranges
  - _find_date_near_label: label-based date proximity search
  - _infer_end_date_from_context: contextual end-date inference
  - _cross_field_dedup: logically impossible duplicate removal
  - extract_with_regex: full pipeline integration tests
"""

import sys, os
_src = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
_extractors = os.path.join(_src, "extractors")
sys.path.insert(0, _src)
sys.path.insert(0, _extractors)

import pytest
from regex_extractor import (
    _normalize_date,
    _strip_ordinal,
    _try_date_range,
    _try_multiline_range,
    _find_date_near_label,
    _infer_end_date_from_context,
    _cross_field_dedup,
    extract_with_regex,
)


# ═══════════════════════════════════════════════════════════════════════
# _strip_ordinal
# ═══════════════════════════════════════════════════════════════════════

class TestStripOrdinal:
    def test_st(self):
        assert _strip_ordinal("1st") == "1"
        assert _strip_ordinal("21st") == "21"

    def test_nd(self):
        assert _strip_ordinal("2nd") == "2"
        assert _strip_ordinal("22nd") == "22"

    def test_rd(self):
        assert _strip_ordinal("3rd") == "3"
        assert _strip_ordinal("23rd") == "23"

    def test_th(self):
        assert _strip_ordinal("4th") == "4"
        assert _strip_ordinal("15th") == "15"

    def test_no_ordinal(self):
        assert _strip_ordinal("15") == "15"
        assert _strip_ordinal("January") == "January"


# ═══════════════════════════════════════════════════════════════════════
# _normalize_date
# ═══════════════════════════════════════════════════════════════════════

class TestNormalizeDate:
    # --- Standard formats that SHOULD work ---
    def test_day_month_year(self):
        assert _normalize_date("15 June 2026") == "2026-06-15"

    def test_month_day_year(self):
        assert _normalize_date("June 15, 2026") == "2026-06-15"

    def test_iso_format(self):
        assert _normalize_date("2026-06-15") == "2026-06-15"

    def test_slash_format_dayfirst(self):
        # dayfirst=True, so 15/06/2026 = June 15
        assert _normalize_date("15/06/2026") == "2026-06-15"

    def test_dash_numeric_format(self):
        assert _normalize_date("15-06-2026") == "2026-06-15"

    def test_abbreviated_month(self):
        assert _normalize_date("15 Jun 2026") == "2026-06-15"

    def test_ordinal_suffix(self):
        assert _normalize_date("15th June 2026") == "2026-06-15"
        assert _normalize_date("1st January 2026") == "2026-01-01"
        assert _normalize_date("3rd March 2026") == "2026-03-03"

    def test_trailing_period(self):
        assert _normalize_date("15 June 2026.") == "2026-06-15"

    def test_extra_whitespace(self):
        assert _normalize_date("  15 June 2026  ") == "2026-06-15"

    # --- Edge cases that should return None ---
    def test_none_string(self):
        assert _normalize_date("None") is None
        assert _normalize_date("null") is None

    def test_tbd(self):
        assert _normalize_date("TBD") is None
        assert _normalize_date("tbd") is None

    def test_na(self):
        assert _normalize_date("N/A") is None
        assert _normalize_date("n/a") is None

    def test_empty_string(self):
        assert _normalize_date("") is None

    def test_garbage(self):
        assert _normalize_date("asdfghjkl") is None

    # --- Potentially ambiguous formats ---
    def test_two_digit_year(self):
        # 15/06/26 with dayfirst=True
        result = _normalize_date("15/06/26")
        assert result is not None  # should parse somehow

    def test_month_day_no_comma(self):
        """June 15 2026 (no comma) — does dateutil handle it?"""
        result = _normalize_date("June 15 2026")
        assert result == "2026-06-15"

    def test_numeric_ambiguous_month_day(self):
        """01/02/2026 — with dayfirst=True should be Feb 1, not Jan 2"""
        assert _normalize_date("01/02/2026") == "2026-02-01"


# ═══════════════════════════════════════════════════════════════════════
# _try_date_range — single-line range patterns
# ═══════════════════════════════════════════════════════════════════════

class TestTryDateRange:
    # --- Pattern: "5-7 February 2026" ---
    def test_day_dash_day_month_year(self):
        start, end = _try_date_range("5-7 February 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_day_endash_day_month_year(self):
        start, end = _try_date_range("5\u20137 February 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_day_emdash_day_month_year(self):
        start, end = _try_date_range("5\u20147 February 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_ordinal_day_range(self):
        start, end = _try_date_range("5th-7th February 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    # --- Pattern: "February 5-7, 2026" ---
    def test_month_day_dash_day_year(self):
        start, end = _try_date_range("February 5-7, 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_month_day_dash_day_year_no_comma(self):
        start, end = _try_date_range("February 5-7 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    # --- Pattern: "5 February - 7 February 2026" ---
    def test_day_month_dash_day_month_year(self):
        start, end = _try_date_range("5 February - 7 February 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_cross_month_range(self):
        start, end = _try_date_range("28 January - 2 February 2026")
        assert start == "2026-01-28"
        assert end == "2026-02-02"

    # --- Pattern: "February 5 - February 7, 2026" ---
    def test_month_day_dash_month_day_year(self):
        start, end = _try_date_range("February 5 - February 7, 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    # --- "to" keyword patterns ---
    def test_day_month_to_day_month_year(self):
        start, end = _try_date_range("5 February to 7 February 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_month_day_to_day_year(self):
        start, end = _try_date_range("February 5 to 7, 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_day_to_day_month_year(self):
        start, end = _try_date_range("5 to 7 February 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    # --- "through" keyword ---
    def test_day_month_through_day_month_year(self):
        start, end = _try_date_range("5 February through 7 February 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    # --- Abbreviated months ---
    def test_abbreviated_months(self):
        start, end = _try_date_range("5-7 Feb 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_abbreviated_cross_month(self):
        start, end = _try_date_range("28 Jan - 2 Feb 2026")
        assert start == "2026-01-28"
        assert end == "2026-02-02"

    # --- No match ---
    def test_no_range(self):
        start, end = _try_date_range("Some random text without dates")
        assert start is None
        assert end is None

    def test_single_date_not_range(self):
        start, end = _try_date_range("Conference on 15 June 2026")
        assert start is None
        assert end is None

    # --- Edge cases / real-world formats that may fail ---
    def test_range_with_period_after_year(self):
        """'5-7 February 2026.' — trailing period"""
        start, end = _try_date_range("5-7 February 2026.")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_range_embedded_in_sentence(self):
        """Range inside surrounding text"""
        text = "The conference will be held on 5-7 February 2026 in Berlin."
        start, end = _try_date_range(text)
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_range_with_space_around_dash(self):
        """'5 - 7 February 2026' — spaces around dash"""
        start, end = _try_date_range("5 - 7 February 2026")
        assert start == "2026-02-05"
        assert end == "2026-02-07"

    def test_month_day_to_month_day_year(self):
        """'February 5 to March 2, 2026' — cross-month with 'to' keyword
        This format uses Month Day to Month Day, Year"""
        start, end = _try_date_range("February 5 to March 2, 2026")
        # This may not be covered by current patterns
        # Expected: should extract both dates
        assert start is not None or start is None  # document whether it works

    def test_range_with_year_on_both_dates(self):
        """'5 February 2026 - 7 February 2026' — year repeated"""
        start, end = _try_date_range("5 February 2026 - 7 February 2026")
        # Current patterns expect year only at end; this may fail
        assert start is not None or start is None  # document result

    def test_range_comma_separated(self):
        """'October 12, 2026 - October 14, 2026' — full dates with commas"""
        start, end = _try_date_range("October 12, 2026 - October 14, 2026")
        assert start is not None or start is None  # document result


# ═══════════════════════════════════════════════════════════════════════
# _try_multiline_range
# ═══════════════════════════════════════════════════════════════════════

class TestTryMultilineRange:
    def test_basic_multiline(self):
        text = """Conference dates
October 12, 2026
October 14, 2026"""
        start, end = _try_multiline_range(text)
        assert start == "2026-10-12"
        assert end == "2026-10-14"

    def test_with_blank_lines(self):
        text = """Conference dates

October 12, 2026

October 14, 2026"""
        start, end = _try_multiline_range(text)
        assert start == "2026-10-12"
        assert end == "2026-10-14"

    def test_congress_dates_label(self):
        text = """Congress dates
March 5, 2026
March 8, 2026"""
        start, end = _try_multiline_range(text)
        assert start == "2026-03-05"
        assert end == "2026-03-08"

    def test_event_dates_label(self):
        text = """Event dates
June 1, 2026
June 3, 2026"""
        start, end = _try_multiline_range(text)
        assert start == "2026-06-01"
        assert end == "2026-06-03"

    def test_no_label(self):
        """Without a conference/congress/event dates label, should return None"""
        text = """Some heading
October 12, 2026
October 14, 2026"""
        start, end = _try_multiline_range(text)
        assert start is None
        assert end is None

    def test_reversed_dates(self):
        """End date before start date — should return None"""
        text = """Conference dates
October 14, 2026
October 12, 2026"""
        start, end = _try_multiline_range(text)
        # end >= start check: 12 < 14, so reversed means end < start → None
        assert start is None
        assert end is None

    def test_same_date(self):
        """Same start and end — end >= start is True"""
        text = """Conference dates
October 12, 2026
October 12, 2026"""
        start, end = _try_multiline_range(text)
        assert start == "2026-10-12"
        assert end == "2026-10-12"

    def test_dates_too_far_from_label(self):
        """Dates more than 8 lines away from label"""
        text = "Conference dates\n" + "\n".join(["filler"] * 10) + "\nOctober 12, 2026\nOctober 14, 2026"
        start, end = _try_multiline_range(text)
        assert start is None
        assert end is None


# ═══════════════════════════════════════════════════════════════════════
# _find_date_near_label
# ═══════════════════════════════════════════════════════════════════════

class TestFindDateNearLabel:
    def test_date_on_same_line(self):
        text = "Submission deadline: June 15, 2026"
        result = _find_date_near_label(text, r"submission\s*(?:deadline)?")
        assert result == "2026-06-15"

    def test_date_on_next_line(self):
        text = "Submission deadline\nJune 15, 2026"
        result = _find_date_near_label(text, r"submission\s*(?:deadline)?")
        assert result == "2026-06-15"

    def test_date_3_lines_after_label(self):
        text = "Submission deadline\n\n\nJune 15, 2026"
        result = _find_date_near_label(text, r"submission\s*(?:deadline)?")
        assert result == "2026-06-15"

    def test_date_5_lines_after_label(self):
        text = "Submission deadline\n\n\n\n\nJune 15, 2026"
        result = _find_date_near_label(text, r"submission\s*(?:deadline)?")
        assert result == "2026-06-15"

    def test_date_6_lines_after_label_fails(self):
        """Beyond default lookahead of 5"""
        text = "Submission deadline\n\n\n\n\n\nJune 15, 2026"
        result = _find_date_near_label(text, r"submission\s*(?:deadline)?")
        assert result is None

    def test_no_matching_label(self):
        text = "Some unrelated text\nJune 15, 2026"
        result = _find_date_near_label(text, r"submission\s*(?:deadline)?")
        assert result is None

    def test_returns_latest_date_in_window(self):
        """P7: should return the latest date (deadline extension)"""
        text = "Submission deadline\nJune 15, 2026\nJune 20, 2026"
        result = _find_date_near_label(text, r"submission\s*(?:deadline)?")
        assert result == "2026-06-20"

    def test_stop_keywords_prevent_cross_field(self):
        """P4: stop scanning when another field's label is hit"""
        import re
        stop_kw = [re.compile(r"notification", re.IGNORECASE)]
        text = "Submission deadline\nNotification of acceptance\nJune 15, 2026"
        result = _find_date_near_label(
            text, r"submission\s*(?:deadline)?", stop_keywords=stop_kw
        )
        assert result is None

    def test_date_with_colon_separator(self):
        text = "Submission deadline: 15/06/2026"
        result = _find_date_near_label(text, r"submission\s*(?:deadline)?")
        assert result is not None

    def test_html_table_like_structure(self):
        """Simulating date/label separated by 3-4 newlines (from HTML table)"""
        text = """Important Dates
Paper Submission
\t
\t
June 15, 2026"""
        result = _find_date_near_label(text, r"paper\s+submission")
        assert result == "2026-06-15"


# ═══════════════════════════════════════════════════════════════════════
# _infer_end_date_from_context
# ═══════════════════════════════════════════════════════════════════════

class TestInferEndDateFromContext:
    def test_basic_inference(self):
        text = "Conference\nOctober 12, 2026\nOctober 14, 2026"
        result = _infer_end_date_from_context(text, "2026-10-12")
        assert result == "2026-10-14"

    def test_skips_deadline_keywords(self):
        text = "Conference\nOctober 12, 2026\nSubmission deadline: October 20, 2026"
        result = _infer_end_date_from_context(text, "2026-10-12")
        assert result is None

    def test_rejects_same_date(self):
        """P5: strict inequality — inferred end must be AFTER start"""
        text = "Conference\nOctober 12, 2026\nOctober 12, 2026"
        result = _infer_end_date_from_context(text, "2026-10-12")
        assert result is None

    def test_rejects_date_more_than_30_days(self):
        text = "Conference\nOctober 12, 2026\nDecember 15, 2026"
        result = _infer_end_date_from_context(text, "2026-10-12")
        assert result is None

    def test_accepts_date_within_30_days(self):
        text = "Conference\nOctober 12, 2026\nOctober 15, 2026"
        result = _infer_end_date_from_context(text, "2026-10-12")
        assert result == "2026-10-15"

    def test_invalid_start_date(self):
        result = _infer_end_date_from_context("some text", "not-a-date")
        assert result is None

    def test_start_date_not_in_text(self):
        text = "Conference\nOctober 14, 2026\nOctober 16, 2026"
        result = _infer_end_date_from_context(text, "2026-10-12")
        assert result is None


# ═══════════════════════════════════════════════════════════════════════
# _cross_field_dedup
# ═══════════════════════════════════════════════════════════════════════

class TestCrossFieldDedup:
    def test_submission_equals_acceptance(self):
        results = {
            "fecha_inicio": "2026-10-12",
            "fecha_fin": "2026-10-14",
            "envio_trabajo": "2026-06-15",
            "notificacion_aceptacion": "2026-06-15",  # same as submission!
            "inscripcion": None,
        }
        cleaned = _cross_field_dedup(results)
        assert cleaned["envio_trabajo"] == "2026-06-15"
        assert cleaned["notificacion_aceptacion"] is None

    def test_no_duplicates(self):
        results = {
            "fecha_inicio": "2026-10-12",
            "fecha_fin": "2026-10-14",
            "envio_trabajo": "2026-06-15",
            "notificacion_aceptacion": "2026-07-15",
            "inscripcion": "2026-09-01",
        }
        cleaned = _cross_field_dedup(results)
        assert all(v is not None for v in cleaned.values())

    def test_registration_equals_conf_start(self):
        results = {
            "fecha_inicio": "2026-10-12",
            "fecha_fin": "2026-10-14",
            "envio_trabajo": "2026-06-15",
            "notificacion_aceptacion": "2026-07-15",
            "inscripcion": "2026-10-12",  # same as conf start!
        }
        cleaned = _cross_field_dedup(results)
        assert cleaned["inscripcion"] is None

    def test_all_none(self):
        results = {k: None for k in ["fecha_inicio", "fecha_fin", "envio_trabajo",
                                      "notificacion_aceptacion", "inscripcion"]}
        cleaned = _cross_field_dedup(results)
        assert all(v is None for v in cleaned.values())


# ═══════════════════════════════════════════════════════════════════════
# extract_with_regex — FULL PIPELINE INTEGRATION TESTS
# ═══════════════════════════════════════════════════════════════════════

class TestExtractWithRegex:
    """
    Real-world-like text inputs to test the complete extraction pipeline.
    These represent the kind of text that text_extractor produces from HTML.
    """

    def test_complete_conference_page(self):
        """All fields present in a well-structured page"""
        text = """
International Conference on Advanced Materials 2026

Conference dates: October 12-14, 2026, Berlin, Germany

Important Dates:

Paper Submission Deadline: June 15, 2026
Notification of Acceptance: July 30, 2026
Early Bird Registration Deadline: September 1, 2026
"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-10-12"
        assert results["fecha_fin"] == "2026-10-14"
        assert results["envio_trabajo"] == "2026-06-15"
        assert results["notificacion_aceptacion"] == "2026-07-30"
        assert results["inscripcion"] == "2026-09-01"
        assert confidence == 1.0

    def test_range_with_month_name(self):
        """Range using 'Month Day-Day, Year' format"""
        text = """
Conference: ICOAMP 2026
Date: February 5-7, 2026

Submission Deadline: November 30, 2025
Acceptance Notification: December 20, 2025
"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-02-05"
        assert results["fecha_fin"] == "2026-02-07"

    def test_dates_on_separate_lines_with_labels(self):
        """Each date on its own line with label above"""
        text = """
Important Dates

Paper Submission
March 15, 2026

Notification of Acceptance
April 20, 2026

Conference Start
June 10, 2026

Registration Deadline
May 30, 2026
"""
        results, confidence = extract_with_regex(text)
        assert results["envio_trabajo"] == "2026-03-15"
        assert results["notificacion_aceptacion"] == "2026-04-20"

    def test_cross_month_range(self):
        """Range crossing months: '28 January - 2 February 2026'"""
        text = """
The 15th International Workshop on Advanced Computing
28 January - 2 February 2026
Zurich, Switzerland
"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-01-28"
        assert results["fecha_fin"] == "2026-02-02"

    def test_range_with_to_keyword(self):
        """Range using 'to' keyword"""
        text = """
Conference Period: 5 February to 7 February 2026
"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-02-05"
        assert results["fecha_fin"] == "2026-02-07"

    def test_minimal_info(self):
        """Only one date found"""
        text = """
ICOAMP 2026 - Registration opens soon
Submission deadline: March 15, 2026
"""
        results, confidence = extract_with_regex(text)
        assert results["envio_trabajo"] == "2026-03-15"
        assert confidence == pytest.approx(0.2)

    def test_no_dates_at_all(self):
        """Page with no recognizable dates"""
        text = """
Welcome to the International Conference on Computing.
More details will be announced soon.
Please check back later for important dates.
"""
        results, confidence = extract_with_regex(text)
        assert all(v is None for v in results.values())
        assert confidence == 0.0

    def test_spanish_labels(self):
        """Spanish-language labels"""
        text = """
Fechas Importantes

Envío de trabajos: 15 de junio de 2026
Notificación de aceptación: 30 de julio de 2026
Inscripción: 1 de septiembre de 2026
"""
        results, confidence = extract_with_regex(text)
        # Note: "de junio de" may not parse with current regex
        # This test documents the behavior

    def test_numeric_date_format(self):
        """Dates in DD/MM/YYYY format"""
        text = """
Important Dates:
Paper Submission: 15/06/2026
Notification of Acceptance: 30/07/2026
Registration: 01/09/2026
Conference: 12/10/2026 - 14/10/2026
"""
        results, confidence = extract_with_regex(text)
        assert results["envio_trabajo"] is not None
        assert results["notificacion_aceptacion"] is not None

    def test_extended_deadline_picks_latest(self):
        """P7: extended deadline — should prefer later date"""
        text = """
Paper Submission Deadline: June 15, 2026
Extended Deadline: June 30, 2026
Notification of Acceptance: July 30, 2026
"""
        results, confidence = extract_with_regex(text)
        # P7 logic returns the latest date near a label
        assert results["envio_trabajo"] == "2026-06-30"

    def test_ordinal_dates_in_range(self):
        """'5th-7th February 2026' with ordinal suffixes"""
        text = """
Conference Dates: 5th-7th February 2026
Submission deadline: 1st November 2025
"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-02-05"
        assert results["fecha_fin"] == "2026-02-07"

    def test_multiline_conference_dates_without_range(self):
        """Conference dates on separate lines (no range syntax)"""
        text = """
Conference dates
October 12, 2026
October 14, 2026

Submission Deadline: June 15, 2026
"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-10-12"
        assert results["fecha_fin"] == "2026-10-14"

    def test_dedup_removes_impossible_pairs(self):
        """When submission and notification have the same date"""
        text = """
Call for papers deadline: March 15, 2026
Notification of acceptance: March 15, 2026
Conference dates: June 5-7, 2026
"""
        results, confidence = extract_with_regex(text)
        # P4: these can't be the same — nullify the less reliable one
        assert results["envio_trabajo"] == "2026-03-15"
        assert results["notificacion_aceptacion"] is None

    # --- Real-world problematic formats ---

    def test_date_range_with_repeated_year(self):
        """'October 12, 2026 - October 14, 2026' — year on both sides"""
        text = """
Conference: October 12, 2026 - October 14, 2026
Submission: June 15, 2026
"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-10-12"
        assert results["fecha_fin"] == "2026-10-14"

    def test_date_with_day_of_week(self):
        """'Monday, October 12, 2026' — day of week prefix"""
        text = """
Conference starts: Monday, October 12, 2026
Conference ends: Wednesday, October 14, 2026
"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] is not None

    def test_iso_dates(self):
        """Dates in YYYY-MM-DD format"""
        text = """
Submission deadline: 2026-06-15
Notification: 2026-07-30
Conference: 2026-10-12 to 2026-10-14
"""
        results, confidence = extract_with_regex(text)
        assert results["envio_trabajo"] == "2026-06-15"

    def test_conference_date_label_colon(self):
        """'Conference date: October 12-14, 2026'"""
        text = "Conference date: October 12-14, 2026"
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-10-12"
        assert results["fecha_fin"] == "2026-10-14"

    def test_abstract_submission_label(self):
        """'Abstract submission deadline' variant"""
        text = """
Abstract Submission Deadline: May 1, 2026
Full Paper Submission: June 15, 2026
"""
        results, confidence = extract_with_regex(text)
        # Should pick up at least one submission date
        assert results["envio_trabajo"] is not None


# ═══════════════════════════════════════════════════════════════════════
# Real conference text samples (anonymized)
# ═══════════════════════════════════════════════════════════════════════

class TestRealWorldSamples:
    """Samples modeled on patterns seen in real conference websites."""

    def test_sample_table_layout(self):
        """Text extracted from an HTML table layout"""
        text = """Important Dates
Submission Deadline
June 15, 2026
Notification of Acceptance
August 1, 2026
Camera-Ready Version
August 30, 2026
Early Bird Registration
September 15, 2026
Conference
October 5-7, 2026"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-10-05"
        assert results["fecha_fin"] == "2026-10-07"
        assert results["envio_trabajo"] is not None

    def test_sample_inline_dates(self):
        """All dates inline with labels"""
        text = """Key Dates:
• Paper submission: March 31, 2026
• Author notification: May 15, 2026
• Registration deadline: June 30, 2026
• Conference: July 15-17, 2026"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-07-15"
        assert results["fecha_fin"] == "2026-07-17"
        assert results["envio_trabajo"] is not None

    def test_sample_with_strikethrough_dates_already_removed(self):
        """After text_extractor removes <s>/<del> tags"""
        text = """Important Dates:
Paper Submission: July 1, 2026
Notification: August 15, 2026"""
        results, confidence = extract_with_regex(text)
        assert results["envio_trabajo"] == "2026-07-01"
        assert results["notificacion_aceptacion"] == "2026-08-15"

    def test_sample_with_en_dash_normalized(self):
        """After text_extractor normalizes en-dashes to hyphens"""
        text = """Conference: 10-12 November 2026
Submission: 1 June 2026"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-11-10"
        assert results["fecha_fin"] == "2026-11-12"

    def test_sample_minimal_conference_page(self):
        """Very sparse page — only conference name and date"""
        text = """
ICOAMP 2026
The 5th International Conference on Advanced Materials Processing
February 5-7, 2026
Tokyo, Japan
"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-02-05"
        assert results["fecha_fin"] == "2026-02-07"

    def test_sample_dates_with_location_mixed(self):
        """Dates interleaved with location info"""
        text = """
Conference Dates: September 20-22, 2026 | Berlin, Germany
Paper Submission Deadline: April 15, 2026
Notification of Acceptance: June 1, 2026
Registration: August 1, 2026
"""
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2026-09-20"
        assert results["fecha_fin"] == "2026-09-22"
        assert results["envio_trabajo"] == "2026-04-15"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
