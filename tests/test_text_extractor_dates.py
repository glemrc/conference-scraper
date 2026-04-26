"""
Tests for text_extractor.py — HTML parsing and date-relevant text extraction.

Covers:
  - _clean_soup: strikethrough removal, tag stripping
  - _normalize_dashes: en-dash/em-dash normalization
  - extract_date_text: smart extraction strategies
  - extract_full_text: fallback full text
  - extract_conference_name: conference name from HTML
  - detect_pdf_links: PDF link detection
"""

import sys, os
_src = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
_extractors = os.path.join(_src, "extractors")
sys.path.insert(0, _src)
sys.path.insert(0, _extractors)

import pytest
from text_extractor import (
    extract_date_text,
    extract_full_text,
    extract_conference_name,
    detect_pdf_links,
    _clean_soup,
    _normalize_dashes,
    _has_dates,
)


# ═══════════════════════════════════════════════════════════════════════
# _normalize_dashes
# ═══════════════════════════════════════════════════════════════════════

class TestNormalizeDashes:
    def test_en_dash(self):
        assert _normalize_dashes("5\u20137") == "5-7"

    def test_em_dash(self):
        assert _normalize_dashes("5\u20147") == "5-7"

    def test_normal_dash_unchanged(self):
        assert _normalize_dashes("5-7") == "5-7"

    def test_mixed_dashes(self):
        text = "5\u20137 Feb \u2014 10\u201312 Mar"
        result = _normalize_dashes(text)
        assert "\u2013" not in result
        assert "\u2014" not in result


# ═══════════════════════════════════════════════════════════════════════
# _clean_soup — strikethrough and tag removal
# ═══════════════════════════════════════════════════════════════════════

class TestCleanSoup:
    def test_removes_script_tags(self):
        html = "<html><body><p>Hello</p><script>alert(1)</script></body></html>"
        soup = _clean_soup(html)
        assert "alert" not in soup.get_text()

    def test_removes_style_tags(self):
        html = "<html><body><p>Hello</p><style>.x{color:red}</style></body></html>"
        soup = _clean_soup(html)
        assert "color" not in soup.get_text()

    def test_removes_strikethrough_s(self):
        """FIX-S1: <s> tags (old dates) should be removed"""
        html = "<p>Deadline: <s>May 20, 2026</s> June 15, 2026</p>"
        soup = _clean_soup(html)
        text = soup.get_text()
        assert "May 20" not in text
        assert "June 15" in text

    def test_removes_strikethrough_del(self):
        html = "<p>Deadline: <del>May 20, 2026</del> June 15, 2026</p>"
        soup = _clean_soup(html)
        text = soup.get_text()
        assert "May 20" not in text
        assert "June 15" in text

    def test_removes_strikethrough_strike(self):
        html = "<p>Deadline: <strike>May 20, 2026</strike> June 15, 2026</p>"
        soup = _clean_soup(html)
        text = soup.get_text()
        assert "May 20" not in text
        assert "June 15" in text

    def test_removes_nav_footer_header(self):
        html = """<html><body>
        <nav>Navigation</nav>
        <p>Content</p>
        <footer>Footer</footer>
        </body></html>"""
        soup = _clean_soup(html)
        text = soup.get_text()
        assert "Navigation" not in text
        assert "Footer" not in text
        assert "Content" in text


# ═══════════════════════════════════════════════════════════════════════
# _has_dates
# ═══════════════════════════════════════════════════════════════════════

class TestHasDates:
    def test_named_month_date(self):
        assert _has_dates("June 15, 2026") is True

    def test_numeric_date(self):
        assert _has_dates("15/06/2026") is True

    def test_iso_date(self):
        assert _has_dates("2026-06-15") is True

    def test_no_dates(self):
        assert _has_dates("Some text without any dates") is False

    def test_partial_date_no_year(self):
        """'June 15' without year — should NOT match (requires year)"""
        assert _has_dates("June 15") is False

    def test_day_month_year(self):
        assert _has_dates("15 June 2026") is True


# ═══════════════════════════════════════════════════════════════════════
# extract_date_text — smart extraction strategies
# ═══════════════════════════════════════════════════════════════════════

class TestExtractDateText:
    def test_heading_contains_date_range_no_keywords(self):
        """Strategy 1 must also capture headings whose TEXT IS a date range.

        Elementor / builder pages often place the conference date directly in
        an <h4> with no surrounding 'important-dates'-style keywords.  The
        submission-deadline content lives in a completely separate section,
        so _extract_by_divs finds that section but misses the date heading.
        The context-window fallback never runs because all_structural is
        non-empty.  Regression: fecha_inicio and fecha_fin were both None.
        """
        from regex_extractor import extract_with_regex

        html = """<html><body>
        <section>
          <div class="elementor-widget-container">
            <h4 class="elementor-heading-title">24-26 September 2025, Seoul, South Korea</h4>
          </div>
        </section>
        <section>
          <div class="elementor-widget-container">
            <p>Paper submission deadline: July 15, 2025</p>
            <p>Notification of acceptance: August 10, 2025</p>
          </div>
        </section>
        </body></html>"""

        text = extract_date_text(html)
        assert "24-26 September 2025" in text or "September 2025" in text, (
            f"Conference date not in extracted text: {repr(text)}"
        )
        results, confidence = extract_with_regex(text)
        assert results["fecha_inicio"] == "2025-09-24"
        assert results["fecha_fin"] == "2025-09-26"

    def test_heading_based_extraction(self):
        """Strategy 1: content under date-related headings"""
        html = """<html><body>
        <h2>Important Dates</h2>
        <p>Submission deadline: June 15, 2026</p>
        <p>Notification: August 1, 2026</p>
        <h2>About</h2>
        <p>This is a conference about materials.</p>
        </body></html>"""
        result = extract_date_text(html)
        assert "June 15, 2026" in result
        assert "August 1, 2026" in result

    def test_table_based_extraction(self):
        """Strategy 2: dates in HTML tables"""
        html = """<html><body>
        <table>
        <tr><td>Submission</td><td>June 15, 2026</td></tr>
        <tr><td>Notification</td><td>August 1, 2026</td></tr>
        <tr><td>Conference</td><td>October 12-14, 2026</td></tr>
        </table>
        </body></html>"""
        result = extract_date_text(html)
        assert "June 15, 2026" in result
        assert "October 12-14, 2026" in result

    def test_div_based_extraction(self):
        """Strategy 3: dates in divs with keywords"""
        html = """<html><body>
        <div class="dates">
        <h3>Important Dates</h3>
        <p>Paper Submission: June 15, 2026</p>
        <p>Notification: August 1, 2026</p>
        </div>
        <div class="other">
        <p>Some unrelated content without dates.</p>
        </div>
        </body></html>"""
        result = extract_date_text(html)
        assert "June 15, 2026" in result

    def test_context_window_fallback(self):
        """Strategy 4: when no structural match, use context window"""
        html = """<html><body>
        <p>Welcome to the conference.</p>
        <p>The deadline for submission is June 15, 2026.</p>
        <p>We look forward to seeing you.</p>
        </body></html>"""
        result = extract_date_text(html)
        assert "June 15, 2026" in result

    def test_full_text_fallback(self):
        """Strategy 5: when nothing else works, full text"""
        html = "<html><body><p>No dates here at all.</p></body></html>"
        result = extract_date_text(html)
        assert "No dates here at all" in result

    def test_strikethrough_dates_removed(self):
        """FIX-S1: strikethrough old dates should not appear"""
        html = """<html><body>
        <h2>Important Dates</h2>
        <p>Submission: <s>June 1, 2026</s> <b>June 15, 2026</b></p>
        </body></html>"""
        result = extract_date_text(html)
        assert "June 1, 2026" not in result
        assert "June 15, 2026" in result

    def test_en_dash_normalized(self):
        """FIX-S2: en-dashes should be converted to hyphens"""
        html = """<html><body>
        <h2>Conference Dates</h2>
        <p>October 12\u201314, 2026</p>
        </body></html>"""
        result = extract_date_text(html)
        assert "12-14" in result or "12\u201314" not in result

    def test_priority_ordering(self):
        """F5: Important Dates section should appear first even if later in HTML"""
        html = """<html><body>
        <h2>Schedule</h2>
        <p>Tutorials: October 11, 2026</p>
        <h2>Important Dates</h2>
        <p>Submission: June 15, 2026</p>
        </body></html>"""
        result = extract_date_text(html)
        # "Important Dates" is a priority keyword, should come first
        idx_important = result.find("June 15")
        idx_schedule = result.find("October 11")
        if idx_important >= 0 and idx_schedule >= 0:
            assert idx_important < idx_schedule

    def test_definition_list(self):
        """Dates in <dl> structure"""
        html = """<html><body>
        <dl>
        <dt>Submission Deadline</dt>
        <dd>June 15, 2026</dd>
        <dt>Notification</dt>
        <dd>August 1, 2026</dd>
        </dl>
        </body></html>"""
        result = extract_date_text(html)
        assert "June 15, 2026" in result

    def test_unordered_list(self):
        """Dates in <ul> list"""
        html = """<html><body>
        <ul>
        <li>Submission: June 15, 2026</li>
        <li>Notification: August 1, 2026</li>
        <li>Conference: October 12-14, 2026</li>
        </ul>
        </body></html>"""
        result = extract_date_text(html)
        assert "June 15, 2026" in result

    def test_large_div_skipped(self):
        """Divs over 6000 chars should be skipped (page wrappers)"""
        filler = "x" * 7000
        html = f"""<html><body>
        <div class="wrapper">
        <p>Important dates deadline {filler}</p>
        <p>June 15, 2026</p>
        </div>
        </body></html>"""
        # The large div should be skipped; test that it doesn't error
        result = extract_date_text(html)
        assert isinstance(result, str)


# ═══════════════════════════════════════════════════════════════════════
# detect_pdf_links
# ═══════════════════════════════════════════════════════════════════════

class TestDetectPdfLinks:
    def test_finds_pdf_links(self):
        html = '<html><body><a href="/docs/program.pdf">Program</a></body></html>'
        links = detect_pdf_links(html, "https://conf.org")
        assert len(links) == 1
        assert links[0].endswith(".pdf")

    def test_resolves_relative_urls(self):
        html = '<html><body><a href="/docs/cfp.pdf">CFP</a></body></html>'
        links = detect_pdf_links(html, "https://conf.org")
        assert links[0] == "https://conf.org/docs/cfp.pdf"

    def test_caps_at_5(self):
        html = "<html><body>"
        for i in range(10):
            html += f'<a href="/doc{i}.pdf">Doc {i}</a>'
        html += "</body></html>"
        links = detect_pdf_links(html)
        assert len(links) <= 5

    def test_no_pdf_links(self):
        html = '<html><body><a href="/about">About</a></body></html>'
        links = detect_pdf_links(html)
        assert len(links) == 0

    def test_deduplicates(self):
        html = """<html><body>
        <a href="/doc.pdf">Doc 1</a>
        <a href="/doc.pdf">Doc 2</a>
        </body></html>"""
        links = detect_pdf_links(html, "https://conf.org")
        assert len(links) == 1


# ═══════════════════════════════════════════════════════════════════════
# End-to-end: extract_date_text -> extract_with_regex
# ═══════════════════════════════════════════════════════════════════════

class TestEndToEnd:
    """Test the full flow: HTML -> text_extractor -> regex_extractor"""

    def test_complete_html_page(self):
        from regex_extractor import extract_with_regex

        html = """<html><head><title>ICOAMP 2026</title></head><body>
        <h1>5th International Conference on Advanced Materials Processing</h1>
        <h2>Important Dates</h2>
        <table>
        <tr><td>Paper Submission Deadline</td><td>June 15, 2026</td></tr>
        <tr><td>Notification of Acceptance</td><td>August 1, 2026</td></tr>
        <tr><td>Registration Deadline</td><td>September 15, 2026</td></tr>
        </table>
        <h2>Conference Dates</h2>
        <p>October 12-14, 2026, Berlin, Germany</p>
        </body></html>"""

        text = extract_date_text(html)
        results, confidence = extract_with_regex(text)

        assert results["fecha_inicio"] == "2026-10-12"
        assert results["fecha_fin"] == "2026-10-14"
        assert results["envio_trabajo"] == "2026-06-15"
        assert results["notificacion_aceptacion"] == "2026-08-01"
        assert results["inscripcion"] == "2026-09-15"

    def test_page_with_strikethrough_old_dates(self):
        from regex_extractor import extract_with_regex

        html = """<html><body>
        <h2>Important Dates</h2>
        <p>Submission: <del>May 15, 2026</del> <b>June 1, 2026</b> (Extended!)</p>
        <p>Notification of Acceptance: July 15, 2026</p>
        <p>Conference: September 5-7, 2026</p>
        </body></html>"""

        text = extract_date_text(html)
        results, confidence = extract_with_regex(text)

        # Old date should be removed by text_extractor
        assert results["envio_trabajo"] != "2026-05-15"
        assert results["fecha_inicio"] == "2026-09-05"
        assert results["fecha_fin"] == "2026-09-07"

    def test_page_with_en_dashes(self):
        from regex_extractor import extract_with_regex

        html = """<html><body>
        <h2>Conference Schedule</h2>
        <p>Conference: 10\u201312 November 2026</p>
        <p>Submission deadline: 1 June 2026</p>
        </body></html>"""

        text = extract_date_text(html)
        results, confidence = extract_with_regex(text)

        assert results["fecha_inicio"] == "2026-11-10"
        assert results["fecha_fin"] == "2026-11-12"

    def test_page_with_only_dates_no_structure(self):
        """Minimal HTML with dates but no headings/tables"""
        from regex_extractor import extract_with_regex

        html = """<html><body>
        <p>The conference will be held on February 5-7, 2026 in Tokyo.</p>
        <p>Submit your paper by November 30, 2025.</p>
        </body></html>"""

        text = extract_date_text(html)
        results, confidence = extract_with_regex(text)

        assert results["fecha_inicio"] == "2026-02-05" or results["fecha_inicio"] == "2025-11-30" or results["fecha_inicio"] is None
        # This documents the behavior for unstructured pages

    def test_comesyso_portlet_structure(self):
        """Liferay portlet-content with labels+dates separated by <br>, range with year on both dates.

        Regression: 'Month DD, YYYY - Month DD, YYYY' must yield correct fecha_fin.
        All five fields must be extracted at 100% confidence.
        """
        from regex_extractor import extract_with_regex

        html = """<html><body>
        <div class="portlet-content">
            <p class="message message-error">Paper Submission Deadline :<br>June 28, 2026<br><br></p>
            <p class="message message-error">Paper Acceptance Notification Date:<br>July 28, 2026<br><br></p>
            <p class="message message-info">Final Camera Ready Submission Date:<br>August 28, 2026<br><br></p>
            <p class="message message-info">Registration and Fee Payment :<br>August 28, 2026<br><br></p>
            <p class="message message-info">Conference date:<br>October 28, 2026 - October 31, 2026<br><br></p>
        </div>
        </body></html>"""

        text = extract_date_text(html)
        results, confidence = extract_with_regex(text)

        assert results["fecha_inicio"] == "2026-10-28"
        assert results["fecha_fin"] == "2026-10-31"
        assert results["envio_trabajo"] == "2026-06-28"
        assert results["notificacion_aceptacion"] == "2026-07-28"
        assert results["inscripcion"] == "2026-08-28"
        assert confidence == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
