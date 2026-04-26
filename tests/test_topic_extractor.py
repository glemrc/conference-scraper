"""
Tests for topic_extractor.py — HTML-based topic/theme extraction.
"""

import sys, os
_src = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
_extractors = os.path.join(_src, "extractors")
sys.path.insert(0, _src)
sys.path.insert(0, _extractors)

import pytest
from topic_extractor import extract_topics, _clean_topic, _is_valid_topic


class TestCleanTopic:
    def test_strips_bullet(self):
        assert _clean_topic("• Machine Learning") == "Machine Learning"

    def test_strips_lettered(self):
        assert _clean_topic("A) Information Systems") == "Information Systems"

    def test_strips_numbered(self):
        assert _clean_topic("1. Artificial Intelligence") == "Artificial Intelligence"

    def test_strips_track_prefix(self):
        assert _clean_topic("Track 1: Data Science") == "Data Science"


class TestIsValidTopic:
    def test_valid_topic(self):
        assert _is_valid_topic("Artificial Intelligence and Machine Learning")

    def test_too_short(self):
        assert not _is_valid_topic("AI")

    def test_noise_submit(self):
        assert not _is_valid_topic("Submit your paper by June 15")

    def test_noise_deadline(self):
        assert not _is_valid_topic("Submission deadline extended")

    def test_noise_url(self):
        assert not _is_valid_topic("https://example.com/cfp")

    def test_noise_author_names(self):
        assert not _is_valid_topic(
            "Impact of AI on Health John Smith, Jane Doe, Bob Johnson"
        )

    def test_noise_abstract_only(self):
        assert not _is_valid_topic("Abstract Only: Authors will be given the chance")

    def test_noise_committee(self):
        assert not _is_valid_topic("General Chairs")

    def test_intro_phrase(self):
        assert not _is_valid_topic("but not limited to the following areas")


class TestExtractTopicsFromList:
    def test_ul_under_heading(self):
        html = """<html><body>
        <h2>Topics of Interest</h2>
        <ul>
            <li>Machine Learning</li>
            <li>Natural Language Processing</li>
            <li>Computer Vision</li>
        </ul>
        </body></html>"""
        topics = extract_topics(html)
        assert "Machine Learning" in topics
        assert "Natural Language Processing" in topics
        assert "Computer Vision" in topics

    def test_ol_under_heading(self):
        html = """<html><body>
        <h2>Conference Tracks</h2>
        <ol>
            <li>Data Science</li>
            <li>Cybersecurity</li>
        </ol>
        </body></html>"""
        topics = extract_topics(html)
        assert "Data Science" in topics
        assert "Cybersecurity" in topics

    def test_nested_list_in_bold(self):
        """ICEPR pattern: <ul><b><li>...</li></b></ul>"""
        html = """<html><body>
        <h2>Conference Topics</h2>
        <ul><b>
            <li>Air pollution</li>
            <li>Water treatment</li>
        </b></ul>
        </body></html>"""
        topics = extract_topics(html)
        assert "Air pollution" in topics
        assert "Water treatment" in topics

    def test_dl_definition_list(self):
        html = """<html><body>
        <h3>Areas of Interest</h3>
        <dl>
            <dt>Renewable Energy</dt>
            <dd>Solar, wind, hydro power systems</dd>
            <dt>Smart Grids</dt>
            <dd>Grid optimization and monitoring</dd>
        </dl>
        </body></html>"""
        topics = extract_topics(html)
        assert "Renewable Energy" in topics
        assert "Smart Grids" in topics


class TestExtractTopicsLettered:
    def test_semicolon_separated(self):
        """WorldCist pattern: A) X; B) Y; C) Z in a single paragraph"""
        html = """<html><body>
        <p>Submitted papers should be related with one or more of the main themes:</p>
        <p>A) Information Systems; B) Software Engineering; C) Data Science</p>
        </body></html>"""
        topics = extract_topics(html)
        assert "Information Systems" in topics
        assert "Software Engineering" in topics
        assert "Data Science" in topics


class TestExtractTopicsContainerAttrs:
    def test_div_with_topic_class(self):
        html = """<html><body>
        <div class="topics-section">
            <ul>
                <li>IoT and Smart Systems</li>
                <li>Cloud Computing</li>
            </ul>
        </div>
        </body></html>"""
        topics = extract_topics(html)
        assert "IoT and Smart Systems" in topics
        assert "Cloud Computing" in topics


class TestDeduplication:
    def test_no_duplicates(self):
        html = """<html><body>
        <h2>Topics</h2>
        <ul>
            <li>Machine Learning</li>
            <li>Machine Learning</li>
            <li>Data Science</li>
        </ul>
        </body></html>"""
        topics = extract_topics(html)
        assert topics.count("Machine Learning") == 1


class TestNoTopics:
    def test_page_without_topics(self):
        html = """<html><body>
        <h1>Welcome to our conference</h1>
        <p>Registration is now open.</p>
        <p>Contact us at info@conf.org</p>
        </body></html>"""
        topics = extract_topics(html)
        assert len(topics) == 0

    def test_noise_only(self):
        html = """<html><body>
        <h2>Call for Papers</h2>
        <p>Submit your paper by June 15, 2026</p>
        <p>Notification of acceptance: August 1, 2026</p>
        </body></html>"""
        topics = extract_topics(html)
        assert len(topics) == 0


class TestBrSeparatedTopics:
    """ICET pattern: topics separated by <br> inside <p> under track headings."""

    def test_br_under_h5_track(self):
        html = """<html><body>
        <h5>Track 1: Inclusive Learning Environments</h5>
        <p><font size="3">
            Universal Design for Learning<br>
            Accessibility Features in EdTech<br>
            Strategies for Addressing the Digital Divide
        </font></p>
        </body></html>"""
        topics = extract_topics(html)
        assert "Universal Design for Learning" in topics
        assert "Accessibility Features in EdTech" in topics
        assert "Strategies for Addressing the Digital Divide" in topics

    def test_multiple_tracks_br(self):
        html = """<html><body>
        <h5>Track 1: AI in Education</h5>
        <p>AI-Powered Learning<br>Chatbots in Support<br>Ethical AI</p>
        <h5>Track 2: Emerging Tech</h5>
        <p>Virtual Reality<br>Blockchain Applications</p>
        </body></html>"""
        topics = extract_topics(html)
        assert "AI-Powered Learning" in topics
        assert "Virtual Reality" in topics


class TestAccordionTopics:
    """EEEU25 pattern: topics in accordion widget titles."""

    def test_elementor_accordion(self):
        html = """<html><body>
        <h5>Topics</h5>
        <h2>Conference Theme</h2>
        <div class="elementor-accordion">
            <a class="elementor-accordion-title" tabindex="0">Track 1: Social Problems</a>
            <div class="elementor-tab-content"><p>Details here</p></div>
            <a class="elementor-accordion-title" tabindex="0">Track 2: Financing</a>
            <div class="elementor-tab-content"><p>Details here</p></div>
            <a class="elementor-accordion-title" tabindex="0">Track 3: Management</a>
        </div>
        </body></html>"""
        topics = extract_topics(html)
        assert any("Social Problems" in t for t in topics)
        assert any("Financing" in t for t in topics)
        assert any("Management" in t for t in topics)

    def test_heading_hierarchy_topic_keyword_continues(self):
        """h5 Topics → h2 Conference Theme should NOT stop the walker."""
        html = """<html><body>
        <h5>Topics</h5>
        <h2>Conference Theme</h2>
        <ul><li>Data Science</li><li>Machine Learning</li></ul>
        </body></html>"""
        topics = extract_topics(html)
        assert "Data Science" in topics
        assert "Machine Learning" in topics


class TestCommaTextTopics:
    """SCIS pattern: comma-separated topics in paragraphs."""

    def test_comma_separated_with_strong_heading(self):
        html = """<html><body>
        <p>The topics covered in the conference are as follows:</p>
        <p><strong>Sustainable Computing:</strong></p>
        <p>Green Computing, Energy-efficient algorithms, Low-power hardware,
        Renewable Energy Integration, Energy-aware computing</p>
        </body></html>"""
        topics = extract_topics(html)
        assert "Sustainable Computing" in topics
        assert "Green Computing" in topics
        assert "Energy-efficient algorithms" in topics

    def test_comma_separated_no_strong(self):
        html = """<html><body>
        <h3>Conference Scope</h3>
        <p>Internet of Things, Information Security, Embedded Systems,
        Cloud Computing, Big Data Analysis, Quantum Computing</p>
        </body></html>"""
        topics = extract_topics(html)
        assert "Internet of Things" in topics
        assert "Cloud Computing" in topics


class TestRealHtmlSamples:
    """Integration tests using saved HTML files from failing conferences."""

    @pytest.fixture
    def samples_dir(self):
        return os.path.join(os.path.dirname(__file__), "html_samples")

    def _read_html(self, samples_dir, filename):
        path = os.path.join(samples_dir, filename)
        if not os.path.exists(path):
            pytest.skip(f"Sample file not found: {filename}")
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()

    def test_icet_2026(self, samples_dir):
        """ICET: <br>-separated topics under <h5>Track N: ...</h5>"""
        html = self._read_html(samples_dir, "ICET 2026.html")
        topics = extract_topics(html)
        assert len(topics) >= 5, f"Expected >=5 topics, got {len(topics)}: {topics}"
        # Check some known sub-topics
        topic_text = " ".join(topics).lower()
        assert "universal design" in topic_text or "ai" in topic_text.lower()

    def test_scis_2026(self, samples_dir):
        """SCIS: comma-separated topics in paragraphs."""
        html = self._read_html(samples_dir, "SCIS2026.html")
        topics = extract_topics(html)
        assert len(topics) >= 2, f"Expected >=2 topics, got {len(topics)}: {topics}"
        topic_text = " ".join(topics).lower()
        assert "computing" in topic_text or "intelligent" in topic_text

    def test_eeeu25(self, samples_dir):
        """EEEU25: accordion widget with Track titles."""
        html = self._read_html(samples_dir, "Home - EEEU25.html")
        topics = extract_topics(html)
        assert len(topics) >= 3, f"Expected >=3 topics, got {len(topics)}: {topics}"
        topic_text = " ".join(topics).lower()
        assert "entrepreneurship" in topic_text or "track" in topic_text


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
