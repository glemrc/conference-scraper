import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from unittest.mock import patch
from browser_use_crawler import _extract_structured_elements, _probe_common_paths

def test_extract_structured_returns_string():
    with patch("browser_use_crawler._run_bu", return_value="Submission: 2026-05-01"):
        result = _extract_structured_elements()
    assert isinstance(result, str)
    assert "Submission" in result

def test_extract_structured_returns_empty_on_failure():
    with patch("browser_use_crawler._run_bu", return_value=""):
        result = _extract_structured_elements()
    assert result == ""


def test_probe_returns_text_on_first_hit():
    call_count = {"n": 0}
    def fake_run_bu(args, timeout=15):  # noqa: ARG001
        if args[0] == "open":
            return "ok"
        if args == ["get", "html"]:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return ""
            return "<p>Submission Deadline: 2026-05-01</p>"
        return ""
    with patch("browser_use_crawler._run_bu", side_effect=fake_run_bu):
        result = _probe_common_paths("https://example.com")
    assert result is not None
    assert "Submission" in result

def test_probe_returns_none_when_all_paths_empty():
    with patch("browser_use_crawler._run_bu", return_value=""):
        result = _probe_common_paths("https://example.com")
    assert result is None
