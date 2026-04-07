import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from unittest.mock import patch
from browser_use_crawler import _extract_structured_elements

def test_extract_structured_returns_string():
    with patch("browser_use_crawler._run_bu", return_value="Submission: 2026-05-01"):
        result = _extract_structured_elements()
    assert isinstance(result, str)
    assert "Submission" in result

def test_extract_structured_returns_empty_on_failure():
    with patch("browser_use_crawler._run_bu", return_value=""):
        result = _extract_structured_elements()
    assert result == ""
