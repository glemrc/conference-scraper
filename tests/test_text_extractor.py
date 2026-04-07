import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from text_extractor import extract_conference_name

def test_extracts_from_title():
    html = "<html><head><title>ICOAMP 2026 - International Conference</title></head><body></body></html>"
    assert extract_conference_name(html) == "ICOAMP 2026"

def test_extracts_from_h1_when_title_has_none():
    html = "<html><head><title>Welcome</title></head><body><h1>SEEU 2026 Conference</h1></body></html>"
    assert extract_conference_name(html) == "SEEU 2026"

def test_returns_none_when_no_match():
    html = "<html><head><title>Conference Homepage</title></head><body></body></html>"
    assert extract_conference_name(html) is None

def test_prefers_title_over_h1():
    html = "<html><head><title>ICOAMP 2026</title></head><body><h1>SEEU 2026</h1></body></html>"
    assert extract_conference_name(html) == "ICOAMP 2026"

def test_handles_lowercase_title():
    html = "<html><head><title>some conference 2026</title></head><body></body></html>"
    assert extract_conference_name(html) is None

def test_extracts_from_h2_when_h1_empty():
    html = "<html><head><title>Home</title></head><body><h1></h1><h2>LACCEI 2026</h2></body></html>"
    assert extract_conference_name(html) == "LACCEI 2026"
