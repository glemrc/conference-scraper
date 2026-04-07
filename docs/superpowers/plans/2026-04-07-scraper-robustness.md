# Scraper Robustness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Strengthen the browser-use fallback, extract conference names from static HTML, and add a `Fields Found` column to the Excel output so the pipeline extracts more dates and makes gaps easy to spot manually.

**Architecture:** Four focused changes across four files — `text_extractor.py` gets a new `extract_conference_name()` function; `browser_use_crawler.py` gets four robustness improvements (deeper navigation, more scrolls, structured element extraction, always-LLM flag); `config.py` gets two new column keys; `scraper_v2.py` wires everything together and updates the Excel writer.

**Tech Stack:** Python 3.11+, BeautifulSoup4, pandas, openpyxl, browser-use CLI, Groq API

---

## File Map

| File | Change type | What changes |
|------|-------------|--------------|
| `text_extractor.py` | Modify | Add `extract_conference_name(html)` |
| `browser_use_crawler.py` | Modify | BU-R1 path probing, BU-R2 more scrolls, BU-R3 table extraction, BU-R4 `needs_llm` flag |
| `config.py` | Modify | Add `conference_name` and `fields_found` to `COLUMN_LABELS` |
| `scraper_v2.py` | Modify | Call `extract_conference_name()`, compute `fields_found`, update `col_order` in `write_excel_report()` |
| `tests/test_text_extractor.py` | Create | Tests for `extract_conference_name()` |
| `tests/test_browser_use_crawler.py` | Create | Tests for BU-R1 path probing and BU-R3 table extraction helpers |

---

## Task 1: `extract_conference_name()` in `text_extractor.py`

**Files:**
- Modify: `text_extractor.py` (add function at end of file, after `detect_pdf_links`)
- Create: `tests/test_text_extractor.py`

- [ ] **Step 1: Create test file and write failing tests**

Create `tests/__init__.py` (empty) and `tests/test_text_extractor.py`:

```python
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
    # Pattern requires uppercase acronym, lowercase title should not match
    html = "<html><head><title>some conference 2026</title></head><body></body></html>"
    assert extract_conference_name(html) is None

def test_extracts_from_h2_when_h1_empty():
    html = "<html><head><title>Home</title></head><body><h1></h1><h2>LACCEI 2026</h2></body></html>"
    assert extract_conference_name(html) == "LACCEI 2026"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd "c:/Users/acer/Documents/BD_VDI_PROCEEDINGS/scraper bdi reporte automataico/Conference Scraper"
python -m pytest tests/test_text_extractor.py -v
```

Expected: `ImportError` or `AttributeError` — `extract_conference_name` does not exist yet.

- [ ] **Step 3: Implement `extract_conference_name()` in `text_extractor.py`**

Add at the end of `text_extractor.py`, after the last existing function:

```python
# ─── Conference name extraction ─────────────────────────────────────

_CONF_NAME_RE = re.compile(r"\b([A-Z]{2,8}\s+\d{4})\b")


def extract_conference_name(html: str) -> str | None:
    """
    Extract conference name in 'ACRONYM YEAR' format (e.g. 'ICOAMP 2026')
    from the page <title>, <h1>, and <h2> tags.

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

    for candidate in candidates:
        m = _CONF_NAME_RE.search(candidate)
        if m:
            return m.group(1)

    return None
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_text_extractor.py -v
```

Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add text_extractor.py tests/__init__.py tests/test_text_extractor.py
git commit -m "feat: add extract_conference_name() to text_extractor"
```

---

## Task 2: BU-R2 — More scroll iterations in `browser_use_crawler.py`

**Files:**
- Modify: `browser_use_crawler.py:124-140` (`_scroll_and_collect`)

- [ ] **Step 1: Update `_scroll_and_collect()` to scroll 5 times**

Replace the existing `_scroll_and_collect` function (lines ~124–140):

```python
def _scroll_and_collect() -> str:
    """Scroll down five times and accumulate page HTML. (BU-R2)"""
    parts = [_get_page_html()]
    for _ in range(5):
        _run_bu(["scroll", "down"], timeout=5)
        time.sleep(1)
        parts.append(_get_page_html())
    # Deduplicate chunks
    seen: set[str] = set()
    unique: list[str] = []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            unique.append(p)
    return "\n".join(unique)
```

- [ ] **Step 2: Run existing test_imports to verify no breakage**

```bash
python test_imports.py
```

Expected: no errors (or same output as before).

- [ ] **Step 3: Commit**

```bash
git add browser_use_crawler.py
git commit -m "feat(browser-use): BU-R2 increase scroll iterations to 5"
```

---

## Task 3: BU-R3 — Structured element extraction in `browser_use_crawler.py`

**Files:**
- Modify: `browser_use_crawler.py` (add `_extract_structured_elements()`, call it in `navigate_and_extract`)
- Modify: `tests/test_browser_use_crawler.py` (create)

- [ ] **Step 1: Write failing test**

Create `tests/test_browser_use_crawler.py`:

```python
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from unittest.mock import patch, MagicMock
from browser_use_crawler import _extract_structured_elements

def test_extract_structured_returns_string():
    # When _run_bu returns some text, the function returns it
    with patch("browser_use_crawler._run_bu", return_value="Submission: 2026-05-01"):
        result = _extract_structured_elements()
    assert isinstance(result, str)
    assert "Submission" in result

def test_extract_structured_returns_empty_on_failure():
    with patch("browser_use_crawler._run_bu", return_value=""):
        result = _extract_structured_elements()
    assert result == ""
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_browser_use_crawler.py -v
```

Expected: `ImportError` — `_extract_structured_elements` does not exist.

- [ ] **Step 3: Implement `_extract_structured_elements()` and call it in `navigate_and_extract()`**

Add this function to `browser_use_crawler.py` after `_scroll_and_collect`:

```python
def _extract_structured_elements() -> str:
    """
    BU-R3: Extract text from <table>, <dl>, and <ul> elements via JS eval.
    These elements often contain structured date lists in conference pages.
    Returns concatenated text, or empty string on failure.
    """
    js = (
        "Array.from(document.querySelectorAll('table, dl, ul'))"
        ".map(el => el.innerText).join('\\n---\\n')"
    )
    return _run_bu(["eval", js], timeout=10)
```

Then in `navigate_and_extract()`, after `rendered_html = _scroll_and_collect()`, add:

```python
        # BU-R3: also extract structured elements (tables, definition lists, lists)
        structured = _extract_structured_elements()
        if structured.strip():
            rendered_html = rendered_html + "\n--- STRUCTURED ---\n" + structured
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_browser_use_crawler.py -v
```

Expected: both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add browser_use_crawler.py tests/test_browser_use_crawler.py
git commit -m "feat(browser-use): BU-R3 extract structured table/dl/ul elements"
```

---

## Task 4: BU-R1 — Deeper navigation via common URL paths in `browser_use_crawler.py`

**Files:**
- Modify: `browser_use_crawler.py` (add `_probe_common_paths()`, integrate into `navigate_and_extract`)

- [ ] **Step 1: Add failing test to `tests/test_browser_use_crawler.py`**

Append to `tests/test_browser_use_crawler.py`:

```python
from browser_use_crawler import _probe_common_paths
from urllib.parse import urlparse

def test_probe_returns_text_on_first_hit():
    # Simulate: first path has no date text, second path returns date text
    call_count = {"n": 0}
    def fake_run_bu(args, timeout=15):
        if args[0] == "open":
            return "ok"
        if args == ["get", "html"]:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return ""  # first path: empty
            return "<p>Submission Deadline: 2026-05-01</p>"  # second path: hit
        return ""
    with patch("browser_use_crawler._run_bu", side_effect=fake_run_bu):
        result = _probe_common_paths("https://example.com")
    assert result is not None
    assert "Submission" in result

def test_probe_returns_none_when_all_paths_empty():
    with patch("browser_use_crawler._run_bu", return_value=""):
        result = _probe_common_paths("https://example.com")
    assert result is None
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_browser_use_crawler.py::test_probe_returns_text_on_first_hit tests/test_browser_use_crawler.py::test_probe_returns_none_when_all_paths_empty -v
```

Expected: `ImportError` — `_probe_common_paths` does not exist.

- [ ] **Step 3: Implement `_probe_common_paths()` and integrate into `navigate_and_extract()`**

Add after `_extract_structured_elements` in `browser_use_crawler.py`:

```python
_COMMON_DATE_PATHS = [
    "/dates",
    "/important-dates",
    "/key-dates",
    "/cfp",
    "/call-for-papers",
    "/submission",
    "/deadlines",
    "/registration",
]


def _probe_common_paths(base_url: str) -> str | None:
    """
    BU-R1: Try appending common date-related paths to the base URL.
    Returns the first path's HTML that contains date-like text, or None.
    """
    from urllib.parse import urlparse, urljoin
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"

    for path in _COMMON_DATE_PATHS:
        candidate = root + path
        log.info("  [BrowserUse] BU-R1 probing: %s", candidate)
        out = _run_bu(["open", candidate], timeout=15)
        time.sleep(_WAIT_AFTER_OPEN)
        if "error" in out.lower():
            continue
        html = _get_page_html()
        if html.strip() and _DATE_PAGE_KEYWORDS.search(html):
            log.info("  [BrowserUse] BU-R1 found date content at: %s", candidate)
            return html
    return None
```

Then in `navigate_and_extract()`, after the block that checks `date_idx is not None`, add the BU-R1 probe when no link was found:

```python
        if date_idx is not None:
            # Navigate to the date sub-page
            _click_index(date_idx)
            log.info("  [BrowserUse] Clicked date link index %d — collecting text.", date_idx)
        else:
            # BU-R1: try common URL paths before falling back to home page
            log.info("  [BrowserUse] No date link found — probing common paths (BU-R1).")
            probe_html = _probe_common_paths(url)
            if probe_html:
                rendered_html = probe_html
                structured = _extract_structured_elements()
                if structured.strip():
                    rendered_html += "\n--- STRUCTURED ---\n" + structured
                date_text = extract_date_text(rendered_html)
                if date_text.strip():
                    log.info("  [BrowserUse] BU-R1 extracted %d chars.", len(date_text))
                    return date_text[:_MAX_BROWSER_TEXT]
            log.info("  [BrowserUse] No path hit — extracting home page.")
```

Note: this early return skips the scroll+collect block below when a probe hits. Make sure the `finally: _close_session()` still runs (it will, since it's in the `finally` block).

- [ ] **Step 4: Run all browser-use tests**

```bash
python -m pytest tests/test_browser_use_crawler.py -v
```

Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add browser_use_crawler.py tests/test_browser_use_crawler.py
git commit -m "feat(browser-use): BU-R1 probe common date URL paths as fallback"
```

---

## Task 5: `config.py` — Add new column labels

**Files:**
- Modify: `config.py`

- [ ] **Step 1: Add two new entries to `COLUMN_LABELS`**

In `config.py`, update `COLUMN_LABELS` to:

```python
COLUMN_LABELS = {
    "conference_name": "Conference Name",
    "url": "URL",
    "fecha_inicio": "Start Date",
    "fecha_fin": "End Date",
    "envio_trabajo": "Submission Deadline",
    "notificacion_aceptacion": "Acceptance Notification",
    "inscripcion": "Registration Deadline",
    "fields_found": "Fields Found",
    "temas": "Topics",
    "extraction_method": "Method",
    "notas": "Notes",
}
```

- [ ] **Step 2: Verify import works**

```bash
python -c "from config import COLUMN_LABELS; print(COLUMN_LABELS)"
```

Expected: dict printed with `Conference Name` and `Fields Found` present.

- [ ] **Step 3: Commit**

```bash
git add config.py
git commit -m "feat(config): add Conference Name and Fields Found column labels"
```

---

## Task 6: Wire `conference_name` and `fields_found` into `scraper_v2.py`

**Files:**
- Modify: `scraper_v2.py` — `procesar_url()` and `write_excel_report()`

- [ ] **Step 1: Import `extract_conference_name` at the top of `scraper_v2.py`**

Find the existing import line:

```python
from text_extractor import extract_date_text, extract_full_text
```

Replace with:

```python
from text_extractor import extract_date_text, extract_full_text, extract_conference_name
```

- [ ] **Step 2: Initialize `conference_name` and extract it in `procesar_url()`**

At the top of `procesar_url()`, right after this line:
```python
    empty = {k: None for k in DATE_KEYS}
    empty["temas"] = []
```

Add:
```python
    conference_name: str | None = None  # populated after download
```

Then, after `html, used_js = descargar_html(url)` and the `if not html:` early return, add:

```python
    # Extract conference name from static HTML (title / h1 / h2)
    conference_name = extract_conference_name(html)
```

This ensures `conference_name` is always defined (initialized to `None`) before any early return, and populated from HTML when available.

- [ ] **Step 3: Update all return points in `procesar_url()` to include new fields**

**Main return** (currently near line 918):
```python
    dates["notas"] = " | ".join(notes_parts)
    return {"url": url, **dates}, changes, method
```
Replace with:
```python
    dates["notas"] = " | ".join(notes_parts)
    filled = sum(1 for k in DATE_KEYS if dates.get(k) is not None)
    record = {
        "conference_name": conference_name,
        "url": url,
        **dates,
        "fields_found": f"{filled}/5",
    }
    return record, changes, method
```

**Error early returns** (html is None, or empty text):
```python
return {"url": url, **empty, "notas": ""}, [], "error"
```
Replace each with:
```python
return {"conference_name": conference_name, "url": url, **empty, "notas": "", "fields_found": "0/5"}, [], "error"
```

**Image-content early return**:
```python
return {"url": url, **empty, "temas": [], "notas": notas}, [], "image-content"
```
Replace with:
```python
return {"conference_name": conference_name, "url": url, **empty, "temas": [], "notas": notas, "fields_found": "0/5"}, [], "image-content"
```

**Cache hit return**:
```python
return {"url": url, **cached}, [], "cache"
```
Replace with:
```python
filled_cache = sum(1 for k in DATE_KEYS if cached.get(k) is not None)
return {"conference_name": conference_name, "url": url, **cached, "fields_found": f"{filled_cache}/5"}, [], "cache"
```

- [ ] **Step 3: Update `write_excel_report()` column order**

In `write_excel_report()`, find:

```python
    col_order = ["url"] + DATE_KEYS + ["temas", "extraction_method", "notas"]
```

Replace with:

```python
    col_order = [
        "conference_name",
        "url",
        "fecha_inicio",
        "fecha_fin",
        "envio_trabajo",
        "notificacion_aceptacion",
        "inscripcion",
        "fields_found",
        "temas",
        "extraction_method",
        "notas",
    ]
```

- [ ] **Step 4: Smoke test — run pipeline on 2 URLs**

```bash
cd "c:/Users/acer/Documents/BD_VDI_PROCEEDINGS/scraper bdi reporte automataico/Conference Scraper"
python -c "
from scraper_v2 import procesar_url
from cache_manager import CacheManager
from change_detector import load_db_dates

cache = CacheManager()
db = load_db_dates()
record, changes, method = procesar_url('https://icoamp.com/index.htm', cache, db)
print('conference_name:', record.get('conference_name'))
print('fields_found:', record.get('fields_found'))
print('method:', method)
"
```

Expected: `conference_name` is a string like `"ICOAMP 2026"` or `None`, `fields_found` is `"N/5"`.

- [ ] **Step 5: Commit**

```bash
git add scraper_v2.py
git commit -m "feat: add conference_name and fields_found to pipeline and Excel output"
```

---

## Task 7: Full run and verification

- [ ] **Step 1: Run all tests**

```bash
python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 2: Run full pipeline**

```bash
python scraper_v2.py
```

Expected: runs without errors, produces `fechas_conferencias_v3.xlsx`.

- [ ] **Step 3: Verify Excel output**

```bash
python -c "
import pandas as pd
df = pd.read_excel('fechas_conferencias_v3.xlsx', sheet_name='Extracted Data')
print('Columns:', list(df.columns))
print('Conference Name non-null:', df['Conference Name'].notna().sum(), '/', len(df))
print('Fields Found distribution:')
print(df['Fields Found'].value_counts().sort_index())
"
```

Expected:
- `Conference Name` column present and populated for most rows.
- `Fields Found` column present with values like `"3/5"`, `"5/5"`, etc.
- More `"5/5"` rows than the previous run.

- [ ] **Step 4: Final commit**

```bash
git add -u
git commit -m "chore: verified full pipeline run with conference_name and fields_found"
```
