"""
browser_use_crawler.py
======================
Fallback level-3: Browser-use powered navigation for problematic conference sites.

Uses the browser-use CLI to:
  1. Open the URL in a headless browser (full JS rendering)
  2. Detect sub-pages for "Important Dates", "Call for Papers", "Key Dates"
  3. Navigate to those sub-pages and extract the visible text
  4. Return the merged date-relevant text for LLM or regex processing

This module is optional — if browser-use is not installed the entire module
degrades gracefully and returns None without raising.

CHANGES:
  FIX-BU1 — Initial implementation of browser-use fallback crawler.
  FIX-BU2 — navigate_and_extract: tries multiple date-related link texts.
  FIX-BU3 — extract_via_browser_use: integrates with scraper_v2 pipeline.
"""

import subprocess
import logging
import re
import time
from typing import Optional

log = logging.getLogger(__name__)

# ─── Constants ──────────────────────────────────────────────────────

# Anchor text / href patterns that indicate a "Important Dates" sub-page
_DATE_PAGE_KEYWORDS = re.compile(
    r"important[.\-_\s]?dates?|key[.\-_\s]?dates?|deadlines?"
    r"|call[.\-_\s]?for[.\-_\s]?papers?|submission|cfp"
    r"|registration|notification|fechas|plazos|convocatoria",
    re.IGNORECASE,
)

# How long to wait (seconds) after navigation commands
_WAIT_AFTER_OPEN  = 3
_WAIT_AFTER_CLICK = 2

# Maximum characters to return
_MAX_BROWSER_TEXT = 4000


# ─── browser-use CLI wrapper ─────────────────────────────────────────

def _run_bu(args: list[str], timeout: int = 15) -> str:
    """Run a `browser-use <args>` command and return stdout as text."""
    cmd = ["browser-use"] + args
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        log.warning("  [BrowserUse] Command timed out: %s", " ".join(cmd))
        return ""
    except FileNotFoundError:
        log.warning("  [BrowserUse] browser-use CLI not found — skipping.")
        return ""
    except Exception as exc:
        log.warning("  [BrowserUse] Unexpected error: %s", exc)
        return ""


def _is_available() -> bool:
    """Check whether the browser-use CLI is installed."""
    out = _run_bu(["doctor"], timeout=8)
    return bool(out)  # any output means it responded


# ─── Core navigation logic ───────────────────────────────────────────

def _open_url(url: str) -> bool:
    """Open a URL in the browser-use headless session."""
    out = _run_bu(["open", url], timeout=20)
    time.sleep(_WAIT_AFTER_OPEN)
    return "error" not in out.lower()


def _get_page_html() -> str:
    """Get the rendered HTML of the current browser page."""
    return _run_bu(["get", "html"], timeout=15)


def _get_state() -> str:
    """Get the list of clickable elements with indices."""
    return _run_bu(["state"], timeout=10)


def _find_date_link_index(state_output: str) -> Optional[int]:
    """
    Parse browser-use `state` output and find the index of a link that
    matches date-related keywords.

    browser-use state output looks like:
        [0] <a> Home
        [1] <a> Submission
        [2] <a> Important Dates
        ...
    """
    for line in state_output.splitlines():
        m = re.match(r"\[(\d+)\].*", line)
        if not m:
            continue
        idx = int(m.group(1))
        if _DATE_PAGE_KEYWORDS.search(line):
            log.info("  [BrowserUse] Found date link at index %d: %s", idx, line.strip())
            return idx
    return None


def _click_index(idx: int) -> None:
    """Click on a browser-use element by its index."""
    _run_bu(["click", str(idx)], timeout=10)
    time.sleep(_WAIT_AFTER_CLICK)


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


def _probe_common_paths(base_url: str) -> Optional[str]:
    """
    BU-R1: Try appending common date-related paths to the base URL.
    Returns the first path's HTML that contains date-like text, or None.
    """
    from urllib.parse import urlparse
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


def _close_session() -> None:
    """Close the browser-use session cleanly."""
    _run_bu(["close"], timeout=8)


# ─── Public API ──────────────────────────────────────────────────────

def navigate_and_extract(url: str) -> Optional[str]:
    """
    FIX-BU2: Open *url* in a headless browser, detect and click through
    to the "Important Dates" / "Call for Papers" sub-page, extract
    the rendered text, and return it for downstream regex/LLM processing.

    Returns
    -------
    str  — date-relevant text extracted from the page/sub-page, or
    None — if browser-use is unavailable or no content was found.
    """
    from text_extractor import extract_date_text  # lazy import to avoid circular dep

    if not _is_available():
        log.info("  [BrowserUse] CLI not available — skipping browser fallback.")
        return None

    log.info("  [BrowserUse] Starting browser navigation for: %s", url)

    try:
        # 1. Open URL
        success = _open_url(url)
        if not success:
            log.warning("  [BrowserUse] Could not open %s", url)
            return None

        # 2. Get interactive elements
        state = _get_state()
        date_idx = _find_date_link_index(state)

        if date_idx is not None:
            # 3. Navigate to the date sub-page
            _click_index(date_idx)
            log.info("  [BrowserUse] Clicked date link index %d — collecting text.", date_idx)
        else:
            # BU-R1: try common URL paths before falling back to home page
            log.info("  [BrowserUse] No date link found — probing common paths (BU-R1).")
            probe_html = _probe_common_paths(url)
            if probe_html:
                structured = _extract_structured_elements()
                if structured.strip():
                    probe_html += "\n--- STRUCTURED ---\n" + structured
                date_text = extract_date_text(probe_html)
                if date_text.strip():
                    log.info("  [BrowserUse] BU-R1 extracted %d chars.", len(date_text))
                    return date_text[:_MAX_BROWSER_TEXT]
            log.info("  [BrowserUse] No path hit — extracting home page.")

        # 4. Collect rendered HTML (with scrolling)
        rendered_html = _scroll_and_collect()

        # BU-R3: also extract structured elements (tables, definition lists, lists)
        structured = _extract_structured_elements()
        if structured.strip():
            rendered_html = rendered_html + "\n--- STRUCTURED ---\n" + structured

        if not rendered_html.strip():
            log.warning("  [BrowserUse] Empty HTML from browser — nothing to extract.")
            return None

        # 5. Smart-extract date-relevant text
        date_text = extract_date_text(rendered_html)
        if not date_text.strip():
            log.warning("  [BrowserUse] No date text extracted from browser HTML.")
            return None

        log.info(
            "  [BrowserUse] Successfully extracted %d chars from %s",
            len(date_text), url,
        )
        return date_text[:_MAX_BROWSER_TEXT]

    except Exception as exc:
        log.error("  [BrowserUse] Unexpected error for %s: %s", url, exc)
        return None
    finally:
        _close_session()


def extract_via_browser_use(
    url: str,
    existing_dates: dict,
) -> tuple[Optional[str], bool]:
    """
    FIX-BU3: High-level integration point for scraper_v2 pipeline.

    Only triggers browser-use if meaningful fields are still missing after
    regex + LLM extraction.

    Parameters
    ----------
    url           : the conference URL
    existing_dates: dict of date fields extracted so far (may have Nones)

    Returns
    -------
    text      : new date text from browser (or None)
    triggered : True if the browser was actually used
    """
    from config import DATE_KEYS

    missing = [k for k in DATE_KEYS if existing_dates.get(k) is None]
    if not missing:
        log.info("  [BrowserUse] All fields found — skipping browser fallback.")
        return None, False

    log.info(
        "  [BrowserUse] %d field(s) still missing (%s) — attempting browser fallback.",
        len(missing), missing,
    )

    text = navigate_and_extract(url)
    return text, text is not None
