# CHANGES:
# P2 — Added _KNOWN_JS_DOMAINS set (gjem.press, worldcist.org) and url parameter to is_js_rendered_page().
#       Domain check fires immediately for known JS sites, bypassing heuristic.

"""
js_renderer.py
==============
F1 fix: Optional JS-rendering fallback using Playwright.

Strategy: only invoked when static HTML yields no date-relevant content.
This keeps Playwright out of the hot path — the browser is only launched
for pages that actually need it.

Install once:
    pip install playwright
    playwright install chromium --with-deps
"""

import logging
from urllib.parse import urlparse
from config import REQUEST_TIMEOUT

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known JS-rendered domains — bypass heuristic, always render with JS
# ---------------------------------------------------------------------------
_KNOWN_JS_DOMAINS = {
    "gjem.press",      # eeeu25.gjem.press, seeu2026.gjem.press — SPA sites
    "worldcist.org",   # worldcist.org — JS-rendered SPA
}

# ---------------------------------------------------------------------------
# Detection: is this HTML "thin" enough to warrant a JS retry?
# ---------------------------------------------------------------------------

# Minimum number of visible text characters that indicate real content was
# loaded.  Empirically: a JS shell typically has < 500 chars of body text;
# a real conference page has 2 000+.
_THIN_HTML_THRESHOLD = 400


def is_js_rendered_page(html: str, date_text: str, url: str = "") -> bool:
    """
    Heuristic: returns True if the page is almost certainly JS-rendered and
    the static download missed the real content.

    Checks:
      0. (P2 new) The URL's domain matches a known JS-rendered domain.
      1. The smart-extracted date text is shorter than the threshold.
      2. The raw HTML contains a JS framework fingerprint.
    Either condition alone is treated as a likely JS page.
    """
    # P2: check known JS domains first
    if url:
        try:
            domain = urlparse(url).netloc.lower()
            # Match exact domain or parent domain (e.g. eeeu25.gjem.press → gjem.press)
            for known in _KNOWN_JS_DOMAINS:
                if domain == known or domain.endswith("." + known):
                    log.info("  [JSRenderer] Known JS domain: %s", domain)
                    return True
        except Exception:
            pass

    if len(date_text.strip()) < _THIN_HTML_THRESHOLD:
        return True

    js_fingerprints = (
        'id="root"', 'id="app"', 'id="__next"',
        'data-reactroot', 'data-v-', 'ng-version',
        '__NEXT_DATA__', 'window.__NUXT__', 'window.__vue__',
    )
    html_lower = html[:20_000]   # only scan the first 20 KB
    if any(fp in html_lower for fp in js_fingerprints):
        return True

    return False


# ---------------------------------------------------------------------------
# Playwright renderer
# ---------------------------------------------------------------------------

def render_with_js(url: str) -> str | None:
    """
    Load *url* with a headless Chromium browser and return the rendered HTML.

    Returns None if:
      - playwright is not installed
      - the page fails to load
      - any other error occurs

    Behaviour:
      - wait_until="networkidle": waits for all network requests to settle,
        which covers most SPA hydration patterns.
      - Extra 1.5 s wait: catches pages that fire XHR after networkidle.
      - JS dialogs (alerts) are auto-dismissed so they don't block navigation.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        log.warning(
            "[JSRenderer] playwright not installed. "
            "Run: pip install playwright && playwright install chromium --with-deps"
        )
        return None

    timeout_ms = REQUEST_TIMEOUT * 1000

    log.info("  [JSRenderer] Launching headless browser for %s", url)
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="en-US",
            )
            page = ctx.new_page()

            # Dismiss JS dialogs automatically
            page.on("dialog", lambda d: d.dismiss())

            try:
                page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            except PWTimeout:
                # networkidle can time out on pages with background polling;
                # fall back to domcontentloaded which is almost always fine.
                log.warning("  [JSRenderer] networkidle timeout — retrying with domcontentloaded")
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            # Extra wait for late-firing XHR / dynamic injection
            page.wait_for_timeout(1500)

            html = page.content()
            browser.close()

            log.info("  [JSRenderer] Got %d chars of rendered HTML", len(html))
            return html

    except Exception as exc:
        log.error("  [JSRenderer] Failed for %s: %s", url, exc)
        return None
