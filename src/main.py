# CHANGES:
# P1 — Removed dead URL "https://icacit.org.pe/symposium/2025/" from URLS_FIJAS.
# P2 — Pass url to is_js_rendered_page() in descargar_html() so known-domain check fires.
# P3 — Added _detect_hallucination() post-LLM guard: rejects impossible date spans and suspiciously uniform spacing.
# P6 — _static_download() now retries with verify=False on SSL/timeout errors.
# P8 — Crawl trigger now requires date_hits < 3 AND len(date_text) < 1500, not based on which fields regex found.
# P9 — Detects image-content pages (HTTP 200, text > 200 chars, 0 date patterns) and labels as method="image-content".
# FIX-BU — browser_use_crawler integrated as level-3 fallback after regex+LLM.
# FIX-C2 — Crawl trigger replaced with field_aware_crawl_needed() from shallow_crawler.
# FIX-P  — PDF links detected via text_extractor.detect_pdf_links(); included in Notes for manual review.
# FIX-YR — validate_temporal_logic relaxed: future years (>= current) are accepted, not just current-1.

"""
scraper_v2.py
=============
Conference Deadline Monitoring System — v2 (modular, optimized).

Pipeline per URL:
  1. Download HTML (static)
  2. Smart-extract date-relevant text  (text_extractor)
  3. [F1] If text is thin/empty AND page looks JS-rendered → retry with
     headless browser (js_renderer)
  4. Hash and compare with cache        (cache_manager)
     → unchanged AND valid cache? return cached dates, skip everything else
  5. Try regex extraction               (regex_extractor)
     → high confidence? use regex results, skip LLM
  6. LLM fallback (Groq API)            only for missing fields
  7. Compare with existing database     (change_detector)
  8. Update cache

Changes vs original:
  F1 — JS rendering: js_renderer imported; descargar_html_completo() added
       as a two-phase downloader (static → JS fallback when needed).
  F3 — Cache gate: split into has_changed() AND has_valid_cache(); invalid
       entries no longer short-circuit extraction.
  F5 — max_tokens raised to 1024; _parse_llm_json() now repairs incomplete
       JSON before raising JSONDecodeError.
  F6 — cargar_urls() deduplicates while preserving order.

Output: Excel workbook with two sheets
  Sheet 1 — "Extracted Data"   : full table of all conferences
  Sheet 2 — "Detected Changes" : changes vs. the previous database

Usage:
    python scraper_v2.py                     # uses URLs from DB or fixed list
    python scraper_v2.py urls.csv            # reads URLs from CSV
    python scraper_v2.py urls.xlsx           # reads URLs from Excel

Requires:
    pip install requests beautifulsoup4 pandas openpyxl python-dateutil groq python-dotenv
Optional (for JS-rendered pages):
    pip install playwright && playwright install chromium --with-deps
"""

import json
import os
import re
import ssl
import sys
import time
from datetime import date as date_type, datetime, timedelta
import logging
from pathlib import Path

import requests
import pandas as pd
from groq import Groq
from dotenv import load_dotenv

# Local modules
from config import (
    GROQ_API_KEY, GROQ_MODEL,
    OUTPUT_FILE, DB_FILE,
    REQUEST_TIMEOUT, DELAY_BETWEEN_REQUESTS,
    MAX_TEXT_CHARS, MAX_SMART_TEXT_CHARS,
    REGEX_CONFIDENCE_HIGH, REGEX_CONFIDENCE_PARTIAL,
    DATE_KEYS, COLUMN_LABELS, HTTP_HEADERS,
    MIN_DATE_PATTERNS_FOR_LLM,
)
from utils.cache_manager import CacheManager
from extractors.text_extractor import extract_date_text, extract_full_text, extract_conference_name
from extractors.regex_extractor import extract_with_regex
from extractors.topic_extractor import extract_topics
from utils.change_detector import (
    detect_changes, load_db_dates, ChangeReport,
)
from crawlers.shallow_crawler import find_date_links, fetch_supplementary_text, field_aware_crawl_needed, fetch_subpage_topics

# FIX-BU: browser_use_crawler is optional — degrades gracefully if not installed
try:
    from crawlers.browser_use_crawler import extract_via_browser_use
    _BROWSER_USE_AVAILABLE = True
except ImportError:
    _BROWSER_USE_AVAILABLE = False

# F1: JS renderer (optional — gracefully absent if playwright not installed)
try:
    from crawlers.js_renderer import is_js_rendered_page, render_with_js
    _JS_RENDERER_AVAILABLE = True
except ImportError:
    _JS_RENDERER_AVAILABLE = False

# ─────────────────────────────────────────────
# Load .env
# ─────────────────────────────────────────────
load_dotenv()
_GROQ_KEY = os.getenv("GROQ_API_KEY", "") or GROQ_API_KEY

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Fixed URL list (fallback)
# P1: Removed dead URL https://icacit.org.pe/symposium/2025/
# ─────────────────────────────────────────────

URLS_FIJAS = [
    "https://comesyso.openpublish.eu/article/download",
    "https://eeeu25.gjem.press",
    "https://scrs.in/conference/csct2025",
    "https://worldcist.org",
    "https://seeu2026.gjem.press",
    "https://acdsa.org/2026/deadlines",
    "https://scrs.in/conference/cml2026",
    "https://scrs.in/conference/icitai2026",
    "https://scrs.in/conference/cvr2026",
    "https://scrs.in/conference/bida2026",
    "https://laccei.org/laccei2026/call-for-papers/",
    "https://congresotaee.es/en/en-home/",
    "https://csoc.openpublish.eu",
    "https://icoamp.com/index.htm",
    "https://www.gkciet.ac.in/peis2026",
    "https://pacis2026.aisconferences.org",
    "https://theioes.org/air2026/index.php",
    "https://stai2026.estindiafoundation.org/",
    "https://scrs.in/conference/icivc2026",
    "https://icepr.org",
    "https://scrs.in/conference/CIMA2026",
    "https://scrs.in/conference/icdsa2026",
    "https://www.icet.org",
    "https://scrs.in/conference/aic2026",
    "https://www.scrs.in/conference/ceee2026",
    "https://icacit.org.pe/symposium/",
    "https://icdici.com/2026/",
    "https://theioes.org/air2026",
    "https://scrs.in/conference/pccda2026",
    "https://theioes.org/conference/ijcaci2026",
    "https://theioes.org/aita2026/",
    "https://scrs.in/conference/cis2026",
    "https://www.scrs.in/conference/iccis2026",
    "https://scrs.in/conference/adcis2026",
    "https://scrs.in/conference/icsiscet2026",
    "https://scrs.in/conference/iti2026",
    "https://scrs.in/conference/scis2026"
]
# ═════════════════════════════════════════════
#  URL LOADING
# ═════════════════════════════════════════════

def _dedup_urls(urls: list[str]) -> list[str]:
    """
    F6 fix: remove duplicate URLs while preserving order.
    Normalises trailing slashes before comparison so that
    "https://example.com" and "https://example.com/" are treated as one.
    """
    seen: set[str] = set()
    result: list[str] = []
    for url in urls:
        key = url.rstrip("/").lower()
        if key not in seen:
            seen.add(key)
            result.append(url)
    removed = len(urls) - len(result)
    if removed:
        log.info("Removed %d duplicate URL(s).", removed)
    return result


def cargar_urls(fuente: str | None = None) -> list[str]:
    """Load URLs from CSV/Excel file, the database, or the fixed list."""
    if fuente is not None:
        path = Path(fuente)
        if path.exists():
            try:
                df = (pd.read_csv(path) if path.suffix.lower() == ".csv"
                      else pd.read_excel(path))
                col = next(
                    (c for c in df.columns if c.strip().lower() == "url"),
                    df.columns[0],
                )
                urls = df[col].dropna().str.strip().tolist()
                log.info("Loaded %d URLs from '%s'.", len(urls), fuente)
                return _dedup_urls(urls)
            except Exception as exc:
                log.error("Error reading '%s': %s. Falling back.", fuente, exc)

    if DB_FILE.exists():
        try:
            df = pd.read_excel(DB_FILE, engine="openpyxl")
            url_col = next(
                (c for c in df.columns if c.strip().lower() == "url"), None
            )
            if url_col:
                urls = df[url_col].dropna().str.strip().tolist()
                if urls:
                    log.info("Loaded %d URLs from database '%s'.",
                             len(urls), DB_FILE.name)
                    return _dedup_urls(urls)
        except Exception:
            pass

    log.info("Using fixed URL list (%d URLs).", len(URLS_FIJAS))
    return _dedup_urls(list(URLS_FIJAS))


# ═════════════════════════════════════════════
#  HTML DOWNLOAD (static + JS fallback)
# ═════════════════════════════════════════════

def _is_ssl_error(exc: Exception) -> bool:
    """Check if an exception is related to SSL verification failure."""
    err_str = str(exc).lower()
    return any(kw in err_str for kw in (
        "ssl", "certificate", "cert", "verify", "handshake",
    ))


def _static_download(url: str) -> str | None:
    """
    Download HTML via requests (no JS execution).

    P6 fix: on SSL errors or timeouts, retry once with verify=False
    and timeout=20 as a fallback.
    """
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.Timeout:
        log.warning("  Timeout on first attempt: %s — retrying with extended timeout...", url)
    except requests.exceptions.ConnectionError as e:
        if _is_ssl_error(e):
            log.warning("  SSL error: %s — retrying with verify=False...", url)
        else:
            log.error("  Connection error: %s", url)
            return None
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code
        # Retry 403/406 with a relaxed Accept header (some hosts, e.g. scrs.in,
        # reject the default Accept and return 406).
        if status in (403, 406):
            log.warning("  HTTP %d on %s — retrying with relaxed headers...", status, url)
            try:
                relaxed = dict(HTTP_HEADERS)
                relaxed["Accept"] = "*/*"
                relaxed["Referer"] = "https://www.google.com/"
                resp = requests.get(url, headers=relaxed, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                log.info("  HTTP %d retry succeeded for %s", status, url)
                return resp.text
            except Exception as retry_exc:
                log.error("  HTTP %d retry failed for %s: %s", status, url, retry_exc)
                return None
        log.error("  HTTP %s: %s", status, url)
        return None
    except requests.exceptions.RequestException as e:
        log.error("  Network error (%s): %s", type(e).__name__, url)
        return None

    # P6: retry with verify=False and extended timeout (SSL fallback / timeout retry)
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        pass
    try:
        log.warning("  [P6] Retrying %s with verify=False, timeout=20", url)
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=20, verify=False)
        resp.raise_for_status()
        log.info("  [P6] Fallback succeeded for %s", url)
        return resp.text
    except Exception as retry_exc:
        log.error("  [P6] Retry also failed for %s: %s", url, retry_exc)
        return None


def descargar_html(url: str) -> tuple[str | None, bool]:
    """
    F1 fix: two-phase HTML acquisition.

    Phase 1 — static download via requests (fast, no browser overhead).
    Phase 2 — JS rendering via Playwright, triggered only when:
               a) playwright is installed, AND
               b) the static HTML appears to be a JS-rendered shell
                  (detected by is_js_rendered_page()).

    Returns
    -------
    html      : the HTML string, or None on total failure
    used_js   : True if the JS renderer was used
    """
    html = _static_download(url)
    if html is None:
        # Static failed entirely (bot block, 4xx, etc.) — try JS render as last resort
        if _JS_RENDERER_AVAILABLE:
            log.info("  [F1] Static download failed — attempting JS render fallback.")
            js_html = render_with_js(url)
            if js_html:
                return js_html, True
        return None, False

    if not _JS_RENDERER_AVAILABLE:
        return html, False

    # Quick pre-check: extract date text from the static HTML to test quality
    try:
        from extractors.text_extractor import extract_date_text as _edt
        preview_text = _edt(html)
    except Exception:
        preview_text = ""

    # P2: pass url so known JS domains are detected immediately
    if is_js_rendered_page(html, preview_text, url=url):
        log.info("  [F1] JS-rendered page detected — switching to headless browser.")
        js_html = render_with_js(url)
        if js_html:
            return js_html, True
        else:
            log.warning("  [F1] JS rendering failed — using static HTML as fallback.")

    return html, False


# ═════════════════════════════════════════════
#  GROQ LLM
# ═════════════════════════════════════════════

_groq_client: Groq | None = None


def _get_groq_client() -> Groq:
    global _groq_client
    if _groq_client is None:
        key = _GROQ_KEY
        if not key:
            raise ValueError(
                "\n\n❌ GROQ_API_KEY not set.\n"
                "   Set it as an environment variable or in a .env file.\n"
                "   Get a free key at: https://console.groq.com\n"
            )
        _groq_client = Groq(api_key=key)
    return _groq_client


_FULL_PROMPT = """\
Extract conference dates from the text below. Output ONLY valid JSON.
RULES:
- All dates MUST be in YYYY-MM-DD format; use null if not found.
- Logical order: envio_trabajo < notificacion_aceptacion < inscripcion <= fecha_inicio <= fecha_fin
- If multiple dates exist for the same field (original + extended/revised), use the MOST RECENT one.
- IGNORE dates marked as strikethrough, "old deadline", "original deadline", or "extended from".
- Only include dates from current or future years (ignore dates from past years).
- temas: list of conference topic areas (empty list [] if not found).

Required JSON structure (no extra text, no markdown):
{{"fecha_inicio":null,"fecha_fin":null,"envio_trabajo":null,"notificacion_aceptacion":null,"inscripcion":null,"temas":[]}}

{text}"""

_PARTIAL_PROMPT = """\
Already found:{found_json}
Find ONLY these missing fields:{missing_fields}
RULES:
- YYYY-MM-DD format or null.
- Logical order: envio_trabajo < notificacion_aceptacion < inscripcion <= fecha_inicio <= fecha_fin
- If multiple versions of a date exist (original + extended), use the MOST RECENT date.
- IGNORE crossed-out dates, "old deadline", "originally" prefixes.
JSON ONLY (no markdown, no explanation):
{missing_template}

{text}"""


def _call_llm(prompt: str) -> str | None:
    """Send a prompt to Groq and return the raw response."""
    client = _get_groq_client()

    for attempt in range(1, 4):
        try:
            log.info("  [LLM] Sending request (attempt %d/3)...", attempt)
            completion = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "Extract conference dates as JSON only.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=512,
            )
            resp = completion.choices[0].message.content
            log.info("  [LLM] Response received (%d chars).", len(resp))
            return resp
        except Exception as exc:
            name = type(exc).__name__
            if "rate_limit" in name.lower() or "429" in str(exc):
                wait = 2 ** attempt * 10
                log.warning("  [LLM] Rate limit. Waiting %ds...", wait)
                time.sleep(wait)
            else:
                log.error("  [LLM] Unexpected error: %s — %s", name, exc)
                return None

    log.error("  [LLM] All retries exhausted.")
    return None


def _repair_truncated_json(json_str: str) -> str:
    """
    F5 fix: attempt to close an incomplete JSON object that was cut off
    by a token limit.  Handles the most common case: missing closing brace
    after the last key-value pair.

    This is a best-effort repair — it only handles simple truncations, not
    deeply nested structures.
    """
    s = json_str.strip()
    if s.endswith("}"):
        return s  # already complete

    # Count open vs closed braces
    open_b  = s.count("{")
    close_b = s.count("}")
    deficit = open_b - close_b
    if deficit <= 0:
        return s  # nothing obvious to fix

    # If the last value is an incomplete string, close it first
    if s.count('"') % 2 != 0:
        s += '"'

    # Close any open list
    open_sq  = s.count("[")
    close_sq = s.count("]")
    if open_sq > close_sq:
        s += "]" * (open_sq - close_sq)

    # Close open objects
    s += "}" * deficit

    log.debug("  [LLM] Repaired truncated JSON (added %d closing brace(s)).", deficit)
    return s


def _parse_llm_json(raw: str | None) -> dict:
    """Parse raw LLM response into a dict."""
    from dateutil import parser as dateutil_parser

    if not raw:
        return {}

    # Strip markdown code fences
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
    json_str = match.group(1).strip() if match else raw.strip()

    # Extract JSON object
    obj_match = re.search(r"\{[\s\S]*\}", json_str)
    if obj_match:
        json_str = obj_match.group(0)
    else:
        log.error("  No JSON object found in LLM response: %.200s", raw)
        return {}

    # F5 fix: attempt repair before giving up
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        json_str = _repair_truncated_json(json_str)
        try:
            data = json.loads(json_str)
            log.info("  [LLM] Successfully parsed repaired JSON.")
        except json.JSONDecodeError as exc:
            log.error("  Invalid JSON from LLM (even after repair): %s | %.200s", exc, raw)
            return {}

    # Normalize date values
    result = {}
    for key in DATE_KEYS:
        val = data.get(key)
        if not val or str(val).strip().lower() in ("none", "null", "n/a", "tbd", ""):
            result[key] = None
        else:
            try:
                dt = dateutil_parser.parse(str(val), dayfirst=False)
                result[key] = dt.strftime("%Y-%m-%d")
            except (ValueError, OverflowError):
                log.warning("  Could not normalize LLM date: '%s'", val)
                result[key] = None

    # Topics
    temas_raw = data.get("temas", [])
    if isinstance(temas_raw, list):
        result["temas"] = [str(t).strip() for t in temas_raw if t]
    elif isinstance(temas_raw, str):
        result["temas"] = [t.strip() for t in temas_raw.split(",") if t.strip()]
    else:
        result["temas"] = []

    return result


_TOPICS_PROMPT = """\
You are extracting the list of topics / tracks / themes / areas of interest \
of an academic conference from the page text below.

RULES:
- Output ONLY valid JSON in the exact form: {{"temas": ["topic1", "topic2", ...]}}
- Each item is a short topic phrase (2-10 words). NO sentences, NO descriptions.
- DO NOT include: dates, deadlines, committee/chair names, university/institution names,
  registration info, navigation labels (Home, Contact, Venue, Awards, Program, etc.),
  paper titles, sponsor names, certificates, or any boilerplate.
- If the page has NO clear topic list, return {{"temas": []}}.
- Maximum 30 topics. Deduplicate.

PAGE TEXT:
{text}"""


def llm_extract_topics(html_text: str) -> list[str]:
    """Last-resort fallback: ask the LLM for the conference topics.

    Called only when every HTML-based heuristic returned an empty list.
    Returns [] on any error so the pipeline continues unchanged.
    """
    if not html_text or not html_text.strip():
        return []

    snippet = html_text[:8000]
    prompt = _TOPICS_PROMPT.format(text=snippet)
    try:
        raw = _call_llm(prompt)
    except Exception as exc:
        log.warning("  [TopicLLM] Call failed: %s", exc)
        return []
    if not raw:
        return []

    # Reuse the JSON repair / extract logic from _parse_llm_json
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
    json_str = match.group(1).strip() if match else raw.strip()
    obj_match = re.search(r"\{[\s\S]*\}", json_str)
    if not obj_match:
        return []
    json_str = obj_match.group(0)
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        try:
            data = json.loads(_repair_truncated_json(json_str))
        except json.JSONDecodeError:
            return []

    temas = data.get("temas", [])
    if isinstance(temas, str):
        temas = [t.strip() for t in temas.split(",")]
    if not isinstance(temas, list):
        return []

    cleaned: list[str] = []
    seen: set[str] = set()
    for t in temas:
        s = str(t).strip().rstrip(".;:")
        if not s or len(s) < 5 or len(s) > 200:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(s)
    return cleaned[:30]


def llm_full_extraction(text: str) -> dict:
    """Full LLM extraction — used when regex found very few fields."""
    prompt = _FULL_PROMPT.format(text=text)
    raw = _call_llm(prompt)
    return _parse_llm_json(raw)


def llm_partial_extraction(text: str, found: dict) -> dict:
    """Partial LLM extraction — only asks for the missing fields."""
    missing = [k for k in DATE_KEYS if found.get(k) is None]
    if not missing:
        return found

    found_json = json.dumps(
        {k: v for k, v in found.items() if k in DATE_KEYS and v is not None},
        indent=2,
    )
    missing_fields = "\n".join(f"- {k}" for k in missing)
    missing_template = json.dumps({k: "YYYY-MM-DD or null" for k in missing}, indent=2)

    prompt = _PARTIAL_PROMPT.format(
        found_json=found_json,
        missing_fields=missing_fields,
        missing_template=missing_template,
        text=text,
    )
    raw = _call_llm(prompt)
    llm_result = _parse_llm_json(raw)

    merged = dict(found)
    for k in missing:
        if llm_result.get(k):
            merged[k] = llm_result[k]

    if "temas" not in merged or not merged.get("temas"):
        merged["temas"] = llm_result.get("temas", [])

    return merged


# ═════════════════════════════════════════════
#  TEMPORAL VALIDATION (Req 4)
# ═════════════════════════════════════════════

def validate_temporal_logic(dates: dict) -> dict:
    """
    Post-extraction sanity check.  Enforces the logical chain:
        envio_trabajo ≤ notificacion_aceptacion ≤ inscripcion ≤ fecha_inicio ≤ fecha_fin

    If any adjacent pair violates the order, the less-reliable field is
    set to None.

    FIX-YR: Relaxed year filter — only discard dates from strictly past years
    (year < current_year - 1). Dates from 2027, 2028, etc. are accepted because
    some URLs may reference an older year while the page content has been
    updated (e.g. icdici.com/2026/ showing ICDICI 2027 content).

    Pure function — does not mutate the input dict.
    """
    result = dict(dates)
    min_year = datetime.now().year - 1  # FIX-YR: accept current year AND future years

    # ── Step A: discard stale dates (past years only) ──
    for key in DATE_KEYS:
        val = result.get(key)
        if not val:
            continue
        try:
            dt = datetime.strptime(val, "%Y-%m-%d").date()
            if dt.year < min_year:
                log.warning("  [Validation] %s = %s is from a past year — nullified.", key, val)
                result[key] = None
        except (ValueError, TypeError):
            pass

    # ── Step B: enforce temporal chain ──
    chain = [
        "envio_trabajo",
        "notificacion_aceptacion",
        "inscripcion",
        "fecha_inicio",
        "fecha_fin",
    ]

    def _to_date(val: str | None) -> date_type | None:
        if not val:
            return None
        try:
            return datetime.strptime(val, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None

    for i in range(len(chain) - 1):
        key_a, key_b = chain[i], chain[i + 1]
        da, db = _to_date(result.get(key_a)), _to_date(result.get(key_b))
        if da and db and da > db:
            delta = (da - db).days
            if delta <= 30:
                log.warning(
                    "  [Validation] %s (%s) > %s (%s) by %d days — preserved (within tolerance).",
                    key_a, result[key_a], key_b, result[key_b], delta,
                )
            else:
                log.warning(
                    "  [Validation] %s (%s) > %s (%s) by %d days — nullified %s.",
                    key_a, result[key_a], key_b, result[key_b], delta, key_a,
                )
                result[key_a] = None

    return result


# ═════════════════════════════════════════════
#  HALLUCINATION DETECTION (P3)
# ═════════════════════════════════════════════

def _detect_hallucination(dates: dict) -> tuple[dict, bool]:
    """
    P3 fix: post-LLM validation guard that detects and rejects hallucinated
    results.

    Rules:
      1. If fecha_fin - fecha_inicio > 60 days, null both fields.
      2. If all non-null deadline fields (envio_trabajo, notificacion_aceptacion,
         inscripcion) are spaced suspiciously uniformly (±3 days of exactly
         30-day gaps between every consecutive pair), null them all.

    Returns
    -------
    dates : dict — cleaned dates (may have nulled fields)
    was_hallucination : bool — True if any hallucination was detected
    """
    result = dict(dates)
    hallucinated = False

    def _to_date(val):
        if not val:
            return None
        try:
            return datetime.strptime(val, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None

    # Rule 1: impossible conference duration
    dt_start = _to_date(result.get("fecha_inicio"))
    dt_end = _to_date(result.get("fecha_fin"))
    if dt_start and dt_end:
        span = (dt_end - dt_start).days
        if span > 60:
            log.warning(
                "  [Hallucination] fecha_inicio=%s to fecha_fin=%s is %d days "
                "(>60) — nullifying both.",
                result["fecha_inicio"], result["fecha_fin"], span,
            )
            result["fecha_inicio"] = None
            result["fecha_fin"] = None
            hallucinated = True

    # Rule 2: suspiciously uniform deadline spacing
    deadline_keys = ["envio_trabajo", "notificacion_aceptacion", "inscripcion"]
    deadline_dates = []
    for k in deadline_keys:
        d = _to_date(result.get(k))
        if d:
            deadline_dates.append((k, d))

    if len(deadline_dates) >= 2:
        # Sort by date
        deadline_dates.sort(key=lambda x: x[1])
        gaps = []
        for i in range(len(deadline_dates) - 1):
            gap = (deadline_dates[i + 1][1] - deadline_dates[i][1]).days
            gaps.append(gap)

        # Check if ALL gaps are within ±3 days of 30
        if gaps and all(abs(g - 30) <= 3 for g in gaps):
            log.warning(
                "  [Hallucination] Deadline spacing is suspiciously uniform "
                "(gaps: %s days) — nullifying all deadline fields.",
                gaps,
            )
            for k in deadline_keys:
                result[k] = None
            hallucinated = True

    return result, hallucinated


# ═════════════════════════════════════════════
#  MAIN PIPELINE (per URL)
# ═════════════════════════════════════════════

def procesar_url(
    url: str,
    cache: CacheManager,
    db_dates: dict[str, dict],
    extraction_mode: str = "all"
) -> tuple[dict, list, str]:
    """
    Full pipeline for one URL.

    Returns
    -------
    record : dict   — extracted data
    changes : list  — list of Change objects
    method : str    — "cache" | "regex" | "regex+llm" | "llm" | "js+..." | "error"
    """
    log.info("Processing: %s", url)
    empty = {k: None for k in DATE_KEYS}
    empty["temas"] = []
    conference_name: str | None = None  # populated after download

    # ── Step 1: Download HTML (static + optional JS fallback) ──
    html, used_js = descargar_html(url)
    if not html:
        return {"conference_name": None, "url": url, **empty, "notas": "", "fields_found": "0/5"}, [], "error"

    # Extract conference name from static HTML (title / h1 / h2)
    conference_name = extract_conference_name(html, url=url)

    # ── Step 1.5: Extract topics from HTML (regex-based) ──
    html_topics = []
    if extraction_mode in ("all", "topics_only"):
        html_topics = extract_topics(html)
        if html_topics:
            log.info("  [TopicExtractor] Found %d topics from HTML.", len(html_topics))

    if extraction_mode == "topics_only":
        if not html_topics:
            topic_links = find_date_links(html, url)
            if topic_links:
                html_topics = fetch_subpage_topics(topic_links)
                if html_topics:
                    log.info("  [TopicCrawl] Found %d topics from sub-pages.", len(html_topics))
        dates = dict(empty)
        dates["temas"] = html_topics or []
        dates["notas"] = "Extraction mode: topics_only"
        
        # Don't cache text, but return immediately to skip LLM & regex
        return {"conference_name": conference_name, "url": url, **dates, "fields_found": "0/5"}, [], "topics_only"

    js_prefix = "js+" if used_js else ""

    # ── Step 2: Smart text extraction ──
    try:
        date_text = extract_date_text(html)
        if not date_text.strip():
            log.warning("  Empty text for %s", url)
            return {"conference_name": conference_name, "url": url, **empty, "notas": "", "fields_found": "0/5"}, [], "error"
        log.info("  Smart-extracted: %d chars", len(date_text))
    except Exception as exc:
        log.error("  Text extraction error: %s", exc)
        return {"conference_name": conference_name, "url": url, **empty, "notas": "", "fields_found": "0/5"}, [], "error"

    # ── Step 2.5: Count date hits for later decisions ──
    crawl_used = False
    from extractors.text_extractor import _DATE_PATTERN  # reuse existing regex
    date_hits = len(_DATE_PATTERN.findall(date_text))

    # ── P9: Detect image-content pages ──
    # Page loaded fine (HTML exists, > 200 chars) but zero date patterns
    # → dates are probably embedded in images.
    if date_hits == 0 and len(html) > 200:
        # FIX-P: detect PDF links for manual review
        from extractors.text_extractor import detect_pdf_links
        pdf_links = detect_pdf_links(html, url)
        notas_parts = ["Dates may be in images — manual review needed"]
        if pdf_links:
            notas_parts.append("PDF links found: " + " ; ".join(pdf_links))
        notas = " | ".join(notas_parts)
        log.info(
            "  [P9] Page loaded (HTTP 200, %d chars HTML) but 0 date patterns "
            "— likely image-content. %s",
            len(html),
            "PDFs: " + str(pdf_links) if pdf_links else "No PDFs found.",
        )
        return {"conference_name": conference_name, "url": url, **empty, "temas": [], "notas": notas, "fields_found": "0/5"}, [], "image-content"

    # ── Step 3: Cache check (F3 fix) ──
    content_unchanged = not cache.has_changed(url, date_text)
    has_valid = cache.has_valid_cache(url)

    if content_unchanged and has_valid:
        log.info("  ✅ Content unchanged, valid cache — skipping extraction.")
        cached = cache.get_cached_dates(url)
        if cached:
            # Prefer freshly extracted HTML topics over stale cache
            cached_topics = cache.get_cached_topics(url)
            topics = html_topics or cached_topics

            # If still no topics, try sub-page crawl for CFP/topics pages
            if not topics:
                topic_links = find_date_links(html, url)
                if topic_links:
                    topics = fetch_subpage_topics(topic_links)
                    if topics:
                        log.info("  [TopicCrawl] Found %d topics from sub-pages (cache path).", len(topics))

            # Last-resort: LLM topic fallback (cache path)
            if not topics:
                try:
                    full_text = extract_full_text(html)[:8000] if html else ""
                    fb = llm_extract_topics(full_text or date_text)
                    if fb:
                        log.info("  [TopicLLM] Fallback recovered %d topics (cache path).", len(fb))
                        topics = fb
                except Exception as exc:
                    log.warning("  [TopicLLM] Fallback error (cache path): %s", exc)

            cached["temas"] = topics
            # Persist newly found topics to cache
            if topics and not cached_topics:
                cache.update(
                    url, date_text,
                    {k: cached.get(k) for k in DATE_KEYS},
                    topics,
                )
            cached["notas"] = ""
            filled_cache = sum(1 for k in DATE_KEYS if cached.get(k) is not None)
            return {"conference_name": conference_name, "url": url, **cached, "fields_found": f"{filled_cache}/5"}, [], "cache"
    elif content_unchanged and not has_valid:
        log.info(
            "  ⚠️  Content unchanged but previous result was invalid — re-extracting."
        )

    # ── Step 4: Regex extraction ──
    regex_dates, confidence = extract_with_regex(date_text)
    method = f"{js_prefix}regex"

    # ── Step 4.5 (FIX-C2): Field-aware shallow crawl trigger ──
    # Replaces the old P8 trigger. Crawl now fires when key fields are missing
    # even if the page has enough text, because dates may be on sub-pages.
    if field_aware_crawl_needed(date_hits, len(date_text), regex_dates):
        sub_links = find_date_links(html, url)
        if sub_links:
            supplement = fetch_supplementary_text(sub_links, date_text)
            if supplement.strip():
                budget = MAX_SMART_TEXT_CHARS - len(date_text) - 20
                if budget > 100:
                    date_text = date_text + "\n--- SUB-PAGE ---\n" + supplement[:budget]
                    crawl_used = True
                    log.info("  [ShallowCrawl] Merged text now %d chars.", len(date_text))
                    # Re-run regex on enriched text
                    regex_dates, confidence = extract_with_regex(date_text)
                    method = f"{js_prefix}regex"

    # ── Step 4.6: Sub-page topic crawl (when main page had no topics) ──
    if extraction_mode == "all" and not html_topics:
        topic_links = find_date_links(html, url)  # reuses same link finder (now includes topic keywords)
        if topic_links:
            sub_topics = fetch_subpage_topics(topic_links)
            if sub_topics:
                html_topics = sub_topics
                log.info("  [TopicCrawl] Found %d topics from sub-pages.", len(html_topics))

    # ── Step 5: LLM decision based on confidence ──
    # P3 gate: skip LLM if page has no date content at all
    if confidence == 0 and date_hits < MIN_DATE_PATTERNS_FOR_LLM:
        log.info("  ⛔ No date content detected — skipping LLM.")
        dates = {k: None for k in DATE_KEYS}
        dates["temas"] = []
        method = "no-content"
    elif confidence >= REGEX_CONFIDENCE_HIGH:
        # P7: even at high confidence, fill missing fields via partial LLM
        missing = [k for k in DATE_KEYS if regex_dates.get(k) is None]
        if missing:
            log.info("  ✅ Regex %.0f%% but %d field(s) missing — partial LLM.",
                     confidence * 100, len(missing))
            try:
                dates = llm_partial_extraction(date_text, regex_dates)
                method = f"{js_prefix}regex+llm"
            except ValueError as exc:
                log.error("  %s", exc)
                dates = regex_dates
                dates.setdefault("temas", [])
        else:
            log.info("  ✅ Regex confidence %.0f%% — skipping LLM.", confidence * 100)
            dates = regex_dates
            dates["temas"] = []
            method = f"{js_prefix}regex"
    elif confidence >= REGEX_CONFIDENCE_PARTIAL:
        log.info("  ⚡ Regex confidence %.0f%% — partial LLM call.", confidence * 100)
        try:
            dates = llm_partial_extraction(date_text, regex_dates)
            method = f"{js_prefix}regex+llm"
        except ValueError as exc:
            log.error("  %s", exc)
            dates = regex_dates
            dates.setdefault("temas", [])
    else:
        log.info("  🤖 Regex confidence %.0f%% — full LLM call.", confidence * 100)
        try:
            dates = llm_full_extraction(date_text)
            if not dates:
                dates = {k: None for k in DATE_KEYS}
                dates["temas"] = []
            method = f"{js_prefix}llm"
        except ValueError as exc:
            log.error("  %s", exc)
            dates = regex_dates
            dates.setdefault("temas", [])

    # ── Step 5.5: Temporal validation (Req 4) ──
    dates = validate_temporal_logic(dates)

    # ── Step 5.6: Hallucination detection (P3) ──
    dates, was_hallucination = _detect_hallucination(dates)

    # ── Step 5.7: browser-use fallback (FIX-BU) ──
    # Level-3 fallback: if key fields are still missing after regex+LLM,
    # use browser-use to navigate the site and extract dates from sub-pages.
    browser_used = False
    if _BROWSER_USE_AVAILABLE and not was_hallucination:
        browser_text, browser_triggered = extract_via_browser_use(url, dates)
        if browser_triggered and browser_text:
            log.info("  [FIX-BU] Got %d chars from browser-use — re-running extraction.",
                     len(browser_text))
            # Merge with existing text and re-run full extraction
            merged_text = date_text + "\n--- BROWSER-USE ---\n" + browser_text
            bu_regex, bu_conf = extract_with_regex(merged_text)
            # Fill only still-missing fields
            for k in DATE_KEYS:
                if dates.get(k) is None and bu_regex.get(k):
                    dates[k] = bu_regex[k]

            # If still missing fields, run partial LLM on browser text
            still_missing = [k for k in DATE_KEYS if dates.get(k) is None]
            if still_missing:
                try:
                    bu_llm = llm_partial_extraction(browser_text, dates)
                    for k in still_missing:
                        if bu_llm.get(k):
                            dates[k] = bu_llm[k]
                    if not dates.get("temas"):
                        dates["temas"] = bu_llm.get("temas", [])
                except ValueError:
                    pass

            dates = validate_temporal_logic(dates)  # re-validate
            browser_used = True
            method += "+browser"

    # Append "+crawl" to method if shallow crawling was used
    if crawl_used:
        method += "+crawl"

    # ── Build notes ──
    notes_parts: list[str] = []
    if was_hallucination:
        notes_parts.append("Hallucination detected — some fields cleared")
    # FIX-P: attach PDF links for manual review
    if html:
        from extractors.text_extractor import detect_pdf_links
        pdf_links = detect_pdf_links(html, url)
        if pdf_links:
            notes_parts.append("PDF links for review: " + " ; ".join(pdf_links))

    # ── Step 5.8: Merge topics — prefer HTML extraction over LLM ──
    if extraction_mode == "dates_only":
        dates["temas"] = []
    else:
        llm_topics = dates.get("temas", [])
        if html_topics:
            dates["temas"] = html_topics
        elif llm_topics:
            dates["temas"] = llm_topics
        else:
            dates["temas"] = []

        # Last-resort fallback: ask the LLM specifically for topics when every
        # HTML-based strategy + sub-page crawl + date-LLM came back empty.
        if not dates["temas"]:
            try:
                full_text = extract_full_text(html)[:8000] if html else ""
                fallback_topics = llm_extract_topics(full_text or date_text)
                if fallback_topics:
                    log.info("  [TopicLLM] Fallback recovered %d topics.", len(fallback_topics))
                    dates["temas"] = fallback_topics
                    method += "+topicllm"
            except Exception as exc:
                log.warning("  [TopicLLM] Fallback error: %s", exc)

    # ── Step 6: Update cache ──
    # P3: if hallucination was detected, store as invalid so next run retries
    cache_is_valid = None  # let CacheManager infer
    if was_hallucination:
        cache_is_valid = False
        log.info("  [P3] Hallucination detected — marking cache as invalid for retry.")

    cache.update(
        url, date_text,
        {k: dates.get(k) for k in DATE_KEYS},
        dates.get("temas", []),
        is_valid=cache_is_valid,
    )

    # ── Step 7: Detect changes vs. database ──
    url_db_dates = db_dates.get(url, {})
    changes = detect_changes(
        {k: dates.get(k) for k in DATE_KEYS},
        url_db_dates,
        url,
    )
    if changes:
        for c in changes:
            log.info("  ⚠️  %s: %s → %s (%s)",
                     c.field, c.old_value, c.new_value, c.change_type)

    dates["notas"] = " | ".join(notes_parts)
    filled = sum(1 for k in DATE_KEYS if dates.get(k) is not None)
    record = {
        "conference_name": conference_name,
        "url": url,
        **dates,
        "fields_found": f"{filled}/5",
    }
    return record, changes, method


# ═════════════════════════════════════════════
#  EXCEL REPORT WRITER
# ═════════════════════════════════════════════

def write_excel_report(
    records: list[dict],
    change_report: ChangeReport,
    output_path: Path | None = None,
):
    """Write the two-sheet Excel report."""
    path = output_path or OUTPUT_FILE

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
    df_data = pd.DataFrame(records)

    for col in col_order:
        if col not in df_data.columns:
            df_data[col] = None

    df_data = df_data[col_order]
    df_data["temas"] = df_data["temas"].apply(
        lambda t: " | ".join(t) if isinstance(t, list) else str(t) if t else ""
    )

    df_data = df_data.rename(columns=COLUMN_LABELS)
    df_changes = change_report.to_dataframe()

    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df_data.to_excel(writer, sheet_name="Extracted Data", index=False)
            df_changes.to_excel(writer, sheet_name="Detected Changes", index=False)

            for sheet_name in ["Extracted Data", "Detected Changes"]:
                ws = writer.sheets[sheet_name]
                for col_cells in ws.columns:
                    max_len = max(
                        len(str(cell.value or "")) for cell in col_cells
                    )
                    col_letter = col_cells[0].column_letter
                    ws.column_dimensions[col_letter].width = min(max_len + 4, 60)

        log.info("✅ Report saved: %s", path)
        log.info("   Sheet 1 — Extracted Data: %d rows", len(df_data))
        log.info("   Sheet 2 — Detected Changes: %d rows", len(df_changes))
    except Exception as exc:
        log.error("Failed to write Excel: %s", exc)
        csv_path = str(path).replace(".xlsx", "_data.csv")
        df_data.to_csv(csv_path, index=False)
        log.info("⚠️  Fallback CSV saved: %s", csv_path)


def main(fuente_urls: str | None = None, progress_callback=None, stop_event=None, pause_event=None, url_list: list[str] | None = None, auto_export: bool = True, extraction_mode: str = "all"):
    """Load URLs, run the pipeline, and generate the report."""
    start_time = time.time()
    urls = url_list if url_list is not None else cargar_urls(fuente_urls)
    if not urls:
        log.error("No URLs to process.")
        return

    cache = CacheManager()
    db_dates = load_db_dates()
    change_report = ChangeReport()

    stats: dict[str, int] = {}

    records = []
    for i, url in enumerate(urls, 1):
        if stop_event and stop_event.is_set():
            log.info("  ⏹️ Pipeline stopped by user.")
            break

        while pause_event and pause_event.is_set():
            time.sleep(0.5)
            if stop_event and stop_event.is_set():
                break

        print(f"\n{'─' * 60}")
        print(f"  [{i}/{len(urls)}] {url}")
        print(f"{'─' * 60}")

        record, changes, method = procesar_url(url, cache, db_dates, extraction_mode)
        record["extraction_method"] = method
        records.append(record)
        change_report.changes.extend(changes)
        stats[method] = stats.get(method, 0) + 1
        
        if progress_callback:
            progress_callback({
                "index": i,
                "total": len(urls),
                "url": url,
                "record": record,
                "method": method,
                "stats": stats.copy()
            })

        if i < len(urls):
            # Check for pause/stop during delay as well
            delay_left = DELAY_BETWEEN_REQUESTS
            while delay_left > 0:
                if stop_event and stop_event.is_set():
                    break
                time.sleep(min(0.5, delay_left))
                delay_left -= 0.5

    cache.save()
    if records and auto_export:
        write_excel_report(records, change_report)

    # ── Summary ──
    total_cache  = sum(v for k, v in stats.items() if k == "cache")
    total_regex  = sum(v for k, v in stats.items() if "regex" in k and "llm" not in k)
    total_rllm   = sum(v for k, v in stats.items() if "regex+llm" in k)
    total_llm    = sum(v for k, v in stats.items() if k in ("llm", "js+llm"))
    total_js     = sum(v for k, v in stats.items() if k.startswith("js+"))
    total_errors = stats.get("error", 0)
    total_img    = stats.get("image-content", 0)

    print(f"\n{'═' * 60}")
    print("  📊 RUN SUMMARY")
    print(f"{'═' * 60}")
    print(f"  Total conferences : {len(records)}")
    print(f"  Cache hits        : {total_cache}")
    print(f"  Regex only        : {total_regex}")
    print(f"  Regex + partial LLM: {total_rllm}")
    print(f"  Full LLM calls    : {total_llm}")
    print(f"  JS rendering used : {total_js}")
    print(f"  Image-content     : {total_img}")
    print(f"  Errors            : {total_errors}")
    print(f"  LLM calls saved   : {total_cache + total_regex} / {max(1, len(records))}")
    
    total_time = time.time() - start_time
    minutes, seconds = divmod(total_time, 60)
    print(f"  Tiempo total      : {int(minutes)}m {int(seconds)}s")
    
    if not _JS_RENDERER_AVAILABLE:
        print(
            "\n  ℹ️  JS rendering unavailable (playwright not installed).\n"
            "     To enable: pip install playwright && playwright install chromium --with-deps"
        )
    if not _BROWSER_USE_AVAILABLE:
        print(
            "\n  ℹ️  browser-use fallback unavailable (browser-use CLI not installed).\n"
            "     To enable: pip install browser-use"
        )
    print()

    if change_report.has_changes:
        print(f"  ⚠️  {len(change_report.changes)} CHANGE(S) DETECTED:")
        print(change_report.summary())
    else:
        print("  ✅ No changes detected vs. database.")
    print()

    if progress_callback:
        progress_callback({
            "done": True, 
            "stats": stats.copy(),
            "records": records,
            "change_report": change_report
        })


if __name__ == "__main__":
    fuente = sys.argv[1] if len(sys.argv) > 1 else None
    main(fuente)
