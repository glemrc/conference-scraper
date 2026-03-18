"""
scraper_v2.py
=============
Conference Deadline Monitoring System — v2 (modular, optimized).

Pipeline per URL:
  1. Download HTML
  2. Smart-extract date-relevant text  (text_extractor)
  3. Hash and compare with cache        (cache_manager)
     → unchanged? return cached dates, skip everything else
  4. Try regex extraction               (regex_extractor)
     → high confidence? use regex results, skip LLM
  5. LLM fallback (Groq API)            only for missing fields
  6. Compare with existing database     (change_detector)
  7. Update cache

Output: Excel workbook with two sheets
  Sheet 1 — "Extracted Data"   : full table of all conferences
  Sheet 2 — "Detected Changes" : changes vs. the previous database

Usage:
    python scraper_v2.py                     # uses URLs from DB or fixed list
    python scraper_v2.py urls.csv            # reads URLs from CSV
    python scraper_v2.py urls.xlsx           # reads URLs from Excel

Requires:
    pip install requests beautifulsoup4 pandas openpyxl python-dateutil groq python-dotenv
"""

import json
import os
import re
import sys
import time
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
    MAX_TEXT_CHARS,
    REGEX_CONFIDENCE_HIGH, REGEX_CONFIDENCE_PARTIAL,
    DATE_KEYS, COLUMN_LABELS, HTTP_HEADERS,
)
from cache_manager import CacheManager
from text_extractor import extract_date_text, extract_full_text
from regex_extractor import extract_with_regex
from change_detector import (
    detect_changes, load_db_dates, ChangeReport,
)

# ─────────────────────────────────────────────
# Load .env (if present) so GROQ_API_KEY is available
# ─────────────────────────────────────────────
load_dotenv()

# Re-read after dotenv (config.py reads os.getenv at import time,
# but .env might not have been loaded yet)
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
# ─────────────────────────────────────────────
URLS_FIJAS =[
"https://icacit.org.pe/symposium/important-dates/",
"https://comesyso.openpublish.eu/article/download",
"https://eeeu25.gjem.press",
"https://scrs.in/conference/csct2025",
"https://www.scrs.in/conference/csct2025",
"https://worldcist.org",
"https://seeu2026.gjem.press",
"https://acdsa.org/2026/deadlines",
"https://scrs.in/conference/icitai2026",
"https://scrs.in/conference/cml2026",
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
"https://scrs.in/conference/CIMA2026",
"https://scrs.in/conference/icdsa2026",
"https://www.icet.org",
"https://scrs.in/conference/aic2026",
"https://comesyso.openpublish.eu/article/download",
"https://ieee-uemcon.org",
"https://www.scrs.in/conference/ceee2026",
"https://icdici.com/2026/"
]


# ═════════════════════════════════════════════
#  URL LOADING
# ═════════════════════════════════════════════

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
                return urls
            except Exception as exc:
                log.error("Error reading '%s': %s. Falling back.", fuente, exc)

    # Try loading from the existing database
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
                    return urls
        except Exception:
            pass

    log.info("Using fixed URL list (%d URLs).", len(URLS_FIJAS))
    return list(URLS_FIJAS)


# ═════════════════════════════════════════════
#  HTML DOWNLOAD
# ═════════════════════════════════════════════

def descargar_html(url: str) -> str | None:
    """Download HTML from a URL. Returns None on error."""
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.Timeout:
        log.error("  Timeout: %s", url)
    except requests.exceptions.HTTPError as e:
        log.error("  HTTP %s: %s", e.response.status_code, url)
    except requests.exceptions.ConnectionError:
        log.error("  Connection error: %s", url)
    except requests.exceptions.RequestException as e:
        log.error("  Network error (%s): %s", type(e).__name__, url)
    return None


# ═════════════════════════════════════════════
#  GROQ LLM (minimal-call layer)
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


# ── Prompt for full extraction ──────────────────────────────────────

_FULL_PROMPT = """\
Extract conference dates from this text. Normalize ALL dates to YYYY-MM-DD.
Use null for missing information.

Return ONLY valid JSON (no markdown, no extra text):
{{
  "fecha_inicio": "YYYY-MM-DD" or null,
  "fecha_fin": "YYYY-MM-DD" or null,
  "envio_trabajo": "YYYY-MM-DD" or null,
  "notificacion_aceptacion": "YYYY-MM-DD" or null,
  "inscripcion": "YYYY-MM-DD" or null,
  "temas": ["topic1", "topic2", ...] or []
}}

Text:
---
{text}
---"""

# ── Prompt for partial extraction (only missing fields) ─────────────

_PARTIAL_PROMPT = """\
I already extracted some dates from a conference page. I need you to find
ONLY the missing fields listed below. Normalize ALL dates to YYYY-MM-DD.

Already found:
{found_json}

Missing fields to find:
{missing_fields}

Return ONLY valid JSON with the missing fields (use null if not found):
{missing_template}

Text:
---
{text}
---"""


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
                        "content": (
                            "You are an expert at extracting structured information "
                            "from academic conference web pages. "
                            "Respond ONLY with valid JSON, no extra text."
                        ),
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

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as exc:
        log.error("  Invalid JSON from LLM: %s | %.200s", exc, raw)
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

    # Merge: keep regex results, fill gaps from LLM
    merged = dict(found)
    for k in missing:
        if llm_result.get(k):
            merged[k] = llm_result[k]

    # Merge topics
    if "temas" not in merged or not merged.get("temas"):
        merged["temas"] = llm_result.get("temas", [])

    return merged


# ═════════════════════════════════════════════
#  MAIN PIPELINE (per URL)
# ═════════════════════════════════════════════

def procesar_url(
    url: str,
    cache: CacheManager,
    db_dates: dict[str, dict],
) -> tuple[dict, list, str]:
    """
    Full pipeline for one URL.

    Returns
    -------
    record : dict   — extracted data
    changes : list  — list of Change objects
    method : str    — "cache" | "regex" | "regex+llm" | "llm"
    """
    log.info("Processing: %s", url)
    empty = {k: None for k in DATE_KEYS}
    empty["temas"] = []

    # ── Step 1: Download HTML ──
    html = descargar_html(url)
    if not html:
        return {"url": url, **empty}, [], "error"

    # ── Step 2: Smart text extraction ──
    try:
        date_text = extract_date_text(html)
        if not date_text.strip():
            log.warning("  Empty text for %s", url)
            return {"url": url, **empty}, [], "error"
        log.info("  Smart-extracted: %d chars", len(date_text))
    except Exception as exc:
        log.error("  Text extraction error: %s", exc)
        return {"url": url, **empty}, [], "error"

    # ── Step 3: Cache check ──
    if not cache.has_changed(url, date_text):
        log.info("  ✅ Content unchanged — using cached dates.")
        cached = cache.get_cached_dates(url)
        if cached:
            cached["temas"] = cache.get_cached_topics(url)
            return {"url": url, **cached}, [], "cache"

    # ── Step 4: Regex extraction ──
    regex_dates, confidence = extract_with_regex(date_text)
    method = "regex"

    # ── Step 5: Decide LLM usage based on confidence ──
    if confidence >= REGEX_CONFIDENCE_HIGH:
        log.info("  ✅ Regex confidence %.0f%% — skipping LLM.", confidence * 100)
        dates = regex_dates
        dates["temas"] = []
        method = "regex"
    elif confidence >= REGEX_CONFIDENCE_PARTIAL:
        log.info("  ⚡ Regex confidence %.0f%% — partial LLM call.", confidence * 100)
        try:
            dates = llm_partial_extraction(date_text, regex_dates)
            method = "regex+llm"
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
            method = "llm"
        except ValueError as exc:
            log.error("  %s", exc)
            dates = regex_dates
            dates.setdefault("temas", [])

    # ── Step 6: Update cache ──
    cache.update(
        url, date_text,
        {k: dates.get(k) for k in DATE_KEYS},
        dates.get("temas", []),
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

    return {"url": url, **dates}, changes, method


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

    # ── Sheet 1: Extracted Data ──
    col_order = ["url"] + DATE_KEYS + ["temas", "extraction_method"]
    df_data = pd.DataFrame(records)

    # Ensure all columns exist
    for col in col_order:
        if col not in df_data.columns:
            df_data[col] = None

    df_data = df_data[col_order]
    df_data["temas"] = df_data["temas"].apply(
        lambda t: " | ".join(t) if isinstance(t, list) else str(t) if t else ""
    )

    # Rename columns for readability
    df_data = df_data.rename(columns=COLUMN_LABELS)

    # ── Sheet 2: Detected Changes ──
    df_changes = change_report.to_dataframe()

    # ── Write ──
    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df_data.to_excel(writer, sheet_name="Extracted Data", index=False)
            df_changes.to_excel(writer, sheet_name="Detected Changes", index=False)

            # Auto-size columns for readability
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


# ═════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════

def main(fuente_urls: str | None = None):
    """Load URLs, run the pipeline, and generate the report."""
    urls = cargar_urls(fuente_urls)
    if not urls:
        log.error("No URLs to process.")
        return

    # Initialize components
    cache = CacheManager()
    db_dates = load_db_dates()
    change_report = ChangeReport()

    # Stats
    stats = {"cache": 0, "regex": 0, "regex+llm": 0, "llm": 0, "error": 0}

    records = []
    for i, url in enumerate(urls, 1):
        print(f"\n{'─' * 60}")
        print(f"  [{i}/{len(urls)}] {url}")
        print(f"{'─' * 60}")

        record, changes, method = procesar_url(url, cache, db_dates)
        record["extraction_method"] = method
        records.append(record)
        change_report.changes.extend(changes)
        stats[method] = stats.get(method, 0) + 1

        if i < len(urls):
            time.sleep(DELAY_BETWEEN_REQUESTS)

    # Save cache
    cache.save()

    # Write report
    write_excel_report(records, change_report)

    # Print summary
    print(f"\n{'═' * 60}")
    print("  📊 RUN SUMMARY")
    print(f"{'═' * 60}")
    print(f"  Total conferences: {len(records)}")
    print(f"  Cache hits (no LLM):  {stats['cache']}")
    print(f"  Regex only (no LLM): {stats['regex']}")
    print(f"  Regex + partial LLM: {stats['regex+llm']}")
    print(f"  Full LLM calls:      {stats['llm']}")
    print(f"  Errors:              {stats['error']}")
    print(f"  LLM calls saved:     {stats['cache'] + stats['regex']}"
          f" / {len(records)}")
    print()

    if change_report.has_changes:
        print(f"  ⚠️  {len(change_report.changes)} CHANGE(S) DETECTED:")
        print(change_report.summary())
    else:
        print("  ✅ No changes detected vs. database.")
    print()


if __name__ == "__main__":
    fuente = sys.argv[1] if len(sys.argv) > 1 else None
    main(fuente)
