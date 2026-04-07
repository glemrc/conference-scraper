# CHANGES:
# P9 — Added "notas" / "Notes" to COLUMN_LABELS for image-content detection output.

"""
config.py
=========
Centralized configuration for the conference scraper v2.
Reads sensitive values from environment variables or a .env file.
"""

import os
from pathlib import Path

# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
CACHE_FILE = BASE_DIR / "scraper_cache.json"
OUTPUT_FILE = BASE_DIR / "fechas_conferencias_v3.xlsx"
DB_FILE = BASE_DIR / "Proceeding BD.xlsx"  # existing database

# ─────────────────────────────────────────────
# Groq API
# ─────────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.3-70b-versatile"

# ─────────────────────────────────────────────
# Scraper tunables
# ─────────────────────────────────────────────
REQUEST_TIMEOUT = 15          # seconds per HTTP request
DELAY_BETWEEN_REQUESTS = 2   # seconds between URLs (politeness)
MAX_TEXT_CHARS = 10_000       # max chars sent to LLM (fallback only)
MAX_SMART_TEXT_CHARS = 3_000  # max chars from smart extraction

# ─────────────────────────────────────────────
# Regex extraction thresholds
# ─────────────────────────────────────────────
REGEX_CONFIDENCE_HIGH = 0.7   # skip LLM entirely
REGEX_CONFIDENCE_PARTIAL = 0.3  # call LLM only for missing fields
CACHE_MIN_FIELDS = 3            # P1: min filled date fields for cache "valid"
MIN_DATE_PATTERNS_FOR_LLM = 2   # P3: min date patterns to justify an LLM call

# ─────────────────────────────────────────────
# Date field keys (internal canonical names)
# ─────────────────────────────────────────────
DATE_KEYS = [
    "fecha_inicio",
    "fecha_fin",
    "envio_trabajo",
    "notificacion_aceptacion",
    "inscripcion",
]

# Column mapping: internal key → Excel column header
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

# ─────────────────────────────────────────────
# HTTP headers
# ─────────────────────────────────────────────
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
}
