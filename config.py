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
OUTPUT_FILE = BASE_DIR / "reporte_conferencias_v2.xlsx"
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
    "url": "URL",
    "fecha_inicio": "Start Date",
    "fecha_fin": "End Date",
    "envio_trabajo": "Submission Deadline",
    "notificacion_aceptacion": "Acceptance Notification",
    "inscripcion": "Registration Deadline",
    "temas": "Topics",
    "extraction_method": "Method",
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
