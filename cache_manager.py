"""
cache_manager.py
================
Layer 1: Content-hash caching to avoid redundant LLM calls.

Stores a JSON file with per-URL entries:
  - content_hash   : SHA-256 of the date-relevant page text
  - last_checked   : ISO timestamp of last successful check
  - last_dates     : dict with the last extracted dates
  - last_topics    : list of topics from last extraction

On each run the scraper hashes the freshly-downloaded date section and
compares it with the stored hash.  If identical → skip LLM entirely and
return cached dates.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from config import CACHE_FILE

log = logging.getLogger(__name__)


# ─── helpers ────────────────────────────────────────────────────────

def _hash_text(text: str) -> str:
    """SHA-256 of the text (ignoring leading/trailing whitespace)."""
    return hashlib.sha256(text.strip().encode("utf-8")).hexdigest()


# ─── public API ─────────────────────────────────────────────────────

class CacheManager:
    """Simple JSON-backed cache for scraped conference data."""

    def __init__(self, path: Path | None = None):
        self.path = path or CACHE_FILE
        self._data: dict[str, dict] = {}
        self._load()

    # ── persistence ──────────────────────────────────────────────

    def _load(self):
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
                log.info("Cache loaded: %d entries from %s", len(self._data), self.path.name)
            except (json.JSONDecodeError, OSError) as exc:
                log.warning("Cache file corrupt, starting fresh: %s", exc)
                self._data = {}
        else:
            log.info("No cache file found — starting fresh.")

    def save(self):
        """Persist cache to disk.  Called once at the end of a run."""
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
            log.info("Cache saved (%d entries).", len(self._data))
        except OSError as exc:
            log.error("Failed to save cache: %s", exc)

    # ── query / update ───────────────────────────────────────────

    def has_changed(self, url: str, date_text: str) -> bool:
        """Return True if the page content has changed since last check."""
        new_hash = _hash_text(date_text)
        entry = self._data.get(url)
        if entry is None:
            return True  # never seen before
        return entry.get("content_hash") != new_hash

    def get_cached_dates(self, url: str) -> dict | None:
        """Return previously extracted dates, or None if not cached."""
        entry = self._data.get(url)
        if entry is None:
            return None
        return entry.get("last_dates")

    def get_cached_topics(self, url: str) -> list[str]:
        """Return previously extracted topics, or empty list."""
        entry = self._data.get(url)
        if entry is None:
            return []
        return entry.get("last_topics", [])

    def update(self, url: str, date_text: str, dates: dict, topics: list[str] | None = None):
        """Store results for a URL after a successful extraction."""
        self._data[url] = {
            "content_hash": _hash_text(date_text),
            "last_checked": datetime.now(timezone.utc).isoformat(),
            "last_dates": dates,
            "last_topics": topics or [],
        }

    def get_last_checked(self, url: str) -> str | None:
        """Return ISO timestamp of last successful check, or None."""
        entry = self._data.get(url)
        return entry.get("last_checked") if entry else None
