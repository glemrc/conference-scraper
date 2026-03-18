"""
cache_manager.py
================
Layer 1: Content-hash caching to avoid redundant LLM calls.

F3 fix (cache poisoning):
  - Every entry now carries an ``is_valid`` boolean.
  - "Valid" means at least one date field was successfully extracted.
  - The cache check gate (used in scraper_v2) is split into two questions:
      a) has_changed(url, text)   → True if content differs (hash mismatch)
      b) has_valid_cache(url)     → True only if the stored result was valid
    The scraper skips extraction only when BOTH are False / True respectively.
  - get_cached_dates() returns None for invalid entries so the caller always
    sees a missing result and re-runs the full pipeline.
  - update() accepts an explicit ``is_valid`` flag; if omitted it is inferred
    from whether any date value is non-None.
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


def _result_is_valid(dates: dict) -> bool:
    """
    A result is considered valid when at least one date field is non-None.
    An all-None dict (e.g. every key maps to None) is *not* valid and must
    not be cached as a successful extraction.
    """
    return any(v is not None for v in dates.values())


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
        """Persist cache to disk. Called once at the end of a run."""
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
            log.info("Cache saved (%d entries).", len(self._data))
        except OSError as exc:
            log.error("Failed to save cache: %s", exc)

    # ── query ────────────────────────────────────────────────────

    def has_changed(self, url: str, date_text: str) -> bool:
        """Return True if the page content has changed since last check."""
        new_hash = _hash_text(date_text)
        entry = self._data.get(url)
        if entry is None:
            return True  # never seen before
        return entry.get("content_hash") != new_hash

    def has_valid_cache(self, url: str) -> bool:
        """
        Return True only if there is a cached entry AND it was previously
        marked as a valid extraction (at least one date found).

        This is the F3 fix: invalid (all-None) entries are stored for hash
        comparison purposes but are *never* served back as results.
        """
        entry = self._data.get(url)
        if entry is None:
            return False
        return entry.get("is_valid", False)

    def get_cached_dates(self, url: str) -> dict | None:
        """
        Return previously extracted dates, or None if:
          - the URL was never cached, OR
          - the cached result was invalid (all-None).

        F3 fix: previously this returned the raw dict which is truthy even
        when all values are None, causing the scraper to skip re-extraction
        for URLs that never yielded any data.
        """
        entry = self._data.get(url)
        if entry is None:
            return None
        if not entry.get("is_valid", False):
            return None          # do not serve invalid cached results
        return entry.get("last_dates")

    def get_cached_topics(self, url: str) -> list[str]:
        """Return previously extracted topics, or empty list."""
        entry = self._data.get(url)
        if entry is None:
            return []
        return entry.get("last_topics", [])

    # ── update ───────────────────────────────────────────────────

    def update(
        self,
        url: str,
        date_text: str,
        dates: dict,
        topics: list[str] | None = None,
        is_valid: bool | None = None,
    ):
        """
        Store results for a URL after an extraction attempt.

        Parameters
        ----------
        url        : the page URL
        date_text  : the text that was hashed (for change detection)
        dates      : extracted date fields (may be all-None for failed runs)
        topics     : list of conference topics (may be empty)
        is_valid   : explicit validity override; if None, inferred from dates
        """
        if is_valid is None:
            is_valid = _result_is_valid(dates)

        self._data[url] = {
            "content_hash": _hash_text(date_text),
            "last_checked": datetime.now(timezone.utc).isoformat(),
            "last_dates": dates,
            "last_topics": topics or [],
            "is_valid": is_valid,
        }

        if not is_valid:
            log.debug(
                "  [Cache] Stored INVALID entry for %s "
                "(will retry next run if content unchanged).",
                url,
            )

    def get_last_checked(self, url: str) -> str | None:
        """Return ISO timestamp of last successful check, or None."""
        entry = self._data.get(url)
        return entry.get("last_checked") if entry else None

    def mark_invalid(self, url: str):
        """
        Explicitly mark a cached entry as invalid without changing the hash.
        Useful when the caller detects that the cached result is stale due to
        a rendering upgrade (e.g. JS fallback now available).
        """
        entry = self._data.get(url)
        if entry:
            entry["is_valid"] = False
            log.debug("  [Cache] Marked %s as invalid.", url)
