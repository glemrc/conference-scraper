"""
change_detector.py
==================
Compares newly extracted conference dates against the existing Excel
database and reports differences.

Each change is classified as:
  - "new"       → field had no value before, now it does
  - "updated"   → the date changed
  - "extension" → the date moved *later* (common for submission deadlines)
  - "removed"   → field had a value before, now it doesn't
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from config import DATE_KEYS, DB_FILE

log = logging.getLogger(__name__)


@dataclass
class Change:
    """A single date-field change for one conference."""
    url: str
    field: str
    old_value: str | None
    new_value: str | None
    change_type: str  # "new" | "updated" | "extension" | "removed"


@dataclass
class ChangeReport:
    """Collection of changes detected in a single run."""
    changes: list[Change] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    @property
    def has_changes(self) -> bool:
        return len(self.changes) > 0

    def summary(self) -> str:
        if not self.changes:
            return "No changes detected."
        lines = [f"Detected {len(self.changes)} change(s):"]
        for c in self.changes:
            lines.append(
                f"  [{c.change_type.upper()}] {c.url} — {c.field}: "
                f"{c.old_value or '(empty)'} → {c.new_value or '(empty)'}"
            )
        return "\n".join(lines)

    def to_dataframe(self) -> pd.DataFrame:
        """Convert changes to a DataFrame for the Excel report."""
        if not self.changes:
            return pd.DataFrame(columns=[
                "URL", "Field", "Old Value", "New Value", "Change Type", "Detected At"
            ])
        rows = [
            {
                "URL": c.url,
                "Field": c.field,
                "Old Value": c.old_value or "",
                "New Value": c.new_value or "",
                "Change Type": c.change_type,
                "Detected At": self.timestamp,
            }
            for c in self.changes
        ]
        return pd.DataFrame(rows)


# ─── helpers ────────────────────────────────────────────────────────

def _classify_change(old: str | None, new: str | None) -> str:
    """Determine the type of change between two date strings."""
    if old is None and new is not None:
        return "new"
    if old is not None and new is None:
        return "removed"
    # Both exist but differ → check if it's an extension (later date)
    try:
        old_dt = datetime.strptime(old, "%Y-%m-%d")
        new_dt = datetime.strptime(new, "%Y-%m-%d")
        return "extension" if new_dt > old_dt else "updated"
    except (ValueError, TypeError):
        return "updated"


# ─── loading the existing database ──────────────────────────────────

def load_db_dates(db_path=None) -> dict[str, dict]:
    """
    Load existing conference dates from the Excel database.
    Returns a dict keyed by URL → {field: date_str, ...}.
    """
    path = db_path or DB_FILE
    if not path.exists():
        log.info("No existing database found at %s", path)
        return {}

    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as exc:
        log.warning("Could not read database: %s", exc)
        return {}

    # Try to find a URL column
    url_col = None
    for col in df.columns:
        if col.strip().lower() == "url":
            url_col = col
            break
    if url_col is None:
        log.warning("No 'URL' column found in database.")
        return {}

    # Map internal keys to possible column names in the DB
    col_map = {
        "fecha_inicio": ["start date", "fecha inicio", "conference start"],
        "fecha_fin": ["end date", "fecha fin", "conference end"],
        "envio_trabajo": ["submission deadline", "envio trabajo", "paper submission"],
        "notificacion_aceptacion": ["acceptance notification", "notificacion aceptacion",
                                     "notification"],
        "inscripcion": ["registration deadline", "inscripcion", "registration"],
    }

    def _find_col(candidates):
        for c in df.columns:
            if c.strip().lower() in candidates:
                return c
        return None

    result = {}
    for _, row in df.iterrows():
        url = str(row[url_col]).strip()
        if not url or url == "nan":
            continue
        dates = {}
        for key, candidates in col_map.items():
            col = _find_col(candidates)
            if col is not None:
                val = row.get(col)
                if pd.notna(val):
                    val_str = str(val).strip()
                    # Try to normalize to YYYY-MM-DD
                    if len(val_str) >= 8:
                        try:
                            dates[key] = datetime.strptime(
                                val_str[:10], "%Y-%m-%d"
                            ).strftime("%Y-%m-%d")
                        except ValueError:
                            dates[key] = val_str
                    else:
                        dates[key] = val_str
                else:
                    dates[key] = None
            else:
                dates[key] = None
        result[url] = dates

    log.info("Loaded %d existing conference records from DB.", len(result))
    return result


# ─── public API ─────────────────────────────────────────────────────

def detect_changes(
    new_dates: dict[str, str | None],
    db_dates: dict[str, str | None],
    url: str,
) -> list[Change]:
    """
    Compare extracted dates against DB dates for a single URL.
    Returns list of Change objects (may be empty).
    """
    changes: list[Change] = []
    for field_key in DATE_KEYS:
        old_val = db_dates.get(field_key)
        new_val = new_dates.get(field_key)
        if old_val == new_val:
            continue
        if old_val is None and new_val is None:
            continue
        change_type = _classify_change(old_val, new_val)
        changes.append(Change(
            url=url,
            field=field_key,
            old_value=old_val,
            new_value=new_val,
            change_type=change_type,
        ))
    return changes
