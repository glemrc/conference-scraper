# Design Spec: Scraper Robustness — browser-use + Conference Name + Fields Found

**Date:** 2026-04-07  
**Branch:** fix/pipeline-v2-improvements  
**Status:** Approved

---

## Problem

`fechas_conferencias_v3.xlsx` has all URLs populated but missing values across all date columns and Topics. The scraper pipeline exists but the browser-use fallback (level 3) is not aggressive enough, and the output lacks fields needed for quick manual review.

## Goals

1. Re-run the full pipeline from scratch to generate a more complete Excel.
2. Strengthen `browser_use_crawler.py` so it extracts dates from pages that defeat static + JS + regex + LLM.
3. Add `Conference Name` (format: `ACRONYM YEAR`, e.g., `ICOAMP 2026`) as a new output column.
4. Add `Fields Found` (e.g., `3/5`) as a new output column for manual review triage.
5. Topics extraction is deferred — out of scope for this spec.

## Approach

**Enfoque A:** Strengthen the existing `browser_use_crawler.py` (already integrated as level-3 fallback) + minimal additions to `scraper_v2.py` and `text_extractor.py`.

Pipeline order remains unchanged:
```
URL
 └─ static download (_static_download)
     └─ [F1] JS renderer (if JS-only page detected)
         └─ cache check
             └─ regex extraction
                 └─ LLM fallback (Groq)
                     └─ [Level-3] browser_use_crawler  ← strengthened
                         └─ Output: fechas_conferencias_v3.xlsx
```

---

## Changes by File

### 1. `browser_use_crawler.py` — 4 robustness improvements

**BU-R1: Deeper navigation via common URL paths**  
If no "Important Dates" link is found in the page state, try appending known paths to the base URL before giving up:
- `/dates`, `/important-dates`, `/cfp`, `/call-for-papers`, `/submission`, `/deadlines`

Each candidate is opened and checked for date text. First hit wins.

**BU-R2: More scroll iterations**  
Increase scroll attempts from 2 to 5 in `_scroll_and_collect()`. Conference pages with long tables or lazy-loaded content are often cut off at 2 scrolls.

**BU-R3: Structured element extraction**  
After getting the HTML, explicitly extract text from `<table>`, `<dl>`, `<ul>` elements via `browser-use eval` with a JS snippet, in addition to the full-page text. These elements commonly contain structured date lists.

**BU-R4: Signal for LLM re-call**  
`navigate_and_extract()` currently returns text but the caller (`scraper_v2.py`) may not re-call the LLM if regex already ran. Add a return flag `needs_llm: bool` to `extract_via_browser_use()` so `scraper_v2.py` always re-runs LLM extraction on browser-sourced text regardless of prior regex results.

---

### 2. `text_extractor.py` — new function

**`extract_conference_name(html: str) -> str | None`**

Logic:
1. Parse `<title>` tag text.
2. Search `<h1>` and `<h2>` tags.
3. Apply regex `[A-Z]{2,8}\s+\d{4}` to each candidate string.
4. Return the first match, or `None` if not found.

Called once per URL during static download in `scraper_v2.py`.

---

### 3. `scraper_v2.py` — two additions

**Addition 1: Extract and store `conference_name`**  
After `_static_download()` succeeds (before JS renderer), call `extract_conference_name(html)` and store the result in the row dict.

**Addition 2: Compute and store `fields_found`**  
At the end of each URL's processing, count how many of the 5 DATE_KEYS have a non-None value. Store as string `"N/5"` (e.g., `"3/5"`).

---

### 4. `config.py` — two new column labels

```python
COLUMN_LABELS = {
    ...
    "conference_name": "Conference Name",  # NEW — first column
    "fields_found": "Fields Found",        # NEW — after Registration Deadline
}
```

**Column order in output Excel:**

| # | Column | Source |
|---|--------|--------|
| 1 | Conference Name | text_extractor (static HTML) |
| 2 | URL | input |
| 3 | Start Date | pipeline |
| 4 | End Date | pipeline |
| 5 | Submission Deadline | pipeline |
| 6 | Acceptance Notification | pipeline |
| 7 | Registration Deadline | pipeline |
| 8 | Fields Found | computed |
| 9 | Method | pipeline |
| 10 | Notes | pipeline |

---

## Out of Scope

- Topics extraction (deferred to next iteration).
- Failed URLs file (replaced by `Fields Found` column for manual triage).
- Changes to `regex_extractor.py`, `js_renderer.py`, `change_detector.py`, `shallow_crawler.py`.

---

## Success Criteria

- Re-running `python scraper_v2.py` produces a new `fechas_conferencias_v3.xlsx` with more filled date fields than before.
- `Conference Name` column is populated for the majority of URLs.
- `Fields Found` column allows sorting by completeness for manual review.
- No regression in URLs that previously extracted dates correctly.
