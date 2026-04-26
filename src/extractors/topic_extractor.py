"""
topic_extractor.py
==================
Extract conference topics/themes/tracks from HTML pages.

Strategies (tried in order, results merged):
  1. Headings-based: find headings with topic keywords, extract list items below
  2. List-based: find <ul>/<ol> near topic-related context
  3. Lettered/numbered paragraphs: "A) Topic Name", "1. Topic Name"
  4. Meta description fallback: extract theme from meta tags or page description

Returns a list of topic strings, cleaned and deduplicated.
"""

import re
import logging
from bs4 import BeautifulSoup, Tag

log = logging.getLogger(__name__)

# ─── Keywords that signal a topic/theme section ───────────────────

_TOPIC_HEADING_KEYWORDS = [
    "call for paper",
    "topics",
    "topic area",
    "theme",
    "track",
    "scope",
    "areas of interest",
    "research area",
    "subject area",
    "conference scope",
    "technical area",
    "symposium topic",
    "workshop topic",
    "submission topic",
    "paper topic",
    "scope of the conference",
    "scope of conference",
    "conference theme",
    "main theme",
    "main topic",
    "topics of interest",
    "topics include",
    "áreas temáticas",
    "temas",
    "ejes temáticos",
    "líneas temáticas",
]

# Keywords in class/id attributes of containers
_TOPIC_ATTR_KEYWORDS = [
    "topic", "track", "theme", "scope", "cfp", "call-for-paper",
    "areas", "subjects",
]

# ─── Noise filters ────────────────────────────────────────────────

# Lines that are NOT real topics — navigation, boilerplate, dates, etc.
_NOISE_PATTERNS = re.compile(
    r"(?i)(?:"
    r"submit|deadline|notification|registration|camera.ready|"
    r"accepted|rejected|download|click here|read more|learn more|"
    r"copyright|all rights reserved|privacy|cookie|"
    r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|"   # dates
    r"\d{4}[/\-]\d{1,2}[/\-]\d{1,2}|"      # ISO dates
    r"^\d+$|"                                # bare numbers
    r"^https?://|"                            # URLs
    r"^[a-z]+@|"                             # emails
    r"university\b|professor\b|"             # people/affiliations
    r"\bdr\.\s|chair\b|co-chair|"            # committee roles
    r"keynote|plenary|invited\s+speaker|"    # speaker sections
    r"inauguration|valedictory|"             # ceremony events
    r"zoom\s+in|digital\s+certificate|"      # UI/boilerplate
    r"admission\s+to|^current:|"             # navigation
    r"call\s+for\s+papers?\b|"              # CFP links (not topics)
    r"conference\s+rules|guidelines\s+for|"  # rules
    r"access\s+here|official|"               # navigation text
    r"^\d+\s+\w+,?\s+\d{4}|"               # dates like "30 September, 2026"
    r"general\s+chair|program\s+chair|"     # committee roles
    r"organizing\s+(?:chair|committee|secretar)|"
    r"technical\s+program|publicity\s+chair|"
    r"template|docx\s+format|opendocument|" # submission templates
    r"camera.?ready|full\s+paper|poster|"   # submission types
    r"doctoral\s+consortium|"
    r"abstracts?\s+submission|"
    r"networking\s+opportunit|"             # registration perks
    r"session\s+recording|"
    r"on-?line\s+access|proceedings|"       # perks/publication info
    # Navigation menu items (TAEE et al.)
    r"^(?:home|news|about|contact|venue|program|programme|sponsors?|"
    r"committees?|awards?|accommodation|wifi|exhibition|gallery|"
    r"important\s+dates?|extended\s+program|summary\s+program|"
    r"abstract\s+book|instructions?\s+for|conference\s+venue|"
    r"about\s+\w+)\s*$|"
    r"\bcertificate\b|"                     # any certificate mention (ICACIT)
    # Pure organization names ("The Institution of...", "X College of...")
    r"^the\s+(?:institut(?:e|ion)|society|association|foundation)\s+of\s+|"
    r"\b(?:college|institute|society|foundation)\s+of\s+(?:engineer|technolog|science)"
    r")"
)

_MIN_TOPIC_LEN = 5
_MAX_TOPIC_LEN = 200

# Intro/boilerplate phrases that are NOT topics
_INTRO_PATTERNS = re.compile(
    r"(?i)(?:"
    r"but not limited to|"
    r"conference aims|"
    r"the following|"
    r"papers should be related|"
    r"encouraged to submit|"
    r"including but not|"
    r"interested in|"
    r"we invite|"
    r"we welcome|"
    r"are welcome|"
    r"abstract only|"
    r"book of abstracts|"
    r"will be published|"
    r"will be given the chance|"
    r"springer book series|"
    r"lecture notes in|"
    r"scopus indexed|"
    r"peer.review|"
    r"blind review|"
    r"book series|"
    r"conference fee|"
    r"payment|"
    r"each submission|"
    r"authors must prepare|"
    r"institute of technology"
    r")"
)
_MAX_TOPICS = 30

# ─── Cleaning helpers ─────────────────────────────────────────────


def _clean_topic(text: str) -> str:
    """Normalize a single topic string."""
    # Strip leading bullets, letters, numbers
    text = re.sub(r"^[\s\-•·▪►▸‣⦁◦○●★☆✦✧]+", "", text)
    text = re.sub(r"^[A-Za-z]\)\s*", "", text)           # "A) Topic"
    text = re.sub(r"^\d+[.)]\s*", "", text)               # "1. Topic" or "1) Topic"
    text = re.sub(r"^(?:Track|Area|Theme)\s*\d*\s*[:\-–—]\s*", "", text, flags=re.I)
    text = text.strip().rstrip(".")
    # Remove trailing parenthetical notes like "(NEW)" or "(session 1)"
    text = re.sub(r"\s*\([^)]{0,30}\)\s*$", "", text)
    return text.strip()


def _is_valid_topic(text: str) -> bool:
    """Return True if text looks like a real conference topic."""
    if len(text) < _MIN_TOPIC_LEN or len(text) > _MAX_TOPIC_LEN:
        return False
    if _NOISE_PATTERNS.search(text):
        return False
    if _INTRO_PATTERNS.search(text):
        return False
    # Must contain at least one letter
    if not re.search(r"[A-Za-z]", text):
        return False
    # Reject likely paper titles: contain author names (comma-separated proper nouns)
    # Pattern: text with multiple comma-separated capitalized names
    if len(re.findall(r"[A-Z][a-z]+\s+[A-Z][a-z]+", text)) >= 3:
        return False
    # Reject committee/affiliation lines: "Name Surname, Title, Institution, Country"
    # Heuristic: starts with capitalized name and contains >=2 commas
    if text.count(",") >= 2 and re.match(r"^[A-Z][a-z]+\s+[A-Z]", text):
        return False
    # Reject lines containing common academic title abbreviations (committee bios)
    if re.search(r"\b(?:M\.Sc\.?|Ph\.?D\.?|Prof\.?|Dr\.?|Mr\.?|Mrs\.?|Ms\.?)\s", text):
        return False
    return True


def _dedup_topics(topics: list[str]) -> list[str]:
    """Remove duplicates (case-insensitive) preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for t in topics:
        key = t.lower().strip()
        if key not in seen:
            seen.add(key)
            result.append(t)
    return result


# ─── Extraction strategies ────────────────────────────────────────


def _matches_topic_keyword(text: str) -> bool:
    """True if text contains a topic-section keyword."""
    low = text.lower()
    return any(kw in low for kw in _TOPIC_HEADING_KEYWORDS)


def _collect_list_items(tag: Tag, max_items: int = 50) -> list[str]:
    """Extract text from <li> descendants of a list tag, or <dd>/<dt> from <dl>.

    Uses recursive=True for <li> because some sites wrap list items in
    formatting tags like <b> or <strong> (e.g., <ul><b><li>...</li></b></ul>).
    """
    items: list[str] = []
    if tag.name in ("ul", "ol"):
        for li in tag.find_all("li"):
            text = li.get_text(separator=" ", strip=True)
            if text:
                items.append(text)
    elif tag.name == "dl":
        for dt in tag.find_all("dt"):
            text = dt.get_text(separator=" ", strip=True)
            if text:
                items.append(text)
    if len(items) > max_items:
        items = items[:max_items]
    return items


def _extract_topics_by_headings(soup: BeautifulSoup) -> list[str]:
    """Strategy 1: find topic-related headings and extract lists/content below.

    Uses find_all_next() (not just siblings) to handle cases where the heading
    and topic list are in different container levels.
    """
    topics: list[str] = []

    for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        heading_text = heading.get_text(strip=True)
        if not _matches_topic_keyword(heading_text):
            continue

        heading_level = int(heading.name[1]) if heading.name[0] == "h" else 99
        found_lists = False

        # Walk forward through ALL descendants (not just siblings)
        for elem in heading.find_all_next():
            if not isinstance(elem, Tag):
                continue
            # Stop at next heading of same or higher level
            if elem.name and elem.name[0] == "h" and elem.name[1:].isdigit():
                if int(elem.name[1]) <= heading_level and elem != heading:
                    # If the blocking heading also has topic keywords, continue
                    if _matches_topic_keyword(elem.get_text(strip=True)):
                        continue
                    break

            # Collect from lists
            if elem.name in ("ul", "ol", "dl"):
                items = _collect_list_items(elem)
                if items:
                    topics.extend(items)
                    found_lists = True

            # Collect from paragraphs with inline topic lists
            if elem.name == "p":
                # <br>-separated topic list (common when no <ul>)
                if elem.find("br"):
                    parts = re.split(r'<br\s*/?>', str(elem))
                    lines = []
                    for part in parts:
                        t = BeautifulSoup(part, 'html.parser').get_text(strip=True)
                        if t:
                            lines.append(t)
                    if len(lines) >= 2:
                        topics.extend(lines)
                        found_lists = True
                        continue

                text = elem.get_text(separator=" ", strip=True)
                # "A) X; B) Y; C) Z" — semicolon-separated lettered list
                if re.search(r"[A-Z]\)\s+\w", text):
                    parts = re.split(r";\s*(?=[A-Z]\))", text)
                    for part in parts:
                        m = re.match(r"[A-Z]\)\s*(.*)", part.strip())
                        if m:
                            topics.append(m.group(1).strip())
                    found_lists = True

            # Stop after collecting enough (don't scan entire page)
            if len(topics) > 40:
                break

        # If no lists were found, try collecting bold/strong/accordion items —
        # but ONLY within a small window after the heading, and only when the
        # <strong>/<b> is inside an <li> or a topic-flagged container. This
        # prevents harvesting committee names, banner subtitles, and dates
        # that happen to live in <strong> tags far below the heading.
        if not found_lists:
            elems_scanned = 0
            for elem in heading.find_all_next():
                if not isinstance(elem, Tag):
                    continue
                elems_scanned += 1
                if elems_scanned > 80:  # hard window after heading
                    break
                if elem.name and elem.name[0] == "h" and elem.name[1:].isdigit():
                    if int(elem.name[1]) <= heading_level and elem != heading:
                        if _matches_topic_keyword(elem.get_text(strip=True)):
                            continue
                        break
                if elem.name in ("strong", "b"):
                    text = elem.get_text(strip=True)
                    if not text or not (_MIN_TOPIC_LEN <= len(text) <= _MAX_TOPIC_LEN):
                        continue
                    # Require parent context that suggests a list item or topic block
                    parent_li = elem.find_parent("li")
                    parent_topic = False
                    for parent in elem.parents:
                        if not isinstance(parent, Tag) or parent.name in (None, "body", "html"):
                            break
                        attrs = " ".join([
                            " ".join(parent.get("class", [])),
                            parent.get("id", "") or "",
                        ]).lower()
                        if any(kw in attrs for kw in _TOPIC_ATTR_KEYWORDS):
                            parent_topic = True
                            break
                    if parent_li or parent_topic:
                        topics.append(text)
                # Accordion/toggle widget titles (Elementor, Bootstrap, etc.)
                if elem.name in ("a", "button", "div", "span"):
                    classes = " ".join(elem.get("class", [])).lower()
                    if any(kw in classes for kw in
                           ("accordion", "toggle", "tab-title", "panel-title")):
                        text = elem.get_text(strip=True)
                        if text and _MIN_TOPIC_LEN <= len(text) <= _MAX_TOPIC_LEN:
                            topics.append(text)
                if len(topics) > 40:
                    break

    return topics


def _extract_topics_by_container_attrs(soup: BeautifulSoup) -> list[str]:
    """Strategy 2: find containers with topic-related class/id attributes."""
    topics: list[str] = []

    for tag in soup.find_all(["div", "section", "article"]):
        attrs_text = " ".join([
            str(tag.get("class", "")),
            str(tag.get("id", "")),
        ]).lower()

        if not any(kw in attrs_text for kw in _TOPIC_ATTR_KEYWORDS):
            continue

        # Skip overly large containers
        text = tag.get_text(separator="\n", strip=True)
        if len(text) > 5000:
            continue

        # Extract from lists inside this container
        for list_tag in tag.find_all(["ul", "ol", "dl"]):
            topics.extend(_collect_list_items(list_tag))

        # If no lists, try extracting from <li> directly (in case nested)
        if not topics:
            for li in tag.find_all("li"):
                text = li.get_text(separator=" ", strip=True)
                if text:
                    topics.append(text)

    return topics


def _extract_topics_by_lettered_paragraphs(soup: BeautifulSoup) -> list[str]:
    """Strategy 3: find lettered/numbered topic lists in paragraphs.

    Matches patterns like:
      "A) Information and Knowledge Management"
      "1. Artificial Intelligence in Education"
      Inline: "A) Topic A; B) Topic B; C) Topic C"
    """
    topics: list[str] = []

    for p in soup.find_all("p"):
        text = p.get_text(separator=" ", strip=True)
        # Check if this paragraph or its context is topic-related
        context = ""
        prev = p.find_previous_sibling(["h1", "h2", "h3", "h4", "h5", "h6", "p"])
        if prev:
            context = prev.get_text(strip=True)

        is_topic_context = _matches_topic_keyword(context) or _matches_topic_keyword(text)

        if not is_topic_context:
            # Also check for "themes proposed", "related with" patterns
            if not re.search(r"(?i)theme|related with.*main|topics? include", text):
                continue

        # Inline semicolon-separated lettered list: "A) X; B) Y; C) Z"
        if re.search(r"[A-Z]\)\s+\w", text) and ";" in text:
            parts = re.split(r";\s*(?=[A-Z]\))", text)
            for part in parts:
                m = re.match(r"[A-Z]\)\s*(.*)", part.strip())
                if m:
                    topics.append(m.group(1).strip().rstrip(";."))
            continue

        # Single lettered line: "A) Topic Name"
        m = re.match(r"^[A-Z]\)\s+(.+)", text)
        if m:
            topics.append(m.group(1))
            continue

        # Numbered line: "1. Topic Name"
        m = re.match(r"^\d+[.)]\s+(.+)", text)
        if m:
            topics.append(m.group(1))
            continue

    return topics


def _extract_topics_from_tables(soup: BeautifulSoup) -> list[str]:
    """Strategy 4: extract topics from tables with track/topic headers."""
    topics: list[str] = []

    for table in soup.find_all("table"):
        table_text = table.get_text(separator=" ", strip=True).lower()
        if not any(kw in table_text for kw in ["track", "topic", "theme", "area", "scope"]):
            continue

        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            for cell in cells:
                text = cell.get_text(separator=" ", strip=True)
                if text and _MIN_TOPIC_LEN <= len(text) <= _MAX_TOPIC_LEN:
                    # Skip cells that are just numbers or dates
                    if not re.match(r"^\d+$", text) and not re.search(r"\d{4}", text):
                        topics.append(text)

    return topics


def _extract_topics_by_accordion(soup: BeautifulSoup) -> list[str]:
    """Strategy 5: extract track/topic names from accordion/toggle widgets.

    Common in Elementor, Bootstrap, and similar page-builder sites where
    tracks are presented as expandable accordion items.
    """
    topics: list[str] = []
    seen: set[str] = set()

    for elem in soup.find_all(["a", "button", "div", "span"], class_=True):
        classes = " ".join(elem.get("class", [])).lower()
        if not any(kw in classes for kw in
                   ("accordion-title", "toggle-title", "tab-title",
                    "panel-title", "collapse-title")):
            continue
        text = elem.get_text(strip=True)
        if not text or text.lower() in seen:
            continue
        if _MIN_TOPIC_LEN <= len(text) <= _MAX_TOPIC_LEN:
            seen.add(text.lower())
            topics.append(text)

    return topics


def _extract_topics_by_comma_text(soup: BeautifulSoup) -> list[str]:
    """Strategy 6: extract topics from comma-separated paragraphs.

    Pattern (e.g. SCIS 2026):
      <p>...topics covered...not limited to...:</p>
      <p><strong>Category Name:</strong></p>
      <p>Topic A, Topic B, Topic C, ...</p>

    Only activates when page text contains a topic-context signal.
    """
    topics: list[str] = []
    in_context = False

    for elem in soup.find_all(["p", "h2", "h3", "h4", "h5", "h6"]):
        text = elem.get_text(separator=" ", strip=True)

        # Detect topic context entry
        if not in_context:
            if _matches_topic_keyword(text):
                in_context = True
            continue

        # Stop context at non-topic headings
        if elem.name in ("h2", "h3", "h4", "h5", "h6"):
            if not _matches_topic_keyword(text):
                if topics:
                    break
                in_context = False
                continue

        # <strong> acting as category heading  ("Sustainable Computing:")
        strong = elem.find(["strong", "b"])
        if strong:
            s_text = strong.get_text(strip=True)
            if s_text.endswith(":") and len(s_text) < 100:
                cat_name = s_text.rstrip(":").strip()
                if _MIN_TOPIC_LEN <= len(cat_name) <= _MAX_TOPIC_LEN:
                    topics.append(cat_name)
                continue

        # Comma-separated items (require >= 4 commas to avoid prose)
        commas = text.count(",")
        if commas >= 4:
            items = [i.strip().rstrip(".") for i in text.split(",")]
            valid = [i for i in items
                     if _MIN_TOPIC_LEN <= len(i) <= _MAX_TOPIC_LEN]
            if len(valid) >= 3:
                # Heuristic: topic phrases average ≤ 8 words
                avg_words = sum(len(i.split()) for i in valid) / len(valid)
                if avg_words <= 8:
                    topics.extend(valid)

    return topics


# ─── Public API ───────────────────────────────────────────────────


def extract_topics(html: str) -> list[str]:
    """
    Extract conference topics/themes/tracks from HTML.

    Tries multiple strategies and merges results:
      1. Headings-based (most reliable)
      2. Container attributes (class/id with topic keywords)
      3. Lettered/numbered paragraphs
      4. Tables with track/topic content

    Returns a cleaned, deduplicated list of topic strings.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove non-content tags
    for tag in soup(["script", "style", "nav", "footer", "header",
                     "noscript", "aside", "iframe", "svg", "form"]):
        tag.decompose()

    # Also decompose <ul>/<ol>/<div> that are clearly navigation/menus,
    # even if they're not inside a <nav> tag (TAEE-style menus).
    _NAV_ATTR_RE = re.compile(
        r"(?:^|[\s_-])(?:menu|nav|navbar|navigation|breadcrumb|sidebar|"
        r"site-?header|site-?footer|topbar|main-?menu|sub-?menu|"
        r"widget|widget_)(?:[\s_-]|$)",
        re.I,
    )
    for tag in soup.find_all(["ul", "ol", "div", "section"]):
        # Skip tags already detached by an earlier decompose()
        if tag.attrs is None or tag.parent is None:
            continue
        classes = tag.get("class", []) or []
        if isinstance(classes, str):
            classes = [classes]
        attrs = " ".join([
            " ".join(classes),
            tag.get("id", "") or "",
            tag.get("role", "") or "",
        ])
        if attrs.strip() and _NAV_ATTR_RE.search(attrs):
            tag.decompose()

    # Try strategies in order
    all_topics: list[str] = []

    heading_topics = _extract_topics_by_headings(soup)
    if heading_topics:
        log.info("  [TopicExtractor] Headings strategy: %d raw items", len(heading_topics))
        all_topics.extend(heading_topics)

    attr_topics = _extract_topics_by_container_attrs(soup)
    if attr_topics:
        log.info("  [TopicExtractor] Container-attrs strategy: %d raw items", len(attr_topics))
        all_topics.extend(attr_topics)

    # Only try secondary strategies if primary didn't yield much
    if len(all_topics) < 3:
        lettered = _extract_topics_by_lettered_paragraphs(soup)
        if lettered:
            log.info("  [TopicExtractor] Lettered-paragraphs strategy: %d raw items", len(lettered))
            all_topics.extend(lettered)

        table_topics = _extract_topics_from_tables(soup)
        if table_topics:
            log.info("  [TopicExtractor] Table strategy: %d raw items", len(table_topics))
            all_topics.extend(table_topics)

        accordion_topics = _extract_topics_by_accordion(soup)
        if accordion_topics:
            log.info("  [TopicExtractor] Accordion strategy: %d raw items", len(accordion_topics))
            all_topics.extend(accordion_topics)

        comma_topics = _extract_topics_by_comma_text(soup)
        if comma_topics:
            log.info("  [TopicExtractor] Comma-text strategy: %d raw items", len(comma_topics))
            all_topics.extend(comma_topics)

    # Clean and filter
    cleaned = [_clean_topic(t) for t in all_topics]
    valid = [t for t in cleaned if _is_valid_topic(t)]
    deduped = _dedup_topics(valid)

    if len(deduped) > _MAX_TOPICS:
        deduped = deduped[:_MAX_TOPICS]

    log.info("  [TopicExtractor] Final: %d topics extracted", len(deduped))
    return deduped
