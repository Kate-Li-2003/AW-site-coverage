"""
config.py
=========
Centralised configuration for the AgendaWatch election-analysis framework.
Edit AGENDAWATCH_* and TARGET_* constants before running.
"""

import os
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent
DATA_DIR    = ROOT / "data"
OUTPUT_DIR  = ROOT / "outputs"
PLOTS_DIR   = OUTPUT_DIR / "plots"
REPORTS_DIR = OUTPUT_DIR / "reports"

for _d in (DATA_DIR, PLOTS_DIR, REPORTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── AgendaWatch credentials (set via env vars or edit here) ───────────────────
AGENDAWATCH_BASE_URL = "https://agendawatch.org"
AGENDAWATCH_EMAIL    = os.getenv("AW_EMAIL", "")        # your MuckRock email
AGENDAWATCH_PASSWORD = os.getenv("AW_PASSWORD", "")     # your MuckRock password

# ── Scraping behaviour ─────────────────────────────────────────────────────────
REQUEST_DELAY_SEC  = 1.5   # polite delay between requests
MAX_PAGES_PER_QUERY = 50   # safety cap; set None for unlimited
CACHE_DIR          = DATA_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── Target scope ───────────────────────────────────────────────────────────────
TARGET_STATES   = ["CO", "TX", "MO"]   # Colorado, Texas, Missouri
TARGET_KEYWORDS = ["election"]          # primary keyword(s) to search

# ── NLP settings ───────────────────────────────────────────────────────────────
EMBEDDING_DIM     = 100    # word2vec vector size
EMBEDDING_WINDOW  = 5      # word2vec context window
EMBEDDING_MIN_COUNT = 3    # ignore words with fewer occurrences
EMBEDDING_EPOCHS  = 10

TFIDF_MAX_FEATURES = 5_000
TFIDF_NGRAM_RANGE  = (1, 2)

UMAP_N_NEIGHBORS   = 15
UMAP_MIN_DIST      = 0.1
UMAP_N_COMPONENTS  = 2

TOP_N_NEIGHBORS    = 20    # how many semantically similar words to return
TOP_N_JURISDICTIONS = 25   # how many jurisdictions to show in bar charts

# ── Stop-words extra list (added on top of NLTK defaults) ─────────────────────
CUSTOM_STOPWORDS = {
    "agenda", "item", "motion", "moved", "seconded", "approved",
    "meeting", "council", "commission", "board", "committee",
    "minutes", "public", "comment", "staff", "report", "discussion",
    "action", "vote", "ayes", "nays", "abstain", "absent", "present",
    "resolution", "ordinance", "hearing", "carried", "unanimously",
    "regular", "special", "session"
}

# ── Column name aliases ─────────────────────────────────────────────────────────
# The framework normalises any input file to these internal column names.
# Keys = internal name; values = list of aliases found in real data files.
COLUMN_ALIASES = {
    "text"        : ["text", "body", "content", "full_text", "description", "summary"],
    "date"        : ["date", "meeting_date", "published", "pub_date", "document_date"],
    "jurisdiction": ["jurisdiction", "entity", "entity_name", "agency", "government",
                     "city", "county", "municipality"],
    "state"       : ["state", "state_code", "st"],
    "county"      : ["county", "county_name", "fips_county"],
    "doc_type"    : ["doc_type", "document_type", "type", "kind"],
    "title"       : ["title", "item_title", "agenda_item", "subject"],
    "url"         : ["url", "link", "source_url", "document_url"],
}
