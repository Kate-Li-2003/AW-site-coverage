"""
corpus.py
=========
Text preprocessing and corpus management.

Responsibilities
----------------
- Clean and tokenise raw meeting-document text
- Build per-document and corpus-level token lists
- Count keyword occurrences (raw + normalised)
- Produce summary metadata used by visualisations
"""

from __future__ import annotations

import re
import logging
from collections import Counter
from typing import Optional

import pandas as pd
import config

log = logging.getLogger(__name__)

# ── Built-in English stopwords (fallback if NLTK is unavailable) ──────────────
_BUILTIN_STOPWORDS = {
    "a","about","above","after","again","against","all","am","an","and","any",
    "are","aren't","as","at","be","because","been","before","being","below",
    "between","both","but","by","can't","cannot","could","couldn't","did",
    "didn't","do","does","doesn't","doing","don't","down","during","each",
    "few","for","from","further","get","had","hadn't","has","hasn't","have",
    "haven't","having","he","he'd","he'll","he's","her","here","here's","hers",
    "herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've",
    "if","in","into","is","isn't","it","it's","its","itself","let's","me",
    "more","most","mustn't","my","myself","no","nor","not","of","off","on",
    "once","only","or","other","ought","our","ours","ourselves","out","over",
    "own","same","shan't","she","she'd","she'll","she's","should","shouldn't",
    "so","some","such","than","that","that's","the","their","theirs","them",
    "themselves","then","there","there's","these","they","they'd","they'll",
    "they're","they've","this","those","through","to","too","under","until",
    "up","very","was","wasn't","we","we'd","we'll","we're","we've","were",
    "weren't","what","what's","when","when's","where","where's","which","while",
    "who","who's","whom","why","why's","will","with","won't","would","wouldn't",
    "you","you'd","you'll","you're","you've","your","yours","yourself","yourselves",
    # common extras
    "also","one","two","three","may","shall","said","per","year","years",
    "new","will","just","use","used","can","us","would","also","however",
    "within","without","including","following","regarding","pursuant","section",
    "fiscal","county","city","town","village","district","department",
}

# ── NLTK (optional – better stopwords + lemmatization) ────────────────────────
_LEMMATIZER = None
_STOP_WORDS: set[str] = _BUILTIN_STOPWORDS | config.CUSTOM_STOPWORDS

try:
    import nltk as _nltk
    from nltk.corpus import stopwords as _sw
    from nltk.stem import WordNetLemmatizer as _WNL
    _STOP_WORDS = set(_sw.words("english")) | config.CUSTOM_STOPWORDS
    _LEMMATIZER = _WNL()
    log.debug("NLTK stopwords and lemmatizer loaded.")
except Exception:
    log.info("NLTK data not available – using built-in stopwords, no lemmatization.")
_RE_NONALPHA = re.compile(r"[^a-zA-Z\s]")
_RE_SPACES   = re.compile(r"\s+")


# ── Token utilities ────────────────────────────────────────────────────────────

def tokenise(text: str, lemmatize: bool = True, min_len: int = 3) -> list[str]:
    """
    Clean and tokenise a single text string.

    Steps
    -----
    1. Lowercase
    2. Strip non-alpha characters
    3. Tokenise on whitespace
    4. Remove stopwords + custom stopwords
    5. (Optional) lemmatize
    6. Drop tokens shorter than min_len

    Returns a list of clean tokens.
    """
    if not isinstance(text, str) or not text.strip():
        return []
    text = text.lower()
    text = _RE_NONALPHA.sub(" ", text)
    text = _RE_SPACES.sub(" ", text).strip()
    tokens = text.split()
    tokens = [t for t in tokens if t not in _STOP_WORDS and len(t) >= min_len]
    if lemmatize and _LEMMATIZER is not None:
        tokens = [_LEMMATIZER.lemmatize(t) for t in tokens]
    return tokens


def keyword_count(text: str, keyword: str) -> int:
    """Count raw (case-insensitive, whole-word) occurrences of keyword in text."""
    if not isinstance(text, str):
        return 0
    pattern = re.compile(r"\b" + re.escape(keyword.lower()) + r"\b")
    return len(pattern.findall(text.lower()))


# ── Corpus class ───────────────────────────────────────────────────────────────

class Corpus:
    """
    Wraps a normalised DataFrame and adds NLP-ready representations.

    Attributes (populated after .build())
    ------
    df           : original normalised DataFrame (with added derived columns)
    sentences    : list[list[str]] – tokenised documents (for Word2Vec)
    token_counts : Counter – corpus-wide term frequencies

    Key added columns in df
    -----------------------
    tokens        : list of clean tokens for this row
    kw_count      : raw count of the target keyword
    kw_per_1k     : keyword count per 1,000 words (normalised density)
    has_keyword   : bool – True if keyword appears at least once
    """

    def __init__(self, df: pd.DataFrame, keyword: str = "election"):
        self.df       = df.copy()
        self.keyword  = keyword.lower()
        self.sentences: list[list[str]] = []
        self.token_counts: Counter = Counter()
        self._built = False

    # ── build ──────────────────────────────────────────────────────────────────

    def build(self, text_col: str = "text", extra_cols: list[str] | None = None) -> "Corpus":
        """
        Process text and populate all derived attributes.

        Parameters
        ----------
        text_col    : name of the column containing document text
        extra_cols  : additional text columns to concatenate (e.g. ["title"])
        """
        log.info(f"Building corpus for keyword='{self.keyword}' on {len(self.df)} docs…")

        # Combine text columns
        combined = self.df[text_col].fillna("")
        if extra_cols:
            for col in extra_cols:
                if col in self.df.columns:
                    combined = combined + " " + self.df[col].fillna("")
        self.df["_combined_text"] = combined

        # Tokenise
        self.df["tokens"]   = self.df["_combined_text"].apply(tokenise)
        self.sentences       = self.df["tokens"].tolist()

        # Corpus-wide frequency
        self.token_counts = Counter(tok for toks in self.sentences for tok in toks)

        # Keyword stats
        self.df["kw_count"]   = self.df["_combined_text"].apply(
            lambda t: keyword_count(t, self.keyword)
        )
        word_counts           = self.df["tokens"].apply(len)
        self.df["kw_per_1k"]  = (
            self.df["kw_count"] / word_counts.replace(0, 1) * 1000
        ).round(4)
        self.df["has_keyword"] = self.df["kw_count"] > 0

        self._built = True
        n_with = self.df["has_keyword"].sum()
        log.info(
            f"Corpus built: {len(self.df)} docs, "
            f"{n_with} contain '{self.keyword}' ({n_with/len(self.df)*100:.1f}%)"
        )
        return self

    # ── summary helpers ────────────────────────────────────────────────────────

    def keyword_over_time(self, freq: str = "M") -> pd.DataFrame:
        """
        Aggregate keyword mentions by time period.

        Parameters
        ----------
        freq : pandas period alias – 'M' (month), 'Q' (quarter), 'Y' (year)

        Returns
        -------
        DataFrame with columns: period | doc_count | kw_mentions | kw_per_doc
        """
        self._check_built()
        df = self.df.dropna(subset=["date"]).copy()
        df["period"] = df["date"].dt.to_period(freq).astype(str)
        agg = (
            df.groupby("period")
              .agg(doc_count=("kw_count", "count"),
                   kw_mentions=("kw_count", "sum"))
              .reset_index()
        )
        agg["kw_per_doc"] = (agg["kw_mentions"] / agg["doc_count"]).round(3)
        return agg.sort_values("period")

    def top_jurisdictions(self, n: int = config.TOP_N_JURISDICTIONS,
                          normalise: bool = True) -> pd.DataFrame:
        """
        Top jurisdictions by keyword usage.

        Parameters
        ----------
        n         : number of jurisdictions to return
        normalise : if True, rank by kw_per_1k (density); else by raw kw_count

        Returns
        -------
        DataFrame: jurisdiction | state | doc_count | kw_mentions | kw_per_1k
        """
        self._check_built()
        group_cols = [c for c in ["jurisdiction", "state"] if c in self.df.columns
                      and self.df[c].notna().any()]
        if not group_cols:
            raise ValueError("No 'jurisdiction' or 'state' column with data.")
        agg = (
            self.df.groupby(group_cols)
                   .agg(doc_count=("kw_count", "count"),
                        kw_mentions=("kw_count", "sum"),
                        kw_per_1k=("kw_per_1k", "mean"))
                   .reset_index()
        )
        sort_col = "kw_per_1k" if normalise else "kw_mentions"
        return agg.nlargest(n, sort_col).reset_index(drop=True)

    def top_cooccurring_terms(self, n: int = 50,
                               context_window: int = 10) -> pd.DataFrame:
        """
        Find terms that most frequently co-occur with the keyword
        within a sliding window of ±context_window tokens.

        Returns DataFrame: term | co_count
        """
        self._check_built()
        kw = self.keyword
        co: Counter = Counter()
        for tokens in self.sentences:
            indices = [i for i, t in enumerate(tokens) if t == kw]
            for idx in indices:
                lo = max(0, idx - context_window)
                hi = min(len(tokens), idx + context_window + 1)
                context = tokens[lo:idx] + tokens[idx+1:hi]
                co.update(context)
        df = pd.DataFrame(co.most_common(n), columns=["term", "co_count"])
        return df

    def sample_sentences(self, n: int = 10, min_kw: int = 1) -> pd.DataFrame:
        """Return n random document excerpts that contain the keyword."""
        self._check_built()
        mask = self.df["kw_count"] >= min_kw
        sample = self.df[mask].sample(min(n, mask.sum()), random_state=42)
        cols = [c for c in ["date", "jurisdiction", "state", "title", "_combined_text"]
                if c in sample.columns]
        return sample[cols].rename(columns={"_combined_text": "text"})

    # ── internal ───────────────────────────────────────────────────────────────

    def _check_built(self):
        if not self._built:
            raise RuntimeError("Call .build() before using analysis methods.")
