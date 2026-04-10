"""
main.py
=======
Single entry-point for the election-analysis framework.

Use this either as:
  (a) a Python API  – import ElectionAnalyzer and call its methods
  (b) a CLI script  – python main.py --source data/myfile.csv --keyword election

The ElectionAnalyzer class wires together fetcher → corpus → embeddings → viz.

Quick-start (Python API)
------------------------
    from main import ElectionAnalyzer

    # From a local CSV you downloaded from AgendaWatch:
    az = ElectionAnalyzer.from_file("data/agendawatch_co_tx_mo.csv")
    az.run_all()                          # builds everything + saves all plots

    # From a live scrape (requires credentials in config.py / env vars):
    az = ElectionAnalyzer.from_live(keyword="election")
    az.run_all()

    # Interactive exploration:
    az.neighbours()                       # semantic neighbours table
    az.trend_chart()                      # time-series figure
    az.jurisdiction_chart()               # top jurisdictions figure
    az.embedding_chart()                  # 2-D word map figure
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

import config
from fetcher      import load_data, AgendaWatchScraper
from corpus       import Corpus
from embeddings   import EmbeddingModel
import visualizations as viz

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")


class ElectionAnalyzer:
    """
    High-level façade that chains all pipeline stages together.

    Architecture at a glance
    ------------------------
    load_data()          ← fetcher.py
        ↓
    Corpus.build()       ← corpus.py
        ↓
    EmbeddingModel.fit() ← embeddings.py
        ↓
    visualizations.*     ← visualizations.py
    """

    def __init__(self, df: pd.DataFrame, keyword: str = "election"):
        self.keyword = keyword.lower()
        self.df      = df
        self.corpus  : Optional[Corpus]         = None
        self.model   : Optional[EmbeddingModel] = None

    # ── constructors ───────────────────────────────────────────────────────────

    @classmethod
    def from_file(cls, path: str | Path,
                  keyword: str         = "election",
                  states: list[str]    = config.TARGET_STATES,
                  filter_keyword: bool = True) -> "ElectionAnalyzer":
        """
        Load from a local CSV or Excel file.

        Parameters
        ----------
        path           : path to CSV / XLSX file
        keyword        : keyword to analyse
        states         : state codes to keep (empty list = all states)
        filter_keyword : if True, keep only rows where keyword appears in text
                         (set False to analyse all docs, not just matching ones)
        """
        kw_filter = keyword if filter_keyword else None
        df = load_data(source=path, keyword=kw_filter, states=states or None)
        return cls(df, keyword)

    @classmethod
    def from_live(cls, keyword: str      = "election",
                  states: list[str]      = config.TARGET_STATES,
                  enrich_full_text: bool = True) -> "ElectionAnalyzer":
        """
        Authenticate with AgendaWatch and scrape live data.
        Requires AW_EMAIL / AW_PASSWORD env vars (or edits to config.py).
        """
        df = load_data(live_scrape=True, keyword=keyword, states=states)
        return cls(df, keyword)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame,
                       keyword: str = "election") -> "ElectionAnalyzer":
        """Wrap an already-loaded DataFrame."""
        return cls(df, keyword)

    # ── pipeline stages ────────────────────────────────────────────────────────

    def build_corpus(self, extra_text_cols: list[str] = None) -> "ElectionAnalyzer":
        """
        Step 1: preprocess text and build corpus-level statistics.
        Must be called before build_model() or any analysis method.
        """
        self.corpus = Corpus(self.df, keyword=self.keyword)
        self.corpus.build(extra_cols=extra_text_cols or ["title"])
        return self

    def build_model(self, **w2v_kwargs) -> "ElectionAnalyzer":
        """
        Step 2: train Word2Vec + TF-IDF.
        build_corpus() must be called first.
        """
        if self.corpus is None:
            self.build_corpus()
        self.model = EmbeddingModel(self.corpus)
        self.model.fit(**w2v_kwargs)
        return self

    def run_all(self, freq: str = "M", save: bool = True,
                fmt: str = "html") -> "ElectionAnalyzer":
        """
        Run the full pipeline and save all plots.

        Parameters
        ----------
        freq : time aggregation for trend chart – 'M', 'Q', or 'Y'
        save : write plots to outputs/plots/
        fmt  : 'html' (interactive) or 'png' / 'svg' (requires kaleido)
        """
        if self.corpus is None:
            self.build_corpus()
        if self.model is None:
            self.build_model()

        log.info("Generating all visualisations…")
        self.trend_chart(freq=freq, save=save, fmt=fmt)
        self.jurisdiction_chart(save=save, fmt=fmt)
        self.embedding_chart(save=save, fmt=fmt)
        self.neighbours_chart(save=save, fmt=fmt)
        self.cooccurrence_chart(save=save, fmt=fmt)
        self.state_chart(save=save, fmt=fmt)

        log.info(f"All plots saved to {config.PLOTS_DIR}")
        return self

    # ── analysis accessors ─────────────────────────────────────────────────────

    def neighbours(self, n: int = config.TOP_N_NEIGHBORS,
                   word: str = None) -> pd.DataFrame:
        """
        Return top-n semantically similar words to keyword (or any word).

        Example output for keyword='election':
          word        similarity  rank
          ballot      0.8821      1
          voting      0.8710      2
          fraud       0.8541      3
          ...
        """
        self._ensure_model()
        return self.model.semantic_neighbours(word or self.keyword, n=n)

    def top_docs(self, n: int = 10, keyword: str = None) -> pd.DataFrame:
        """Return n most keyword-dense documents (by TF-IDF score)."""
        self._ensure_model()
        return self.model.tfidf_top_docs(keyword or self.keyword, n=n)

    def trend_data(self, freq: str = "M") -> pd.DataFrame:
        """Return time-series aggregation of keyword frequency."""
        self._ensure_corpus()
        return self.corpus.keyword_over_time(freq=freq)

    def jurisdiction_data(self, n: int = config.TOP_N_JURISDICTIONS,
                          normalise: bool = True) -> pd.DataFrame:
        """Return top-n jurisdictions by keyword usage."""
        self._ensure_corpus()
        return self.corpus.top_jurisdictions(n=n, normalise=normalise)

    def cooccurrence_data(self, n: int = 50) -> pd.DataFrame:
        """Return top-n terms co-occurring with the keyword."""
        self._ensure_corpus()
        return self.corpus.top_cooccurring_terms(n=n)

    def sample_docs(self, n: int = 5) -> pd.DataFrame:
        """Return random sample documents that mention the keyword."""
        self._ensure_corpus()
        return self.corpus.sample_sentences(n=n)

    # ── chart methods ──────────────────────────────────────────────────────────

    def trend_chart(self, freq: str = "M", metric: str = "kw_per_doc",
                    save: bool = True, fmt: str = "html"):
        """Keyword frequency over time chart."""
        td = self.trend_data(freq)
        freq_label = {"M": "Month", "Q": "Quarter", "Y": "Year"}.get(freq, freq)
        return viz.keyword_trend(td, self.keyword, freq_label, metric, save, fmt)

    def jurisdiction_chart(self, metric: str = "kw_per_1k",
                           save: bool = True, fmt: str = "html"):
        """Top jurisdictions horizontal bar chart."""
        jd = self.jurisdiction_data()
        return viz.top_jurisdictions(jd, self.keyword, metric, save=save, fmt=fmt)

    def embedding_chart(self, n_words: int = 200,
                        seed_words: list[str] = None,
                        save: bool = True, fmt: str = "html"):
        """2-D word embedding scatter plot."""
        self._ensure_model()
        edf = self.model.embedding_dataframe(
            seed_words=seed_words or [self.keyword],
            n_words=n_words,
        )
        return viz.embedding_scatter(edf, self.keyword,
                                     seed_words=seed_words, save=save, fmt=fmt)

    def neighbours_chart(self, n: int = 20,
                          save: bool = True, fmt: str = "html"):
        """Bar chart of top semantic neighbours."""
        nd = self.neighbours(n=n)
        return viz.semantic_neighbours_bar(nd, self.keyword, save=save, fmt=fmt)

    def cooccurrence_chart(self, n: int = 30,
                            save: bool = True, fmt: str = "html"):
        """Bar chart of co-occurring terms."""
        cd = self.cooccurrence_data(n=n)
        return viz.cooccurrence_bar(cd, self.keyword, n=n, save=save, fmt=fmt)

    def state_chart(self, save: bool = True, fmt: str = "html"):
        """Box plot comparing keyword density across states."""
        self._ensure_corpus()
        return viz.state_comparison(self.corpus.df, self.keyword, save=save, fmt=fmt)

    # ── export ─────────────────────────────────────────────────────────────────

    def export_csv(self, path: str | Path = None) -> Path:
        """Export the enriched corpus DataFrame to CSV."""
        self._ensure_corpus()
        path = Path(path or config.REPORTS_DIR / f"corpus_{self.keyword}.csv")
        cols = [c for c in self.corpus.df.columns if not c.startswith("_")]
        self.corpus.df[cols].to_csv(path, index=False)
        log.info(f"Corpus exported → {path}")
        return path

    def save_model(self, path: str | Path = None) -> Path:
        """Save the Word2Vec model to disk for re-use."""
        self._ensure_model()
        return self.model.save(path)

    # ── internal ───────────────────────────────────────────────────────────────

    def _ensure_corpus(self):
        if self.corpus is None:
            self.build_corpus()

    def _ensure_model(self):
        if self.model is None:
            self.build_model()


# ── CLI entry point ────────────────────────────────────────────────────────────

def _cli():
    parser = argparse.ArgumentParser(
        description="Election keyword analysis of AgendaWatch documents"
    )
    parser.add_argument("--source",    help="Path to local CSV/Excel data file")
    parser.add_argument("--keyword",   default="election", help="Keyword to analyse")
    parser.add_argument("--states",    default="CO,TX,MO",
                        help="Comma-separated state codes (default: CO,TX,MO)")
    parser.add_argument("--freq",      default="M",
                        choices=["M", "Q", "Y"],
                        help="Time aggregation: M=monthly Q=quarterly Y=yearly")
    parser.add_argument("--fmt",       default="html",
                        choices=["html", "png", "svg"],
                        help="Plot output format")
    parser.add_argument("--live",      action="store_true",
                        help="Scrape AgendaWatch live (requires credentials)")
    parser.add_argument("--no-filter", action="store_true",
                        help="Don't filter rows by keyword (analyse all docs)")
    args = parser.parse_args()

    states = [s.strip().upper() for s in args.states.split(",")]

    if args.source:
        az = ElectionAnalyzer.from_file(
            args.source,
            keyword=args.keyword,
            states=states,
            filter_keyword=not args.no_filter,
        )
    elif args.live:
        az = ElectionAnalyzer.from_live(keyword=args.keyword, states=states)
    else:
        parser.error("Provide --source <file> or --live")

    az.run_all(freq=args.freq, fmt=args.fmt)
    print(f"\nDone. Plots in: {config.PLOTS_DIR}")
    print(f"Top 10 semantic neighbours of '{args.keyword}':")
    print(az.neighbours(10).to_string(index=False))


if __name__ == "__main__":
    _cli()
