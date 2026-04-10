"""
fetcher.py
==========
Two-mode data loader for the election-analysis framework:

  Mode A – Local file  : load a CSV / Excel file already on disk
  Mode B – Live scrape : authenticate with AgendaWatch and pull documents
                         for TARGET_STATES filtered by keyword

The output of both modes is a normalised pandas DataFrame with columns
defined in config.COLUMN_ALIASES.
"""

from __future__ import annotations

import json
import time
import hashlib
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

import config

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# ── Column normalisation ────────────────────────────────────────────────────────

def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename raw columns to the internal schema defined in config.COLUMN_ALIASES."""
    alias_map = {}
    for canonical, aliases in config.COLUMN_ALIASES.items():
        for col in df.columns:
            if col.lower().strip() in aliases:
                alias_map[col] = canonical
                break
    df = df.rename(columns=alias_map)
    # Ensure every expected column exists (fill with NaN if absent)
    for canon in config.COLUMN_ALIASES:
        if canon not in df.columns:
            df[canon] = pd.NA
    return df


def _parse_date(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce the 'date' column to datetime (UTC-naive)."""
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=False)
        df["year"]  = df["date"].dt.year
        df["month"] = df["date"].dt.to_period("M").astype(str)
    return df


# ── Mode A: local file ─────────────────────────────────────────────────────────

def load_local(path: str | Path,
               state_filter: Optional[list[str]] = None,
               keyword_filter: Optional[str] = None) -> pd.DataFrame:
    """
    Load a CSV or Excel file from disk.

    Parameters
    ----------
    path          : path to .csv or .xlsx file
    state_filter  : e.g. ["CO", "TX", "MO"] – keep only these states (None = all)
    keyword_filter: if given, keep only rows whose 'text' column contains this string

    Returns
    -------
    Normalised DataFrame
    """
    path = Path(path)
    log.info(f"Loading local file: {path}")

    if path.suffix.lower() in (".xlsx", ".xls", ".xlsm"):
        df = pd.read_excel(path, dtype=str)
    elif path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=str, low_memory=False)
    elif path.suffix.lower() == ".json":
        df = pd.read_json(path, dtype=str)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")

    df = _normalise_columns(df)
    df = _parse_date(df)

    if state_filter:
        state_filter_up = [s.upper() for s in state_filter]
        mask = df["state"].str.upper().isin(state_filter_up)
        df = df[mask].copy()
        log.info(f"After state filter {state_filter_up}: {len(df)} rows")

    if keyword_filter:
        text_col = df["text"].fillna("") + " " + df["title"].fillna("")
        mask = text_col.str.contains(keyword_filter, case=False, regex=False)
        df = df[mask].copy()
        log.info(f"After keyword filter '{keyword_filter}': {len(df)} rows")

    log.info(f"Loaded {len(df)} rows from {path.name}")
    return df.reset_index(drop=True)


# ── Mode B: live AgendaWatch scrape ────────────────────────────────────────────

class AgendaWatchScraper:
    """
    Authenticated session scraper for agendawatch.org.

    Usage
    -----
    >>> scraper = AgendaWatchScraper()
    >>> scraper.login()                                     # uses config credentials
    >>> df = scraper.search("election", states=["CO","TX","MO"])
    >>> df.to_csv("data/raw_election_docs.csv", index=False)
    """

    BASE = config.AGENDAWATCH_BASE_URL

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (research-bot/election-analysis; contact: kate@stanford.edu)"
        })
        self._logged_in = False

    # ── authentication ──────────────────────────────────────────────────────────

    def login(self,
              email: str    = config.AGENDAWATCH_EMAIL,
              password: str = config.AGENDAWATCH_PASSWORD) -> None:
        """Log in to AgendaWatch (MuckRock) account."""
        if not email or not password:
            raise ValueError(
                "Set AW_EMAIL and AW_PASSWORD env vars, or edit config.py "
                "with your MuckRock credentials."
            )
        login_url = f"{self.BASE}/login/"
        r = self.session.get(login_url)
        r.raise_for_status()

        # Extract CSRF token
        soup = BeautifulSoup(r.text, "html.parser")
        csrf = (soup.find("input", {"name": "csrfmiddlewaretoken"}) or {}).get("value", "")

        payload = {
            "csrfmiddlewaretoken": csrf,
            "username": email,
            "password": password,
        }
        r2 = self.session.post(login_url, data=payload, headers={"Referer": login_url})
        if r2.url == login_url or "login" in r2.url:
            raise RuntimeError("Login failed – check your credentials.")
        self._logged_in = True
        log.info("Logged in to AgendaWatch successfully.")

    # ── search ──────────────────────────────────────────────────────────────────

    def search(self,
               keyword: str,
               states: list[str] = config.TARGET_STATES,
               max_pages: int    = config.MAX_PAGES_PER_QUERY) -> pd.DataFrame:
        """
        Search AgendaWatch for documents matching `keyword` in `states`.
        Returns a normalised DataFrame.
        """
        if not self._logged_in:
            raise RuntimeError("Call .login() before .search()")

        all_rows = []
        for state in states:
            rows = self._search_state(keyword, state, max_pages)
            all_rows.extend(rows)
            log.info(f"  {state}: {len(rows)} documents found")

        df = pd.DataFrame(all_rows)
        if df.empty:
            log.warning("No results returned – check keyword / states / login.")
            return pd.DataFrame(columns=list(config.COLUMN_ALIASES.keys()))

        df = _normalise_columns(df)
        df = _parse_date(df)
        return df.reset_index(drop=True)

    def _search_state(self, keyword: str, state: str, max_pages: int) -> list[dict]:
        """Paginate through search results for one state."""
        rows = []
        page = 1
        while True:
            cache_key = hashlib.md5(f"{keyword}|{state}|{page}".encode()).hexdigest()
            cache_file = config.CACHE_DIR / f"{cache_key}.json"

            if cache_file.exists():
                with open(cache_file) as f:
                    page_rows = json.load(f)
                log.debug(f"Cache hit: {state} p{page}")
            else:
                params = {"q": keyword, "state": state, "page": page}
                r = self.session.get(f"{self.BASE}/search/", params=params)
                r.raise_for_status()
                page_rows = self._parse_search_page(r.text, state)
                with open(cache_file, "w") as f:
                    json.dump(page_rows, f)
                time.sleep(config.REQUEST_DELAY_SEC)

            if not page_rows:
                break
            rows.extend(page_rows)
            page += 1
            if max_pages and page > max_pages:
                log.warning(f"Reached max_pages={max_pages} for {state}|{keyword}")
                break

        return rows

    def _parse_search_page(self, html: str, state: str) -> list[dict]:
        """
        Parse one page of AgendaWatch search results.

        NOTE: CSS selectors here are best-guesses based on typical Django/Wagtail
        search result pages. You may need to update these if AgendaWatch changes
        their HTML structure. Use browser DevTools on /search/ to verify.
        """
        soup = BeautifulSoup(html, "html.parser")
        rows = []

        # Try common result container patterns
        result_items = (
            soup.select(".search-result")
            or soup.select("article.result")
            or soup.select(".agenda-item")
            or soup.select("li.document")
            or soup.select(".result-item")
        )

        for item in result_items:
            row = {"state": state}

            # Title / link
            link_el = item.select_one("a[href]")
            if link_el:
                row["title"] = link_el.get_text(strip=True)
                href = link_el.get("href", "")
                row["url"] = href if href.startswith("http") else f"{self.BASE}{href}"

            # Date
            date_el = (item.select_one("time") or
                       item.select_one(".date") or
                       item.select_one("[data-date]"))
            if date_el:
                row["date"] = (date_el.get("datetime") or
                               date_el.get("data-date") or
                               date_el.get_text(strip=True))

            # Jurisdiction
            juris_el = (item.select_one(".jurisdiction") or
                        item.select_one(".entity") or
                        item.select_one(".agency"))
            if juris_el:
                row["jurisdiction"] = juris_el.get_text(strip=True)

            # Text snippet / body
            text_el = (item.select_one(".snippet") or
                       item.select_one(".excerpt") or
                       item.select_one("p"))
            if text_el:
                row["text"] = text_el.get_text(strip=True)

            # Document type
            type_el = item.select_one(".doc-type, .type, .badge")
            if type_el:
                row["doc_type"] = type_el.get_text(strip=True)

            rows.append(row)

        return rows

    def fetch_full_text(self, url: str) -> str:
        """
        Fetch the full text body of a single document URL.
        Returns empty string on failure.
        """
        cache_key = hashlib.md5(url.encode()).hexdigest()
        cache_file = config.CACHE_DIR / f"doc_{cache_key}.txt"

        if cache_file.exists():
            return cache_file.read_text()

        try:
            r = self.session.get(url, timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            # Try to find the main document body
            body = (soup.select_one(".document-body, .content, article, main") or
                    soup.find("body"))
            text = body.get_text(separator=" ", strip=True) if body else ""
            cache_file.write_text(text)
            time.sleep(config.REQUEST_DELAY_SEC)
            return text
        except Exception as e:
            log.warning(f"Could not fetch {url}: {e}")
            return ""

    def enrich_with_full_text(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        For each row in df, fetch the full document text and store in 'text' column.
        Only fetches if 'text' is missing or very short (< 200 chars).
        """
        df = df.copy()
        needs_text = df["text"].isna() | (df["text"].str.len() < 200)
        urls = df.loc[needs_text & df["url"].notna(), "url"]
        log.info(f"Enriching {len(urls)} documents with full text…")
        for idx, url in urls.items():
            df.at[idx, "text"] = self.fetch_full_text(url)
        return df


# ── Convenience function ───────────────────────────────────────────────────────

def load_data(source: str | Path | None = None,
              keyword: str = "election",
              states: list[str] = config.TARGET_STATES,
              live_scrape: bool = False) -> pd.DataFrame:
    """
    Master loader. Returns a normalised DataFrame.

    Parameters
    ----------
    source      : path to a local CSV/Excel, OR None to use live scrape
    keyword     : keyword to filter (Mode A) or search (Mode B)
    states      : state codes to include
    live_scrape : if True and source is None, scrape AgendaWatch live

    Examples
    --------
    # From a local file you downloaded:
    df = load_data("data/agendawatch_co_tx_mo.csv", keyword="election")

    # From a live scrape (requires credentials in config):
    df = load_data(live_scrape=True, keyword="election")
    """
    if source is not None:
        return load_local(source, state_filter=states, keyword_filter=keyword)
    elif live_scrape:
        scraper = AgendaWatchScraper()
        scraper.login()
        df = scraper.search(keyword, states=states)
        df = scraper.enrich_with_full_text(df)
        out = config.DATA_DIR / f"raw_{keyword}_{'_'.join(states)}.csv"
        df.to_csv(out, index=False)
        log.info(f"Saved raw data → {out}")
        return df
    else:
        raise ValueError("Provide either 'source' (local file path) or live_scrape=True")
