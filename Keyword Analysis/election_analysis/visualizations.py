"""
visualizations.py
=================
All charts for the election-analysis framework.

Three plot families:

  1. keyword_trend()     – line/bar chart of keyword frequency over time
  2. top_jurisdictions() – horizontal bar chart of top counties/cities
  3. embedding_scatter() – 2-D word-embedding map (UMAP or PCA)

Each function returns a plotly Figure and optionally saves it to disk.
Interactive HTML files open in any browser; static PNG/SVG via kaleido.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import config

log = logging.getLogger(__name__)

_BRAND_COLOR   = "#1a56db"      # primary accent
_HIGHLIGHT     = "#e11d48"      # seed / keyword colour
_NEUTRAL       = "#94a3b8"      # non-highlighted words
_BG            = "white"
_FONT_FAMILY   = "Inter, Arial, sans-serif"


def _save(fig: go.Figure, name: str, fmt: str = "html") -> Path:
    out = config.PLOTS_DIR / f"{name}.{fmt}"
    if fmt == "html":
        fig.write_html(str(out))
    else:
        fig.write_image(str(out))
    log.info(f"Saved plot → {out}")
    return out


# ── 1. Keyword trend over time ─────────────────────────────────────────────────

def keyword_trend(trend_df: pd.DataFrame,
                  keyword: str = "election",
                  freq_label: str = "Month",
                  metric: str = "kw_per_doc",
                  save: bool = True,
                  fmt: str = "html") -> go.Figure:
    """
    Time-series chart of keyword frequency across the corpus.

    Parameters
    ----------
    trend_df   : output of Corpus.keyword_over_time()
    keyword    : label for chart titles
    freq_label : 'Month', 'Quarter', or 'Year'
    metric     : 'kw_per_doc' (normalised) or 'kw_mentions' (raw count)
    save       : write file to outputs/plots/
    fmt        : 'html' (interactive) or 'png'/'svg' (requires kaleido)

    Returns
    -------
    plotly Figure
    """
    metric_label = {
        "kw_per_doc" : f"'{keyword}' mentions per document",
        "kw_mentions": f"Total '{keyword}' mentions",
        "doc_count"  : "Number of documents",
    }.get(metric, metric)

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.06,
        subplot_titles=[
            f"<b>'{keyword.title()}' frequency over time</b>",
            "Number of documents"
        ],
    )

    # Top panel: keyword metric
    fig.add_trace(
        go.Scatter(
            x=trend_df["period"],
            y=trend_df[metric],
            mode="lines+markers",
            name=metric_label,
            line=dict(color=_BRAND_COLOR, width=2.5),
            marker=dict(size=5, color=_BRAND_COLOR),
            hovertemplate=f"%{{x}}<br>{metric_label}: %{{y:.3f}}<extra></extra>",
        ),
        row=1, col=1,
    )

    # 6-period rolling average
    rolling = trend_df[metric].rolling(6, center=True, min_periods=1).mean()
    fig.add_trace(
        go.Scatter(
            x=trend_df["period"],
            y=rolling,
            mode="lines",
            name="6-period rolling avg",
            line=dict(color=_HIGHLIGHT, width=1.5, dash="dash"),
            hovertemplate="%{x}<br>Rolling avg: %{y:.3f}<extra></extra>",
        ),
        row=1, col=1,
    )

    # Bottom panel: document count
    fig.add_trace(
        go.Bar(
            x=trend_df["period"],
            y=trend_df["doc_count"],
            name="Doc count",
            marker_color=_NEUTRAL,
            hovertemplate="%{x}<br>Docs: %{y}<extra></extra>",
        ),
        row=2, col=1,
    )

    fig.update_layout(
        height=550,
        font_family=_FONT_FAMILY,
        plot_bgcolor=_BG,
        paper_bgcolor=_BG,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis2_title=freq_label,
        yaxis_title=metric_label,
        yaxis2_title="# Documents",
        hovermode="x unified",
        margin=dict(t=70, b=40, l=60, r=20),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="#f0f0f0")

    if save:
        _save(fig, f"trend_{keyword}_{metric}", fmt)
    return fig


# ── 2. Top jurisdictions ───────────────────────────────────────────────────────

def top_jurisdictions(juris_df: pd.DataFrame,
                      keyword: str = "election",
                      metric: str = "kw_per_1k",
                      color_by: str = "state",
                      n: int = config.TOP_N_JURISDICTIONS,
                      save: bool = True,
                      fmt: str = "html") -> go.Figure:
    """
    Horizontal bar chart of jurisdictions with most keyword usage.

    Parameters
    ----------
    juris_df : output of Corpus.top_jurisdictions()
    keyword  : label for chart titles
    metric   : 'kw_per_1k' (density) or 'kw_mentions' (raw count)
    color_by : column to use for bar colour (e.g. 'state')
    n        : max bars to show
    """
    df = juris_df.head(n).copy()

    # Build label = "Jurisdiction (STATE)"
    if "jurisdiction" in df.columns and "state" in df.columns:
        df["label"] = df["jurisdiction"].fillna("Unknown") + " (" + df["state"].fillna("?") + ")"
    elif "jurisdiction" in df.columns:
        df["label"] = df["jurisdiction"].fillna("Unknown")
    else:
        df["label"] = df["state"].fillna("Unknown")

    metric_label = {
        "kw_per_1k"  : f"'{keyword}' mentions per 1,000 words",
        "kw_mentions": f"Total '{keyword}' mentions",
    }.get(metric, metric)

    color_col = color_by if color_by in df.columns else None

    fig = px.bar(
        df.sort_values(metric),
        x=metric,
        y="label",
        orientation="h",
        color=color_col,
        color_discrete_sequence=px.colors.qualitative.Set2,
        text=metric,
        hover_data={c: True for c in ["doc_count", "kw_mentions", "kw_per_1k"]
                    if c in df.columns},
        title=f"<b>Top jurisdictions by '{keyword}' usage</b>",
        labels={metric: metric_label, "label": "Jurisdiction"},
    )
    fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    fig.update_layout(
        height=max(400, n * 22),
        font_family=_FONT_FAMILY,
        plot_bgcolor=_BG,
        paper_bgcolor=_BG,
        yaxis=dict(autorange="reversed"),
        margin=dict(t=60, b=40, l=200, r=80),
        showlegend=color_col is not None,
    )
    fig.update_xaxes(showgrid=True, gridcolor="#f0f0f0")
    fig.update_yaxes(showgrid=False)

    if save:
        _save(fig, f"jurisdictions_{keyword}_{metric}", fmt)
    return fig


# ── 3. 2-D embedding scatter ───────────────────────────────────────────────────

def embedding_scatter(embed_df: pd.DataFrame,
                      keyword: str = "election",
                      seed_words: list[str] = None,
                      n_label: int = 30,
                      save: bool = True,
                      fmt: str = "html") -> go.Figure:
    """
    2-D scatter plot of the word embedding space.

    Words are coloured by:
      - RED   if they are seed / highlighted words
      - BLUE  gradient based on cosine similarity to the keyword
      - GREY  all other words

    Parameters
    ----------
    embed_df   : output of EmbeddingModel.embedding_dataframe()
    keyword    : label for chart title
    seed_words : words to label and highlight in red
    n_label    : how many top-similar words to label (non-seed)
    """
    df = embed_df.copy()
    seed_words = [w.lower() for w in (seed_words or [keyword])]

    # Assign display category
    df["category"] = df.apply(
        lambda r: "seed" if r["word"] in seed_words
                  else ("top_similar" if r["rank_sim"] <= n_label else "other")
                  if "rank_sim" in df.columns
                  else ("seed" if r["word"] in seed_words else "background"),
        axis=1,
    )

    # Rank by similarity (needed for label selection)
    df = df.sort_values("similarity_to_kw", ascending=False)
    df["rank_sim"] = range(1, len(df) + 1)
    df["category"] = df.apply(
        lambda r: "seed"       if r["word"] in seed_words
                  else "top"   if r["rank_sim"] <= n_label
                  else "other",
        axis=1,
    )

    size_map     = {"seed": 14, "top": 8, "other": 5}
    opacity_map  = {"seed": 1.0, "top": 0.85, "other": 0.35}
    color_map    = {"seed": _HIGHLIGHT, "top": _BRAND_COLOR, "other": _NEUTRAL}

    fig = go.Figure()

    for cat in ["other", "top", "seed"]:
        sub = df[df["category"] == cat]
        fig.add_trace(go.Scatter(
            x=sub["x"],
            y=sub["y"],
            mode="markers+text" if cat in ("seed", "top") else "markers",
            name={"seed": "Seed words", "top": f"Top {n_label} similar", "other": "Other"}[cat],
            text=sub["word"] if cat in ("seed", "top") else None,
            textposition="top center",
            textfont=dict(size=9 if cat == "top" else 11,
                          color=color_map[cat]),
            marker=dict(
                size=sub["frequency"].apply(lambda f: min(max(size_map[cat], f / 500), 18))
                     if cat == "other" else size_map[cat],
                color=color_map[cat],
                opacity=opacity_map[cat],
                line=dict(width=0),
            ),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Similarity to keyword: %{customdata[0]:.3f}<br>"
                "Corpus frequency: %{customdata[1]}<extra></extra>"
            ),
            customdata=sub[["similarity_to_kw", "frequency"]].values,
        ))

    fig.update_layout(
        title=f"<b>Word embedding space — '{keyword}' semantic neighbourhood</b><br>"
              f"<sup>Each dot = one word. Distance ≈ semantic similarity. "
              f"Red = seed words. Blue = top {n_label} most similar.</sup>",
        height=680,
        font_family=_FONT_FAMILY,
        plot_bgcolor="#fafafa",
        paper_bgcolor=_BG,
        xaxis=dict(showticklabels=False, showgrid=False, zeroline=False, title=""),
        yaxis=dict(showticklabels=False, showgrid=False, zeroline=False, title=""),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
        margin=dict(t=100, b=20, l=20, r=20),
    )

    if save:
        _save(fig, f"embedding_scatter_{keyword}", fmt)
    return fig


# ── 4. Semantic neighbours bar chart ──────────────────────────────────────────

def semantic_neighbours_bar(neighbours_df: pd.DataFrame,
                             keyword: str = "election",
                             save: bool = True,
                             fmt: str = "html") -> go.Figure:
    """
    Horizontal bar chart of the top semantically similar words to `keyword`.

    Parameters
    ----------
    neighbours_df : output of EmbeddingModel.semantic_neighbours()
    """
    df = neighbours_df.copy()

    fig = px.bar(
        df.sort_values("similarity"),
        x="similarity",
        y="word",
        orientation="h",
        text="similarity",
        color="similarity",
        color_continuous_scale=[[0, "#e0f2fe"], [1, _BRAND_COLOR]],
        title=f"<b>Words most semantically similar to '{keyword}'</b><br>"
              f"<sup>Cosine similarity in Word2Vec embedding space</sup>",
        labels={"similarity": "Cosine similarity", "word": ""},
    )
    fig.update_traces(texttemplate="%{text:.3f}", textposition="outside")
    fig.update_layout(
        height=max(350, len(df) * 24),
        font_family=_FONT_FAMILY,
        plot_bgcolor=_BG,
        paper_bgcolor=_BG,
        yaxis=dict(autorange="reversed"),
        coloraxis_showscale=False,
        margin=dict(t=80, b=40, l=130, r=60),
    )
    fig.update_xaxes(range=[0, 1.05], showgrid=True, gridcolor="#f0f0f0")
    fig.update_yaxes(showgrid=False)

    if save:
        _save(fig, f"semantic_neighbours_{keyword}", fmt)
    return fig


# ── 5. Co-occurrence bar chart ─────────────────────────────────────────────────

def cooccurrence_bar(cooc_df: pd.DataFrame,
                     keyword: str = "election",
                     n: int = 30,
                     save: bool = True,
                     fmt: str = "html") -> go.Figure:
    """
    Bar chart of terms that most often appear near `keyword`.

    Parameters
    ----------
    cooc_df : output of Corpus.top_cooccurring_terms()
    """
    df = cooc_df.head(n).copy()

    fig = px.bar(
        df.sort_values("co_count"),
        x="co_count",
        y="term",
        orientation="h",
        text="co_count",
        color="co_count",
        color_continuous_scale=[[0, "#fef3c7"], [1, "#d97706"]],
        title=f"<b>Terms most frequently co-occurring with '{keyword}'</b><br>"
              f"<sup>Within ±10-token window</sup>",
        labels={"co_count": "Co-occurrence count", "term": ""},
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(
        height=max(350, n * 22),
        font_family=_FONT_FAMILY,
        plot_bgcolor=_BG,
        paper_bgcolor=_BG,
        yaxis=dict(autorange="reversed"),
        coloraxis_showscale=False,
        margin=dict(t=80, b=40, l=130, r=60),
    )

    if save:
        _save(fig, f"cooccurrence_{keyword}", fmt)
    return fig


# ── 6. State comparison ────────────────────────────────────────────────────────

def state_comparison(corpus_df: pd.DataFrame,
                     keyword: str = "election",
                     save: bool = True,
                     fmt: str = "html") -> go.Figure:
    """
    Box plot comparing keyword density distributions across states.
    """
    df = corpus_df[corpus_df["has_keyword"]].copy()
    if "state" not in df.columns or df["state"].isna().all():
        log.warning("No 'state' column found; skipping state_comparison plot.")
        return go.Figure()

    fig = px.box(
        df,
        x="state",
        y="kw_per_1k",
        color="state",
        points="outliers",
        color_discrete_sequence=px.colors.qualitative.Set2,
        title=f"<b>Distribution of '{keyword}' density by state</b><br>"
              f"<sup>kw_per_1k = keyword mentions per 1,000 words; only docs containing keyword shown</sup>",
        labels={"kw_per_1k": f"'{keyword}' per 1,000 words", "state": "State"},
    )
    fig.update_layout(
        height=450,
        font_family=_FONT_FAMILY,
        plot_bgcolor=_BG,
        paper_bgcolor=_BG,
        showlegend=False,
        margin=dict(t=80, b=40, l=60, r=20),
    )

    if save:
        _save(fig, f"state_comparison_{keyword}", fmt)
    return fig
