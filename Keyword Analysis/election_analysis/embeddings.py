"""
embeddings.py
=============
Word embedding analysis on top of a built Corpus.

Provides three complementary lenses:

  1. Word2Vec semantic neighbours
     "What words live in the same semantic space as 'election'?"

  2. TF-IDF keyword importance
     "Which documents are most defined by election-related language?"

  3. 2-D embedding projection (UMAP → plotly scatter)
     "Show the whole vocabulary as a map; highlight the election cluster."

All heavy objects (Word2Vec model, TF-IDF matrix) are cached in-memory so
the notebook can call multiple query methods without re-fitting.
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import config
from corpus import Corpus

log = logging.getLogger(__name__)

try:
    import umap
    _HAS_UMAP = True
except ImportError:
    _HAS_UMAP = False
    log.warning("umap-learn not installed; 2-D projection will use PCA instead.")

try:
    from sklearn.decomposition import PCA as _PCA
except ImportError:
    _PCA = None


# ── EmbeddingModel ─────────────────────────────────────────────────────────────

class EmbeddingModel:
    """
    Trains and queries word embeddings for a Corpus.

    Quick-start
    -----------
    >>> em = EmbeddingModel(corpus).fit()
    >>> em.semantic_neighbours("election", n=20)
    >>> em.tfidf_top_docs("election", n=10)
    >>> em.embedding_dataframe(seed_words=["election","fraud","ballot"])
    """

    def __init__(self, corpus: Corpus):
        if not corpus._built:
            raise RuntimeError("Corpus must be .build()'d before creating EmbeddingModel.")
        self.corpus       = corpus
        self.keyword      = corpus.keyword
        self._w2v: Optional[Word2Vec] = None
        self._tfidf: Optional[TfidfVectorizer] = None
        self._tfidf_matrix = None
        self._fitted = False

    # ── fit ────────────────────────────────────────────────────────────────────

    def fit(self,
            vector_size: int  = config.EMBEDDING_DIM,
            window: int       = config.EMBEDDING_WINDOW,
            min_count: int    = config.EMBEDDING_MIN_COUNT,
            epochs: int       = config.EMBEDDING_EPOCHS) -> "EmbeddingModel":
        """
        Train Word2Vec on the corpus sentences and fit a TF-IDF vectoriser.

        Word2Vec approach (skip-gram by default):
        - Each meeting document = one "sentence"
        - Words close in the vector space share similar contexts
        - 'election' vector neighbours = words used in similar discussion contexts

        TF-IDF approach:
        - Represents each document as a weighted bag-of-words
        - High TF-IDF score = word is distinctive for that document
        - Used to find "most election-focused documents"
        """
        sentences = self.corpus.sentences
        n_sentences = len(sentences)
        log.info(f"Training Word2Vec on {n_sentences} documents "
                 f"(dim={vector_size}, window={window}, min_count={min_count})…")

        if n_sentences < 10:
            warnings.warn(
                f"Only {n_sentences} documents – Word2Vec quality will be poor. "
                "Consider using a larger corpus or lowering min_count."
            )

        self._w2v = Word2Vec(
            sentences=sentences,
            vector_size=vector_size,
            window=window,
            min_count=min_count,
            workers=4,
            sg=1,           # skip-gram (better for rare words)
            epochs=epochs,
            seed=42,
        )
        log.info(f"Vocabulary size: {len(self._w2v.wv)} words")

        # TF-IDF on raw (un-lemmatised) combined text for doc retrieval
        texts = self.corpus.df["_combined_text"].fillna("").tolist()
        self._tfidf = TfidfVectorizer(
            max_features=config.TFIDF_MAX_FEATURES,
            ngram_range=config.TFIDF_NGRAM_RANGE,
            stop_words="english",
            sublinear_tf=True,
        )
        self._tfidf_matrix = self._tfidf.fit_transform(texts)
        log.info("TF-IDF matrix fitted.")

        self._fitted = True
        return self

    # ── Word2Vec queries ───────────────────────────────────────────────────────

    def semantic_neighbours(self,
                             word: str = None,
                             n: int = config.TOP_N_NEIGHBORS) -> pd.DataFrame:
        """
        Return the top-n words most semantically similar to `word`.

        Returns
        -------
        DataFrame: word | similarity | rank
            similarity is cosine similarity in the embedding space (0–1)

        Example
        -------
        For word='election' you might get:
          ballot, voting, poll, fraud, certification, recount, candidate, …
        """
        self._check_fitted()
        word = (word or self.keyword).lower()
        if word not in self._w2v.wv:
            raise KeyError(
                f"'{word}' not in vocabulary. "
                f"Try a synonym or lower min_count in config.py."
            )
        similar = self._w2v.wv.most_similar(word, topn=n)
        df = pd.DataFrame(similar, columns=["word", "similarity"])
        df["rank"] = range(1, len(df) + 1)
        df["similarity"] = df["similarity"].round(4)
        return df

    def analogy(self, positive: list[str], negative: list[str],
                n: int = 10) -> pd.DataFrame:
        """
        Word analogy query: positive - negative → nearest neighbours.
        Example: positive=["election", "fraud"] negative=["vote"] → ?
        """
        self._check_fitted()
        results = self._w2v.wv.most_similar(positive=positive, negative=negative, topn=n)
        return pd.DataFrame(results, columns=["word", "similarity"])

    def in_vocabulary(self, word: str) -> bool:
        """Check whether a word is in the trained vocabulary."""
        self._check_fitted()
        return word.lower() in self._w2v.wv

    def vocabulary(self) -> list[str]:
        """Return all words in the vocabulary, sorted by frequency."""
        self._check_fitted()
        return sorted(self._w2v.wv.key_to_index.keys())

    # ── TF-IDF queries ─────────────────────────────────────────────────────────

    def tfidf_top_docs(self, keyword: str = None,
                       n: int = 10) -> pd.DataFrame:
        """
        Return the n documents with the highest TF-IDF score for `keyword`.
        These are documents where `keyword` is both frequent AND distinctive.

        Returns
        -------
        DataFrame: date | jurisdiction | state | title | tfidf_score | kw_count
        """
        self._check_fitted()
        keyword = (keyword or self.keyword).lower()
        vocab = self._tfidf.get_feature_names_out()
        if keyword not in vocab:
            raise KeyError(f"'{keyword}' not in TF-IDF vocabulary.")
        col_idx = list(vocab).index(keyword)
        scores = np.asarray(self._tfidf_matrix[:, col_idx].todense()).flatten()

        df = self.corpus.df.copy()
        df["tfidf_score"] = scores.round(5)
        keep_cols = [c for c in ["date", "jurisdiction", "state", "title",
                                  "tfidf_score", "kw_count", "url"]
                     if c in df.columns]
        return (df[keep_cols]
                .nlargest(n, "tfidf_score")
                .reset_index(drop=True))

    def tfidf_top_terms(self, n: int = 30) -> pd.DataFrame:
        """
        Global top TF-IDF terms across the whole corpus.
        Useful for understanding the overall vocabulary without a keyword lens.
        """
        self._check_fitted()
        scores = np.asarray(self._tfidf_matrix.mean(axis=0)).flatten()
        vocab  = self._tfidf.get_feature_names_out()
        df = pd.DataFrame({"term": vocab, "mean_tfidf": scores})
        return df.nlargest(n, "mean_tfidf").reset_index(drop=True)

    # ── 2-D projection ─────────────────────────────────────────────────────────

    def embedding_dataframe(self,
                             seed_words: list[str] = None,
                             n_words: int = 200,
                             method: str = "umap") -> pd.DataFrame:
        """
        Project word vectors into 2-D for plotting.

        Parameters
        ----------
        seed_words : words to highlight (defaults to keyword + top neighbours)
        n_words    : total number of words to include in the projection
        method     : 'umap' (recommended) or 'pca'

        Returns
        -------
        DataFrame: word | x | y | is_seed | similarity_to_kw | frequency
        """
        self._check_fitted()
        kw = self.keyword

        # Pick the n most frequent words (by corpus count) that are in the W2V vocab
        vocab_set = set(self._w2v.wv.key_to_index.keys())
        top_words = [w for w, _ in self.corpus.token_counts.most_common()
                     if w in vocab_set][:n_words]

        # Ensure seed words are included
        seed_words = seed_words or [kw]
        seed_words = [w.lower() for w in seed_words if w.lower() in vocab_set]
        for sw in seed_words:
            if sw not in top_words:
                top_words.append(sw)

        vectors = np.array([self._w2v.wv[w] for w in top_words])

        # Dimensionality reduction
        if method == "umap" and _HAS_UMAP:
            reducer = umap.UMAP(
                n_neighbors=min(config.UMAP_N_NEIGHBORS, len(top_words) - 1),
                min_dist=config.UMAP_MIN_DIST,
                n_components=2,
                random_state=42,
                metric="cosine",
            )
            coords = reducer.fit_transform(vectors)
        elif _PCA is not None:
            pca = _PCA(n_components=2, random_state=42)
            coords = pca.fit_transform(vectors)
        else:
            raise ImportError("Install umap-learn or scikit-learn for 2-D projection.")

        # Similarity to keyword
        if kw in self._w2v.wv:
            kw_vec = self._w2v.wv[kw].reshape(1, -1)
            word_vecs = vectors
            sims = cosine_similarity(kw_vec, word_vecs).flatten()
        else:
            sims = np.zeros(len(top_words))

        df = pd.DataFrame({
            "word"              : top_words,
            "x"                 : coords[:, 0],
            "y"                 : coords[:, 1],
            "is_seed"           : [w in seed_words for w in top_words],
            "similarity_to_kw"  : sims.round(4),
            "frequency"         : [self.corpus.token_counts.get(w, 0) for w in top_words],
        })
        return df

    def save(self, path: str | Path = None) -> Path:
        """Save the Word2Vec model to disk."""
        self._check_fitted()
        path = Path(path or config.OUTPUT_DIR / "word2vec.model")
        self._w2v.save(str(path))
        log.info(f"Word2Vec model saved → {path}")
        return path

    @classmethod
    def load(cls, corpus: Corpus, model_path: str | Path) -> "EmbeddingModel":
        """Load a previously saved Word2Vec model."""
        em = cls(corpus)
        em._w2v = Word2Vec.load(str(model_path))
        # Re-fit TF-IDF (fast, no need to cache)
        texts = corpus.df["_combined_text"].fillna("").tolist()
        em._tfidf = TfidfVectorizer(
            max_features=config.TFIDF_MAX_FEATURES,
            ngram_range=config.TFIDF_NGRAM_RANGE,
            stop_words="english",
            sublinear_tf=True,
        )
        em._tfidf_matrix = em._tfidf.fit_transform(texts)
        em._fitted = True
        return em

    # ── internal ───────────────────────────────────────────────────────────────

    def _check_fitted(self):
        if not self._fitted:
            raise RuntimeError("Call .fit() before using query methods.")
