"""Sentiment scoring with a graceful capability ladder.

  1. FinBERT (ProsusAI/finbert) — finance-tuned, the real thing
  2. FinVADER                   — finance-tuned lexicon, no torch
  3. VADER                      — generic lexicon, always available

Tier 1 matters more than it looks. Scoring 672 real Mag 7 headlines with plain
VADER, the most "negative" articles were:

    -0.91  Silicon Valley developer accused of murder after ... Tesla ...
    -0.90  Fake song appears on jailed rapper's Apple Music profile ...

Maximal lexical negativity, near-zero *financial* risk to TSLA or AAPL. A
generic sentiment model reads "murder" and panics. A finance-tuned one scores
these near neutral, which is the entire point of the swap — and why the old
repo's `nlptown/bert-base-multilingual-uncased-sentiment` (a 1-5 star review
model, mislabelled "financial DistilBERT") was the wrong tool.

FinBERT is also scored as p(positive) - p(negative) over the softmax, giving a
genuinely continuous [-1, 1] rather than the old star-count bucketing.
"""
from typing import List, Optional, Sequence
import logging
import os

log = logging.getLogger(__name__)

MODEL_NAME = os.getenv("SENTIMENT_MODEL", "ProsusAI/finbert")
MAX_TOKENS = int(os.getenv("SENTIMENT_MAX_TOKENS", "128"))   # headlines only


class SentimentEngine:
    """Loads the best available tier once and scores batches."""

    def __init__(self, model_name: str = MODEL_NAME):
        self.model_name = model_name
        self.tier = "none"
        self._pipeline = None
        self._tokenizer = None
        self._model = None
        self._torch = None
        self._vader = None

    # -- loading ---------------------------------------------------------
    def load(self) -> str:
        if self._try_finbert():
            self.tier = "finbert"
        elif self._try_finvader():
            self.tier = "finvader"
        elif self._try_vader():
            self.tier = "vader"
        else:
            self.tier = "none"
            log.error("no sentiment backend available; everything will score 0.0")
        log.info("sentiment tier: %s", self.tier)
        return self.tier

    def _try_finbert(self) -> bool:
        try:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer

            self._torch = torch
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self._model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            self._model.eval()

            # Map label names to indices once; FinBERT's order is not guaranteed.
            labels = {v.lower(): k for k, v in self._model.config.id2label.items()}
            self._pos_idx = labels.get("positive")
            self._neg_idx = labels.get("negative")
            if self._pos_idx is None or self._neg_idx is None:
                log.warning("model %s lacks positive/negative labels", self.model_name)
                return False

            self.score_batch(["Warm up the graph."])   # first call is slow; do it now
            return True
        except Exception as exc:                       # noqa: BLE001
            log.warning("FinBERT unavailable (%s); falling back", exc)
            self._model = self._tokenizer = self._torch = None
            return False

    def _try_finvader(self) -> bool:
        try:
            from finvader import finvader           # noqa: F401
            self._finvader = finvader
            return True
        except Exception:                              # noqa: BLE001
            return False

    def _try_vader(self) -> bool:
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            self._vader = SentimentIntensityAnalyzer()
            return True
        except Exception as exc:                       # noqa: BLE001
            log.warning("VADER unavailable: %s", exc)
            return False

    # -- scoring ---------------------------------------------------------
    def score_batch(self, texts: Sequence[str]) -> List[float]:
        if not texts:
            return []

        if self._model is not None:
            return self._score_finbert(texts)
        if self.tier == "finvader" or getattr(self, "_finvader", None):
            return [self._score_finvader(t) for t in texts]
        if self._vader is not None:
            return [float(self._vader.polarity_scores(t)["compound"]) for t in texts]
        return [0.0] * len(texts)

    def _score_finbert(self, texts: Sequence[str]) -> List[float]:
        torch = self._torch
        # One tokenizer call for the whole batch — this is what the batch
        # endpoint exists for; looping a pipeline per text throws it away.
        encoded = self._tokenizer(
            list(texts), padding=True, truncation=True,
            max_length=MAX_TOKENS, return_tensors="pt",
        )
        with torch.no_grad():
            logits = self._model(**encoded).logits
            probs = torch.softmax(logits, dim=-1)

        pos = probs[:, self._pos_idx]
        neg = probs[:, self._neg_idx]
        return [round(float(v), 4) for v in (pos - neg)]

    def _score_finvader(self, text: str) -> float:
        try:
            return float(self._finvader(text, use_sentibignomics=True, use_henry=True,
                                        indicator="compound"))
        except Exception:                              # noqa: BLE001
            return 0.0
