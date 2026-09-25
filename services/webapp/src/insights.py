"""Read-side aggregates for the UI: sparklines, the baseline distribution, counters.

Everything here is derived, public-safe data (scores of public news), so the
landing page can show the live system to anonymous visitors. Nothing reads a
user record.
"""
from typing import Dict, List, Optional, Sequence
import json

from riskcore import config
from riskcore.baseline import percentile
from riskcore.entities import COMPANIES
from riskcore.models import safe_url
from riskcore.stages import history_key


def history(redis_client, ticker: str, n: int = 48) -> List[Dict]:
    """The last n windows for a ticker, oldest first."""
    raw = redis_client.lrange(history_key(ticker), 0, max(0, n - 1))
    out = []
    for item in reversed(raw):
        try:
            out.append(json.loads(item))
        except (TypeError, ValueError):
            continue
    return out


def histogram(values: Sequence[float], bins: int = 28,
              hi: Optional[float] = None) -> List[Dict]:
    """Fixed-width bins over [0, hi]. Anything above hi lands in the last bin."""
    if not values:
        return []
    top = hi if hi is not None else max(values)
    top = max(top, 1e-6)
    width = top / bins
    counts = [0] * bins
    for v in values:
        idx = min(bins - 1, max(0, int(v / width)))
        counts[idx] += 1
    return [{"x0": round(i * width, 4), "x1": round((i + 1) * width, 4), "count": c}
            for i, c in enumerate(counts)]


def _counter(redis_client, name: str) -> int:
    try:
        return int(redis_client.get(f"stats:{name}") or 0)
    except (TypeError, ValueError):
        return 0


def ticker_row(redis_client, baselines, ticker: str, points: int = 48) -> Dict:
    raw = redis_client.get(f"feat:{ticker}:latest")
    feats = json.loads(raw) if raw else None
    threshold = baselines.threshold(ticker)
    return {
        "ticker": ticker,
        "name": COMPANIES[ticker].name if ticker in COMPANIES else ticker,
        "risk_score": feats.get("risk_score") if feats else None,
        "alert_score": (feats.get("alert_score", feats.get("risk_score")) if feats else None),
        "sentiment_score": feats.get("sentiment_score") if feats else None,
        "total_mentions": feats.get("total_mentions") if feats else 0,
        "window_end": feats.get("window_end") if feats else None,
        "top_headline": feats.get("top_headline") if feats else "",
        "top_url": safe_url(feats.get("top_url")) if feats else "",
        "simulated": bool(feats.get("simulated")) if feats else False,
        "baseline": round(threshold, 3) if threshold is not None else None,
        "baseline_samples": baselines.sample_count(ticker),
        "baseline_ready": threshold is not None,
        "min_samples": baselines.min_samples,
        "history": history(redis_client, ticker, points),
    }


def overview(redis_client, baselines, featured: str = "TSLA") -> Dict:
    """Everything the public landing page animates, in one cached payload."""
    tickers = [ticker_row(redis_client, baselines, t, points=32) for t in config.MAG7]

    scores = baselines.scores(featured)
    dist = None
    if scores:
        cut = percentile(scores, baselines.percentile_p) if len(scores) >= 2 else None
        latest = next((t["alert_score"] for t in tickers if t["ticker"] == featured), None)
        hi = max(max(scores), cut or 0, latest or 0) * 1.08
        dist = {
            "ticker": featured,
            "name": COMPANIES[featured].name if featured in COMPANIES else featured,
            "samples": len(scores),
            "bins": histogram(scores, hi=hi),
            "cut": round(cut, 3) if cut is not None else None,
            "median": round(percentile(scores, 50), 3),
            "latest": latest,
            "max": round(hi, 3),
        }

    return {
        "percentile": baselines.percentile_p,
        "window_minutes": config.WINDOW_SIZE_SECONDS // 60,
        "slide_minutes": config.WINDOW_SLIDE_SECONDS // 60,
        "tickers": tickers,
        "distribution": dist,
        "totals": {
            "articles": _counter(redis_client, "articles"),
            "windows": _counter(redis_client, "windows"),
            "alerts": _counter(redis_client, "alerts"),
            "baseline_samples": sum(t["baseline_samples"] for t in tickers),
        },
    }
