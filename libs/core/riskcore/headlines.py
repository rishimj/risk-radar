"""Recent-headlines ring buffer in Redis, read by the dashboard and landing page."""
from typing import Any, Dict, List, Optional
import json
import logging

from . import config
from .models import SIMULATED_SOURCE, safe_url

log = logging.getLogger(__name__)


def push(redis_client, article: Dict[str, Any], max_len: Optional[int] = None) -> None:
    cap = max_len if max_len is not None else config.HEADLINES_MAX
    article = {**article, "url": safe_url(article.get("url"))}
    pipe = redis_client.pipeline()
    pipe.lpush(config.HEADLINES_KEY, json.dumps(article, separators=(",", ":")))
    pipe.ltrim(config.HEADLINES_KEY, 0, cap - 1)
    pipe.execute()


def recent(redis_client, limit: int = 50, ticker: Optional[str] = None,
           include_simulated: bool = True) -> List[Dict[str, Any]]:
    raw = redis_client.lrange(config.HEADLINES_KEY, 0, max(limit * 4, limit) - 1)
    out: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, bytes):
            item = item.decode()
        try:
            doc = json.loads(item)
        except json.JSONDecodeError:
            continue
        doc["url"] = safe_url(doc.get("url"))          # rows written before sanitising
        if not include_simulated and doc.get("source") == SIMULATED_SOURCE:
            continue
        if ticker:
            tickers = [c.get("ticker") for c in doc.get("companies", [])]
            if ticker not in tickers:
                continue
        out.append(doc)
        if len(out) >= limit:
            break
    return out
