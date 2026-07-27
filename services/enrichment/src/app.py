"""Enrichment service: sentiment + Mag 7 entity matching, batched.

POST /v1/enrich/batch is called from inside the Flink micro-batch operator, so
it must be fast and must never 500 on a single bad article.
"""
from contextlib import asynccontextmanager
from typing import Dict, List, Optional
import logging
import os
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from riskcore import config
from riskcore.entities import detect_companies

from sentiment import SentimentEngine

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("enrichment")

MAX_BATCH = int(os.getenv("MAX_BATCH", "100"))

engine = SentimentEngine()
STATS = {"requests": 0, "articles": 0, "batches": 0, "errors": 0, "total_ms": 0.0}


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine.load()          # blocking, but this is startup — better than a cold first request
    yield


app = FastAPI(title="RiskRadar enrichment", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def trace_id(request: Request, call_next):
    tid = request.headers.get("x-trace-id") or uuid.uuid4().hex[:12]
    started = time.perf_counter()
    response = await call_next(request)
    elapsed = (time.perf_counter() - started) * 1000
    response.headers["x-trace-id"] = tid
    response.headers["x-elapsed-ms"] = f"{elapsed:.1f}"
    if request.url.path.startswith("/v1/"):
        log.info("%s %s %s %.1fms", tid, request.method, request.url.path, elapsed)
    return response


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------
class ArticleIn(BaseModel):
    article_id: str = ""
    title: str = ""
    summary: str = ""


class BatchIn(BaseModel):
    articles: List[ArticleIn] = Field(default_factory=list)


class ResultOut(BaseModel):
    article_id: str
    sentiment: float
    companies: List[Dict[str, str]]


class BatchOut(BaseModel):
    results: List[ResultOut]
    total_articles: int
    processing_time_ms: float
    model: str


# ---------------------------------------------------------------------------
# routes
# ---------------------------------------------------------------------------
@app.get("/healthz")
def healthz():
    return {"status": "ok", "tier": engine.tier}


@app.get("/stats")
def stats():
    avg = STATS["total_ms"] / STATS["batches"] if STATS["batches"] else 0.0
    return {
        "tier": engine.tier,                 # dashboard status pill reads this
        "model": engine.model_name if engine.tier == "finbert" else engine.tier,
        "max_batch": MAX_BATCH,
        "avg_batch_ms": round(avg, 1),
        **STATS,
    }


@app.post("/v1/enrich/batch", response_model=BatchOut)
def enrich_batch(batch: BatchIn):
    started = time.perf_counter()
    STATS["requests"] += 1

    articles = batch.articles[:MAX_BATCH]
    if len(batch.articles) > MAX_BATCH:
        log.warning("batch truncated %d -> %d", len(batch.articles), MAX_BATCH)

    if not articles:
        return BatchOut(results=[], total_articles=0, processing_time_ms=0.0,
                        model=engine.tier)

    # Google News carries no body text, so the headline IS the document. Where a
    # wire feed does give a summary, append it for entity matching but keep the
    # scored text short — FinBERT is a sentence model.
    texts = [(a.title or "").strip() for a in articles]

    try:
        sentiments = engine.score_batch(texts)
    except Exception as exc:                           # noqa: BLE001
        STATS["errors"] += 1
        log.exception("scoring failed, returning neutral: %s", exc)
        sentiments = [0.0] * len(articles)

    results = []
    for article, sentiment in zip(articles, sentiments):
        try:
            companies = detect_companies(article.title or "", article.summary or "")
        except Exception:                              # noqa: BLE001
            STATS["errors"] += 1
            companies = []
        results.append(ResultOut(
            article_id=article.article_id,
            sentiment=float(sentiment),
            companies=companies,
        ))

    elapsed = (time.perf_counter() - started) * 1000
    STATS["batches"] += 1
    STATS["articles"] += len(results)
    STATS["total_ms"] += elapsed

    return BatchOut(
        results=results,
        total_articles=len(results),
        processing_time_ms=round(elapsed, 2),
        model=engine.tier,
    )


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    STATS["errors"] += 1
    log.exception("unhandled error on %s", request.url.path)
    return JSONResponse(status_code=500, content={"error": str(exc)})
