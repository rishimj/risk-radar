#!/usr/bin/env python3
"""Populate per-ticker baselines from real news so cold start isn't a spam window.

At a 3-minute slide, MIN_BASELINE_SAMPLES=50 is ~2.5 hours of warmup per ticker.
Alerting stays silent during warmup by design — falling back to an absolute
threshold would mean falling back to the 0.7 cut that fires on 13.5% of windows,
so a fresh `make demo` would spam alerts for exactly the hours someone is
watching it.

This replays the last 24h of real news through the same scorer and the same
window assignment the job uses, and writes the resulting distribution into
`base:{ticker}`. Run it once at startup; it is idempotent enough to re-run.

    python tools/seed_baseline.py
    python tools/seed_baseline.py --hours 24 --dry-run
"""
from collections import defaultdict
from typing import Dict, List
import argparse
import logging
import sys

import redis

from riskcore import config
from riskcore.baseline import BaselineStore

from replay import build

log = logging.getLogger("seed")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=int, default=24, help="corpus length (default 24)")
    ap.add_argument("--redis-url", default=config.REDIS_URL)
    ap.add_argument("--dry-run", action="store_true", help="compute but do not write")
    ap.add_argument("--force", action="store_true",
                    help="seed even for tickers already above the sample floor")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    client = redis.from_url(args.redis_url)
    try:
        client.ping()
    except redis.RedisError as exc:
        print(f"redis unreachable at {args.redis_url}: {exc}")
        return 1

    store = BaselineStore(client)

    already = {t: store.sample_count(t) for t in config.MAG7}
    ready = [t for t, n in already.items() if n >= store.min_samples]
    if ready and not args.force:
        print(f"already seeded: {', '.join(sorted(ready))}")
    targets = [t for t in config.MAG7 if args.force or already[t] < store.min_samples]
    if not targets:
        print("all tickers already have a usable baseline; nothing to do")
        return 0

    print(f"seeding {len(targets)} ticker(s) from ~{args.hours}h of real news...")
    scores, articles, model = build(args.hours, tickers=targets)
    if not scores:
        print("no windows produced — is the network reachable?")
        return 1

    grouped: Dict[str, List[tuple]] = defaultdict(list)
    for s in scores:
        grouped[s.ticker].append((s.end_epoch, s.risk))

    print(f"\nmodel: {model}   articles: {len(articles)}   windows: {len(scores)}\n")
    print(f"  {'ticker':<8}{'windows':>9}{'written':>9}{'total':>8}  status")

    for ticker in sorted(targets):
        observations = grouped.get(ticker, [])
        written = 0
        if observations and not args.dry_run:
            written = store.record_many(ticker, observations)

        total = store.sample_count(ticker)
        status = "ready" if total >= store.min_samples else f"short of {store.min_samples}"
        if args.dry_run:
            status = f"dry-run ({len(observations)} would be written)"
        print(f"  {ticker:<8}{len(observations):>9}{written:>9}{total:>8}  {status}")

    short = [t for t in targets if store.sample_count(t) < store.min_samples]
    if short and not args.dry_run:
        print(f"\n  {', '.join(sorted(short))} still below the floor — these stay SILENT")
        print("  until live windows top them up. That is intended: a missed alert in")
        print("  the first minutes beats a burst of false ones.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
