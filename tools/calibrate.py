#!/usr/bin/env python3
"""Replay real news and report what the alerting policy would actually do.

Run this before locking any threshold, and again whenever the sentiment model
or the feed mix changes. Target roughly 1-3 alerts per ticker per day.

    python tools/calibrate.py --hours 6
    python tools/calibrate.py --hours 6 --sweep

Why this exists: the risk formula inherited from the previous system fired on
13.5% of windows against a fixed 0.7 cut — a single mildly negative headline
scores 0.8125 — while every mean-sentiment reformulation fired on 0.0%. Neither
is discoverable by reading the code. It only shows up when you replay real news.
"""
from collections import defaultdict
from typing import Dict, List, Sequence
import argparse
import logging
import statistics
import sys

from riskcore import config
from riskcore.baseline import percentile

from replay import WindowScore, build


def simulate(scores: Sequence[WindowScore], p: float,
             min_samples: int, min_mentions: int, cooldown_s: int) -> Dict[str, dict]:
    """Apply the real alerting policy to replayed windows, per ticker.

    Mirrors BaselineStore.evaluate: a window fires when it exceeds the Pth
    percentile of that ticker's *prior* observations, given enough samples and
    enough mentions — then the cooldown suppresses follow-ups.
    """
    by_ticker: Dict[str, List[WindowScore]] = defaultdict(list)
    for s in scores:
        by_ticker[s.ticker].append(s)

    report: Dict[str, dict] = {}
    for ticker, windows in by_ticker.items():
        windows = sorted(windows, key=lambda w: w.window.end_ms)
        seen: List[float] = []
        crossings = 0
        alerts: List[WindowScore] = []
        last_fired_ms = None

        for w in windows:
            if len(seen) >= min_samples and w.mentions >= min_mentions:
                cut = percentile(seen, p)
                if w.risk > cut:
                    crossings += 1
                    if last_fired_ms is None or (w.window.end_ms - last_fired_ms) >= cooldown_s * 1000:
                        alerts.append(w)
                        last_fired_ms = w.window.end_ms
            seen.append(w.risk)

        report[ticker] = {
            "windows": len(windows),
            "crossings": crossings,
            "alerts": alerts,
            "risk_p50": round(statistics.median([w.risk for w in windows]), 3) if windows else 0.0,
            "risk_max": round(max((w.risk for w in windows), default=0.0), 3),
            "warmed_up": len(windows) >= min_samples,
        }
    return report


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=int, default=6, help="corpus length (default 6)")
    ap.add_argument("--percentile", type=float, default=config.ALERT_PERCENTILE)
    ap.add_argument("--min-samples", type=int, default=config.MIN_BASELINE_SAMPLES)
    ap.add_argument("--min-mentions", type=int, default=config.MIN_MENTIONS_FOR_ALERT)
    ap.add_argument("--cooldown", type=int, default=config.ALERT_COOLDOWN_MINUTES * 60)
    ap.add_argument("--window", type=int, default=config.WINDOW_SIZE_SECONDS)
    ap.add_argument("--slide", type=int, default=config.WINDOW_SLIDE_SECONDS)
    ap.add_argument("--sweep", action="store_true", help="try several percentiles")
    ap.add_argument("--show-alerts", action="store_true", help="print each firing headline")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print(f"fetching ~{args.hours}h of real Mag 7 news...")
    scores, articles, model = build(args.hours, size_s=args.window, slide_s=args.slide)
    if not scores:
        print("no windows produced — is the network reachable?")
        return 1

    span_h = (max(s.window.end_ms for s in scores)
              - min(s.window.start_ms for s in scores)) / 3_600_000 or 1

    print(f"\nmodel        : {model}")
    print(f"articles     : {len(articles)}")
    print(f"windows      : {len(scores)}  ({args.window}s / {args.slide}s slide)")
    print(f"span         : {span_h:.1f}h")
    if model.startswith("vader"):
        print("  ! VADER stands in for FinBERT here. It scores non-financial")
        print("    negativity ('murder', 'jailed') as extreme. Re-run with torch")
        print("    installed before trusting these numbers.")

    percentiles = [90, 95, 97, 99, 99.5] if args.sweep else [args.percentile]

    print(f"\n{'p':>6} {'crossings':>10} {'alerts':>7} {'per ticker/day':>15}")
    print("-" * 42)
    for p in percentiles:
        report = simulate(scores, p, args.min_samples, args.min_mentions, args.cooldown)
        crossings = sum(r["crossings"] for r in report.values())
        alerts = sum(len(r["alerts"]) for r in report.values())
        per_day = alerts * 24 / span_h / max(len(report), 1)
        flag = "  <- target 1-3" if 1 <= per_day <= 3 else ""
        print(f"{p:>6} {crossings:>10} {alerts:>7} {per_day:>15.2f}{flag}")

    report = simulate(scores, args.percentile, args.min_samples,
                      args.min_mentions, args.cooldown)

    print(f"\nper ticker (p={args.percentile}):")
    print(f"  {'ticker':<8}{'windows':>8}{'p50':>7}{'max':>7}{'alerts':>8}  {'baseline':>9}")
    for ticker in sorted(report):
        r = report[ticker]
        state = "ready" if r["warmed_up"] else f"warming({r['windows']}/{args.min_samples})"
        print(f"  {ticker:<8}{r['windows']:>8}{r['risk_p50']:>7}{r['risk_max']:>7}"
              f"{len(r['alerts']):>8}  {state:>9}")

    warming = [t for t, r in report.items() if not r["warmed_up"]]
    if warming:
        print(f"\n  {len(warming)} ticker(s) below --min-samples in this corpus; alerting")
        print(f"  stays SILENT for them until seeded. Run tools/seed_baseline.py.")

    if args.show_alerts:
        print("\nfiring windows:")
        for ticker in sorted(report):
            for w in report[ticker]["alerts"]:
                print(f"  {ticker:<6} risk={w.risk:.3f} n={w.mentions:<3} {w.top_headline[:62]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
