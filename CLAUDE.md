# RiskRadar — session handoff (2026-07-27)

Status of a session that took the repo from "compose stack never started" to a
running end-to-end pipeline on an M-series MacBook (16 GB RAM, Docker VM 7.7 GB).

## TL;DR

`make demo` now works. Six real bugs fixed, all committed to the working tree
(uncommitted — see `git status`). The pipeline ingests real news, enriches with
FinBERT, computes sliding-window risk, and writes baselines.

**One thing is still broken and needs YOUR decision: no alert can fire.** See
"Open decision" below. Everything else is verified working.

## Verified working

- `make demo` builds and starts all services; Flink job reaches **RUNNING**,
  all 3 tasks deployed. `localhost:8081` shows the job, not an empty cluster.
- **186/186 tests pass** (`.venv/bin/python -m pytest tests/`). The 24 formerly
  "skipped" tests are DynamoDB-gated and execute once the stack is up.
- **The micro-batch operator works** — this was flagged as the riskiest
  unexercised path (blocking HTTP inside a processing-time timer). Under load:
  3 requests, 15 articles, **3 batches of exactly 5** (`ENRICH_BATCH_SIZE`),
  **0 errors**, FinBERT ~238 ms/batch. PyFlink's harness did not object.
  **The per-record synchronous fallback is not needed.**
- Live ingestion: sweeps every 120 s, ~160-190 articles fetched per sweep.
- Baseline seeding from real news: 668 articles → 1555 windows across Mag 7.
- Sliding risk windows fire and write `feat:<TICKER>:latest` to Redis.
- Simulated TSLA crisis is detected correctly: `risk_score=1.0`,
  FinBERT `sentiment=-0.926`, 10 mentions.
- Endpoints: webapp 3000, Flink 8081, enrichment 8082 — all HTTP 200.

## Bugs found and fixed

1. **`finvader==1.0.3` does not exist on PyPI** (`services/enrichment/requirements.txt`).
   Available: 1.0.0-1.0.2, 1.0.4. Enrichment image could not build at all.
   → pinned `1.0.4`.

2. **`--break-system-packages` on pip 22.0.2** (`flink/Dockerfile`). That flag
   landed in pip 23; the Flink base image ships 22.0.2, so the install aborted.
   Ubuntu 22.04 has no PEP 668 marker, so the flag was never needed.
   → removed from both `RUN` lines.

3. **`seed-baseline` exited 1** — `tools/replay.py:77` imports `vaderSentiment`
   unguarded as the fallback after FinBERT fails, but the shared webapp image
   installed no sentiment library. torch is absent there by design, so the
   fallback is the path that *always* runs.
   → added `tools/requirements.txt` (vaderSentiment) + pip step in
   `services/webapp/Dockerfile`.

4. **`riskcore` silently not installed in the Flink image.** The base image has
   **setuptools 59.6.0**; PEP 621 `[project]` metadata needs ≥61. So
   `pip install /libs/core` built an empty `UNKNOWN-0.0.0` wheel **and exited 0**.
   `pip list` showed `apache-flink` but no `riskcore`. Surfaced far from its
   cause, as `ModuleNotFoundError: riskcore` at job submit.
   → `pip install --upgrade pip setuptools wheel` before any install.

5. **Checkpoint dir permission.** `flink/Dockerfile` does `USER root`, but the
   official Flink entrypoint `gosu`s down to `flink` before starting the JVM.
   The named volume was created root-owned, so the JobMaster could not create
   `/tmp/flink-checkpoints/<job-id>/shared` → job died at `env.execute()`.
   → `mkdir -p` + `chown flink:flink` at build time so the volume inherits it.
   **Not Mac-specific — this reproduces on any fresh volume, including EC2.**

6. **Alert evaluation ordering** (`flink/job/news_job.py`). Topology was
   `FeatureWriter → AlertFanout`, so the current window's risk was folded into
   the baseline *before* being compared against it. A window cannot be an
   outlier relative to a sample set it already belongs to. Worse: a 900 s window
   sliding 180 s emits **5 overlapping max-risk windows** per spike.
   → swapped to `AlertFanout → FeatureWriter`.
   Independent confirmation this was a bug: `tools/calibrate.py`'s docstring
   says it "Mirrors BaselineStore.evaluate: a window fires when it exceeds the
   Pth percentile of that ticker's ***prior*** observations." The harness and
   the live job disagreed. The fix makes them agree.

## OPEN DECISION — why no alert fires (needs your call)

The ordering fix (#6) was necessary but **not sufficient**. Measured, not guessed:

- `risk_score` is capped: `round(min(1.0, sentiment_risk * volume_multiplier * negative_bias), 3)`
  (`libs/core/riskcore/risk.py:68`).
- A **freshly seeded** TSLA baseline (223 samples, zero spike contamination)
  already contains **4 samples at exactly 1.0** → **p99 = 1.0**.
  (p99.5 = 1.0, p98 = 0.9959, p95 = 0.978.)
- `BaselineStore.evaluate` tests `risk_score > cut` — strict
  (`libs/core/riskcore/baseline.py:160`).
- So `1.0 > 1.0` is false. **A maxed-out risk score cannot exceed a saturated
  threshold. TSLA can never alert at `ALERT_PERCENTILE=99`.**

Confirmed empirically after the ordering fix, polled every 10 s for ~7 minutes
(many 180 s window slides): `feat:TSLA:latest` held `risk_score: 1.0` the whole
time and `/api/alerts` never left `{"alerts":[]}`. Not a one-off snapshot.

Real negative news saturates the cap often enough that ~1.8% of seeded windows
hit 1.0. Options (I did NOT choose — it changes product behavior):

- **Lower `ALERT_PERCENTILE`** to ~95 (p95 = 0.978, so 1.0 fires). Raises alert
  rate; prior VADER runs gave 3.7 alerts/ticker/day at p99, 3.2 at p99.5,
  against a 1-3 target.
- **Change `>` to `>=`.** Then capped windows fire; rate ≈ fraction of capped
  windows (~1.8% × 480 windows/day ≈ 8.6/day before cooldown; the 10 min
  cooldown cuts this substantially).
- **Un-cap or rescale `risk_score`** so percentiles stay meaningful — the
  principled fix, but a real change to the score's semantics.
- **Exclude saturated values from the baseline distribution.**

**Recommended next step:** run the calibration sweep to get FinBERT-based
numbers before picking. This was never run in this session (permission
declined at the end):

```bash
PYTHONPATH=tools .venv/bin/python tools/calibrate.py --hours 6 --sweep
```

Note the seeded baselines are **VADER**-derived (torch is absent from the
webapp/tools image by design), while the live path uses **FinBERT**. That
mismatch is itself worth deciding on — the distribution the threshold is
calibrated against is not the one the pipeline produces.

## Memory tuning (you hit "out of application memory" previously)

Docker VM is 7.7 GB of 16 GB. Nothing in the stack bounded its JVM heap. Added:

| Change | Approx. saved |
|---|---|
| Flink TM `taskmanager.memory.process.size: 1400m` (default 1728m) | ~330 MB |
| Flink JM `jobmanager.memory.process.size: 1024m` (default 1600m) | ~575 MB |
| Kafka `KAFKA_HEAP_OPTS: -Xmx512m -Xms256m` (default 1g) | ~500 MB |
| `kafka-ui` moved to `tools` profile, **off by default** | ~400 MB |
| DynamoDB Local `-Xmx256m` | ~250 MB |

Also cut taskmanager slots 4 → 2 (`parallelism.default` is 1; idle slots carve
up managed memory and network buffers).

Start the Kafka UI only when wanted:
```bash
docker compose --profile tools up -d kafka-ui
```

**Not measured:** actual per-container RSS. `docker stats` was declined during
this session, so the savings above are from known defaults, not observation.
Worth verifying with `docker stats --no-stream` if memory pressure returns.

The two Flink containers are `linux/amd64` under Rosetta (no aarch64 PyFlink
wheel exists) — that emulation is a real cost and cannot be avoided on ARM.

## Also changed

- **`.dockerignore` added** (there was none). `.venv/` and the `riskcore.egg-info/`
  left by a local editable install were being copied into every build context.
- Corrected a comment in `flink/Dockerfile`: base image is **Ubuntu 22.04 /
  python3.10**, not "Debian bookworm python3.11". The cp38-cp311 wheel claim
  still holds for 3.10, so the version choice was fine — the comment was not.
- **Undeclared test dep:** `libs/core[test]` declares only pytest + fakeredis,
  but `test_webapp.py` / `test_enrichment.py` need `httpx` for
  `fastapi.testclient`. Collection hard-fails on a clean env without it.
  **Not fixed** — add `httpx` to the `[project.optional-dependencies] test`
  list in `libs/core/pyproject.toml`.

## How to resume

```bash
# host venv used for tests + tools (NOT committed; .dockerignore excludes it)
python3 -m venv .venv
.venv/bin/pip install -e "libs/core[test]" -r services/webapp/requirements.txt httpx vaderSentiment==3.3.2

.venv/bin/python -m pytest tests/        # expect 186 passed with stack up
docker compose up -d --build             # ~15 min cold; Flink image builds under emulation
curl -s localhost:8081/jobs/overview     # want "state":"RUNNING"
curl -s localhost:8082/stats             # want tier:finbert, errors:0
```

`.env` already exists locally with a generated `SECRET_KEY` (gitignored).

Exercising the pipeline requires an authed user with the ticker on their
watchlist — `/api/simulate` 403s otherwise:

```bash
CJ=/tmp/cj.txt
curl -s -c $CJ -X POST localhost:3000/signup -d "email=demo@local.dev&password=testpass123"
curl -s -b $CJ -X POST localhost:3000/api/watchlist -H 'Content-Type: application/json' -d '{"tickers":["TSLA"]}'
curl -s -b $CJ -X POST localhost:3000/api/simulate -H 'Content-Type: application/json' -d '{"ticker":"TSLA"}'
```

If a baseline gets poisoned by spike windows, clear and re-seed:
```bash
docker compose exec -T redis redis-cli --scan --pattern 'base:*' | xargs docker compose exec -T redis redis-cli DEL
docker compose run --rm seed-baseline
```

## Still unverified

- **No alert has ever fired end-to-end** (blocked on the open decision above).
- `make calibrate` never run.
- Long-run soak — organic alerts arriving on their own is the real acceptance
  test, and it has not been done. Only the simulate path was exercised.
- Slack fan-out (`/api/slack`) untested; no webhook configured.
- SEC EDGAR feed disabled (`ENABLE_SEC=false`).
- Nothing tested on the x86_64 EC2 target.

## Caution on prior claims

The earlier handoff stated the Flink job graph was "verified building against a
real PyFlink 1.20.1 runtime." That cannot have been in the container — bug #4
meant `riskcore` was absent from the image, so the job died at import, well
before `build_pipeline()`. The graph verification and the containerized runtime
were not the same environment. Treat "verified" claims as environment-specific
unless the environment is named.
