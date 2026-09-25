# RiskRadar — session handoff (2026-09-25, public-demo release)

## UI redesign (latest)

- Stripe-style UI: `static/app.css` is one token-driven design system;
  landing (animated gradient hero, live preview, animated pipeline SVG, live
  baseline histogram, count-up counters, engineering band), app shell for
  dashboard/settings/onboarding (`templates/_app.html`), split auth pages
  (`_auth.html`). Motion is off under prefers-reduced-motion.
- Charts are hand-built SVG in `static/app.js` (sparkline with cut line +
  crosshair tooltip + keyboard arrows; histogram with per-bar hover). All
  server data goes through esc()/textContent/safeUrl(); CSP still script-src 'self'.
- New data: `hist:<ticker>` capped list (96 windows) written in
  `stages.write_features`; `stats:*` counters; public cached
  `GET /api/public/overview` (`webapp/src/insights.py`, no user data);
  `/api/headlines?relevant=true`. Standalone backfills `hist:*` from
  `feat:<ticker>:<window_end>` keys on start, so an upgrade shows history at once.
- Also fixed: double-encoded feed titles ("&amp;") now decoded at ingestion;
  syndicated duplicate headlines listed once; alerts store `alert_score`.

## TL;DR (public demo)

- **Public demo target:** `https://risk-radar.20.25.227.252.sslip.io` on the
  owner's shared Azure VM (also hosts podcast-qna on :3000; do not touch it).
  Deploy from the owner's Mac (SSH key + IP allowlist live there):
  `VM=<ssh-user>@<vm-ip> SITE_HOST=risk-radar.<vm-ip>.sslip.io deploy/azure/deploy.sh`
  (connection details are the owner's, kept out of the repo).
  The kit was verified end to end in an Ubuntu 24.04 **systemd** container
  mimicking the VM (Caddy from its apt repo, a podcast-qna stand-in): install,
  Caddy append + validate + reload, restart survival, podcast-qna untouched,
  `systemd-analyze security` 3.0. The real VM deploy is the owner's step.
- **`services/standalone/riskradar_node.py`**: the whole pipeline in one process
  (webapp + ingestion thread + lite StreamEngine + in-process FinBERT), Kafka
  replaced by `riskcore.kafka.set_local_sink` -> queue, DynamoDB by
  `DB_BACKEND=sqlite` (`riskcore/sqlitedb.py`). ~0.8-0.9 GB RSS with FinBERT,
  seeds baselines with the SAME model at startup (~40 s; alerting silent until then).
- **Public hardening** (`services/webapp/src/guard.py`, app.py): rate limits
  (per IP/user/email + 300 accounts/day site-wide + one simulate per 240 s
  site-wide), CSRF via Origin/Sec-Fetch-Site (Origin "null" refused) + JSON-only
  /api writes, strict CSP (no inline script: pages use `<script data-init>`),
  body cap, docs off, guest demo (`POST /demo`, no password, no Slack, purged
  after 48 h), exact Slack webhook regex, clamped list limits.
  `tests/test_security.py` pins them.
- **Bugs fixed this round:** simulated alerts were tagged "live" and simulated
  windows entered (poisoned) the baseline; `javascript:` feed URLs reached
  href attributes (XSS); `pytest` against a running stack truncated the app's
  real DynamoDB tables (dynamodb-local `-sharedDb` ignores the access key) ->
  `TABLE_PREFIX`, tests use `rrtest_`; undeclared `httpx` test dep; feed reads
  unbounded (5 MB cap); README linked a nonexistent deploy/aws/NOTES.md.
- Tests: **346**, table tests parametrized over SQLite (always) and DynamoDB
  Local (when reachable). `.venv/bin/python -m pytest tests/ -o addopts=""`.
- Gotchas: Ubuntu 24.04 is Python 3.12 and `finvader==1.0.4` requires <3.12
  (marker in services/standalone/requirements.txt). `riskcore` is a regular
  install in venvs: after changing libs/core, reinstall it (install.sh
  force-reinstalls every deploy). Int8-quantizing FinBERT was measured and
  rejected: no RSS saving, scores shift (-0.95 -> -0.77).

---

# Earlier handoff (2026-09-25, engines)

## TL;DR (engines)

- **Alerts now fire end to end, on both engines.** The open decision below was
  resolved with the "un-cap" option: the baseline stores `alert_score`, the
  risk formula without the 1.0 cap. The 0-1 `risk_score` gauge and severity
  bands are unchanged.
- **New lightweight engine.** `make demo-lite` runs `services/processor`, the
  Flink topology as one Python process (~62 MB RSS vs ~960 MB for JM+TM).
  `make demo` still runs Flink. Run ONE at a time (both consume `news.raw`).
  Engines are compose **profiles** (`flink`, `lite`): a bare
  `docker compose up` now starts NO engine. Use the make targets or
  `docker compose --profile flink|lite up -d` (`deploy/aws/user-data.sh` updated).
- **Two more real bugs fixed:** #7 PyFlink dropped the event-time assigner,
  #8 `kafka-init` never created a topic. Details below.
- Verified in a Linux x86_64 container (Flink native, not Rosetta): simulate
  -> alert in ~0.4 s (lite) and ~3.5 s (Flink). Not re-verified on the Mac.

### Resolution of the open decision (no alert could fire)

Chose un-capping, the principled option, over `>=` or a lower percentile:
`Aggregation.alert_score()` is the uncapped formula (0 .. 1.875) and
`risk()` is `min(1.0, alert_score())`. `RiskFeatures.alert_score` carries it;
`BaselineStore.record/evaluate` (via `riskcore.stages`) and `tools/replay.py`
use it, so calibration, seeding and live alerting stay in one unit. Old
payloads without the field fall back to `risk_score`. Dashboard compares
`alert_score` against the cut (`services/webapp/src/static/app.js`).
Measured on a fresh VADER seed: 15/258 TSLA windows would have been capped
(p99 = 1.0, unalertable); uncapped p99 = 1.057, and the simulated crisis
scores 1.806. Self-damping still holds: after several identical crises within
minutes, p99 reached 1.806 and a further identical one correctly did not fire.

### Bug 7: PyFlink silently dropped `MentionTimestampAssigner`

`WatermarkStrategy.with_idleness()` returns `WatermarkStrategy(j_strategy)`,
a new wrapper without the Python-side assigner. The job called it AFTER
`with_timestamp_assigner`, so windows ran on **Kafka produce time**, not
`published_at`. Symptom: the window operator's watermark was exactly
`kafka_ts - 30001`, and simulate's +4 min filler never closed the crisis
window (it only closed when the next ingestion sweep's produce time caught up,
which is why earlier sessions saw features "eventually"). Reproduced in the
PyFlink 1.20.1 image with a 20-line job (bounded source -> "Record has Java
Long.MIN_VALUE timestamp"); reordered, windows land at the right event times.
Pinned statically by `tests/test_flink_job.py` (no PyFlink on the host).

### Bug 8: `kafka-init` never created a topic

Compose word-splits `command:` itself, and the backslash-newline inside the
quoted string did not survive: log showed `/bin/sh: --create: not found`.
Topics only existed via broker auto-create. Put on one line.

### Lite engine design (`services/processor/src/engine.py`)

- Outcome logic lives in `libs/core/riskcore/stages.py` and is called by BOTH
  engines (`enrich_batch`, `write_headline`, `evaluate_and_alert`,
  `write_features`). The Flink job was refactored to call it.
- The engine is I/O-free and mirrors Flink's scheduling: flush at
  `ENRICH_BATCH_SIZE` or `ENRICH_BATCH_TIMEOUT_MS`; watermark
  `max_ts - delay - 1` advanced after each batch (Flink emits periodically);
  window late once `end - 1 <= watermark`; fire when `watermark >= end - 1`;
  alert before baseline write. No processing-time idleness advancement, to
  match Flink with one input channel. `tests/test_processor.py` pins it.
- Trade-off: open windows are in memory only. Offsets commit only when the
  enrichment buffer is empty, so a restart resumes from Kafka (verified: it
  caught up on 26 articles produced while stopped).
- Webapp status pill ("Stream") probes lite `/stats`, then Flink.

### Gotchas seen this session

- Consecutive simulates within ~4 min: the previous filler already pushed the
  watermark 4 min ahead, so the new crisis lands in windows that only close
  once real news passes that point (next ingestion sweep or two). Identical in
  both engines; it is the simulate design, not an engine bug.
- Compose interpolates `${AWS_ACCESS_KEY_ID:-local}` from the SHELL before
  `.env`. An exported AWS key reaches dynamodb-local, which namespaces by key
  (and rejects keys with hyphens). Unset it when running locally.

---

# Previous handoff (2026-07-27), kept for history

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

## ~~OPEN DECISION~~ RESOLVED 2026-09-25 (un-capped alert score, see top)

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

.venv/bin/python -m pytest tests/        # expect 220 passed with dynamodb-local up
docker compose --profile flink up -d --build   # ~15 min cold; Flink image builds under emulation
# or: docker compose --profile lite up -d --build  (no Flink images at all)
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

- ~~No alert has ever fired end-to-end~~ (fired on both engines, 2026-09-25).
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
