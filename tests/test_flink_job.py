"""Static guards on flink/job/news_job.py (PyFlink is not installed on the host)."""
import re
from pathlib import Path

JOB = (Path(__file__).resolve().parents[1] / "flink" / "job" / "news_job.py").read_text()


def _watermark_chain() -> str:
    start = JOB.index("assign_timestamps_and_watermarks(")
    return JOB[start:JOB.index('.name("watermarks")', start)]


def test_timestamp_assigner_is_the_last_call_in_the_watermark_chain():
    """Regression: PyFlink's with_idleness() drops a Python timestamp assigner.

    WatermarkStrategy.with_idleness returns WatermarkStrategy(j_strategy), a new
    wrapper that does not carry the Python-side assigner. Calling it AFTER
    with_timestamp_assigner made the job window on Kafka produce time instead
    of each article's published_at. Reproduced against PyFlink 1.20.1.
    """
    calls = re.findall(r"\.(with_\w+|for_\w+)\(", _watermark_chain())
    assert "with_timestamp_assigner" in calls
    assert calls[-1] == "with_timestamp_assigner", calls


def test_alerting_runs_before_the_baseline_write():
    tail = JOB[JOB.index("def build_pipeline"):]
    assert tail.index("AlertFanout()") < tail.index("FeatureWriter()")


def test_job_delegates_outcomes_to_the_shared_stages():
    for fn in ("enrich_batch", "write_headline", "evaluate_and_alert", "write_features"):
        assert f"stages.{fn}(" in JOB, fn
