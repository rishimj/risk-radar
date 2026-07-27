"""Sliding window assignment must match Flink's, or the replay harness lies."""
import pytest

from riskcore.windows import Window, assign_windows, bucket, window_count, window_start_with_offset

MIN = 60_000
SIZE = 15 * MIN     # WINDOW_SIZE_SECONDS = 900
SLIDE = 3 * MIN     # WINDOW_SLIDE_SECONDS = 180


def test_element_lands_in_size_over_slide_windows():
    assert window_count(SIZE, SLIDE) == 5
    assert len(assign_windows(7 * MIN, SIZE, SLIDE)) == 5


def test_windows_are_contiguous_by_slide():
    ws = assign_windows(7 * MIN, SIZE, SLIDE)
    starts = [w.start_ms for w in ws]
    assert all(b - a == SLIDE for a, b in zip(starts, starts[1:]))


def test_every_assigned_window_actually_contains_the_timestamp():
    for ts in (0, 1, 7 * MIN, 61 * MIN + 17_000):
        for w in assign_windows(ts, SIZE, SLIDE):
            assert w.start_ms <= ts < w.end_ms


def test_window_size_is_preserved():
    for w in assign_windows(7 * MIN, SIZE, SLIDE):
        assert w.size_ms == SIZE


def test_start_is_aligned_to_the_slide():
    for w in assign_windows(7 * MIN + 12_345, SIZE, SLIDE):
        assert w.start_ms % SLIDE == 0


def test_element_on_a_boundary_belongs_to_the_new_window():
    """Windows are [start, end) — half-open, like Flink's."""
    ws = assign_windows(SLIDE, SIZE, SLIDE)
    assert Window(SLIDE, SLIDE + SIZE) in ws
    # and the window ending exactly at this timestamp must NOT be included
    assert all(w.end_ms > SLIDE for w in ws)


def test_earliest_window_ends_just_after_the_timestamp():
    """This is what the simulate path's watermark arithmetic depends on."""
    ts = 100 * MIN
    earliest = min(assign_windows(ts, SIZE, SLIDE))
    assert ts < earliest.end_ms <= ts + SLIDE


def test_window_start_with_offset_floors():
    assert window_start_with_offset(7 * MIN, 0, SLIDE) == 6 * MIN
    assert window_start_with_offset(6 * MIN, 0, SLIDE) == 6 * MIN


def test_tumbling_case_assigns_exactly_one_window():
    ws = assign_windows(7 * MIN, SIZE, SIZE)
    assert len(ws) == 1


def test_bucket_groups_payloads():
    items = [(0, "a"), (SLIDE, "b"), (SIZE + SLIDE, "c")]
    grouped = bucket(items, SIZE, SLIDE)
    # "a" and "c" are SIZE+SLIDE apart, so they never share a window
    for payloads in grouped.values():
        assert not ("a" in payloads and "c" in payloads)
    assert any("a" in p and "b" in p for p in grouped.values())


@pytest.mark.parametrize("bad", [(0, SLIDE), (SIZE, 0), (-1, SLIDE)])
def test_invalid_size_or_slide_rejected(bad):
    with pytest.raises(ValueError):
        assign_windows(0, bad[0], bad[1])
