"""Sliding event-time window assignment, mirroring Flink's semantics.

The calibration/seeding harness replays historical articles offline, but its
numbers are only meaningful if it buckets them exactly the way the running job
does. So this reimplements Flink's SlidingEventTimeWindows assignment rather
than approximating it:

    lastStart = timestamp - floorMod(timestamp - offset, slide)
    for start = lastStart; start > timestamp - size; start -= slide:
        emit window [start, start + size)

Note floorMod, not `%` — Python's `%` already floors for positive divisors, but
being explicit documents the intent and keeps negative epochs sane.
"""
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, order=True)
class Window:
    start_ms: int
    end_ms: int

    @property
    def size_ms(self) -> int:
        return self.end_ms - self.start_ms


def _floormod(a: int, b: int) -> int:
    return a - (a // b) * b


def window_start_with_offset(timestamp_ms: int, offset_ms: int, size_ms: int) -> int:
    return timestamp_ms - _floormod(timestamp_ms - offset_ms, size_ms)


def assign_windows(timestamp_ms: int, size_ms: int, slide_ms: int,
                   offset_ms: int = 0) -> List[Window]:
    """Every sliding window that contains `timestamp_ms`."""
    if size_ms <= 0 or slide_ms <= 0:
        raise ValueError("size and slide must be positive")

    last_start = window_start_with_offset(timestamp_ms, offset_ms, slide_ms)
    out: List[Window] = []
    start = last_start
    while start > timestamp_ms - size_ms:
        out.append(Window(start, start + size_ms))
        start -= slide_ms
    out.reverse()
    return out


def window_count(size_ms: int, slide_ms: int) -> int:
    """How many windows any single element lands in."""
    return -(-size_ms // slide_ms)          # ceil division


def bucket(items: Iterable[Tuple[int, T]], size_ms: int, slide_ms: int,
           offset_ms: int = 0) -> Dict[Window, List[T]]:
    """Group (timestamp_ms, payload) pairs into their sliding windows."""
    out: Dict[Window, List[T]] = {}
    for ts, payload in items:
        for w in assign_windows(ts, size_ms, slide_ms, offset_ms):
            out.setdefault(w, []).append(payload)
    return out
