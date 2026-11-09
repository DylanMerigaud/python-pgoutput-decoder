"""Decode speed against pypgoutput, on the row messages captured from PostgreSQL 17.

    pip install pypgoutput psycopg2-binary pydantic   # pypgoutput's own dependencies
    python benchmarks/bench.py

pypgoutput decodes protocol version 1 only (no streaming, no two phase, no logical messages), so
the comparison uses the messages both libraries read: Begin, Relation, Insert, Update, Delete,
Truncate and Commit from tests/fixtures/pg17/basic.json, decoded in a loop by each library.
"""

from __future__ import annotations

import json
import os
import sys
import timeit
from typing import Callable, Dict, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from pgoutput import decode_message  # noqa: E402

FIXTURE = os.path.join(ROOT, "tests", "fixtures", "pg17", "basic.json")
TAGS = set(b"BRIUDTC")


def payloads() -> List[bytes]:
    with open(FIXTURE) as fh:
        rows = json.load(fh)["rows"]
    out = [bytes.fromhex(h) for _, _, h in rows]
    return [p for p in out if p[0] in TAGS]


def ours(msgs: List[bytes]) -> Callable[[], None]:
    def run() -> None:
        for m in msgs:
            decode_message(m)

    return run


def theirs(msgs: List[bytes]) -> Callable[[], None]:
    import pypgoutput

    by_tag: Dict[int, Callable[[bytes], object]] = {
        ord("B"): pypgoutput.Begin,
        ord("R"): pypgoutput.Relation,
        ord("I"): pypgoutput.Insert,
        ord("U"): pypgoutput.Update,
        ord("D"): pypgoutput.Delete,
        ord("T"): pypgoutput.Truncate,
        ord("C"): pypgoutput.Commit,
    }

    def run() -> None:
        for m in msgs:
            by_tag[m[0]](m)

    return run


def main() -> None:
    msgs = payloads()
    loops = 2000
    results = {"pgoutput": ours(msgs)}
    try:
        results["pypgoutput"] = theirs(msgs)
    except ImportError:
        print("pypgoutput is not installed: timing pgoutput alone")
    timings = {}
    for name, fn in results.items():
        best = min(timeit.repeat(fn, number=loops, repeat=5))
        timings[name] = best / (loops * len(msgs)) * 1e6
        print(f"{name:12s} {timings[name]:7.2f} microseconds per message ({len(msgs)} messages)")
    if len(timings) == 2:
        print(f"ratio        {timings['pypgoutput'] / timings['pgoutput']:.1f}x")


if __name__ == "__main__":
    main()
