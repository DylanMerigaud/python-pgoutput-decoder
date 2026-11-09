"""Check that a fresh capture decodes to the same sequence of messages as the stored fixtures.

    python tools/compare_fixtures.py <fresh dir> <stored dir>

LSNs, transaction ids and timestamps differ between runs; the message kinds, relations and row
values do not. A stored directory that does not exist yet passes (a new major version).
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, List, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from pgoutput import Decoder, Delete, Insert, Update  # noqa: E402


def shape(path: str) -> List[Tuple[Any, ...]]:
    with open(path) as fh:
        rows = json.load(fh)["rows"]
    dec = Decoder()
    out: List[Tuple[Any, ...]] = []
    for _, _, h in rows:
        m = dec.decode(bytes.fromhex(h))
        if isinstance(m, (Insert, Update)):
            name = m.relation.qualified_name if m.relation else None
            out.append((type(m).__name__, name, m.new.values))
        elif isinstance(m, Delete):
            name = m.relation.qualified_name if m.relation else None
            out.append(("Delete", name, m.old_kind, m.old.values))
        else:
            out.append((type(m).__name__,))
    return out


def main(fresh: str, stored: str) -> int:
    if not os.path.isdir(stored):
        print(f"{stored} does not exist: nothing to compare")
        return 0
    bad = 0
    for name in sorted(os.listdir(fresh)):
        old = os.path.join(stored, name)
        if not os.path.exists(old):
            print(f"{name}: not stored yet")
            continue
        a, b = shape(os.path.join(fresh, name)), shape(old)
        if a != b:
            bad += 1
            print(f"{name}: DIFFERS ({len(a)} messages fresh, {len(b)} stored)")
        else:
            print(f"{name}: same {len(a)} messages")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
