"""Parse the output of PostgreSQL's ``test_decoding`` plugin, the reference the fixtures and the
live tests compare pgoutput against.

A change line looks like:
    table pgo.items: INSERT: id[integer]:1 name[text]:'apple' flag[boolean]:true
    table pgo.items: UPDATE: old-key: id[integer]:2 new-tuple: id[integer]:3 name[text]:null
    table pgo.items: DELETE: id[integer]:3
    table pgo.bulk, pgo.full_rows: TRUNCATE: restart_seqs cascade
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

UNCHANGED = "unchanged-toast-datum"


@dataclass
class RefChange:
    tables: Tuple[str, ...]
    action: str
    new: Optional[Dict[str, Union[str, None]]] = None
    old: Optional[Dict[str, Union[str, None]]] = None
    truncate_flags: Tuple[str, ...] = ()


def _columns(s: str) -> Dict[str, Union[str, None]]:
    out: Dict[str, Union[str, None]] = {}
    i = 0
    while i < len(s):
        while i < len(s) and s[i] == " ":
            i += 1
        if i >= len(s):
            break
        lb = s.index("[", i)
        name = s[i:lb]
        # The type name may hold brackets (text[]), never "]:": the value starts after it.
        i = s.index("]:", lb) + 2
        if s[i] == "'":
            i += 1
            buf = []
            while True:
                if s[i] == "'":
                    if s[i + 1 : i + 2] == "'":
                        buf.append("'")
                        i += 2
                        continue
                    i += 1
                    break
                buf.append(s[i])
                i += 1
            out[name] = "".join(buf)
        else:
            end = s.find(" ", i)
            end = len(s) if end < 0 else end
            token = s[i:end]
            i = end
            if token == "null":
                out[name] = None
            elif token == "true":
                out[name] = "t"
            elif token == "false":
                out[name] = "f"
            else:
                out[name] = token
    return out


def parse(lines: List[str]) -> List[RefChange]:
    changes: List[RefChange] = []
    for line in lines:
        if not line.startswith("table "):
            continue
        head, _, tail = line[len("table ") :].partition(": ")
        action, _, rest = tail.partition(":")
        rest = rest.strip()
        tables = tuple(t.strip() for t in head.split(","))
        if action == "TRUNCATE":
            flags = tuple(f for f in rest.split() if f != "(no-flags)")
            changes.append(RefChange(tables, action, truncate_flags=flags))
        elif action == "UPDATE" and rest.startswith("old-key: "):
            old, new = rest[len("old-key: ") :].split(" new-tuple: ", 1)
            changes.append(RefChange(tables, action, new=_columns(new), old=_columns(old)))
        elif action == "DELETE":
            changes.append(RefChange(tables, action, old=_columns(rest)))
        else:
            changes.append(RefChange(tables, action, new=_columns(rest)))
    return changes
