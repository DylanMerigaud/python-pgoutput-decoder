"""Byte builders for pgoutput messages, written from the PostgreSQL logical replication
message formats page. All integers are big endian, a String is zero terminated.

Each function takes an optional ``xid``: the transaction id slot that is present only inside
a streamed block (between Stream Start and Stream Stop).
"""

from __future__ import annotations

import struct
from collections.abc import Iterable, Sequence


def i8(v: int) -> bytes:
    return struct.pack(">B", v)


def i16(v: int) -> bytes:
    return struct.pack(">h", v)


def i32(v: int) -> bytes:
    """Int32 field: accepts signed or unsigned values (xids, oids are unsigned in practice)."""
    return struct.pack(">I", v) if v >= 0 else struct.pack(">i", v)


def i64(v: int) -> bytes:
    return struct.pack(">Q", v) if v >= 0 else struct.pack(">q", v)


def cstr(s: str | bytes) -> bytes:
    raw = s.encode("utf-8") if isinstance(s, str) else s
    return raw + b"\x00"


def _x(xid: int | None) -> bytes:
    return b"" if xid is None else i32(xid)


# Tuple data. A column is ("n",), ("u",), ("t", bytes) or ("b", bytes).
Col = tuple


def tuple_data(cols: Sequence[Col]) -> bytes:
    out = i16(len(cols))
    for col in cols:
        kind = col[0]
        out += kind.encode("ascii")
        if kind in ("t", "b"):
            out += i32(len(col[1])) + col[1]
    return out


def begin(final_lsn: int, ts: int, xid: int) -> bytes:
    return b"B" + i64(final_lsn) + i64(ts) + i32(xid)


def commit(flags: int, commit_lsn: int, end_lsn: int, ts: int) -> bytes:
    return b"C" + i8(flags) + i64(commit_lsn) + i64(end_lsn) + i64(ts)


def origin(lsn: int, name: str) -> bytes:
    return b"O" + i64(lsn) + cstr(name)


def relation(
    oid: int,
    namespace: str,
    name: str,
    replica_identity: str,
    columns: Iterable[tuple[int, str, int, int]],
    xid: int | None = None,
) -> bytes:
    """``columns``: (flags, name, type oid, typmod)."""
    cols = list(columns)
    out = b"R" + _x(xid) + i32(oid) + cstr(namespace) + cstr(name)
    out += replica_identity.encode("ascii") + i16(len(cols))
    for flags, cname, type_oid, typmod in cols:
        out += i8(flags) + cstr(cname) + i32(type_oid) + i32(typmod)
    return out


def type_msg(oid: int, namespace: str, name: str, xid: int | None = None) -> bytes:
    return b"Y" + _x(xid) + i32(oid) + cstr(namespace) + cstr(name)


def insert(relid: int, new: Sequence[Col], xid: int | None = None) -> bytes:
    return b"I" + _x(xid) + i32(relid) + b"N" + tuple_data(new)


def update(
    relid: int,
    new: Sequence[Col],
    old: Sequence[Col] | None = None,
    old_marker: str | None = None,
    xid: int | None = None,
) -> bytes:
    out = b"U" + _x(xid) + i32(relid)
    if old is not None:
        out += old_marker.encode("ascii") + tuple_data(old)
    return out + b"N" + tuple_data(new)


def delete(relid: int, old: Sequence[Col], marker: str, xid: int | None = None) -> bytes:
    return b"D" + _x(xid) + i32(relid) + marker.encode("ascii") + tuple_data(old)


def truncate(relids: Sequence[int], options: int, xid: int | None = None) -> bytes:
    out = b"T" + _x(xid) + i32(len(relids)) + i8(options)
    for r in relids:
        out += i32(r)
    return out


def logical_message(
    flags: int, lsn: int, prefix: str, content: bytes, xid: int | None = None
) -> bytes:
    return b"M" + _x(xid) + i8(flags) + i64(lsn) + cstr(prefix) + i32(len(content)) + content


def stream_start(xid: int, first_segment: int) -> bytes:
    return b"S" + i32(xid) + i8(first_segment)


def stream_stop() -> bytes:
    return b"E"


def stream_commit(xid: int, flags: int, commit_lsn: int, end_lsn: int, ts: int) -> bytes:
    return b"c" + i32(xid) + i8(flags) + i64(commit_lsn) + i64(end_lsn) + i64(ts)


def stream_abort(
    xid: int, subxid: int, abort_lsn: int | None = None, abort_ts: int | None = None
) -> bytes:
    out = b"A" + i32(xid) + i32(subxid)
    if abort_lsn is not None:
        out += i64(abort_lsn) + i64(abort_ts)
    return out


def begin_prepare(prepare_lsn: int, end_lsn: int, ts: int, xid: int, gid: str) -> bytes:
    return b"b" + i64(prepare_lsn) + i64(end_lsn) + i64(ts) + i32(xid) + cstr(gid)


def prepare(
    flags: int, prepare_lsn: int, end_lsn: int, ts: int, xid: int, gid: str, tag: bytes = b"P"
) -> bytes:
    return tag + i8(flags) + i64(prepare_lsn) + i64(end_lsn) + i64(ts) + i32(xid) + cstr(gid)


def stream_prepare(
    flags: int, prepare_lsn: int, end_lsn: int, ts: int, xid: int, gid: str
) -> bytes:
    return prepare(flags, prepare_lsn, end_lsn, ts, xid, gid, tag=b"p")


def commit_prepared(
    flags: int, commit_lsn: int, end_lsn: int, ts: int, xid: int, gid: str
) -> bytes:
    return prepare(flags, commit_lsn, end_lsn, ts, xid, gid, tag=b"K")


def rollback_prepared(
    flags: int,
    prepare_end_lsn: int,
    rollback_end_lsn: int,
    prepare_ts: int,
    rollback_ts: int,
    xid: int,
    gid: str,
) -> bytes:
    return (
        b"r"
        + i8(flags)
        + i64(prepare_end_lsn)
        + i64(rollback_end_lsn)
        + i64(prepare_ts)
        + i64(rollback_ts)
        + i32(xid)
        + cstr(gid)
    )


BIG_LSN = 2**64 - 5
BIG_XID = 2**32 - 3


def sample_messages():
    """One valid encoding of every message type, as (bytes, in_stream) pairs."""
    cols = [(1, "id", 23, -1), (0, "name", 25, 68)]
    row = [("t", b"7"), ("n",), ("u",), ("b", b"\x00\x01"), ("t", b"")]
    out = [
        (begin(BIG_LSN, -5, BIG_XID), False),
        (commit(0, 10, BIG_LSN, 123456789), False),
        (origin(77, "node_a"), False),
        (relation(16384, "public", "users", "d", cols), False),
        (relation(16384, "public", "users", "f", cols, xid=BIG_XID), True),
        (relation(1, "", "t", "n", []), False),
        (type_msg(16500, "public", "mood"), False),
        (type_msg(16500, "public", "mood", xid=9), True),
        (insert(16384, row), False),
        (insert(16384, row, xid=9), True),
        (update(16384, row), False),
        (update(16384, row, row[:2], "K"), False),
        (update(16384, row, row, "O", xid=BIG_XID), True),
        (delete(16384, row, "K"), False),
        (delete(16384, row, "O", xid=9), True),
        (truncate([1, 2, 3], 3), False),
        (truncate([1, 2, BIG_XID], 1, xid=9), True),
        (truncate([], 0), False),
        (logical_message(1, 5, "pfx", b"a\x00b"), False),
        (logical_message(0, 5, "pfx", b"", xid=9), True),
        (stream_start(BIG_XID, 1), False),
        (stream_stop(), True),
        (stream_commit(BIG_XID, 0, BIG_LSN, 5, 6), False),
        (stream_abort(1, 2), False),
        (stream_abort(1, 2, BIG_LSN, 99), False),
        (begin_prepare(BIG_LSN, 2, 3, BIG_XID, "gid-1"), False),
        (prepare(0, BIG_LSN, 2, 3, 4, "gid-1"), False),
        (commit_prepared(0, BIG_LSN, 2, 3, 4, "gid-1"), False),
        (rollback_prepared(0, BIG_LSN, 2, 3, 4, 5, "gid-1"), False),
        (stream_prepare(0, BIG_LSN, 2, 3, 4, "gid-1"), False),
    ]
    return out
