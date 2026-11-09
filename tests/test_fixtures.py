"""Decode the bytes captured from real PostgreSQL servers (tools/capture.py) and check them
against the scenario that produced them and against test_decoding's output for the same
changes."""

from __future__ import annotations

import glob
import json
import math
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import pytest

from pgoutput import (
    UNCHANGED_TOAST,
    Begin,
    BeginPrepare,
    Commit,
    CommitPrepared,
    Converter,
    Decoder,
    Delete,
    Insert,
    LogicalMessage,
    Origin,
    Prepare,
    Relation,
    RollbackPrepared,
    StreamAbort,
    StreamCommit,
    StreamPrepare,
    StreamStart,
    StreamStop,
    Truncate,
    TupleKind,
    Type,
    Update,
    parse_lsn,
)

from reference import UNCHANGED, parse

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = sorted(glob.glob(os.path.join(HERE, "fixtures", "pg*", "*.json")))


def _id(path: str) -> str:
    return f"{os.path.basename(os.path.dirname(path))}-{os.path.basename(path)[:-5]}"


def load(path: str) -> Tuple[Dict[str, Any], List[Tuple[int, int, Any]]]:
    with open(path) as fh:
        fx = json.load(fh)
    dec = Decoder()
    msgs = [(parse_lsn(lsn), int(xid), dec.decode(bytes.fromhex(h))) for lsn, xid, h in fx["rows"]]
    return fx, msgs


def by_scenario(name: str) -> List[str]:
    return [p for p in FIXTURES if os.path.basename(p) == f"{name}.json"]


def test_fixtures_exist() -> None:
    assert FIXTURES, "run tools/capture.py against a server first"


@pytest.mark.parametrize("path", FIXTURES, ids=_id)
def test_every_message_decodes_and_matches_the_server_envelope(path: str) -> None:
    _, msgs = load(path)
    assert msgs
    in_stream = False
    stream_xid = None
    for lsn, xid, msg in msgs:
        if isinstance(msg, (Begin, BeginPrepare)):
            assert msg.xid == xid
        if isinstance(msg, Commit):
            # The changes function reports a commit at the end of its WAL record.
            assert msg.end_lsn == lsn
            assert msg.end_lsn > msg.commit_lsn
        if isinstance(msg, StreamStart):
            assert not in_stream
            in_stream, stream_xid = True, msg.xid
            assert msg.xid == xid
        elif isinstance(msg, StreamStop):
            assert in_stream
            in_stream = False
        elif in_stream:
            # Inside a streamed block every row and catalog message names the transaction, or
            # the subtransaction (a savepoint) that made the change, which comes after it.
            assert stream_xid is not None
            assert getattr(msg, "xid", stream_xid) >= stream_xid
        if isinstance(msg, (Insert, Update)):
            assert msg.relation is not None
            assert len(msg.new) == len(msg.relation.columns)
    assert not in_stream


def _text_rows(msgs: List[Tuple[int, int, Any]]) -> List[Tuple[Tuple[str, ...], str, Any, Any]]:
    out = []
    for _, _, m in msgs:
        if isinstance(m, Insert):
            out.append(((m.relation.qualified_name,), "INSERT", m.new.as_dict(m.relation), None))
        elif isinstance(m, Update):
            old = m.old.as_dict(m.relation) if m.old is not None else None
            out.append(((m.relation.qualified_name,), "UPDATE", m.new.as_dict(m.relation), old))
        elif isinstance(m, Delete):
            out.append(((m.relation.qualified_name,), "DELETE", None, m.old.as_dict(m.relation)))
        elif isinstance(m, Truncate):
            names = tuple(r.qualified_name for r in m.relations or ())
            out.append((names, "TRUNCATE", m, None))
    return out


def _raw(v: Any) -> Any:
    if v.kind == TupleKind.NULL:
        return None
    if v.kind == TupleKind.UNCHANGED_TOAST:
        return UNCHANGED
    return v.data.decode("utf-8")


REFERENCED = [p for p in FIXTURES if os.path.basename(p) in ("basic.json", "origin.json")]


@pytest.mark.parametrize("path", REFERENCED, ids=_id)
def test_text_values_equal_test_decoding(path: str) -> None:
    fx, msgs = load(path)
    ours = _text_rows(msgs)
    ref = parse(fx["reference"])
    assert len(ours) == len(ref)
    for (tables, action, new, old), r in zip(ours, ref):
        assert tables == r.tables and action == r.action
        if action == "TRUNCATE":
            flags = set(r.truncate_flags)
            assert new.cascade == ("cascade" in flags)
            assert new.restart_identity == ("restart_seqs" in flags)
            continue
        if new is not None:
            assert {k: _raw(v) for k, v in new.items()} == r.new
        if old is not None and r.old is not None:
            # An old key carries every column, null outside the key: test_decoding prints the
            # key columns only.
            ours_old = {k: _raw(v) for k, v in old.items()}
            assert {k: ours_old[k] for k in r.old} == r.old


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a):
        return math.isnan(b)
    return bool(a == b)


@pytest.mark.parametrize("path", by_scenario("binary"), ids=_id)
def test_binary_values_convert_like_the_text_ones(path: str) -> None:
    fx, msgs = load(path)
    conv = Converter()
    for _, _, m in msgs:
        if isinstance(m, Type):  # an enum: its send format is its label
            conv.register(m.type_oid, binary=lambda b: b.decode("utf-8"))
    ref = parse(fx["reference"])
    rows = [m for _, _, m in msgs if isinstance(m, (Insert, Update))]
    assert len(rows) == len(ref) == 3
    for m, r in zip(rows, ref):
        assert all(v.kind != TupleKind.TEXT for v in m.new)
        ours = conv.row(m.relation, m.new)
        for col in m.relation.columns:
            text = r.new[col.name]
            if text is None:
                assert ours[col.name] is None
            elif text == UNCHANGED:
                assert ours[col.name] is UNCHANGED_TOAST
            else:
                assert _same(ours[col.name], conv.text(col.type_oid, text)), col.name
    first = conv.row(rows[0].relation, rows[0].new)
    assert first["price"] == Decimal("1.50")
    assert first["tags"] == ["a", "b c", None]
    assert first["created"] == datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)
    assert first["payload"] == b"\xde\xad\xbe\xef\x00"


@pytest.mark.parametrize("path", by_scenario("basic"), ids=_id)
def test_basic_scenario(path: str) -> None:
    _, msgs = load(path)
    m = [x for _, _, x in msgs]
    rels = {r.qualified_name: r for r in m if isinstance(r, Relation)}
    # pgo.bulk is announced for the TRUNCATE only.
    assert set(rels) == {"pgo.items", "pgo.full_rows", "pgo.bulk"}
    assert rels["pgo.items"].replica_identity == "d"
    assert rels["pgo.full_rows"].replica_identity == "f"
    assert [c.name for c in rels["pgo.items"].key_columns] == ["id"]
    assert any(isinstance(t, Type) and t.name == "mood" and t.namespace == "pgo" for t in m)

    updates = [x for x in m if isinstance(x, Update)]
    toast_update, key_change, full_update = updates
    assert toast_update.old is None
    big = [c.name for c in rels["pgo.items"].columns].index("big")
    assert toast_update.new[big].is_unchanged_toast
    assert key_change.old_kind == "K"
    assert key_change.old is not None and key_change.old[0].text() == "2"
    assert full_update.old_kind == "O"
    assert full_update.old is not None and full_update.old[1].text() == "one"

    deletes = [x for x in m if isinstance(x, Delete)]
    assert [d.old_kind for d in deletes] == ["K", "O"]

    lm = [x for x in m if isinstance(x, LogicalMessage)]
    assert [(x.prefix, x.content, x.transactional) for x in lm] == [
        ("pgo", b"transactional hello", True),
        ("pgo", b"plain hello", False),
    ]
    (trunc,) = [x for x in m if isinstance(x, Truncate)]
    assert trunc.cascade and trunc.restart_identity
    assert trunc.relations is not None and len(trunc.relations) == 2


@pytest.mark.parametrize("path", by_scenario("origin"), ids=_id)
def test_origin_scenario(path: str) -> None:
    _, msgs = load(path)
    m = [x for _, _, x in msgs]
    (origin,) = [x for x in m if isinstance(x, Origin)]
    assert origin.name == "pgo_origin"
    assert origin.commit_lsn == parse_lsn("0/ABCDEF")
    (begin,) = [x for x in m if isinstance(x, Begin)]
    assert begin.commit_time == datetime(2024, 5, 6, 7, 8, 9, tzinfo=timezone.utc)


def _inserts(m: List[Any]) -> int:
    return sum(isinstance(x, Insert) for x in m)


@pytest.mark.parametrize("path", by_scenario("stream"), ids=_id)
def test_stream_scenario(path: str) -> None:
    _, msgs = load(path)
    m = [x for _, _, x in msgs]
    starts = [x for x in m if isinstance(x, StreamStart)]
    assert starts and starts[0].first_segment
    aborts = [x for x in m if isinstance(x, StreamAbort)]
    # The savepoint rolled back (subxid differs) and the whole transaction rolled back.
    assert len(aborts) == 2
    assert aborts[0].subxid != aborts[0].xid
    assert aborts[1].subxid == aborts[1].xid
    assert all(a.abort_lsn is None for a in aborts)
    (commit,) = [x for x in m if isinstance(x, StreamCommit)]
    assert commit.xid == starts[0].xid
    # The rows of the rolled back savepoint carry its subxid; they are 1001 to 1160.
    sub = aborts[0].subxid
    rolled = sorted(int(x.new[0].text()) for x in m if isinstance(x, Insert) and x.xid == sub)
    assert rolled and rolled == list(range(1001, 1001 + len(rolled)))
    # ROLLBACK TO SAVEPOINT keeps the savepoint open: the later row has a third xid, neither the
    # transaction's nor the aborted subtransaction's.
    aborted = {a.subxid for a in aborts}
    kept = [x for x in m if isinstance(x, Insert) and x.xid not in aborted]
    assert any(x.new[1].text() == "streamed" for x in kept)


@pytest.mark.parametrize("path", by_scenario("stream_parallel"), ids=_id)
def test_stream_parallel_scenario(path: str) -> None:
    _, msgs = load(path)
    aborts = [x for _, _, x in msgs if isinstance(x, StreamAbort)]
    assert len(aborts) == 2
    for a in aborts:
        assert a.abort_lsn is not None and a.abort_lsn > 0
        assert a.abort_time is not None and a.abort_time.year >= 2024


@pytest.mark.parametrize("path", by_scenario("two_phase"), ids=_id)
def test_two_phase_scenario(path: str) -> None:
    _, msgs = load(path)
    m = [x for _, _, x in msgs]
    assert [x.gid for x in m if isinstance(x, BeginPrepare)] == ["pgo_gid_1", "pgo_gid_2"]
    assert [x.gid for x in m if isinstance(x, Prepare)] == ["pgo_gid_1", "pgo_gid_2"]
    (cp,) = [x for x in m if isinstance(x, CommitPrepared)]
    (rp,) = [x for x in m if isinstance(x, RollbackPrepared)]
    assert cp.gid == "pgo_gid_1" and rp.gid == "pgo_gid_2"
    prep2 = [x for x in m if isinstance(x, Prepare)][1]
    assert rp.prepare_end_lsn == prep2.end_lsn
    assert rp.rollback_time >= prep2.prepare_time


@pytest.mark.parametrize("path", by_scenario("stream_prepare"), ids=_id)
def test_stream_prepare_scenario(path: str) -> None:
    _, msgs = load(path)
    m = [x for _, _, x in msgs]
    (sp,) = [x for x in m if isinstance(x, StreamPrepare)]
    (cp,) = [x for x in m if isinstance(x, CommitPrepared)]
    assert sp.gid == cp.gid == "pgo_gid_3"
    assert _inserts(m) == 160
