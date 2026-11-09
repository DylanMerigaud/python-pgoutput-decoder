"""Live tests against a real PostgreSQL server: set PGOUTPUT_TEST_DSN to a libpq connection
string of a superuser on a server with ``wal_level=logical`` and ``max_prepared_transactions``
above 0, e.g. ``host=127.0.0.1 port=5432 user=postgres dbname=postgres``.

They run every capture scenario fresh, then the same checks as the stored fixtures (including the
comparison with test_decoding), and drive the psycopg2 streaming and SQL adapters end to end.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any, Iterator

import pytest

DSN = os.environ.get("PGOUTPUT_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="PGOUTPUT_TEST_DSN is not set")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))

import test_fixtures as checks  # noqa: E402

from pgoutput import Commit, Decoder, Insert, Relation, parse_lsn  # noqa: E402
from pgoutput.sql import read_changes  # noqa: E402

psycopg2 = pytest.importorskip("psycopg2")
extras = pytest.importorskip("psycopg2.extras")


@pytest.fixture()
def conn() -> Iterator[Any]:
    c = psycopg2.connect(DSN)
    c.autocommit = True
    yield c
    c.close()


def test_every_scenario_live(conn: Any, tmp_path: Path) -> None:
    import capture

    written = capture.capture(DSN, str(tmp_path))
    assert written
    names = {Path(p).stem for p, _ in written}
    assert {"basic", "binary", "origin", "stream"} <= names
    for path, _ in written:
        name = Path(path).stem
        checks.test_every_message_decodes_and_matches_the_server_envelope(path)
        if name in ("basic", "origin"):
            checks.test_text_values_equal_test_decoding(path)
        scenario_check = getattr(checks, f"test_{name}_scenario", None)
        if scenario_check is not None:
            scenario_check(path)
        if name == "binary":
            checks.test_binary_values_convert_like_the_text_ones(path)


def _drop_slot(cur: Any, slot: str) -> None:
    # A closed replication connection's walsender can hold the slot for a moment.
    deadline = time.monotonic() + 10
    while True:
        cur.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot,))
        row = cur.fetchone()
        if row is None:
            return
        if not row[0] or time.monotonic() > deadline:
            cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
            return
        time.sleep(0.1)


def _fresh_table(cur: Any, slot: str) -> None:
    _drop_slot(cur, slot)
    cur.execute("DROP PUBLICATION IF EXISTS pgo_live_pub")
    cur.execute("DROP TABLE IF EXISTS pgo_live")
    cur.execute("CREATE TABLE pgo_live (id int PRIMARY KEY, note text)")
    cur.execute("CREATE PUBLICATION pgo_live_pub FOR TABLE pgo_live")


def test_psycopg2_stream_and_ack(conn: Any) -> None:
    from pgoutput.psycopg2 import ack, events, start

    slot = "pgo_live_stream"
    cur = conn.cursor()
    _fresh_table(cur, slot)
    rconn = psycopg2.connect(DSN, connection_factory=extras.LogicalReplicationConnection)
    rcur = rconn.cursor()
    try:
        rcur.create_replication_slot(slot, output_plugin="pgoutput")
        cur.execute("INSERT INTO pgo_live VALUES (1, 'one'), (2, 'two')")
        start(rcur, slot, "pgo_live_pub", proto_version=1)
        seen = []
        commit = None
        for event in events(rcur, idle_timeout=3):
            seen.append(event.message)
            if isinstance(event.message, Commit):
                commit = event.message
                ack(rcur, commit.end_lsn)
                break
        inserts = [m for m in seen if isinstance(m, Insert)]
        assert [i.new[1].text() for i in inserts] == ["one", "two"]
        assert inserts[0].relation is not None and inserts[0].relation.name == "pgo_live"
        assert commit is not None
        # psycopg2 sends the feedback with the next status update: read until the server
        # shows the slot's confirmed position at the commit.
        deadline = time.monotonic() + 10
        confirmed = 0
        while time.monotonic() < deadline:
            rcur.send_feedback(force=True)
            cur.execute(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s",
                (slot,),
            )
            confirmed = parse_lsn(cur.fetchone()[0])
            if confirmed >= commit.end_lsn:
                break
            time.sleep(0.2)
        assert confirmed >= commit.end_lsn
    finally:
        rconn.close()
        _drop_slot(cur, slot)


def test_sql_peek_then_get(conn: Any) -> None:
    slot = "pgo_live_sql"
    cur = conn.cursor()
    _fresh_table(cur, slot)
    cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
    try:
        cur.execute("INSERT INTO pgo_live VALUES (1, 'one')")
        cur.execute("UPDATE pgo_live SET note = 'uno' WHERE id = 1")
        peeked = read_changes(cur, slot, "pgo_live_pub")
        again = read_changes(cur, slot, "pgo_live_pub")
        assert [type(c.message).__name__ for c in peeked] == [
            type(c.message).__name__ for c in again
        ]
        assert any(isinstance(c.message, Relation) for c in peeked)
        got = read_changes(cur, slot, "pgo_live_pub", peek=False, decoder=Decoder())
        assert len(got) == len(peeked)
        assert read_changes(cur, slot, "pgo_live_pub") == []
        commits = [c for c in got if isinstance(c.message, Commit)]
        assert len(commits) == 2 and all(c.lsn == c.message.end_lsn for c in commits)
    finally:
        cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))


def test_sql_with_psycopg3() -> None:
    psycopg = pytest.importorskip("psycopg")
    slot = "pgo_live_psycopg3"
    with psycopg.connect(DSN, autocommit=True) as c:
        cur = c.cursor()
        _fresh_table(cur, slot)
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
        try:
            cur.execute("INSERT INTO pgo_live VALUES (5, 'five')")
            changes = read_changes(cur, slot, "pgo_live_pub", binary=True)
            (ins,) = [c.message for c in changes if isinstance(c.message, Insert)]
            assert ins.new[0].data == (5).to_bytes(4, "big")
        finally:
            cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
