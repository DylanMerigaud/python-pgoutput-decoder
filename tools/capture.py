"""Capture pgoutput bytes from a live PostgreSQL server, for the test fixtures.

Each scenario creates its own slots (one pgoutput, one test_decoding as the reference), runs
its SQL, then reads both slots through the SQL interface. The pgoutput rows are stored as hex
next to the test_decoding lines for the same changes, in
``tests/fixtures/pg<major>/<scenario>.json``.

    python tools/capture.py "host=127.0.0.1 port=5432 user=postgres dbname=postgres"

The server needs ``wal_level=logical`` and ``max_prepared_transactions`` above 0. The same
functions run the live integration tests (``tests/integration``).
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from pgoutput.options import plugin_options  # noqa: E402

PUBLICATION = "pgo_pub"

SCHEMA = """
DROP PUBLICATION IF EXISTS pgo_pub;
DROP SCHEMA IF EXISTS pgo CASCADE;
CREATE SCHEMA pgo;
CREATE TYPE pgo.mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE pgo.items (
    id int PRIMARY KEY,
    name text,
    price numeric(10, 2),
    tags text[],
    meta jsonb,
    created timestamptz,
    day date,
    mood pgo.mood,
    big text,
    payload bytea,
    uid uuid,
    flag boolean,
    ratio float8,
    span interval,
    counts int[]
);
ALTER TABLE pgo.items ALTER COLUMN big SET STORAGE EXTERNAL;
CREATE TABLE pgo.full_rows (id int PRIMARY KEY, note text);
ALTER TABLE pgo.full_rows REPLICA IDENTITY FULL;
CREATE TABLE pgo.bulk (id int PRIMARY KEY, filler text);
CREATE PUBLICATION pgo_pub FOR TABLE pgo.items, pgo.full_rows, pgo.bulk;
"""

FULL_ROW = (
    "INSERT INTO pgo.items VALUES (1, 'apple', 1.50, '{a,\"b c\",NULL}', "
    "'{\"k\": [1, 2.5, null], \"s\": \"x\"}', '2024-01-02 03:04:05.123456+00', '2024-02-29', "
    "'happy', repeat('x', 4000), '\\xdeadbeef00', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', "
    "true, 0.5, '1 year 2 mons 3 days 04:05:06.5', '{1,2,3}')"
)


def big_tx(start: int, rows: int = 160) -> str:
    """Enough rows to pass logical_decoding_work_mem=64kB, so the server streams."""
    return (
        f"INSERT INTO pgo.bulk SELECT g, repeat(md5(g::text), 16) "
        f"FROM generate_series({start}, {start + rows - 1}) g"
    )


@dataclass
class Scenario:
    name: str
    min_version: int
    sql: List[str]
    options: Dict[str, Any] = field(default_factory=dict)
    two_phase_slot: bool = False
    reference: bool = True


SCENARIOS: List[Scenario] = [
    Scenario(
        "basic",
        140000,
        [
            FULL_ROW,
            "INSERT INTO pgo.items (id, name) VALUES (2, NULL)",
            "UPDATE pgo.items SET price = 2.25 WHERE id = 1",
            "UPDATE pgo.items SET id = 3 WHERE id = 2",
            "DELETE FROM pgo.items WHERE id = 3",
            "INSERT INTO pgo.full_rows VALUES (1, 'one')",
            "UPDATE pgo.full_rows SET note = 'uno' WHERE id = 1",
            "DELETE FROM pgo.full_rows WHERE id = 1",
            "BEGIN; INSERT INTO pgo.full_rows VALUES (2, 'two'); "
            "SELECT pg_logical_emit_message(true, 'pgo', 'transactional hello'); COMMIT",
            "SELECT pg_logical_emit_message(false, 'pgo', 'plain hello')",
            "TRUNCATE pgo.bulk, pgo.full_rows RESTART IDENTITY CASCADE",
        ],
        {"proto_version": 1, "messages": True},
    ),
    Scenario(
        "binary",
        140000,
        [
            FULL_ROW,
            "INSERT INTO pgo.items (id, price, created, ratio) "
            "VALUES (2, -12345.67, 'infinity', 'NaN')",
            "UPDATE pgo.items SET price = 0.01 WHERE id = 1",
        ],
        {"proto_version": 1, "binary": True},
    ),
    Scenario(
        "origin",
        140000,
        [
            "SELECT pg_replication_origin_create('pgo_origin') WHERE NOT EXISTS "
            "(SELECT 1 FROM pg_replication_origin WHERE roname = 'pgo_origin')",
            "SELECT pg_replication_origin_session_setup('pgo_origin')",
            "BEGIN; SELECT pg_replication_origin_xact_setup('0/ABCDEF', "
            "'2024-05-06 07:08:09+00'); INSERT INTO pgo.full_rows VALUES (7, 'from origin'); "
            "COMMIT",
            "SELECT pg_replication_origin_session_reset()",
        ],
        {"proto_version": 1},
    ),
    Scenario(
        "stream",
        140000,
        [
            "BEGIN; "
            + big_tx(1)
            + "; SAVEPOINT s1; "
            + big_tx(1001)
            + "; ROLLBACK TO SAVEPOINT s1; "
            "INSERT INTO pgo.items (id, name) VALUES (10, 'streamed'); COMMIT",
            "BEGIN; " + big_tx(2001) + "; ROLLBACK",
            # A later transaction makes the server decode past the abort record.
            "INSERT INTO pgo.full_rows VALUES (30, 'after the abort')",
        ],
        {"proto_version": 2, "streaming": True},
        reference=False,
    ),
    Scenario(
        "two_phase",
        150000,
        [
            "BEGIN; INSERT INTO pgo.items (id, name) VALUES (20, 'prepared'); "
            "PREPARE TRANSACTION 'pgo_gid_1'",
            "COMMIT PREPARED 'pgo_gid_1'",
            "BEGIN; INSERT INTO pgo.items (id, name) VALUES (21, 'rolled back'); "
            "PREPARE TRANSACTION 'pgo_gid_2'",
            "ROLLBACK PREPARED 'pgo_gid_2'",
        ],
        {"proto_version": 3, "two_phase": True},
        two_phase_slot=True,
        reference=False,
    ),
    Scenario(
        "stream_prepare",
        150000,
        [
            "BEGIN; " + big_tx(3001) + "; PREPARE TRANSACTION 'pgo_gid_3'",
            "COMMIT PREPARED 'pgo_gid_3'",
        ],
        {"proto_version": 3, "streaming": True, "two_phase": True},
        two_phase_slot=True,
        reference=False,
    ),
    Scenario(
        "stream_parallel",
        160000,
        [
            "BEGIN; "
            + big_tx(4001)
            + "; SAVEPOINT s1; "
            + big_tx(5001)
            + "; ROLLBACK TO SAVEPOINT s1; COMMIT",
            "BEGIN; " + big_tx(6001) + "; ROLLBACK",
            "INSERT INTO pgo.full_rows VALUES (31, 'after the abort')",
        ],
        {"proto_version": 4, "streaming": "parallel"},
        reference=False,
    ),
]


def _exec(cur: Any, sql: str) -> None:
    cur.execute(sql)


def run_scenario(conn: Any, scenario: Scenario) -> Dict[str, Any]:
    """Run one scenario on an autocommit psycopg2 connection; return the fixture dict."""
    cur = conn.cursor()
    cur.execute("SHOW server_version_num")
    version = int(cur.fetchone()[0])
    _exec(cur, SCHEMA)
    slot = f"pgo_{scenario.name}"
    ref_slot = f"pgo_{scenario.name}_ref"
    for s in (slot, ref_slot):
        cur.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (s,),
        )
    if scenario.two_phase_slot:
        cur.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput', false, true)", (slot,)
        )
    else:
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
    if scenario.reference:
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", (ref_slot,))
    for sql in scenario.sql:
        _exec(cur, sql)
    cur.execute("SET logical_decoding_work_mem = '64kB'")
    opts = plugin_options(PUBLICATION, **scenario.options)
    flat: List[str] = []
    for k, v in opts.items():
        flat += [k, v]
    cur.execute(
        "SELECT lsn::text, xid::text, data FROM "
        "pg_logical_slot_get_binary_changes(%s, NULL, NULL, VARIADIC %s::text[])",
        (slot, flat),
    )
    rows = [(lsn, xid, bytes(data).hex()) for lsn, xid, data in cur.fetchall()]
    reference: Optional[List[str]] = None
    if scenario.reference:
        cur.execute(
            "SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL, "
            "'include-xids', '0', 'skip-empty-xacts', '1')",
            (ref_slot,),
        )
        reference = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT pg_drop_replication_slot(%s)", (ref_slot,))
    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
    cur.execute("RESET logical_decoding_work_mem")
    return {
        "scenario": scenario.name,
        "server_version_num": version,
        "options": opts,
        "rows": rows,
        "reference": reference,
    }


def capture(dsn: str, out_dir: Optional[str] = None) -> List[Tuple[str, int]]:
    import psycopg2

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SHOW server_version_num")
    version = int(cur.fetchone()[0])
    major = version // 10000
    target = out_dir or os.path.join(ROOT, "tests", "fixtures", f"pg{major}")
    os.makedirs(target, exist_ok=True)
    written = []
    for scenario in SCENARIOS:
        if version < scenario.min_version:
            continue
        fixture = run_scenario(conn, scenario)
        path = os.path.join(target, f"{scenario.name}.json")
        with open(path, "w") as fh:
            json.dump(fixture, fh, indent=1)
            fh.write("\n")
        written.append((path, len(fixture["rows"])))
    conn.close()
    return written


if __name__ == "__main__":
    for path, n in capture(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None):
        print(f"{n:5d} messages  {os.path.relpath(path, ROOT)}")
