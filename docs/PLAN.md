# Plan: pgoutput, a Python decoder for PostgreSQL logical replication

## Goal

A pure Python 3.9+ package, `pgoutput` on PyPI (name checked free on 2026-11-09), MIT, zero
runtime dependencies, that turns the bytes of the built-in `pgoutput` logical decoding plugin
into typed dataclasses, for every message of protocol versions 1 to 4. Projects that hand-roll
this today (PostHog's warehouse CDC, dlt's `pg_replication`, Sentry monarch, AWS
dsql-migration-toolkit, Fyle pgevents) can replace one module with it.

## Sources

- Message formats: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
- Protocol versions and options: https://www.postgresql.org/docs/current/protocol-logical-replication.html
- Server side encoder, the ground truth: `src/backend/replication/logical/proto.c` and
  `src/backend/replication/pgoutput/pgoutput.c` (PostgreSQL 14 to 18).
- Binary send formats of the built-in types: `src/backend/utils/adt/*.c` (`*_send`).

## Public API

- `pgoutput.Decoder`: stateful, sans-IO. `decode(data: bytes) -> Message`. Keeps the relation
  and type caches and the "inside a streamed block" state (between Stream Start and Stream Stop
  the row messages carry a transaction id). No protocol version argument is needed: every
  difference between versions is visible on the wire.
- `pgoutput.decode_message(data, *, in_stream=False)`: the stateless function under it.
- Dataclasses, frozen, one per message: `Begin`, `Commit`, `Origin`, `Relation` (with
  `Column`), `Type`, `Insert`, `Update`, `Delete`, `Truncate`, `LogicalMessage`, `StreamStart`,
  `StreamStop`, `StreamCommit`, `StreamAbort`, `BeginPrepare`, `Prepare`, `CommitPrepared`,
  `RollbackPrepared`, `StreamPrepare`. Row data is `TupleData` of `TupleValue`s (`kind` null,
  unchanged TOAST, text or binary) with `as_dict()` helpers.
- `pgoutput.values.Converter`: type OID to Python value, text and binary formats, for the
  built-in types (bool, integers, oid, floats, numeric, text types, bytea, uuid, json, jsonb,
  date, time, timetz, timestamp, timestamptz, interval as text, one dimensional arrays of those
  in text format), extensible with `register()`.
- `pgoutput.lsn`: `parse_lsn("16/B374D848") -> int`, `format_lsn(int) -> str`.
- `pgoutput.sql`: the SQL interface path (`pg_logical_slot_peek_binary_changes` /
  `get_binary_changes`) with any DB-API cursor: builds the query with its options, decodes the
  rows. Works with psycopg 3, psycopg2, asyncpg (the query is plain SQL).
- `pgoutput.psycopg2`: the streaming replication path through psycopg2's
  `LogicalReplicationConnection`: `start_options()` and `stream()` that decodes each payload and
  sends LSN feedback. psycopg2 stays optional (import inside the module).
- A single file copy: `pgoutput/core.py` holds the decoder and the dataclasses with stdlib
  imports only, so a project that prefers in-tree code can vendor that one file.

## Test strategy

1. Unit tests from the spec: each message built byte by byte (a small test-side encoder written
   from the documentation, not from the decoder), decoded, checked field by field; truncated and
   corrupted input raises `DecodeError`, never another exception.
2. Captured fixtures: `tools/capture.py` runs a fixed scenario (inserts, updates with and without
   replica identity full, TOAST unchanged, deletes, truncate, logical messages, origins, a
   streamed large transaction, a prepared transaction committed and one rolled back, binary
   mode) on a live server for protocol versions 1 to 4 and stores the bytes as hex JSON under
   `tests/fixtures/pg<major>/`. Tests decode every fixture and check the expected rows.
3. Live integration (`tests/integration`, run when `PGOUTPUT_TEST_DSN` is set): the CI runs it
   against PostgreSQL 14, 15, 16, 17 and 18 containers; each change decoded from pgoutput is
   compared with the `test_decoding` plugin's output for the same transaction (the reference
   decoder that ships with PostgreSQL).
4. Fuzzing: random and mutated payloads, seeded, assert only `DecodeError`.
5. Benchmark: `benchmarks/bench.py` against pypgoutput on the same messages.
6. Contract workflow `.github/workflows/contract.yml`: weekly and on dispatch, installs the
   published package fresh from PyPI and runs the live integration tests against the newest
   PostgreSQL images.

## The library harness, criterion by criterion

- quickstart: the README opens with an install line and a runnable block that imports the
  package and decodes real captured bytes, offline.
- CI: `.github/workflows/ci.yml` (lint, mypy strict, tests on 3.9 to 3.13, the PostgreSQL
  matrix, the built wheel's examples).
- typed: `py.typed`, every public signature annotated, mypy strict.
- lint config: `ruff.toml`.
- why this one: a README comparison with pypgoutput, pgoutput-decoder (Rust), psycopg 3, and the
  in-tree decoders.
- examples: `examples/*.py` run offline from the fixtures.

## Done criteria

- Every message type of the documentation decodes, with tests; fixtures from a live server for
  protocol 1 to 4 decode; `pytest` green; `ruff check` and `mypy --strict` clean.
- `python3 scripts/oss_lib_harness.py check` (growth-cockpit) says PASS.
- An independent review passed with every finding fixed.
