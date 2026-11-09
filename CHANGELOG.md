# Changelog

## 0.1.0 (2026-11-09)

First release.

- Decoder for every pgoutput message of protocol versions 1 to 4 (plain, streamed, two phase,
  parallel streaming), into frozen dataclasses; a stateful `Decoder` with the relation cache and
  the streamed block state.
- `Converter`: text and binary column values to Python types, extensible per type OID.
- `pgoutput.sql`: read a slot with `pg_logical_slot_peek_binary_changes` or
  `pg_logical_slot_get_binary_changes` from any DB-API driver; `changes_query` for asyncpg.
- `pgoutput.psycopg2`: stream a slot over a psycopg2 replication connection, with `ack`.
- `parse_lsn`, `format_lsn`, `plugin_options`.
- Fixtures captured from PostgreSQL 14 to 18, checked against `test_decoding`.
