# pgoutput

[![CI](https://github.com/DylanMerigaud/python-pgoutput-decoder/actions/workflows/ci.yml/badge.svg)](https://github.com/DylanMerigaud/python-pgoutput-decoder/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pgoutput.svg)](https://pypi.org/project/pgoutput/)
[![Python](https://img.shields.io/pypi/pyversions/pgoutput.svg)](https://pypi.org/project/pgoutput/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Decode PostgreSQL logical replication in Python. `pgoutput` turns the bytes of PostgreSQL's
built-in `pgoutput` plugin (the one behind `CREATE SUBSCRIPTION`, available on every server
since PostgreSQL 10 and on managed services like RDS, Cloud SQL, Azure and Supabase) into typed
dataclasses, for every message of the
[logical replication protocol](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html),
versions 1 to 4: plain transactions, streamed transactions, two phase commit and parallel
streaming.

It is for people building change data capture in Python: a CDC pipeline into a warehouse, a
cache invalidator, an audit log, an outbox reader, a zero downtime migration. The decoder is
pure Python 3.9+, sans IO and has no dependency, so it works with any driver and any event loop.

## Install

```bash
pip install pgoutput
```

## Quickstart

Decode the bytes a server sent (here a Relation, then an Insert and an Update captured from
PostgreSQL 17), with no database needed:

```python
from pgoutput import Converter, Decoder, Insert, Update

decoder = Decoder()  # keeps the relation cache and the streaming state
converter = Converter()  # type OIDs to Python values

for payload in [
    "520000415070676f0066756c6c5f726f7773006600020169640000000017ffffffff016e6f74650000000019ffffffff",
    "49000041504e000274000000013174000000036f6e65",
    "55000041504f000274000000013174000000036f6e654e00027400000001317400000003756e6f",
]:
    message = decoder.decode(bytes.fromhex(payload))
    if isinstance(message, (Insert, Update)):
        print(
            type(message).__name__,
            message.relation.qualified_name,
            converter.row(message.relation, message.new),
        )
```

Output:

```text
Insert pgo.full_rows {'id': 1, 'note': 'one'}
Update pgo.full_rows {'id': 1, 'note': 'uno'}
```

## Features

- Every message of protocol versions 1 to 4: Begin, Commit, Origin, Relation, Type, Insert,
  Update, Delete, Truncate, Message, Stream Start, Stream Stop, Stream Commit, Stream Abort
  (with the version 4 abort LSN and timestamp), Begin Prepare, Prepare, Commit Prepared,
  Rollback Prepared, Stream Prepare. Each is a frozen dataclass with typed fields.
- A stateful `Decoder` that tracks streamed blocks (where row messages carry a transaction id)
  and attaches the `Relation` to each Insert, Update, Delete and Truncate.
- Old rows as the server sends them: the old key (`old_kind == "K"`) or the whole old row with
  `REPLICA IDENTITY FULL` (`"O"`), and unchanged TOAST values marked as such, never confused with
  NULL.
- Text and binary (`binary 'true'`) column formats, converted by `Converter` to `int`,
  `Decimal`, `datetime`, `UUID`, parsed JSON, `bytes`, `ipaddress` objects, nested lists for
  arrays and an `Interval` type; extensible with `Converter.register` for extension types.
- Two ways to read a slot: the streaming replication protocol through psycopg2
  (`pgoutput.psycopg2`), or plain SQL through `pg_logical_slot_peek_binary_changes` with any
  driver, psycopg 3 and asyncpg included (`pgoutput.sql`).
- LSN helpers (`parse_lsn`, `format_lsn`) and a checked option builder (`plugin_options`) that
  refuses option combinations the server would reject (streaming before protocol 2, two phase
  before 3, parallel before 4).
- A single file to vendor: `pgoutput/core.py` holds the whole decoder with standard library
  imports only.
- Typed (`py.typed`, mypy strict).

## Usage

### Stream a publication with psycopg2

<!-- harness: skip -->
```python
# needs a live server
import psycopg2
import psycopg2.extras

from pgoutput import Commit, Converter, Delete, Insert, Update
from pgoutput.psycopg2 import ack, events, start

conn = psycopg2.connect(dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
cur = conn.cursor()
cur.create_replication_slot("my_slot", output_plugin="pgoutput")
start(cur, "my_slot", "my_pub", proto_version=2, streaming=True)

converter = Converter()
for event in events(cur):
    msg = event.message
    if isinstance(msg, (Insert, Update)):
        print(msg.relation.qualified_name, converter.row(msg.relation, msg.new))
    elif isinstance(msg, Delete):
        print("deleted", converter.row(msg.relation, msg.old))
    elif isinstance(msg, Commit):
        ack(cur, msg.end_lsn)  # the slot may now release this WAL
```

`ack` takes the `end_lsn` of a Commit once its changes are durable on your side. psycopg2
handles keepalives inside `events`.

### Read a slot with SQL (psycopg 3, asyncpg, any driver)

psycopg 3 has no replication protocol support yet
([psycopg issue 71](https://github.com/psycopg/psycopg/issues/71)), and neither has asyncpg. The
SQL interface needs none:

<!-- harness: skip -->
```python
# needs a live server
import psycopg
from pgoutput.sql import read_changes

with psycopg.connect(dsn, autocommit=True) as conn:
    changes = read_changes(conn.cursor(), "my_slot", "my_pub", peek=False, upto_nchanges=1000)
    for change in changes:
        print(change.lsn, change.xid, change.message)
```

`peek=True` leaves the changes in the slot, `peek=False` consumes them. For asyncpg, build the
query with `changes_query(..., paramstyle="numeric")` and decode the rows with `decode_rows`.

### Decode without state

```python
from pgoutput import Begin, decode_message

message = decode_message(bytes.fromhex("42000000000174c6b00002c9786029500000000315"))
assert isinstance(message, Begin)
print(message.xid, message.final_lsn, message.commit_time.isoformat())
```

Between a Stream Start and its Stream Stop, pass `in_stream=True`: there the row messages carry a
transaction id. `Decoder` does this for you.

### Streamed and prepared transactions

<!-- harness: skip -->
```python
# needs a live server
start(cur, "my_slot", "my_pub", proto_version=4, streaming="parallel", two_phase=True)
```

With streaming, a large transaction arrives in blocks before it commits: `StreamStart`, its
changes (each with `xid`), `StreamStop`, more blocks, then `StreamCommit` or `StreamAbort`. A
`StreamAbort` whose `subxid` differs from `xid` rolls back a savepoint only: drop the changes
whose `xid` is that `subxid`. With two phase, `BeginPrepare` and `Prepare` frame the changes,
then `CommitPrepared` or `RollbackPrepared` settles them by `gid`.

### Convert values

```python
from pgoutput import Converter, TupleValue
from pgoutput.values import oids

conv = Converter()
print(conv.convert(oids.NUMERIC, TupleValue("t", b"12.50")))  # Decimal('12.50')
print(conv.convert(oids.TIMESTAMPTZ, TupleValue("t", b"2024-01-02 03:04:05+00")))
print(conv.convert(oids.INT4, TupleValue("b", b"\x00\x00\x00\x2a")))  # 42
conv.register(16385, text=str.upper)  # an extension type, by the OID a Type message gave
```

Unknown types come back as `str` (text format) or `bytes` (binary format). Text parsing assumes
`DateStyle` ISO and `IntervalStyle` postgres, the defaults.

### Vendor the decoder

Projects that prefer in-tree code can copy `pgoutput/core.py`, a single module with standard
library imports only, and keep its MIT notice.

## Why this one

| | pgoutput | [pypgoutput](https://github.com/dgea005/pypgoutput) |
| --- | --- | --- |
| Protocol versions | 1 to 4 | 1 |
| Streamed transactions (v2) | yes | no |
| Two phase commit (v3) | yes | no |
| Parallel streaming abort info (v4) | yes | no |
| Logical messages (`pg_logical_emit_message`) | yes | no |
| Binary column format | yes | no |
| Value conversion | built-in types, text and binary, extensible | a few types, read from the catalog over a second connection |
| Drivers | any (SQL path), psycopg2 (streaming) | psycopg2 |
| Python | 3.9+ | 3.8+ |
| Runtime dependencies | none | psycopg2, pydantic |
| Last release | 0.1.0, 2026 | 0.0.3, February 2022 |

pypgoutput was the first Python decoder and is a good read; its last push was in April 2024 and
it decodes protocol version 1 only. [pgoutput-decoder](https://pypi.org/project/pgoutput-decoder/)
is a different kind of tool: a Rust extension for Python 3.12 and later that connects with
tokio-postgres and emits Debezium-format events; use it if you want that pipeline, and this
package if you want the protocol messages themselves in pure Python with your own driver. psycopg
3 has no replication protocol yet ([issue 71](https://github.com/psycopg/psycopg/issues/71)).
Many CDC projects carry their own decoder module, usually for protocol version 1; this package is
meant to replace that module, with bytes captured from real servers as its test suite.

`wal2json` and `test_decoding` are other output plugins: they produce JSON or text on the server
and need to be installed or enabled there (test_decoding is a contrib module meant for testing).
pgoutput is built into the server and is what managed services and subscriptions use.

## Migrating from pypgoutput

| pypgoutput | pgoutput |
| --- | --- |
| `pypgoutput.Insert(buffer)` | `decode_message(buffer)` or `Decoder().decode(buffer)` |
| `msg.byte1` | `type(msg)` (one dataclass per message) |
| `msg.relation_id`, `msg.new_tuple.column_data` | `msg.relation_id`, `msg.new.values` |
| `col_data_category` (`n`, `u`, `t`) | `TupleValue.kind` (`n`, `u`, `t`, `b`) |
| `col_data` (str) | `TupleValue.data` (bytes) or `Converter().row(relation, msg.new)` |
| `msg.commit_ts` (datetime) | `msg.commit_time` (and the raw `commit_timestamp`) |
| `LogicalReplicationReader(...)` | `pgoutput.psycopg2.start` and `events` |

## How it is tested

- Every message type is built byte by byte from the protocol documentation and decoded field by
  field, including LSNs and transaction ids above 2**31, every truncation of every message, and
  48,000 seeded fuzz cases that must raise `DecodeError` and nothing else.
- `tools/capture.py` runs seven scenarios (plain changes, binary format, replication origins,
  streaming with savepoint and full aborts, two phase commit, streamed prepare, parallel
  streaming) on a real server and stores the bytes in `tests/fixtures/pg<major>/`. The tests
  decode them, check the transaction ids and LSNs against the server's own columns, and compare
  every row with what PostgreSQL's reference `test_decoding` plugin printed for the same changes,
  in text and in binary format.
- CI runs the live tests (`tests/integration`, `PGOUTPUT_TEST_DSN`) against PostgreSQL 14, 15,
  16, 17 and 18, including the psycopg2 streaming path with LSN acknowledgement and the SQL path
  with psycopg2 and psycopg 3.
- A weekly workflow installs the published package from PyPI and runs the same live tests
  against the newest PostgreSQL images, so a server change shows up the week it ships.

`benchmarks/bench.py` times the decoder against pypgoutput on the same captured messages; on the
messages both decode, the two run at about the same speed.

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check . && mypy pgoutput
# live tests and fresh fixtures, against a server with wal_level=logical:
PGOUTPUT_TEST_DSN="host=localhost user=postgres dbname=postgres" pytest tests/integration
python tools/capture.py "host=localhost user=postgres dbname=postgres"
```

## License

MIT, see [LICENSE](LICENSE).
