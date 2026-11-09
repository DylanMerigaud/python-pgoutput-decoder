# Contributing

Bug reports with the bytes that fail are the most useful thing: a hex dump of the payload
(`payload.hex()`), the PostgreSQL version and the plugin options you used.

## Setup

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]" "psycopg[binary]"
pytest
ruff check . && ruff format --check . && mypy pgoutput
```

## Live tests and fixtures

Start a server with logical decoding, for example:

```bash
docker run -d --name pg -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:18 \
  -c wal_level=logical -c max_prepared_transactions=10
PGOUTPUT_TEST_DSN="host=127.0.0.1 user=postgres dbname=postgres" pytest tests/integration
python tools/capture.py "host=127.0.0.1 user=postgres dbname=postgres"   # refresh fixtures
```

A new message or option comes with a scenario in `tools/capture.py`, fixtures captured from
every supported major version, and a test in `tests/test_fixtures.py`.

## Rules

- `pgoutput/core.py` imports the standard library only, so it stays a single vendorable file.
- No runtime dependency.
- Python 3.9 compatible.
