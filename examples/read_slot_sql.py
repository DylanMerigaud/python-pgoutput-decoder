"""Read a pgoutput slot with plain SQL (no replication connection), here with psycopg 3.

    pip install pgoutput "psycopg[binary]"
    PGOUTPUT_DSN="host=localhost user=postgres dbname=postgres" python read_slot_sql.py

Create the slot once: SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
"""

import os

from pgoutput import Commit, Insert
from pgoutput.sql import read_changes

DSN = os.environ.get("PGOUTPUT_DSN")


def main(dsn: str) -> None:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        cur = conn.cursor()
        # peek=True leaves the changes in the slot; peek=False consumes them.
        for change in read_changes(cur, "my_slot", "my_pub", peek=True, upto_nchanges=1000):
            if isinstance(change.message, Insert):
                print(change.lsn, change.message.relation.qualified_name)
            elif isinstance(change.message, Commit):
                print("commit", change.message.end_lsn)


if __name__ == "__main__":
    if DSN:
        main(DSN)
    else:
        print("set PGOUTPUT_DSN to a libpq connection string to read a live slot")
