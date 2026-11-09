"""Stream a publication over a replication connection with psycopg2 and acknowledge each
committed transaction.

    pip install "pgoutput[psycopg2]"
    PGOUTPUT_DSN="host=localhost user=postgres dbname=postgres" python stream_psycopg2.py

The server needs wal_level=logical, and a publication: CREATE PUBLICATION my_pub FOR ALL TABLES.
"""

import contextlib
import os

from pgoutput import Commit, Converter, Delete, Insert, StreamCommit, Update

DSN = os.environ.get("PGOUTPUT_DSN")


def main(dsn: str) -> None:
    import psycopg2
    import psycopg2.extras

    from pgoutput.psycopg2 import ack, events, start

    conn = psycopg2.connect(dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    cur = conn.cursor()
    with contextlib.suppress(psycopg2.errors.DuplicateObject):  # the slot exists already
        cur.create_replication_slot("pgoutput_example", output_plugin="pgoutput")
    start(cur, "pgoutput_example", "my_pub", proto_version=2, streaming=True)
    converter = Converter()
    # Each message is handled before the next one is read, so ack_idle may acknowledge the
    # server's position between transactions: the slot keeps moving on a quiet publication.
    for event in events(cur, ack_idle=True):
        msg = event.message
        if isinstance(msg, (Insert, Update)):
            print(
                type(msg).__name__,
                msg.relation.qualified_name,
                converter.row(msg.relation, msg.new),
            )
        elif isinstance(msg, Delete):
            print("Delete", msg.relation.qualified_name, converter.row(msg.relation, msg.old))
        elif isinstance(msg, (Commit, StreamCommit)):
            ack(cur, msg.end_lsn)


if __name__ == "__main__":
    if DSN:
        main(DSN)
    else:
        print("set PGOUTPUT_DSN to a libpq connection string to stream a live server")
