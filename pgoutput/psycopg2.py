"""Stream a pgoutput slot with psycopg2's replication protocol support.

psycopg2 (2.8+) speaks the streaming replication protocol through
``psycopg2.extras.LogicalReplicationConnection``; psycopg 3 does not yet (psycopg issue 71), so
with psycopg 3 or asyncpg use ``pgoutput.sql`` instead. psycopg2 is not a dependency of this
package: this module takes the cursor you created.

    import psycopg2, psycopg2.extras
    from pgoutput.psycopg2 import start, events, ack

    conn = psycopg2.connect(dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    cur = conn.cursor()
    start(cur, "my_slot", "my_pub", proto_version=2, streaming=True)
    for event in events(cur, ack_idle=True):
        handle(event.message)
        if isinstance(event.message, (Commit, StreamCommit)):
            ack(cur, event.message.end_lsn)
"""

from __future__ import annotations

import select
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Iterator, Optional, Set, Union

from .core import (
    Begin,
    BeginPrepare,
    Commit,
    Decoder,
    Message,
    Prepare,
    StreamAbort,
    StreamCommit,
    StreamPrepare,
    StreamStart,
)

__all__ = ["Event", "start", "events", "ack"]


@dataclass(frozen=True)
class Event:
    """A decoded message with the replication envelope psycopg2 gives it."""

    message: Message
    data_start: int
    wal_end: int
    send_time: datetime


def start(
    cursor: Any,
    slot_name: str,
    publications: Union[str, Iterable[str]],
    *,
    start_lsn: int = 0,
    status_interval: float = 10.0,
    **options: Any,
) -> None:
    """``START_REPLICATION`` on ``slot_name`` with the pgoutput options (see
    ``pgoutput.options.plugin_options``). ``start_lsn=0`` resumes from the slot's confirmed
    position."""
    from .options import plugin_options

    cursor.start_replication(
        slot_name=slot_name,
        decode=False,
        start_lsn=start_lsn,
        status_interval=status_interval,
        options=plugin_options(publications, **options),
    )


def events(
    cursor: Any,
    decoder: Optional[Decoder] = None,
    *,
    idle_timeout: Optional[float] = None,
    poll_interval: float = 1.0,
    ack_idle: bool = False,
) -> Iterator[Event]:
    """Yield each decoded message as it arrives. Keepalives and status updates are handled by
    psycopg2 inside ``read_message``. With ``idle_timeout``, stop after that many seconds
    without a message; without it, run until the connection closes.

    ``ack_idle``: when ``poll_interval`` passes with no message and no transaction is open
    (every Begin, Begin Prepare and streamed transaction received so far has reached its
    Commit, Prepare, Stream Commit, Stream Prepare or Stream Abort), acknowledge
    ``cursor.wal_end``. Use it when you apply each message before reading the next one: it keeps
    the slot moving on a quiet publication (see ``ack``)."""
    dec = decoder if decoder is not None else Decoder()
    last = time.monotonic()
    in_tx = False
    streams: Set[int] = set()
    while True:
        msg = cursor.read_message()
        if msg is not None:
            last = time.monotonic()
            message = dec.decode(msg.payload)
            if isinstance(message, (Begin, BeginPrepare)):
                in_tx = True
            elif isinstance(message, (Commit, Prepare)):
                in_tx = False
            elif isinstance(message, StreamStart):
                streams.add(message.xid)
            elif isinstance(message, (StreamCommit, StreamPrepare)) or (
                isinstance(message, StreamAbort) and message.subxid == message.xid
            ):
                streams.discard(message.xid)
            yield Event(message, msg.data_start, msg.wal_end, msg.send_time)
            continue
        if idle_timeout is not None and time.monotonic() - last >= idle_timeout:
            return
        ready, _, _ = select.select([cursor], [], [], poll_interval)
        if not ready and ack_idle and not in_tx and not streams and cursor.wal_end:
            ack(cursor, cursor.wal_end)


def ack(cursor: Any, lsn: int) -> None:
    """Tell the server everything up to ``lsn`` is safely stored: the slot may release that WAL.

    Acknowledge the ``end_lsn`` of a Commit, Stream Commit or Commit Prepared once its changes
    are durable on your side, never a position inside a transaction you have not finished
    applying. Since PostgreSQL 15 the server skips transactions that touch no published table,
    so a quiet publication on a busy server sends no Commit at all: when every transaction you
    received is applied, also acknowledge ``cursor.wal_end`` (updated by keepalives), or the
    slot keeps that WAL on disk. ``events(..., ack_idle=True)`` does that."""
    cursor.send_feedback(write_lsn=lsn, flush_lsn=lsn)
