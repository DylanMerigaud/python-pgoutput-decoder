"""Read a pgoutput slot through SQL: ``pg_logical_slot_peek_binary_changes`` and
``pg_logical_slot_get_binary_changes``.

This path needs no replication connection, so it works with any driver: psycopg 3, psycopg2,
asyncpg (``paramstyle="numeric"``), or a pool. Each call is a new decoding session, so the
server sends the Relation messages again and a fresh ``Decoder`` is used per call.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Tuple, Union

from .core import Decoder, Message
from .lsn import format_lsn, parse_lsn
from .options import plugin_options

__all__ = ["Change", "changes_query", "read_changes", "decode_rows"]


@dataclass(frozen=True)
class Change:
    """One row of the changes function: the message's LSN, its transaction id, the message."""

    lsn: int
    xid: int
    message: Message


def changes_query(
    slot: str,
    publications: Union[str, Iterable[str]],
    *,
    peek: bool = True,
    upto_lsn: Optional[int] = None,
    upto_nchanges: Optional[int] = None,
    paramstyle: str = "format",
    **options: Any,
) -> Tuple[str, List[Any]]:
    """Build the SQL and its parameters.

    ``peek=True`` leaves the slot where it is; ``peek=False`` (``get``) consumes the changes.
    ``paramstyle`` is ``"format"`` (``%s``: psycopg 3, psycopg2) or ``"numeric"`` (``$1``:
    asyncpg). ``options`` go to ``plugin_options`` (``proto_version``, ``binary``,
    ``messages``, ``streaming``, ``two_phase``, ``origin``).
    """
    opts = plugin_options(publications, **options)
    flat: List[str] = []
    for key, value in opts.items():
        flat += [key, value]
    fn = "pg_logical_slot_peek_binary_changes" if peek else "pg_logical_slot_get_binary_changes"
    if paramstyle == "format":
        marks = ["%s", "%s", "%s", "%s"]
    elif paramstyle == "numeric":
        marks = ["$1", "$2", "$3", "$4"]
    else:
        raise ValueError(f"paramstyle must be 'format' or 'numeric', got {paramstyle!r}")
    sql = (
        f"SELECT lsn::text, xid::text, data FROM {fn}"
        f"({marks[0]}, {marks[1]}::pg_lsn, {marks[2]}::int, VARIADIC {marks[3]}::text[])"
    )
    params = [
        slot,
        None if upto_lsn is None else format_lsn(upto_lsn),
        upto_nchanges,
        flat,
    ]
    return sql, params


def decode_rows(
    rows: Iterable[Tuple[Any, Any, Any]], decoder: Optional[Decoder] = None
) -> List[Change]:
    """Decode ``(lsn, xid, data)`` rows as the changes function returns them."""
    dec = decoder if decoder is not None else Decoder()
    out = []
    for lsn, xid, data in rows:
        lsn_int = parse_lsn(lsn) if isinstance(lsn, str) else int(lsn)
        out.append(Change(lsn_int, int(xid), dec.decode(bytes(data))))
    return out


def read_changes(
    cursor: Any,
    slot: str,
    publications: Union[str, Iterable[str]],
    *,
    peek: bool = True,
    upto_lsn: Optional[int] = None,
    upto_nchanges: Optional[int] = None,
    decoder: Optional[Decoder] = None,
    **options: Any,
) -> List[Change]:
    """Run the changes function on a DB-API cursor (``%s`` placeholders) and decode the rows.

    ``upto_nchanges`` counts transactions' changes as the server does: it stops after the
    transaction in which the count is reached, so a batch never ends inside a transaction.
    """
    sql, params = changes_query(
        slot, publications, peek=peek, upto_lsn=upto_lsn, upto_nchanges=upto_nchanges, **options
    )
    cursor.execute(sql, params)
    return decode_rows(cursor.fetchall(), decoder)
