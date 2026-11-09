"""The options the ``pgoutput`` plugin takes, built and checked in one place.

https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS
"""

from __future__ import annotations

from typing import Dict, Iterable, Optional, Union

__all__ = ["plugin_options", "quote_ident"]

PROTOCOL_STREAMING = 2
PROTOCOL_TWO_PHASE = 3
PROTOCOL_STREAM_PARALLEL = 4


def quote_ident(name: str) -> str:
    """Quote an identifier the way PostgreSQL's ``quote_ident`` does, always."""
    return '"' + name.replace('"', '""') + '"'


def plugin_options(
    publications: Union[str, Iterable[str]],
    *,
    proto_version: int = 1,
    binary: bool = False,
    messages: bool = False,
    streaming: Union[bool, str, None] = None,
    two_phase: bool = False,
    origin: Optional[str] = None,
) -> Dict[str, str]:
    """The option map for ``START_REPLICATION`` or ``pg_logical_slot_*_binary_changes``.

    - ``publications``: one name or several; each is quoted, so any name works.
    - ``proto_version``: 1 to 4. Streaming needs 2, two phase 3, ``streaming="parallel"`` 4.
    - ``binary``: values in binary (send) format instead of text (PostgreSQL 14+).
    - ``messages``: include ``pg_logical_emit_message`` messages (PostgreSQL 14+).
    - ``streaming``: True (or ``"on"``) streams large running transactions, ``"parallel"``
      also sends the abort LSN and timestamp (PostgreSQL 16+).
    - ``two_phase``: decode prepared transactions at PREPARE time (PostgreSQL 15+).
    - ``origin``: ``"any"`` or ``"none"`` (only changes with no replication origin, 16+).
    """
    if isinstance(publications, str):
        publications = [publications]
    names = [quote_ident(p) for p in publications]
    if not names:
        raise ValueError("at least one publication is needed")
    if not 1 <= proto_version <= 4:
        raise ValueError(f"proto_version must be 1 to 4, got {proto_version}")
    opts = {"proto_version": str(proto_version), "publication_names": ",".join(names)}
    if binary:
        opts["binary"] = "true"
    if messages:
        opts["messages"] = "true"
    if streaming:
        mode = "on" if streaming is True else str(streaming)
        if mode not in ("on", "parallel"):
            raise ValueError(f"streaming must be True, 'on' or 'parallel', got {streaming!r}")
        need = PROTOCOL_STREAM_PARALLEL if mode == "parallel" else PROTOCOL_STREAMING
        if proto_version < need:
            raise ValueError(f"streaming {mode!r} needs proto_version {need} or more")
        opts["streaming"] = mode
    if two_phase:
        if proto_version < PROTOCOL_TWO_PHASE:
            raise ValueError(f"two_phase needs proto_version {PROTOCOL_TWO_PHASE} or more")
        opts["two_phase"] = "true"
    if origin is not None:
        if origin not in ("any", "none"):
            raise ValueError(f"origin must be 'any' or 'none', got {origin!r}")
        opts["origin"] = origin
    return opts
