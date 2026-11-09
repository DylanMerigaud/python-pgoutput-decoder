"""pgoutput: decode PostgreSQL logical replication (the built-in ``pgoutput`` plugin) in Python.

The decoder (``pgoutput.core``) is sans IO and has no dependency. ``pgoutput.values`` converts
column values to Python types, ``pgoutput.sql`` reads a slot through plain SQL with any driver,
``pgoutput.psycopg2`` streams it over a psycopg2 replication connection.
"""

from .core import (
    Begin,
    BeginPrepare,
    Column,
    Commit,
    CommitPrepared,
    DecodeError,
    Decoder,
    Delete,
    Insert,
    LogicalMessage,
    Message,
    Origin,
    Prepare,
    Relation,
    ReplicaIdentity,
    RollbackPrepared,
    StreamAbort,
    StreamCommit,
    StreamPrepare,
    StreamStart,
    StreamStop,
    Truncate,
    TupleData,
    TupleKind,
    TupleValue,
    Type,
    UnknownRelationError,
    Update,
    decode_message,
)
from .lsn import format_lsn, parse_lsn
from .options import plugin_options
from .values import UNCHANGED_TOAST, Converter, Interval

__version__ = "0.1.0"

__all__ = [
    "Begin",
    "BeginPrepare",
    "Column",
    "Commit",
    "CommitPrepared",
    "Converter",
    "DecodeError",
    "Decoder",
    "Delete",
    "Insert",
    "Interval",
    "LogicalMessage",
    "Message",
    "Origin",
    "Prepare",
    "Relation",
    "ReplicaIdentity",
    "RollbackPrepared",
    "StreamAbort",
    "StreamCommit",
    "StreamPrepare",
    "StreamStart",
    "StreamStop",
    "Truncate",
    "TupleData",
    "TupleKind",
    "TupleValue",
    "Type",
    "UNCHANGED_TOAST",
    "UnknownRelationError",
    "Update",
    "decode_message",
    "format_lsn",
    "parse_lsn",
    "plugin_options",
]
