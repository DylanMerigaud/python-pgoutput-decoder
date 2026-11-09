"""Decoder for the messages of PostgreSQL's ``pgoutput`` logical decoding plugin.

This module has no import outside the standard library, so it can be copied as a single file
into a project that prefers in-tree code. Everything else in the package (value conversion, the
SQL and psycopg2 adapters) is built on it.

Wire format: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
Protocol versions 1 to 4 are covered. Version 2 adds streamed transactions (Stream Start, Stop,
Commit, Abort, and a transaction id on row messages inside a streamed block), version 3 adds
two phase commit (Begin Prepare, Prepare, Commit Prepared, Rollback Prepared, Stream Prepare),
version 4 adds the abort LSN and timestamp to Stream Abort when ``streaming = parallel``.

The ``xid`` of a row or catalog message (Relation, Type, Insert, Update, Delete, Truncate,
LogicalMessage) is set only inside a streamed block, where the server sends the id of the
transaction or subtransaction that made the change; it is None everywhere else.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Iterator, List, Mapping, Optional, Tuple, Union

__all__ = [
    "DecodeError",
    "UnknownRelationError",
    "ReplicaIdentity",
    "TupleKind",
    "TupleValue",
    "TupleData",
    "Column",
    "Begin",
    "Commit",
    "Origin",
    "Relation",
    "Type",
    "Insert",
    "Update",
    "Delete",
    "Truncate",
    "LogicalMessage",
    "StreamStart",
    "StreamStop",
    "StreamCommit",
    "StreamAbort",
    "BeginPrepare",
    "Prepare",
    "CommitPrepared",
    "RollbackPrepared",
    "StreamPrepare",
    "Message",
    "decode_message",
    "Decoder",
    "pg_timestamp_to_datetime",
]

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


class DecodeError(ValueError):
    """The bytes are not a valid pgoutput message (truncated, unknown tag, bad field)."""


class UnknownRelationError(KeyError):
    """A row message names a relation id that no Relation message announced first."""


def pg_timestamp_to_datetime(micros: int) -> datetime:
    """Convert a PostgreSQL timestamp (microseconds since 2000-01-01 UTC) to an aware datetime.

    Raises OverflowError when the value lies outside what ``datetime`` can hold.
    """
    return PG_EPOCH + timedelta(microseconds=micros)


class ReplicaIdentity:
    """Values of ``Relation.replica_identity`` (``pg_class.relreplident``)."""

    DEFAULT = "d"
    NOTHING = "n"
    FULL = "f"
    INDEX = "i"


class TupleKind:
    """Values of ``TupleValue.kind``."""

    NULL = "n"
    UNCHANGED_TOAST = "u"
    TEXT = "t"
    BINARY = "b"


@dataclass(frozen=True)
class TupleValue:
    """One column of a row as sent by the server.

    ``kind`` is one of ``TupleKind``. ``data`` holds the raw bytes for ``text`` (the type's
    output function, in the server encoding) and ``binary`` (the type's send function), and is
    None for ``null`` and ``unchanged_toast`` (a TOASTed value the update did not change, so the
    server did not send it).
    """

    kind: str
    data: Optional[bytes] = None

    @property
    def is_null(self) -> bool:
        return self.kind == TupleKind.NULL

    @property
    def is_unchanged_toast(self) -> bool:
        return self.kind == TupleKind.UNCHANGED_TOAST

    def text(self, encoding: str = "utf-8") -> Optional[str]:
        """The value decoded as a string for a text column, None for null or unchanged."""
        if self.data is None:
            return None
        if self.kind != TupleKind.TEXT:
            raise ValueError("text() needs a text format value, this one is binary")
        return self.data.decode(encoding)


@dataclass(frozen=True)
class TupleData:
    """A row: one ``TupleValue`` per column, in the order of the relation's columns."""

    values: Tuple[TupleValue, ...]

    def __len__(self) -> int:
        return len(self.values)

    def __iter__(self) -> Iterator[TupleValue]:
        return iter(self.values)

    def __getitem__(self, index: int) -> TupleValue:
        return self.values[index]

    def as_dict(self, relation: Relation) -> Dict[str, TupleValue]:
        """Map column names of ``relation`` to the raw values of this row."""
        if len(relation.columns) != len(self.values):
            raise ValueError(
                f"relation {relation.namespace}.{relation.name} has {len(relation.columns)} "
                f"columns, the row has {len(self.values)}"
            )
        return {col.name: value for col, value in zip(relation.columns, self.values)}


@dataclass(frozen=True)
class Column:
    """A column of a Relation message."""

    name: str
    type_oid: int
    type_modifier: int
    is_key: bool


# Transaction messages.


@dataclass(frozen=True)
class Begin:
    """Start of a transaction (tag ``B``)."""

    final_lsn: int
    commit_timestamp: int
    xid: int

    @property
    def commit_time(self) -> datetime:
        return pg_timestamp_to_datetime(self.commit_timestamp)


@dataclass(frozen=True)
class Commit:
    """End of a transaction (tag ``C``)."""

    flags: int
    commit_lsn: int
    end_lsn: int
    commit_timestamp: int

    @property
    def commit_time(self) -> datetime:
        return pg_timestamp_to_datetime(self.commit_timestamp)


@dataclass(frozen=True)
class Origin:
    """Replication origin of the transaction (tag ``O``)."""

    commit_lsn: int
    name: str


# Catalog messages.


@dataclass(frozen=True)
class Relation:
    """Description of a table (tag ``R``), sent before the first row of that table."""

    relation_id: int
    namespace: str
    name: str
    replica_identity: str
    columns: Tuple[Column, ...]
    xid: Optional[int] = None

    @property
    def qualified_name(self) -> str:
        return f"{self.namespace}.{self.name}"

    @property
    def key_columns(self) -> Tuple[Column, ...]:
        return tuple(c for c in self.columns if c.is_key)


@dataclass(frozen=True)
class Type:
    """Description of a non built-in data type (tag ``Y``)."""

    type_oid: int
    namespace: str
    name: str
    xid: Optional[int] = None


# Row messages. ``relation`` is filled by ``Decoder`` from its cache, and is None when the
# stateless ``decode_message`` is used.


@dataclass(frozen=True)
class Insert:
    """A new row (tag ``I``)."""

    relation_id: int
    new: TupleData
    xid: Optional[int] = None
    relation: Optional[Relation] = field(default=None, compare=False)


@dataclass(frozen=True)
class Update:
    """A changed row (tag ``U``).

    ``old`` is the old key (``old_kind == "K"``, when the key changed and the replica identity
    is default or index) or the whole old row (``old_kind == "O"``, replica identity full). It is
    None when the server sent neither.
    """

    relation_id: int
    new: TupleData
    old: Optional[TupleData] = None
    old_kind: Optional[str] = None
    xid: Optional[int] = None
    relation: Optional[Relation] = field(default=None, compare=False)


@dataclass(frozen=True)
class Delete:
    """A removed row (tag ``D``): its key (``old_kind == "K"``) or whole row (``"O"``)."""

    relation_id: int
    old: TupleData
    old_kind: str
    xid: Optional[int] = None
    relation: Optional[Relation] = field(default=None, compare=False)


@dataclass(frozen=True)
class Truncate:
    """One TRUNCATE statement over one or more tables (tag ``T``)."""

    options: int
    relation_ids: Tuple[int, ...]
    xid: Optional[int] = None
    relations: Optional[Tuple[Relation, ...]] = field(default=None, compare=False)

    CASCADE = 1
    RESTART_IDENTITY = 2

    @property
    def cascade(self) -> bool:
        return bool(self.options & self.CASCADE)

    @property
    def restart_identity(self) -> bool:
        return bool(self.options & self.RESTART_IDENTITY)


@dataclass(frozen=True)
class LogicalMessage:
    """A message written by ``pg_logical_emit_message`` (tag ``M``).

    Sent only when the subscriber asked for ``messages 'true'``.
    """

    flags: int
    lsn: int
    prefix: str
    content: bytes
    xid: Optional[int] = None

    @property
    def transactional(self) -> bool:
        return bool(self.flags & 1)


# Streaming (protocol version 2 and later).


@dataclass(frozen=True)
class StreamStart:
    """Start of a block of a streamed, still running transaction (tag ``S``)."""

    xid: int
    first_segment: bool


@dataclass(frozen=True)
class StreamStop:
    """End of a block of a streamed transaction (tag ``E``)."""


@dataclass(frozen=True)
class StreamCommit:
    """Commit of a streamed transaction (tag ``c``)."""

    xid: int
    flags: int
    commit_lsn: int
    end_lsn: int
    commit_timestamp: int

    @property
    def commit_time(self) -> datetime:
        return pg_timestamp_to_datetime(self.commit_timestamp)


@dataclass(frozen=True)
class StreamAbort:
    """Abort of a streamed (sub)transaction (tag ``A``).

    ``abort_lsn`` and ``abort_timestamp`` are set with protocol version 4 and
    ``streaming 'parallel'``, None otherwise.
    """

    xid: int
    subxid: int
    abort_lsn: Optional[int] = None
    abort_timestamp: Optional[int] = None

    @property
    def abort_time(self) -> Optional[datetime]:
        if self.abort_timestamp is None:
            return None
        return pg_timestamp_to_datetime(self.abort_timestamp)


# Two phase commit (protocol version 3 and later, ``two_phase 'true'``).


@dataclass(frozen=True)
class BeginPrepare:
    """Start of a prepared transaction (tag ``b``)."""

    prepare_lsn: int
    end_lsn: int
    prepare_timestamp: int
    xid: int
    gid: str

    @property
    def prepare_time(self) -> datetime:
        return pg_timestamp_to_datetime(self.prepare_timestamp)


@dataclass(frozen=True)
class Prepare:
    """PREPARE TRANSACTION (tag ``P``)."""

    flags: int
    prepare_lsn: int
    end_lsn: int
    prepare_timestamp: int
    xid: int
    gid: str

    @property
    def prepare_time(self) -> datetime:
        return pg_timestamp_to_datetime(self.prepare_timestamp)


@dataclass(frozen=True)
class CommitPrepared:
    """COMMIT PREPARED (tag ``K``)."""

    flags: int
    commit_lsn: int
    end_lsn: int
    commit_timestamp: int
    xid: int
    gid: str

    @property
    def commit_time(self) -> datetime:
        return pg_timestamp_to_datetime(self.commit_timestamp)


@dataclass(frozen=True)
class RollbackPrepared:
    """ROLLBACK PREPARED (tag ``r``)."""

    flags: int
    prepare_end_lsn: int
    rollback_end_lsn: int
    prepare_timestamp: int
    rollback_timestamp: int
    xid: int
    gid: str

    @property
    def rollback_time(self) -> datetime:
        return pg_timestamp_to_datetime(self.rollback_timestamp)


@dataclass(frozen=True)
class StreamPrepare:
    """PREPARE TRANSACTION of a streamed transaction (tag ``p``)."""

    flags: int
    prepare_lsn: int
    end_lsn: int
    prepare_timestamp: int
    xid: int
    gid: str

    @property
    def prepare_time(self) -> datetime:
        return pg_timestamp_to_datetime(self.prepare_timestamp)


Message = Union[
    Begin,
    Commit,
    Origin,
    Relation,
    Type,
    Insert,
    Update,
    Delete,
    Truncate,
    LogicalMessage,
    StreamStart,
    StreamStop,
    StreamCommit,
    StreamAbort,
    BeginPrepare,
    Prepare,
    CommitPrepared,
    RollbackPrepared,
    StreamPrepare,
]

_I8 = struct.Struct(">b")
_U8 = struct.Struct(">B")
_I16 = struct.Struct(">h")
_I32 = struct.Struct(">i")
_U32 = struct.Struct(">I")
_I64 = struct.Struct(">q")
_U64 = struct.Struct(">Q")


class _Reader:
    __slots__ = ("buf", "encoding", "pos")

    def __init__(self, buf: bytes, encoding: str) -> None:
        self.buf = buf
        self.pos = 0
        self.encoding = encoding

    def _take(self, s: struct.Struct, what: str) -> int:
        end = self.pos + s.size
        if end > len(self.buf):
            raise DecodeError(
                f"truncated message: {what} needs {s.size} bytes at offset {self.pos}"
            )
        (v,) = s.unpack_from(self.buf, self.pos)
        self.pos = end
        return int(v)

    def u8(self, what: str) -> int:
        return self._take(_U8, what)

    def i16(self, what: str) -> int:
        return self._take(_I16, what)

    def i32(self, what: str) -> int:
        return self._take(_I32, what)

    def u32(self, what: str) -> int:
        return self._take(_U32, what)

    def i64(self, what: str) -> int:
        return self._take(_I64, what)

    def u64(self, what: str) -> int:
        return self._take(_U64, what)

    def byte(self, what: str) -> str:
        return chr(self.u8(what))

    def string(self, what: str) -> str:
        end = self.buf.find(b"\x00", self.pos)
        if end < 0:
            raise DecodeError(f"truncated message: {what} has no terminating zero byte")
        raw = self.buf[self.pos : end]
        self.pos = end + 1
        try:
            return raw.decode(self.encoding)
        except UnicodeDecodeError as exc:
            raise DecodeError(f"{what} is not valid {self.encoding}: {exc}") from None

    def raw(self, n: int, what: str) -> bytes:
        if n < 0:
            raise DecodeError(f"{what} has a negative length {n}")
        end = self.pos + n
        if end > len(self.buf):
            raise DecodeError(f"truncated message: {what} needs {n} bytes at offset {self.pos}")
        data = bytes(self.buf[self.pos : end])
        self.pos = end
        return data

    def remaining(self) -> int:
        return len(self.buf) - self.pos

    def done(self, tag: str) -> None:
        if self.pos != len(self.buf):
            raise DecodeError(
                f"message {tag!r} has {len(self.buf) - self.pos} unexpected trailing bytes"
            )


_NULL_VALUE = TupleValue(TupleKind.NULL)
_UNCHANGED_VALUE = TupleValue(TupleKind.UNCHANGED_TOAST)
_KIND_NULL, _KIND_UNCHANGED, _KIND_TEXT, _KIND_BINARY = b"nutb"


def _tuple(r: _Reader) -> TupleData:
    # The hot path of every row message: read the buffer directly, build error text only on
    # failure.
    n = r.i16("tuple column count")
    if n < 0:
        raise DecodeError(f"negative tuple column count {n}")
    buf = r.buf
    pos = r.pos
    size = len(buf)
    unpack_i32 = _I32.unpack_from
    values: List[TupleValue] = []
    append = values.append
    for i in range(n):
        if pos >= size:
            raise DecodeError(f"truncated message: column {i} kind at offset {pos}")
        kind = buf[pos]
        pos += 1
        if kind == _KIND_NULL:
            append(_NULL_VALUE)
        elif kind == _KIND_UNCHANGED:
            append(_UNCHANGED_VALUE)
        elif kind in (_KIND_TEXT, _KIND_BINARY):
            if pos + 4 > size:
                raise DecodeError(f"truncated message: column {i} length at offset {pos}")
            (length,) = unpack_i32(buf, pos)
            pos += 4
            if length < 0:
                raise DecodeError(f"column {i} value has a negative length {length}")
            end = pos + length
            if end > size:
                raise DecodeError(
                    f"truncated message: column {i} value needs {length} bytes at offset {pos}"
                )
            append(
                TupleValue(TupleKind.TEXT if kind == _KIND_TEXT else TupleKind.BINARY, buf[pos:end])
            )
            pos = end
        else:
            raise DecodeError(f"unknown tuple value kind {chr(kind)!r} in column {i}")
    r.pos = pos
    return TupleData(tuple(values))


def _xid(r: _Reader, in_stream: bool) -> Optional[int]:
    return r.u32("transaction id") if in_stream else None


def _decode(r: _Reader, in_stream: bool) -> Message:
    if r.remaining() == 0:
        raise DecodeError("empty message")
    tag = r.byte("message tag")
    msg: Message
    if tag == "B":
        msg = Begin(r.u64("final LSN"), r.i64("commit timestamp"), r.u32("xid"))
    elif tag == "C":
        msg = Commit(
            r.u8("flags"), r.u64("commit LSN"), r.u64("end LSN"), r.i64("commit timestamp")
        )
    elif tag == "O":
        msg = Origin(r.u64("origin commit LSN"), r.string("origin name"))
    elif tag == "R":
        xid = _xid(r, in_stream)
        rel_id = r.u32("relation id")
        namespace = r.string("namespace")
        name = r.string("relation name")
        ident = r.byte("replica identity")
        ncols = r.i16("column count")
        if ncols < 0:
            raise DecodeError(f"negative column count {ncols}")
        cols: List[Column] = []
        for _ in range(ncols):
            flags = r.u8("column flags")
            cname = r.string("column name")
            type_oid = r.u32("column type oid")
            typmod = r.i32("column type modifier")
            cols.append(Column(cname, type_oid, typmod, bool(flags & 1)))
        msg = Relation(rel_id, namespace, name, ident, tuple(cols), xid)
    elif tag == "Y":
        xid = _xid(r, in_stream)
        msg = Type(r.u32("type oid"), r.string("type namespace"), r.string("type name"), xid)
    elif tag == "I":
        xid = _xid(r, in_stream)
        rel_id = r.u32("relation id")
        marker = r.byte("new tuple marker")
        if marker != "N":
            raise DecodeError(f"insert expects new tuple marker 'N', got {marker!r}")
        msg = Insert(rel_id, _tuple(r), xid)
    elif tag == "U":
        xid = _xid(r, in_stream)
        rel_id = r.u32("relation id")
        marker = r.byte("tuple marker")
        old: Optional[TupleData] = None
        old_kind: Optional[str] = None
        if marker in ("K", "O"):
            old_kind = marker
            old = _tuple(r)
            marker = r.byte("new tuple marker")
        if marker != "N":
            raise DecodeError(f"update expects tuple marker 'K', 'O' or 'N', got {marker!r}")
        msg = Update(rel_id, _tuple(r), old, old_kind, xid)
    elif tag == "D":
        xid = _xid(r, in_stream)
        rel_id = r.u32("relation id")
        marker = r.byte("old tuple marker")
        if marker not in ("K", "O"):
            raise DecodeError(f"delete expects tuple marker 'K' or 'O', got {marker!r}")
        msg = Delete(rel_id, _tuple(r), marker, xid)
    elif tag == "T":
        xid = _xid(r, in_stream)
        nrels = r.i32("relation count")
        if nrels < 0:
            raise DecodeError(f"negative relation count {nrels}")
        options = r.u8("truncate options")
        rel_ids = tuple(r.u32(f"relation id {i}") for i in range(nrels))
        msg = Truncate(options, rel_ids, xid)
    elif tag == "M":
        xid = _xid(r, in_stream)
        flags = r.u8("flags")
        lsn = r.u64("message LSN")
        prefix = r.string("prefix")
        length = r.i32("content length")
        msg = LogicalMessage(flags, lsn, prefix, r.raw(length, "content"), xid)
    elif tag == "S":
        msg = StreamStart(r.u32("xid"), r.u8("first segment flag") == 1)
    elif tag == "E":
        msg = StreamStop()
    elif tag == "c":
        msg = StreamCommit(
            r.u32("xid"),
            r.u8("flags"),
            r.u64("commit LSN"),
            r.u64("end LSN"),
            r.i64("commit timestamp"),
        )
    elif tag == "A":
        xid = r.u32("xid")
        subxid = r.u32("subtransaction xid")
        if r.remaining():
            msg = StreamAbort(xid, subxid, r.u64("abort LSN"), r.i64("abort timestamp"))
        else:
            msg = StreamAbort(xid, subxid)
    elif tag == "b":
        msg = BeginPrepare(
            r.u64("prepare LSN"),
            r.u64("end LSN"),
            r.i64("prepare timestamp"),
            r.u32("xid"),
            r.string("gid"),
        )
    elif tag == "P":
        msg = Prepare(
            r.u8("flags"),
            r.u64("prepare LSN"),
            r.u64("end LSN"),
            r.i64("prepare timestamp"),
            r.u32("xid"),
            r.string("gid"),
        )
    elif tag == "K":
        msg = CommitPrepared(
            r.u8("flags"),
            r.u64("commit LSN"),
            r.u64("end LSN"),
            r.i64("commit timestamp"),
            r.u32("xid"),
            r.string("gid"),
        )
    elif tag == "r":
        msg = RollbackPrepared(
            r.u8("flags"),
            r.u64("prepare end LSN"),
            r.u64("rollback end LSN"),
            r.i64("prepare timestamp"),
            r.i64("rollback timestamp"),
            r.u32("xid"),
            r.string("gid"),
        )
    elif tag == "p":
        msg = StreamPrepare(
            r.u8("flags"),
            r.u64("prepare LSN"),
            r.u64("end LSN"),
            r.i64("prepare timestamp"),
            r.u32("xid"),
            r.string("gid"),
        )
    else:
        raise DecodeError(f"unknown message tag {tag!r}")
    r.done(tag)
    return msg


def decode_message(
    data: Union[bytes, bytearray, memoryview], *, in_stream: bool = False, encoding: str = "utf-8"
) -> Message:
    """Decode one pgoutput message, without any state.

    ``in_stream`` must be True between a Stream Start and its Stream Stop: there the row and
    catalog messages (Relation, Type, Insert, Update, Delete, Truncate, Message) carry the id of
    the transaction, or of the subtransaction (savepoint) that made the change. ``encoding`` is
    the server encoding, used for the names (schema, table, column, origin, prefix, gid).

    Row messages come back with ``relation`` None; ``Decoder`` fills it from its cache.
    Raises ``DecodeError`` on any malformed input.
    """
    return _decode(_Reader(bytes(data), encoding), in_stream)


class Decoder:
    """Stateful decoder for a pgoutput stream, sans IO.

    Feed it the payload of each replication message, in order, with ``decode``. It tracks the
    streamed block state (so ``in_stream`` is never passed by hand), keeps the Relation and Type
    messages it saw, and attaches the matching ``Relation`` to every Insert, Update, Delete and
    Truncate.

    >>> dec = Decoder()
    >>> msg = dec.decode(payload)  # doctest: +SKIP
    """

    def __init__(self, *, encoding: str = "utf-8", strict_relations: bool = True) -> None:
        """``strict_relations``: raise ``UnknownRelationError`` for a row whose relation was
        never announced (the default). When False the row comes back with ``relation`` None."""
        self.encoding = encoding
        self.strict_relations = strict_relations
        self.relations: Dict[int, Relation] = {}
        self.types: Dict[int, Type] = {}
        self.in_stream = False
        self.stream_xid: Optional[int] = None

    def _relation(self, rel_id: int) -> Optional[Relation]:
        rel = self.relations.get(rel_id)
        if rel is None and self.strict_relations:
            raise UnknownRelationError(
                f"relation id {rel_id} used before any Relation message announced it"
            )
        return rel

    def decode(self, data: Union[bytes, bytearray, memoryview]) -> Message:
        """Decode one message and update the decoder state."""
        msg = decode_message(data, in_stream=self.in_stream, encoding=self.encoding)
        if isinstance(msg, Relation):
            self.relations[msg.relation_id] = msg
        elif isinstance(msg, Type):
            self.types[msg.type_oid] = msg
        elif isinstance(msg, (Insert, Update, Delete)):
            rel = self._relation(msg.relation_id)
            if rel is not None:
                object.__setattr__(msg, "relation", rel)
        elif isinstance(msg, Truncate):
            rels = [self._relation(i) for i in msg.relation_ids]
            if all(r is not None for r in rels):
                object.__setattr__(msg, "relations", tuple(r for r in rels if r is not None))
        elif isinstance(msg, StreamStart):
            if self.in_stream:
                raise DecodeError("Stream Start received inside a streamed block")
            self.in_stream = True
            self.stream_xid = msg.xid
        elif isinstance(msg, StreamStop):
            if not self.in_stream:
                raise DecodeError("Stream Stop received outside a streamed block")
            self.in_stream = False
            self.stream_xid = None
        return msg

    def decode_all(self, payloads: Iterable[Union[bytes, bytearray, memoryview]]) -> List[Message]:
        """Decode a sequence of payloads in order."""
        return [self.decode(p) for p in payloads]

    def reset(self) -> None:
        """Forget every relation, type and stream state (a new replication session)."""
        self.relations.clear()
        self.types.clear()
        self.in_stream = False
        self.stream_xid = None

    @property
    def relations_by_name(self) -> Mapping[str, Relation]:
        """The known relations keyed by ``schema.table``."""
        return {r.qualified_name: r for r in self.relations.values()}
