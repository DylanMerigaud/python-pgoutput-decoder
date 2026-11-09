"""Convert pgoutput column values to Python objects, from the column's type OID.

pgoutput sends each value either in text format (the type's output function, the default) or in
binary format (the type's send function, with the ``binary 'true'`` option, PostgreSQL 14 and
later). ``Converter`` handles both for the built-in types below and returns ``str`` (text) or
``bytes`` (binary) for any other type, which ``register`` can teach it.

Text parsing assumes the defaults a replication session runs with: ``DateStyle`` ISO and
``IntervalStyle`` postgres.

| PostgreSQL | Python |
| --- | --- |
| bool | bool |
| smallint, integer, bigint, oid | int |
| real, double precision | float |
| numeric | decimal.Decimal |
| text, varchar, char(n), name, "char", xml, enum and unknown types | str |
| money | str (text format), int count of the smallest unit (binary format) |
| bytea | bytes |
| uuid | uuid.UUID |
| json, jsonb | the parsed JSON value |
| date | datetime.date |
| time | datetime.time |
| timetz | datetime.time with tzinfo |
| timestamp | datetime.datetime (naive) |
| timestamptz | datetime.datetime (aware, UTC offset as sent) |
| interval | pgoutput.values.Interval |
| inet, cidr | ipaddress interface or network |
| arrays of the above | nested lists |

``infinity`` and ``-infinity`` dates and timestamps become the ``max`` and ``min`` of their
Python type; infinite intervals (PostgreSQL 17+) become ``INTERVAL_INFINITY`` and
``INTERVAL_NEG_INFINITY``. A date Python cannot hold (BC, or past year 9999) comes back as a
``str``: the server's text in text format, the day or microsecond count in binary format.
"""

from __future__ import annotations

import ipaddress
import json
import re
import struct
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Union

from .core import Relation, TupleData, TupleKind, TupleValue

__all__ = [
    "Converter",
    "Interval",
    "INTERVAL_INFINITY",
    "INTERVAL_NEG_INFINITY",
    "UNCHANGED_TOAST",
    "UnchangedToast",
    "oids",
]


class UnchangedToast:
    """Sentinel for a TOASTed value the server did not send because the update left it as is."""

    _instance: Optional[UnchangedToast] = None

    def __new__(cls) -> UnchangedToast:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNCHANGED_TOAST"

    def __bool__(self) -> bool:
        return False


UNCHANGED_TOAST = UnchangedToast()


@dataclass(frozen=True)
class Interval:
    """A PostgreSQL interval: months, days and microseconds are kept apart, as the server does,
    because a month has no fixed length."""

    months: int = 0
    days: int = 0
    microseconds: int = 0

    @property
    def is_infinite(self) -> bool:
        return self in (INTERVAL_INFINITY, INTERVAL_NEG_INFINITY)

    def to_timedelta(self) -> timedelta:
        """The interval as a timedelta; raises ValueError when it has months or is infinite."""
        if self.is_infinite:
            raise ValueError("an infinite interval has no timedelta")
        if self.months:
            raise ValueError("an interval with months has no fixed length")
        return timedelta(days=self.days, microseconds=self.microseconds)


# PostgreSQL 17 adds infinite intervals: every field at its maximum (or minimum).
INTERVAL_INFINITY = Interval(2**31 - 1, 2**31 - 1, 2**63 - 1)
INTERVAL_NEG_INFINITY = Interval(-(2**31), -(2**31), -(2**63))


class oids:  # noqa: N801  (a namespace of constants)
    """Built-in type OIDs (``pg_type.oid``), stable across PostgreSQL versions."""

    BOOL = 16
    BYTEA = 17
    CHAR = 18
    NAME = 19
    INT8 = 20
    INT2 = 21
    INT4 = 23
    TEXT = 25
    OID = 26
    JSON = 114
    XML = 142
    CIDR = 650
    FLOAT4 = 700
    FLOAT8 = 701
    MONEY = 790
    INET = 869
    BPCHAR = 1042
    VARCHAR = 1043
    DATE = 1082
    TIME = 1083
    TIMESTAMP = 1114
    TIMESTAMPTZ = 1184
    INTERVAL = 1186
    TIMETZ = 1266
    NUMERIC = 1700
    UUID = 2950
    JSONB = 3802


# Array type OID to element type OID.
ARRAY_ELEMENTS: Dict[int, int] = {
    1000: oids.BOOL,
    1001: oids.BYTEA,
    1002: oids.CHAR,
    1003: oids.NAME,
    1016: oids.INT8,
    1005: oids.INT2,
    1007: oids.INT4,
    1009: oids.TEXT,
    1028: oids.OID,
    199: oids.JSON,
    143: oids.XML,
    651: oids.CIDR,
    1021: oids.FLOAT4,
    1022: oids.FLOAT8,
    791: oids.MONEY,
    1041: oids.INET,
    1014: oids.BPCHAR,
    1015: oids.VARCHAR,
    1182: oids.DATE,
    1183: oids.TIME,
    1115: oids.TIMESTAMP,
    1185: oids.TIMESTAMPTZ,
    1187: oids.INTERVAL,
    1270: oids.TIMETZ,
    1231: oids.NUMERIC,
    2951: oids.UUID,
    3807: oids.JSONB,
}

PG_EPOCH_DATE = date(2000, 1, 1)
PG_EPOCH = datetime(2000, 1, 1)
_INT32_MAX = 2**31 - 1
_INT32_MIN = -(2**31)
_INT64_MAX = 2**63 - 1
_INT64_MIN = -(2**63)

TextParser = Callable[[str], Any]
BinaryParser = Callable[[bytes], Any]


# Text format.


def _bool_text(s: str) -> bool:
    if s == "t":
        return True
    if s == "f":
        return False
    raise ValueError(f"not a boolean: {s!r}")


def _bytea_text(s: str) -> bytes:
    if s.startswith("\\x"):
        return bytes.fromhex(s[2:])
    out = bytearray()
    i = 0
    raw = s.encode("utf-8")  # escape format output is ASCII: other bytes come as octal
    while i < len(raw):
        c = raw[i]
        if c == 0x5C:  # backslash, escape format
            if raw[i + 1 : i + 2] == b"\\":
                out.append(0x5C)
                i += 2
            else:
                out.append(int(raw[i + 1 : i + 4], 8))
                i += 4
        else:
            out.append(c)
            i += 1
    return bytes(out)


_DATE_RE = re.compile(r"^(\d{4,})-(\d\d)-(\d\d)$")
_TIME_RE = re.compile(r"^(\d\d):(\d\d):(\d\d)(?:\.(\d{1,6}))?([+-]\d\d(?::?\d\d)?(?::?\d\d)?)?$")


def _micro(frac: Optional[str]) -> int:
    return int((frac or "").ljust(6, "0")) if frac else 0


def _offset(s: Optional[str]) -> Optional[timezone]:
    if not s:
        return None
    sign = -1 if s[0] == "-" else 1
    digits = s[1:].replace(":", "")
    hours = int(digits[0:2])
    minutes = int(digits[2:4] or 0)
    seconds = int(digits[4:6] or 0)
    return timezone(sign * timedelta(hours=hours, minutes=minutes, seconds=seconds))


def _date_text(s: str) -> Union[date, str]:
    if s == "infinity":
        return date.max
    if s == "-infinity":
        return date.min
    m = _DATE_RE.match(s)
    if not m:
        return s
    y, mo, d = (int(x) for x in m.groups())
    if y > 9999:
        return s
    return date(y, mo, d)


def _time_text(s: str) -> time:
    m = _TIME_RE.match(s)
    if not m:
        raise ValueError(f"not a time: {s!r}")
    h, mi, se, frac, off = m.groups()
    hour = int(h)
    if hour == 24:  # PostgreSQL allows 24:00:00
        return time(23, 59, 59, 999999, tzinfo=_offset(off))
    return time(hour, int(mi), int(se), _micro(frac), tzinfo=_offset(off))


def _timestamp_text(s: str) -> Union[datetime, str]:
    if s == "infinity":
        return datetime.max
    if s == "-infinity":
        return datetime.min
    if " " not in s or s.endswith(" BC"):
        return s
    d, t = s.split(" ", 1)
    dv = _date_text(d)
    if not isinstance(dv, date):
        return s
    tv = _time_text(t)
    return datetime.combine(dv, tv.replace(tzinfo=None), tzinfo=tv.tzinfo)


def _timestamptz_text(s: str) -> Union[datetime, str]:
    v = _timestamp_text(s)
    if isinstance(v, datetime) and v in (datetime.max, datetime.min):
        return v.replace(tzinfo=timezone.utc)
    return v


_INTERVAL_PART = re.compile(r"([+-]?\d+) (year|mon|day)s?")
_INTERVAL_TIME = re.compile(r"([+-])?(\d+):(\d\d):(\d\d)(?:\.(\d{1,6}))?$")


def _interval_text(s: str) -> Union[Interval, str]:
    if s == "infinity":
        return INTERVAL_INFINITY
    if s == "-infinity":
        return INTERVAL_NEG_INFINITY
    months = days = micros = 0
    for m in _INTERVAL_PART.finditer(s):
        n, unit = int(m.group(1)), m.group(2)
        if unit == "year":
            months += 12 * n
        elif unit == "mon":
            months += n
        else:
            days += n
    rest = _INTERVAL_PART.sub("", s).strip()
    if rest:
        tm = _INTERVAL_TIME.match(rest)
        if not tm:
            return s
        sign, h, mi, se, frac = tm.groups()
        micros = ((int(h) * 60 + int(mi)) * 60 + int(se)) * 1_000_000 + _micro(frac)
        if sign == "-":
            micros = -micros
    return Interval(months, days, micros)


def _inet_text(s: str) -> Any:
    return ipaddress.ip_interface(s)


def _cidr_text(s: str) -> Any:
    return ipaddress.ip_network(s)


def _parse_array_text(s: str, element: Callable[[str], Any]) -> Any:
    """Parse the text form of an array (``{1,2,NULL}``, ``{{a,b},{c,d}}``, ``{"x y"}``)."""
    if s.startswith("["):  # explicit bounds: [1:2]={...}
        s = s[s.index("=") + 1 :]
    pos = 0

    def parse() -> List[Any]:
        nonlocal pos
        if s[pos] != "{":
            raise ValueError(f"array expected '{{' at {pos}: {s!r}")
        pos += 1
        out: List[Any] = []
        if s[pos] == "}":
            pos += 1
            return out
        while True:
            if s[pos] == "{":
                out.append(parse())
            elif s[pos] == '"':
                pos += 1
                buf = []
                while s[pos] != '"':
                    if s[pos] == "\\":
                        pos += 1
                    buf.append(s[pos])
                    pos += 1
                pos += 1
                out.append(element("".join(buf)))
            else:
                start = pos
                while s[pos] not in ",}":
                    pos += 1
                token = s[start:pos].strip()
                out.append(None if token == "NULL" else element(token))
            if s[pos] == ",":
                pos += 1
            elif s[pos] == "}":
                pos += 1
                return out
            else:
                raise ValueError(f"array expected ',' or '}}' at {pos}: {s!r}")

    return parse()


def _char_text(s: str) -> str:
    # "char" is one raw byte; charout prints a byte above 127 as \ooo.
    if len(s) == 4 and s[0] == "\\":
        return chr(int(s[1:], 8))
    return s


TEXT_PARSERS: Dict[int, TextParser] = {
    oids.CHAR: _char_text,
    oids.BOOL: _bool_text,
    oids.BYTEA: _bytea_text,
    oids.INT2: int,
    oids.INT4: int,
    oids.INT8: int,
    oids.OID: int,
    oids.FLOAT4: float,
    oids.FLOAT8: float,
    oids.NUMERIC: Decimal,
    oids.UUID: uuid.UUID,
    oids.JSON: json.loads,
    oids.JSONB: json.loads,
    oids.DATE: _date_text,
    oids.TIME: _time_text,
    oids.TIMETZ: _time_text,
    oids.TIMESTAMP: _timestamp_text,
    oids.TIMESTAMPTZ: _timestamptz_text,
    oids.INTERVAL: _interval_text,
    oids.INET: _inet_text,
    oids.CIDR: _cidr_text,
}


# Binary format (the *_send functions of src/backend/utils/adt).


def _unpack(fmt: str, b: bytes) -> Any:
    s = struct.Struct(fmt)
    if len(b) != s.size:
        raise ValueError(f"expected {s.size} bytes, got {len(b)}")
    return s.unpack(b)[0]


def _date_bin(b: bytes) -> Union[date, str]:
    days = _unpack(">i", b)
    if days == _INT32_MAX:
        return date.max
    if days == _INT32_MIN:
        return date.min
    try:
        return PG_EPOCH_DATE + timedelta(days=days)
    except OverflowError:
        return f"{days} days from 2000-01-01"


def _timestamp_bin(b: bytes) -> Union[datetime, str]:
    us = _unpack(">q", b)
    if us == _INT64_MAX:
        return datetime.max
    if us == _INT64_MIN:
        return datetime.min
    try:
        return PG_EPOCH + timedelta(microseconds=us)
    except OverflowError:
        return f"{us} microseconds from 2000-01-01"


def _timestamptz_bin(b: bytes) -> Union[datetime, str]:
    v = _timestamp_bin(b)
    return v.replace(tzinfo=timezone.utc) if isinstance(v, datetime) else v


def _time_from_micros(us: int, tz: Optional[timezone] = None) -> time:
    if us >= 86_400_000_000:  # 24:00:00
        return time(23, 59, 59, 999999, tzinfo=tz)
    s, micro = divmod(us, 1_000_000)
    m, sec = divmod(s, 60)
    h, mi = divmod(m, 60)
    return time(h, mi, sec, micro, tzinfo=tz)


def _time_bin(b: bytes) -> time:
    return _time_from_micros(_unpack(">q", b))


def _timetz_bin(b: bytes) -> time:
    if len(b) != 12:
        raise ValueError(f"timetz expects 12 bytes, got {len(b)}")
    us, zone = struct.unpack(">qi", b)
    # The server stores the zone as seconds WEST of UTC.
    return _time_from_micros(us, timezone(timedelta(seconds=-zone)))


def _interval_bin(b: bytes) -> Interval:
    if len(b) != 16:
        raise ValueError(f"interval expects 16 bytes, got {len(b)}")
    us, days, months = struct.unpack(">qii", b)
    return Interval(months, days, us)


_NUMERIC_POS, _NUMERIC_NEG, _NUMERIC_NAN = 0x0000, 0x4000, 0xC000
_NUMERIC_PINF, _NUMERIC_NINF = 0xD000, 0xF000


def _numeric_bin(b: bytes) -> Decimal:
    if len(b) < 8:
        raise ValueError("numeric expects at least 8 bytes")
    ndigits, weight, sign, dscale = struct.unpack(">hhHh", b[:8])
    if sign == _NUMERIC_NAN:
        return Decimal("NaN")
    if sign == _NUMERIC_PINF:
        return Decimal("Infinity")
    if sign == _NUMERIC_NINF:
        return Decimal("-Infinity")
    if len(b) != 8 + 2 * ndigits:
        raise ValueError(f"numeric with {ndigits} digits expects {8 + 2 * ndigits} bytes")
    digits = struct.unpack(f">{ndigits}H", b[8:])
    coef = 0
    for d in digits:
        coef = coef * 10000 + d
    # The first base 10000 digit sits at 10000**weight; the value is shown with dscale decimals.
    # Built from its digits, never through a decimal context, so no precision limit applies.
    shift = (weight - ndigits + 1) * 4 + dscale
    coef = coef * 10**shift if shift >= 0 else coef // 10**-shift
    return Decimal((1 if sign == _NUMERIC_NEG else 0, tuple(int(c) for c in str(coef)), -dscale))


def _jsonb_bin(b: bytes) -> Any:
    if not b or b[0] != 1:
        raise ValueError("jsonb binary format version 1 expected")
    return json.loads(b[1:].decode("utf-8"))


def _inet_bin(b: bytes) -> Any:
    if len(b) < 4:
        raise ValueError("inet expects at least 4 bytes")
    family, bits, is_cidr, nb = b[0], b[1], b[2], b[3]
    addr = b[4 : 4 + nb]
    if len(addr) != nb or nb not in (4, 16) or family not in (2, 3):
        raise ValueError("malformed inet value")
    ip = ipaddress.ip_address(addr)
    if is_cidr:
        return ipaddress.ip_network(f"{ip}/{bits}")
    return ipaddress.ip_interface(f"{ip}/{bits}")


def _bool_bin(b: bytes) -> bool:
    return bool(_unpack(">B", b))


BINARY_PARSERS: Dict[int, BinaryParser] = {
    oids.BOOL: _bool_bin,
    oids.BYTEA: bytes,
    oids.INT2: lambda b: _unpack(">h", b),
    oids.INT4: lambda b: _unpack(">i", b),
    oids.INT8: lambda b: _unpack(">q", b),
    oids.OID: lambda b: _unpack(">I", b),
    oids.FLOAT4: lambda b: _unpack(">f", b),
    oids.FLOAT8: lambda b: _unpack(">d", b),
    oids.NUMERIC: _numeric_bin,
    oids.UUID: lambda b: uuid.UUID(bytes=bytes(b)),
    oids.JSON: lambda b: json.loads(b.decode("utf-8")),
    oids.JSONB: _jsonb_bin,
    oids.DATE: _date_bin,
    oids.TIME: _time_bin,
    oids.TIMETZ: _timetz_bin,
    oids.TIMESTAMP: _timestamp_bin,
    oids.TIMESTAMPTZ: _timestamptz_bin,
    oids.INTERVAL: _interval_bin,
    oids.INET: _inet_bin,
    oids.CIDR: _inet_bin,
    # cash_send: an int64 count of the currency's smallest unit (cents for lc_monetary en_US).
    oids.MONEY: lambda b: _unpack(">q", b),
    # "char" is one raw byte, the same character the text format's \ooo escape names.
    oids.CHAR: lambda b: bytes(b).decode("latin-1"),
}

_TEXT_LIKE = (oids.TEXT, oids.VARCHAR, oids.BPCHAR, oids.NAME, oids.XML)


class Converter:
    """Turn ``TupleValue``s into Python values from their type OID.

    >>> conv = Converter()
    >>> conv.convert(23, TupleValue("t", b"42"))
    42
    """

    def __init__(self, encoding: str = "utf-8") -> None:
        self.encoding = encoding
        self._text: Dict[int, TextParser] = dict(TEXT_PARSERS)
        self._binary: Dict[int, BinaryParser] = dict(BINARY_PARSERS)

    def register(
        self,
        type_oid: int,
        text: Optional[TextParser] = None,
        binary: Optional[BinaryParser] = None,
    ) -> None:
        """Teach the converter a type: ``text`` parses the text form (a ``str``), ``binary`` the
        send format (``bytes``). Use it for extension types (their OIDs come in Type messages)."""
        if text is not None:
            self._text[type_oid] = text
        if binary is not None:
            self._binary[type_oid] = binary

    def text(self, type_oid: int, s: str) -> Any:
        """Parse the text form ``s`` of a value of type ``type_oid``."""
        element = ARRAY_ELEMENTS.get(type_oid)
        if element is not None and type_oid not in self._text:
            parse_element = self._text.get(element, str)
            return _parse_array_text(s, parse_element)
        parser = self._text.get(type_oid)
        return s if parser is None else parser(s)

    def binary(self, type_oid: int, b: bytes) -> Any:
        """Parse the binary (send) form ``b`` of a value of type ``type_oid``."""
        if type_oid in _TEXT_LIKE and type_oid not in self._binary:
            return b.decode(self.encoding)
        parser = self._binary.get(type_oid)
        if parser is not None:
            return parser(b)
        if type_oid in ARRAY_ELEMENTS:
            return self._array_binary(b)
        return bytes(b)

    def _array_binary(self, b: bytes) -> Any:
        try:
            return self._array_binary_unchecked(b)
        except struct.error as exc:
            raise ValueError(f"malformed binary array: {exc}") from None

    def _array_binary_unchecked(self, b: bytes) -> Any:
        if len(b) < 12:
            raise ValueError("array expects at least 12 bytes")
        ndim, _flags, elem_oid = struct.unpack(">iiI", b[:12])
        pos = 12
        dims: List[int] = []
        for _ in range(ndim):
            size, _lbound = struct.unpack(">ii", b[pos : pos + 8])
            dims.append(size)
            pos += 8
        flat: List[Any] = []
        total = 1
        for d in dims:
            total *= d
        if ndim == 0:
            total = 0
        for _ in range(total):
            (length,) = struct.unpack(">i", b[pos : pos + 4])
            pos += 4
            if length < 0:
                flat.append(None)
                continue
            flat.append(self.binary(elem_oid, b[pos : pos + length]))
            pos += length
        if pos != len(b):
            raise ValueError("array has trailing bytes")

        def nest(items: List[Any], shape: List[int]) -> List[Any]:
            if len(shape) <= 1:
                return items
            step = len(items) // shape[0]
            return [nest(items[i * step : (i + 1) * step], shape[1:]) for i in range(shape[0])]

        return nest(flat, dims)

    def convert(self, type_oid: int, value: TupleValue) -> Any:
        """Convert one column value: None for null, ``UNCHANGED_TOAST`` for an unchanged TOAST
        value, else the parsed Python value."""
        if value.kind == TupleKind.NULL:
            return None
        if value.kind == TupleKind.UNCHANGED_TOAST:
            return UNCHANGED_TOAST
        assert value.data is not None
        if value.kind == TupleKind.BINARY:
            return self.binary(type_oid, value.data)
        return self.text(type_oid, value.data.decode(self.encoding))

    def row(self, relation: Relation, data: TupleData) -> Dict[str, Any]:
        """Convert a whole row to ``{column name: value}``."""
        if len(relation.columns) != len(data.values):
            raise ValueError(
                f"relation {relation.qualified_name} has {len(relation.columns)} columns, "
                f"the row has {len(data.values)}"
            )
        return {
            col.name: self.convert(col.type_oid, v) for col, v in zip(relation.columns, data.values)
        }
