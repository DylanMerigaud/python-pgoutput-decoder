"""Unit tests for pgoutput.values: text and binary (send) formats, from PostgreSQL wire layouts."""

import ipaddress
import math
import struct
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from pgoutput.core import Column, Relation, TupleData, TupleValue
from pgoutput.values import UNCHANGED_TOAST, Converter, Interval, UnchangedToast, oids

CONV = Converter()


def to_text(oid, s):
    return CONV.text(oid, s)


def to_bin(oid, b):
    return CONV.binary(oid, b)


def tz(hours=0, minutes=0):
    return timezone(timedelta(hours=hours, minutes=minutes))


# ---------------------------------------------------------------- sentinel, Interval, oids


def test_unchanged_toast_is_falsy_singleton():
    assert not UNCHANGED_TOAST
    assert UnchangedToast() is UNCHANGED_TOAST
    assert repr(UNCHANGED_TOAST) == "UNCHANGED_TOAST"


def test_interval_to_timedelta():
    iv = Interval(0, 2, 3_500_000)
    assert iv.to_timedelta() == timedelta(days=2, seconds=3, microseconds=500_000)
    assert Interval().to_timedelta() == timedelta(0)
    assert Interval(0, -1, 7_200_000_000).to_timedelta() == timedelta(days=-1, hours=2)


def test_interval_with_months_has_no_timedelta():
    with pytest.raises(ValueError):
        Interval(months=1).to_timedelta()


def test_interval_is_frozen_and_comparable():
    assert Interval(1, 2, 3) == Interval(1, 2, 3)
    with pytest.raises(AttributeError):
        Interval().months = 3  # type: ignore[misc]


@pytest.mark.parametrize(
    "name,value",
    [
        ("BOOL", 16),
        ("BYTEA", 17),
        ("INT8", 20),
        ("INT2", 21),
        ("INT4", 23),
        ("TEXT", 25),
        ("OID", 26),
        ("JSON", 114),
        ("FLOAT4", 700),
        ("FLOAT8", 701),
        ("INET", 869),
        ("DATE", 1082),
        ("TIME", 1083),
        ("TIMESTAMP", 1114),
        ("TIMESTAMPTZ", 1184),
        ("INTERVAL", 1186),
        ("TIMETZ", 1266),
        ("NUMERIC", 1700),
        ("UUID", 2950),
        ("JSONB", 3802),
    ],
)
def test_oids_match_pg_type(name, value):
    assert getattr(oids, name) == value


# ---------------------------------------------------------------- text format


def test_text_bool():
    assert to_text(16, "t") is True
    assert to_text(16, "f") is False
    with pytest.raises(ValueError):
        to_text(16, "true")


@pytest.mark.parametrize("oid", [21, 23, 20, 26])
def test_text_integers(oid):
    assert to_text(oid, "42") == 42
    assert to_text(oid, "-7") == -7


def test_text_int8_extremes_and_oid_max():
    assert to_text(20, "9223372036854775807") == 2**63 - 1
    assert to_text(20, "-9223372036854775808") == -(2**63)
    assert to_text(26, "4294967295") == 2**32 - 1


@pytest.mark.parametrize("oid", [700, 701])
def test_text_float(oid):
    assert to_text(oid, "1.5") == 1.5
    assert to_text(oid, "-2.25e-3") == -0.00225
    assert math.isnan(to_text(oid, "NaN"))
    assert to_text(oid, "Infinity") == math.inf
    assert to_text(oid, "-Infinity") == -math.inf


def test_text_numeric():
    assert to_text(1700, "12345.6700") == Decimal("12345.6700")
    assert str(to_text(1700, "1.50")) == "1.50"
    assert to_text(1700, "NaN").is_nan()
    assert to_text(1700, "Infinity") == Decimal("Infinity")
    assert to_text(1700, "-Infinity") == Decimal("-Infinity")
    big = "123456789012345678901234567890.123456789012345678901234567890"
    assert to_text(1700, big) == Decimal(big)
    assert str(to_text(1700, big)) == big


def test_text_bytea_hex():
    assert to_text(17, "\\x") == b""
    assert to_text(17, "\\x00ff61") == b"\x00\xffa"
    assert to_text(17, "\\xDEADbeef") == b"\xde\xad\xbe\xef"


def test_text_bytea_escape_format():
    # PostgreSQL escape output: a, b, c, a NUL as \000, 0xff as \377, a backslash as \\.
    assert to_text(17, "abc\\000\\377\\\\") == b"abc\x00\xff\\"
    assert to_text(17, "") == b""
    assert to_text(17, "plain") == b"plain"


def test_text_uuid():
    u = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    assert to_text(2950, u) == uuid.UUID(u)


@pytest.mark.parametrize("oid", [114, 3802])
def test_text_json_and_jsonb(oid):
    assert to_text(oid, '{"a": [1, 2, {"b": null}], "c": "x"}') == {
        "a": [1, 2, {"b": None}],
        "c": "x",
    }
    assert to_text(oid, "[1, 2.5, true]") == [1, 2.5, True]
    assert to_text(oid, '"s"') == "s"


def test_text_date():
    assert to_text(1082, "2024-02-29") == date(2024, 2, 29)
    assert to_text(1082, "0001-01-01") == date(1, 1, 1)
    assert to_text(1082, "9999-12-31") == date(9999, 12, 31)


def test_text_date_infinity():
    assert to_text(1082, "infinity") == date.max
    assert to_text(1082, "-infinity") == date.min


def test_text_date_bc_returns_the_text():
    assert to_text(1082, "0044-03-15 BC") == "0044-03-15 BC"


def test_text_date_beyond_year_9999_returns_the_text():
    assert to_text(1082, "10000-01-01") == "10000-01-01"


def test_text_time():
    assert to_text(1083, "04:05:06") == time(4, 5, 6)
    assert to_text(1083, "04:05:06.5") == time(4, 5, 6, 500000)
    assert to_text(1083, "04:05:06.000001") == time(4, 5, 6, 1)
    assert to_text(1083, "00:00:00") == time(0, 0, 0)
    assert to_text(1083, "23:59:59.999999") == time(23, 59, 59, 999999)


def test_text_timetz_offsets():
    v = to_text(1266, "10:20:30+05:30")
    assert v == time(10, 20, 30, tzinfo=tz(5, 30))
    assert v.utcoffset() == timedelta(hours=5, minutes=30)
    v = to_text(1266, "10:20:30-03")
    assert v.utcoffset() == timedelta(hours=-3)
    assert to_text(1266, "10:20:30+00").utcoffset() == timedelta(0)
    assert to_text(1266, "10:20:30.25-08").microsecond == 250000


@pytest.mark.parametrize(
    "text,micro",
    [
        ("2024-01-02 03:04:05", 0),
        ("2024-01-02 03:04:05.1", 100000),
        ("2024-01-02 03:04:05.12", 120000),
        ("2024-01-02 03:04:05.123", 123000),
        ("2024-01-02 03:04:05.1234", 123400),
        ("2024-01-02 03:04:05.12345", 123450),
        ("2024-01-02 03:04:05.123456", 123456),
    ],
)
def test_text_timestamp_fraction_digits(text, micro):
    v = to_text(1114, text)
    assert v == datetime(2024, 1, 2, 3, 4, 5, micro)
    assert v.tzinfo is None


def test_text_timestamp_infinity_and_bc():
    assert to_text(1114, "infinity") == datetime.max
    assert to_text(1114, "-infinity") == datetime.min
    assert to_text(1114, "0044-03-15 10:00:00 BC") == "0044-03-15 10:00:00 BC"


def test_text_timestamptz_offsets():
    v = to_text(1184, "2024-01-02 03:04:05+00")
    assert v == datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert v.utcoffset() == timedelta(0)
    v = to_text(1184, "2024-01-02 03:04:05.5+05:30")
    assert v.utcoffset() == timedelta(hours=5, minutes=30)
    assert v == datetime(2024, 1, 1, 21, 34, 5, 500000, tzinfo=timezone.utc)
    v = to_text(1184, "1999-12-31 23:59:59.999999-08")
    assert v.utcoffset() == timedelta(hours=-8)
    assert v.microsecond == 999999


def test_text_timestamptz_infinity_is_aware():
    hi = to_text(1184, "infinity")
    lo = to_text(1184, "-infinity")
    assert hi.tzinfo is not None and lo.tzinfo is not None
    assert hi == datetime.max.replace(tzinfo=timezone.utc)
    assert lo == datetime.min.replace(tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "text,expected",
    [
        ("1 year 2 mons 3 days 04:05:06.5", Interval(14, 3, 14_706_500_000)),
        ("-1 days +02:00:00", Interval(0, -1, 7_200_000_000)),
        ("00:00:00", Interval(0, 0, 0)),
        ("3 mons", Interval(3, 0, 0)),
        ("-04:05:06", Interval(0, 0, -14_706_000_000)),
        ("2 years", Interval(24, 0, 0)),
        ("1 year", Interval(12, 0, 0)),
        ("1 day", Interval(0, 1, 0)),
        ("100:00:00", Interval(0, 0, 360_000_000_000)),
        ("00:00:00.000001", Interval(0, 0, 1)),
        ("-1 years -2 mons +3 days -04:05:06", Interval(-14, 3, -14_706_000_000)),
    ],
)
def test_text_interval_postgres_style(text, expected):
    assert to_text(1186, text) == expected


def test_text_inet_and_cidr():
    assert to_text(869, "192.168.1.5/24") == ipaddress.ip_interface("192.168.1.5/24")
    assert to_text(869, "10.0.0.1") == ipaddress.ip_interface("10.0.0.1/32")
    assert to_text(869, "2001:db8::1") == ipaddress.ip_interface("2001:db8::1/128")
    assert to_text(650, "10.1.0.0/16") == ipaddress.ip_network("10.1.0.0/16")
    assert to_text(650, "2001:db8::/32") == ipaddress.ip_network("2001:db8::/32")


def test_text_array_ints():
    assert to_text(1007, "{1,2,3}") == [1, 2, 3]
    assert to_text(1005, "{-1}") == [-1]
    assert to_text(1016, "{9223372036854775807}") == [2**63 - 1]


def test_text_array_with_null():
    assert to_text(1007, "{1,NULL,3}") == [1, None, 3]


def test_text_array_text_quotes_escapes_and_null():
    s = '{plain,"with space","a,b","say \\"hi\\"","back\\\\slash","NULL",NULL,""}'
    assert to_text(1009, s) == [
        "plain",
        "with space",
        "a,b",
        'say "hi"',
        "back\\slash",
        "NULL",
        None,
        "",
    ]


def test_text_array_empty():
    assert to_text(1007, "{}") == []
    assert to_text(1009, "{}") == []


def test_text_array_2d():
    assert to_text(1007, "{{1,2},{3,4}}") == [[1, 2], [3, 4]]
    assert to_text(1009, '{{a,"b c"},{d,NULL}}') == [["a", "b c"], ["d", None]]


def test_text_array_explicit_bounds():
    assert to_text(1007, "[0:1]={1,2}") == [1, 2]
    assert to_text(1007, "[1:2][1:2]={{1,2},{3,4}}") == [[1, 2], [3, 4]]


def test_text_array_of_other_element_types():
    assert to_text(1000, "{t,f,NULL}") == [True, False, None]
    assert to_text(2951, "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}") == [
        uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    ]
    assert to_text(1115, '{"2024-01-02 03:04:05"}') == [datetime(2024, 1, 2, 3, 4, 5)]
    assert to_text(1231, "{1.50,NaN}")[0] == Decimal("1.50")
    assert to_text(1001, '{"\\\\x0102"}') == [b"\x01\x02"]
    assert to_text(1022, "{1.5,Infinity}") == [1.5, math.inf]
    assert to_text(3807, '{"{\\"a\\": 1}"}') == [{"a": 1}]


def test_text_malformed_array():
    with pytest.raises(ValueError):
        to_text(1007, "1,2")
    with pytest.raises(ValueError):
        to_text(1007, "{1;2}")


def test_text_unknown_oid_returns_str():
    assert to_text(99999, "anything") == "anything"
    assert to_text(25, "hello") == "hello"
    assert to_text(790, "$1,000.00") == "$1,000.00"


def test_array_oids_match_pg_type_catalog():
    # pg_type.oid of the array types, with their typelem.
    catalog = {
        1000: 16,
        1001: 17,
        1002: 18,
        1003: 19,
        1005: 21,
        1007: 23,
        1009: 25,
        1014: 1042,
        1015: 1043,
        1016: 20,
        1021: 700,
        1022: 701,
        1028: 26,
        1041: 869,
        1115: 1114,
        1182: 1082,
        1183: 1083,
        1185: 1184,
        1187: 1186,
        1231: 1700,
        1270: 1266,
        2951: 2950,
        3807: 3802,
        199: 114,
        651: 650,
    }
    from pgoutput.values import ARRAY_ELEMENTS

    for array_oid, elem in catalog.items():
        assert ARRAY_ELEMENTS[array_oid] == elem


# ---------------------------------------------------------------- binary format


def numeric(ndigits, weight, sign, dscale, digits=()):
    return struct.pack(">hhHh", ndigits, weight, sign, dscale) + struct.pack(
        f">{len(digits)}H", *digits
    )


def test_binary_bool():
    assert to_bin(16, b"\x01") is True
    assert to_bin(16, b"\x00") is False


def test_binary_ints():
    assert to_bin(21, struct.pack(">h", -2)) == -2
    assert to_bin(23, struct.pack(">i", -123456)) == -123456
    assert to_bin(20, struct.pack(">q", -(2**63))) == -(2**63)
    assert to_bin(20, struct.pack(">q", 2**63 - 1)) == 2**63 - 1
    assert to_bin(26, struct.pack(">I", 4294967295)) == 4294967295


def test_binary_floats():
    assert to_bin(700, struct.pack(">f", 1.5)) == 1.5
    assert to_bin(701, struct.pack(">d", -0.1)) == -0.1
    assert math.isnan(to_bin(701, struct.pack(">d", math.nan)))
    assert math.isnan(to_bin(700, struct.pack(">f", math.nan)))
    assert to_bin(701, struct.pack(">d", math.inf)) == math.inf
    assert to_bin(700, struct.pack(">f", -math.inf)) == -math.inf


@pytest.mark.parametrize(
    "payload,expected",
    [
        (numeric(0, 0, 0x0000, 0), Decimal("0")),
        (numeric(2, 0, 0x0000, 2, [1, 5000]), Decimal("1.50")),
        (numeric(3, 1, 0x4000, 2, [1, 2345, 6700]), Decimal("-12345.67")),
        (numeric(1, -1, 0x0000, 4, [1]), Decimal("0.0001")),
        (numeric(1, 1, 0x0000, 0, [1]), Decimal("10000")),
        (numeric(1, 5, 0x0000, 0, [1]), Decimal("100000000000000000000")),
        (numeric(1, 0, 0x0000, 0, [7]), Decimal("7")),
        (numeric(2, 0, 0x0000, 3, [12, 3400]), Decimal("12.340")),
        (numeric(3, 2, 0x0000, 0, [1, 0, 0]), Decimal("100000000")),
        (numeric(2, -1, 0x4000, 8, [100, 1]), Decimal("-0.01000001")),
    ],
)
def test_binary_numeric(payload, expected):
    got = to_bin(1700, payload)
    assert got == expected


def test_binary_numeric_keeps_display_scale():
    assert str(to_bin(1700, numeric(2, 0, 0x0000, 2, [1, 5000]))) == "1.50"
    assert str(to_bin(1700, numeric(2, 0, 0x0000, 3, [12, 3400]))) == "12.340"
    assert str(to_bin(1700, numeric(1, -1, 0x0000, 4, [1]))) == "0.0001"


def test_binary_numeric_special_values():
    assert to_bin(1700, numeric(0, 0, 0xC000, 0)).is_nan()
    assert to_bin(1700, numeric(0, 0, 0xD000, 0)) == Decimal("Infinity")
    assert to_bin(1700, numeric(0, 0, 0xF000, 0)) == Decimal("-Infinity")


def test_binary_numeric_wrong_length():
    with pytest.raises(ValueError):
        to_bin(1700, b"\x00\x01")
    with pytest.raises(ValueError):
        to_bin(1700, numeric(2, 0, 0, 0, [1]))  # claims 2 digits, carries 1


def test_binary_uuid():
    u = uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    assert to_bin(2950, u.bytes) == u
    with pytest.raises(ValueError):
        to_bin(2950, b"\x00" * 15)


def test_binary_bytea():
    assert to_bin(17, b"\x00\xffabc") == b"\x00\xffabc"
    assert to_bin(17, memoryview(b"\x01\x02")) == b"\x01\x02"


def test_binary_json_and_jsonb():
    assert to_bin(114, b'{"a": [1, 2]}') == {"a": [1, 2]}
    assert to_bin(3802, b"\x01" + b'{"a": [1, 2]}') == {"a": [1, 2]}
    assert to_bin(3802, b"\x01" + '"é"'.encode()) == "é"
    with pytest.raises(ValueError):
        to_bin(3802, b'{"a": 1}')  # no version byte
    with pytest.raises(ValueError):
        to_bin(3802, b"\x02{}")
    with pytest.raises(ValueError):
        to_bin(3802, b"")


def test_binary_date():
    assert to_bin(1082, struct.pack(">i", 0)) == date(2000, 1, 1)
    assert to_bin(1082, struct.pack(">i", 366)) == date(2001, 1, 1)
    assert to_bin(1082, struct.pack(">i", -1)) == date(1999, 12, 31)
    assert to_bin(1082, struct.pack(">i", 8766)) == date(2024, 1, 1)


def test_binary_date_infinities():
    assert to_bin(1082, struct.pack(">i", 2**31 - 1)) == date.max
    assert to_bin(1082, struct.pack(">i", -(2**31))) == date.min


def test_binary_timestamp():
    assert to_bin(1114, struct.pack(">q", 0)) == datetime(2000, 1, 1)
    assert to_bin(1114, struct.pack(">q", 1)) == datetime(2000, 1, 1, 0, 0, 0, 1)
    assert to_bin(1114, struct.pack(">q", -1)) == datetime(1999, 12, 31, 23, 59, 59, 999999)
    us = ((8766 * 86400) + 3 * 3600 + 4 * 60 + 5) * 1_000_000 + 123456
    assert to_bin(1114, struct.pack(">q", us)) == datetime(2024, 1, 1, 3, 4, 5, 123456)


def test_binary_timestamp_infinities():
    assert to_bin(1114, struct.pack(">q", 2**63 - 1)) == datetime.max
    assert to_bin(1114, struct.pack(">q", -(2**63))) == datetime.min


def test_binary_timestamptz_is_utc_aware():
    v = to_bin(1184, struct.pack(">q", 1_000_000))
    assert v == datetime(2000, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    assert v.utcoffset() == timedelta(0)


def test_binary_timestamptz_infinities_are_aware():
    hi = to_bin(1184, struct.pack(">q", 2**63 - 1))
    lo = to_bin(1184, struct.pack(">q", -(2**63)))
    assert hi == datetime.max.replace(tzinfo=timezone.utc)
    assert lo == datetime.min.replace(tzinfo=timezone.utc)


def test_binary_time():
    assert to_bin(1083, struct.pack(">q", 0)) == time(0, 0)
    us = (4 * 3600 + 5 * 60 + 6) * 1_000_000 + 500000
    assert to_bin(1083, struct.pack(">q", us)) == time(4, 5, 6, 500000)
    assert to_bin(1083, struct.pack(">q", 86_399_999_999)) == time(23, 59, 59, 999999)


def test_binary_timetz_zone_is_seconds_west_of_utc():
    us = 10 * 3600 * 1_000_000
    v = to_bin(1266, struct.pack(">qi", us, 3600))
    assert v == time(10, 0, tzinfo=tz(-1))
    assert v.utcoffset() == timedelta(hours=-1)
    v = to_bin(1266, struct.pack(">qi", us, -19800))  # UTC+05:30
    assert v.utcoffset() == timedelta(hours=5, minutes=30)
    v = to_bin(1266, struct.pack(">qi", us, 0))
    assert v.utcoffset() == timedelta(0)


def test_binary_timetz_wrong_length():
    with pytest.raises(ValueError):
        to_bin(1266, struct.pack(">q", 0))


def test_binary_interval_layout_is_micros_days_months():
    payload = struct.pack(">qii", 14_706_500_000, 3, 14)
    assert to_bin(1186, payload) == Interval(14, 3, 14_706_500_000)
    assert to_bin(1186, struct.pack(">qii", -1, -2, -3)) == Interval(-3, -2, -1)
    assert to_bin(1186, struct.pack(">qii", 0, 0, 0)) == Interval()
    with pytest.raises(ValueError):
        to_bin(1186, b"\x00" * 15)


def test_binary_inet_ipv4():
    payload = bytes([2, 24, 0, 4]) + bytes([192, 168, 1, 5])
    assert to_bin(869, payload) == ipaddress.ip_interface("192.168.1.5/24")
    payload = bytes([2, 32, 0, 4]) + bytes([10, 0, 0, 1])
    assert to_bin(869, payload) == ipaddress.ip_interface("10.0.0.1/32")


def test_binary_inet_ipv6():
    addr = ipaddress.ip_address("2001:db8::1").packed
    payload = bytes([3, 128, 0, 16]) + addr
    assert to_bin(869, payload) == ipaddress.ip_interface("2001:db8::1/128")


def test_binary_cidr():
    payload = bytes([2, 16, 1, 4]) + bytes([10, 1, 0, 0])
    assert to_bin(650, payload) == ipaddress.ip_network("10.1.0.0/16")
    addr = ipaddress.ip_address("2001:db8::").packed
    payload = bytes([3, 32, 1, 16]) + addr
    assert to_bin(650, payload) == ipaddress.ip_network("2001:db8::/32")


def test_binary_inet_malformed():
    with pytest.raises(ValueError):
        to_bin(869, b"\x02\x18")
    with pytest.raises(ValueError):
        to_bin(869, bytes([2, 24, 0, 4]) + bytes([1, 2]))  # address shorter than nb


def array(elem_oid, dims, items, lbound=1):
    has_null = 1 if any(i is None for i in items) else 0
    out = struct.pack(">iiI", len(dims), has_null, elem_oid)
    for d in dims:
        out += struct.pack(">ii", d, lbound)
    for i in items:
        out += struct.pack(">i", -1) if i is None else struct.pack(">i", len(i)) + i
    return out


def i4(n):
    return struct.pack(">i", n)


def test_binary_array_int4_with_null():
    payload = array(23, [3], [i4(1), None, i4(3)])
    assert to_bin(1007, payload) == [1, None, 3]


def test_binary_array_2d():
    payload = array(23, [2, 2], [i4(1), i4(2), i4(3), i4(4)])
    assert to_bin(1007, payload) == [[1, 2], [3, 4]]


def test_binary_array_2d_uneven_shape():
    payload = array(23, [2, 3], [i4(n) for n in range(6)])
    assert to_bin(1007, payload) == [[0, 1, 2], [3, 4, 5]]


def test_binary_array_3d():
    payload = array(23, [2, 1, 2], [i4(n) for n in range(4)])
    assert to_bin(1007, payload) == [[[0, 1]], [[2, 3]]]


def test_binary_array_empty():
    assert to_bin(1007, struct.pack(">iiI", 0, 0, 23)) == []


def test_binary_array_text():
    payload = array(25, [3], [b"a", "é b".encode(), None])
    assert to_bin(1009, payload) == ["a", "é b", None]


def test_binary_array_nonunit_lower_bound():
    payload = array(23, [2], [i4(5), i4(6)], lbound=0)
    assert to_bin(1007, payload) == [5, 6]


def test_binary_array_other_element_types():
    payload = array(16, [2], [b"\x01", b"\x00"])
    assert to_bin(1000, payload) == [True, False]
    u = uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    assert to_bin(2951, array(2950, [1], [u.bytes])) == [u]


def test_binary_array_malformed():
    with pytest.raises(ValueError):
        to_bin(1007, b"\x00" * 4)
    good = array(23, [2], [i4(1), i4(2)])
    with pytest.raises(ValueError):
        to_bin(1007, good + b"\x00")  # trailing bytes
    with pytest.raises(ValueError):
        to_bin(1007, good[:-2])  # last element cut short


def test_binary_array_truncated_dimension_header_is_value_error():
    header = struct.pack(">iiI", 1, 0, 23) + b"\x00\x00"
    with pytest.raises(ValueError):
        to_bin(1007, header)


@pytest.mark.parametrize("oid", [25, 1043, 1042, 19])
def test_binary_text_like_decode_utf8(oid):
    assert to_bin(oid, "héllo".encode()) == "héllo"
    assert to_bin(oid, b"") == ""


def test_binary_char_one_byte():
    assert to_bin(18, b"x") == "x"


def test_binary_money_is_an_int64_of_cents():
    cents = struct.pack(">q", 1000)
    assert to_bin(790, cents) == 1000


def test_binary_unknown_oid_returns_bytes():
    got = to_bin(99999, b"\x01\x02")
    assert got == b"\x01\x02"
    assert isinstance(got, bytes)
    assert isinstance(to_bin(99999, memoryview(b"\x01")), bytes)


@pytest.mark.parametrize(
    "oid,payload",
    [
        (16, b"\x01\x01"),
        (21, b"\x00"),
        (23, b"\x00\x00\x00"),
        (20, b"\x00" * 7),
        (26, b"\x00" * 5),
        (700, b"\x00" * 8),
        (701, b"\x00" * 4),
        (1082, b"\x00" * 3),
        (1083, b"\x00" * 4),
        (1114, b"\x00" * 9),
        (1184, b""),
    ],
)
def test_binary_wrong_length_raises_value_error(oid, payload):
    with pytest.raises(ValueError):
        to_bin(oid, payload)


# ---------------------------------------------------------------- Converter.convert, register, row


def test_convert_null():
    assert CONV.convert(23, TupleValue("n")) is None


def test_convert_unchanged_toast():
    got = CONV.convert(25, TupleValue("u"))
    assert got is UNCHANGED_TOAST
    assert not got


def test_convert_text_kind():
    assert CONV.convert(23, TupleValue("t", b"42")) == 42
    assert CONV.convert(25, TupleValue("t", "héllo".encode())) == "héllo"


def test_convert_binary_kind():
    assert CONV.convert(23, TupleValue("b", struct.pack(">i", 42))) == 42
    assert CONV.convert(25, TupleValue("b", b"txt")) == "txt"


def test_convert_text_kind_with_other_encoding():
    conv = Converter(encoding="latin-1")
    assert conv.convert(25, TupleValue("t", "é".encode("latin-1"))) == "é"
    assert conv.convert(25, TupleValue("b", "é".encode("latin-1"))) == "é"


def test_register_custom_text_parser():
    conv = Converter()
    conv.register(55555, text=lambda s: s.upper())
    assert conv.convert(55555, TupleValue("t", b"abc")) == "ABC"
    # binary stays unknown: raw bytes
    assert conv.convert(55555, TupleValue("b", b"abc")) == b"abc"


def test_register_custom_binary_parser():
    conv = Converter()
    conv.register(55555, binary=lambda b: len(b))
    assert conv.convert(55555, TupleValue("b", b"abcd")) == 4
    assert conv.convert(55555, TupleValue("t", b"abcd")) == "abcd"


def test_register_both_formats_and_does_not_leak_to_other_converters():
    conv = Converter()
    conv.register(55555, text=int, binary=lambda b: int.from_bytes(b, "big"))
    assert conv.convert(55555, TupleValue("t", b"12")) == 12
    assert conv.convert(55555, TupleValue("b", b"\x01\x00")) == 256
    assert Converter().convert(55555, TupleValue("t", b"12")) == "12"


def test_register_overrides_a_builtin():
    conv = Converter()
    conv.register(23, text=lambda s: int(s) * 2)
    assert conv.convert(23, TupleValue("t", b"21")) == 42
    assert conv.convert(25, TupleValue("b", b"x")) == "x"


def test_register_binary_overrides_text_like_decoding():
    conv = Converter()
    conv.register(25, binary=lambda b: bytes(b))
    assert conv.convert(25, TupleValue("b", b"x")) == b"x"


def relation(columns):
    return Relation(
        relation_id=16385,
        namespace="public",
        name="t",
        replica_identity="d",
        columns=tuple(Column(n, o, -1, k) for n, o, k in columns),
    )


def test_row_converts_every_column():
    rel = relation(
        [("id", 23, True), ("name", 25, False), ("note", 25, False), ("blob", 25, False)]
    )
    data = TupleData(
        (
            TupleValue("t", b"7"),
            TupleValue("t", b"bob"),
            TupleValue("n"),
            TupleValue("u"),
        )
    )
    row = CONV.row(rel, data)
    assert list(row) == ["id", "name", "note", "blob"]
    assert row["id"] == 7
    assert row["name"] == "bob"
    assert row["note"] is None
    assert row["blob"] is UNCHANGED_TOAST


def test_row_binary_values():
    rel = relation([("id", 23, True), ("ok", 16, False)])
    data = TupleData((TupleValue("b", struct.pack(">i", 9)), TupleValue("b", b"\x01")))
    assert CONV.row(rel, data) == {"id": 9, "ok": True}


def test_row_column_count_mismatch():
    rel = relation([("id", 23, True), ("name", 25, False)])
    with pytest.raises(ValueError, match=r"public\.t"):
        CONV.row(rel, TupleData((TupleValue("t", b"1"),)))
    with pytest.raises(ValueError):
        CONV.row(rel, TupleData((TupleValue("n"), TupleValue("n"), TupleValue("n"))))
