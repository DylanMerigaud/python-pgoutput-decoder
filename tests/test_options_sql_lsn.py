"""Unit tests for pgoutput.options, pgoutput.lsn and pgoutput.sql."""

import struct

import pytest

from pgoutput.core import Decoder, Insert, Relation, TupleValue
from pgoutput.lsn import format_lsn, parse_lsn
from pgoutput.options import plugin_options, quote_ident
from pgoutput.sql import Change, changes_query, decode_rows, read_changes

# ---------------------------------------------------------------- options


def test_quote_ident_always_quotes():
    assert quote_ident("pub") == '"pub"'
    assert quote_ident("My Pub") == '"My Pub"'
    assert quote_ident("") == '""'


def test_quote_ident_doubles_a_double_quote():
    assert quote_ident('a"b') == '"a""b"'
    assert quote_ident('"') == '""""'
    assert quote_ident('""') == '""""""'


def test_defaults():
    assert plugin_options("pub") == {"proto_version": "1", "publication_names": '"pub"'}


def test_publication_name_with_quote_and_comma():
    assert plugin_options('we"ird,name')["publication_names"] == '"we""ird,name"'


def test_publication_list():
    assert plugin_options(["a", "b c"])["publication_names"] == '"a","b c"'
    assert plugin_options(("a",))["publication_names"] == '"a"'
    assert plugin_options(n for n in ["x", "y"])["publication_names"] == '"x","y"'


def test_empty_publication_list_raises():
    with pytest.raises(ValueError):
        plugin_options([])
    with pytest.raises(ValueError):
        plugin_options(iter(()))


@pytest.mark.parametrize("version", [1, 2, 3, 4])
def test_proto_version(version):
    assert plugin_options("p", proto_version=version)["proto_version"] == str(version)


@pytest.mark.parametrize("version", [0, 5, -1])
def test_proto_version_out_of_range(version):
    with pytest.raises(ValueError):
        plugin_options("p", proto_version=version)


def test_binary_and_messages():
    o = plugin_options("p", binary=True, messages=True)
    assert o["binary"] == "true"
    assert o["messages"] == "true"
    o = plugin_options("p", binary=False, messages=False)
    assert "binary" not in o and "messages" not in o


def test_streaming_needs_protocol_2():
    with pytest.raises(ValueError):
        plugin_options("p", proto_version=1, streaming=True)
    assert plugin_options("p", proto_version=2, streaming=True)["streaming"] == "on"
    assert plugin_options("p", proto_version=2, streaming="on")["streaming"] == "on"


def test_streaming_parallel_needs_protocol_4():
    with pytest.raises(ValueError):
        plugin_options("p", proto_version=3, streaming="parallel")
    assert plugin_options("p", proto_version=4, streaming="parallel")["streaming"] == "parallel"


def test_streaming_invalid_value():
    with pytest.raises(ValueError):
        plugin_options("p", proto_version=4, streaming="sometimes")


def test_streaming_false_or_none_omits_the_option():
    assert "streaming" not in plugin_options("p", streaming=False)
    assert "streaming" not in plugin_options("p", streaming=None)


def test_two_phase_needs_protocol_3():
    with pytest.raises(ValueError):
        plugin_options("p", proto_version=2, two_phase=True)
    assert plugin_options("p", proto_version=3, two_phase=True)["two_phase"] == "true"
    assert plugin_options("p", proto_version=4, two_phase=True)["two_phase"] == "true"
    assert "two_phase" not in plugin_options("p", proto_version=3)


def test_origin():
    assert plugin_options("p", origin="any")["origin"] == "any"
    assert plugin_options("p", origin="none")["origin"] == "none"
    with pytest.raises(ValueError):
        plugin_options("p", origin="all")
    assert "origin" not in plugin_options("p")


def test_every_option_together():
    o = plugin_options(
        ["a", "b"],
        proto_version=4,
        binary=True,
        messages=True,
        streaming="parallel",
        two_phase=True,
        origin="none",
    )
    assert o == {
        "proto_version": "4",
        "publication_names": '"a","b"',
        "binary": "true",
        "messages": "true",
        "streaming": "parallel",
        "two_phase": "true",
        "origin": "none",
    }
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in o.items())


# ---------------------------------------------------------------- lsn


@pytest.mark.parametrize(
    "text,value",
    [
        ("0/0", 0),
        ("0/1", 1),
        ("16/B374D848", 0x16B374D848),
        ("1/0", 2**32),
        ("FFFFFFFF/FFFFFFFF", 2**64 - 1),
    ],
)
def test_lsn_round_trip(text, value):
    assert parse_lsn(text) == value
    assert format_lsn(value) == text


def test_lsn_lowercase_input():
    assert parse_lsn("16/b374d848") == 0x16B374D848
    assert parse_lsn("ffffffff/ffffffff") == 2**64 - 1


def test_lsn_surrounding_whitespace():
    assert parse_lsn(" 16/B374D848 ") == 0x16B374D848


def test_lsn_format_is_uppercase_without_padding():
    assert format_lsn(0x0000000A0000000B) == "A/B"
    assert format_lsn(0xABC0000000000DEF) == "ABC00000/DEF"


@pytest.mark.parametrize(
    "text", ["", "abc", "1", "1/2/3", "G/0", "0/G", "/", "1/", "/1", "-1/0", "0/-1"]
)
def test_lsn_malformed(text):
    with pytest.raises(ValueError):
        parse_lsn(text)


@pytest.mark.parametrize("text", ["100000000/0", "0/100000000"])
def test_lsn_out_of_range(text):
    with pytest.raises(ValueError):
        parse_lsn(text)


@pytest.mark.parametrize("value", [-1, 2**64])
def test_format_lsn_out_of_range(value):
    with pytest.raises(ValueError):
        format_lsn(value)


@pytest.mark.parametrize("text", ["0x1/0", "1_0/0", "+1/0"])
def test_lsn_rejects_what_postgres_rejects(text):
    with pytest.raises(ValueError):
        parse_lsn(text)


# ---------------------------------------------------------------- sql


def test_changes_query_peek_format():
    sql, params = changes_query("slot", "pub")
    assert sql == (
        "SELECT lsn::text, xid::text, data FROM pg_logical_slot_peek_binary_changes"
        "(%s, %s::pg_lsn, %s::int, VARIADIC %s::text[])"
    )
    assert params == [
        "slot",
        None,
        None,
        ["proto_version", "1", "publication_names", '"pub"'],
    ]


def test_changes_query_get_consumes():
    sql, _ = changes_query("slot", "pub", peek=False)
    assert "pg_logical_slot_get_binary_changes" in sql
    assert "peek" not in sql


def test_changes_query_numeric_paramstyle():
    sql, params = changes_query("slot", "pub", paramstyle="numeric")
    assert sql == (
        "SELECT lsn::text, xid::text, data FROM pg_logical_slot_peek_binary_changes"
        "($1, $2::pg_lsn, $3::int, VARIADIC $4::text[])"
    )
    assert "%s" not in sql
    assert len(params) == 4


def test_changes_query_upto_lsn_is_text_and_nchanges_int():
    _, params = changes_query("slot", "pub", upto_lsn=0x16B374D848, upto_nchanges=50)
    assert params[1] == "16/B374D848"
    assert params[2] == 50


def test_changes_query_bad_paramstyle():
    with pytest.raises(ValueError):
        changes_query("slot", "pub", paramstyle="qmark")


def test_changes_query_options_flattened_pairwise():
    _, params = changes_query("s", ["a", "b"], proto_version=2, binary=True, streaming=True)
    assert params[3] == [
        "proto_version",
        "2",
        "publication_names",
        '"a","b"',
        "binary",
        "true",
        "streaming",
        "on",
    ]


def test_changes_query_invalid_option_raises():
    with pytest.raises(ValueError):
        changes_query("s", "p", proto_version=1, two_phase=True)
    with pytest.raises(TypeError):
        changes_query("s", "p", nonsense=True)


def cstr(s):
    return s.encode() + b"\x00"


def relation_msg(rel_id=16385, ns="public", name="t", ident=b"d", cols=(("id", 23), ("v", 25))):
    out = b"R" + struct.pack(">I", rel_id) + cstr(ns) + cstr(name) + ident
    out += struct.pack(">H", len(cols))
    for cname, oid in cols:
        flags = struct.pack(">B", 1 if cname == "id" else 0)
        out += flags + cstr(cname) + struct.pack(">Ii", oid, -1)
    return out


def insert_msg(rel_id, values):
    out = b"I" + struct.pack(">I", rel_id) + b"N" + struct.pack(">H", len(values))
    for v in values:
        out += b"t" + struct.pack(">I", len(v)) + v
    return out


def test_decode_rows_text_lsn_and_memoryview():
    rows = [
        ("0/16B374D8", "735", memoryview(relation_msg())),
        ("0/16B374D9", "735", memoryview(insert_msg(16385, [b"42", b"hello"]))),
    ]
    changes = decode_rows(rows)
    assert [c.lsn for c in changes] == [0x16B374D8, 0x16B374D9]
    assert [c.xid for c in changes] == [735, 735]
    rel, ins = changes[0].message, changes[1].message
    assert isinstance(rel, Relation)
    assert (rel.relation_id, rel.namespace, rel.name) == (16385, "public", "t")
    assert [(c.name, c.type_oid, c.is_key) for c in rel.columns] == [
        ("id", 23, True),
        ("v", 25, False),
    ]
    assert isinstance(ins, Insert)
    assert ins.new.values == (TupleValue("t", b"42"), TupleValue("t", b"hello"))
    assert ins.relation == rel


def test_decode_rows_int_lsn_and_bytes():
    rows = [
        (100, 7, relation_msg()),
        (101, 7, bytearray(insert_msg(16385, [b"1", b"x"]))),
    ]
    changes = decode_rows(rows)
    assert changes[0] == Change(100, 7, changes[0].message)
    assert changes[1].lsn == 101
    assert changes[1].xid == 7


def test_decode_rows_shared_decoder_keeps_relations():
    dec = Decoder()
    decode_rows([("0/1", "1", relation_msg())], dec)
    out = decode_rows([("0/2", "1", insert_msg(16385, [b"1", b"x"]))], dec)
    assert out[0].message.relation.name == "t"


def test_decode_rows_empty():
    assert decode_rows([]) == []


def test_decode_rows_fresh_decoder_per_call_needs_relation():
    from pgoutput.core import UnknownRelationError

    with pytest.raises(UnknownRelationError):
        decode_rows([("0/2", "1", insert_msg(16385, [b"1", b"x"]))])


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return self.rows


def test_read_changes_peek():
    cur = FakeCursor(
        [
            ("0/10", "5", relation_msg()),
            ("0/11", "5", insert_msg(16385, [b"3", b"z"])),
        ]
    )
    changes = read_changes(cur, "slot", "pub")
    sql, params = changes_query("slot", "pub")
    assert cur.calls == [(sql, params)]
    assert "peek" in cur.calls[0][0]
    assert [c.lsn for c in changes] == [0x10, 0x11]
    assert changes[1].message.new.values[0] == TupleValue("t", b"3")


def test_read_changes_get_with_limits_and_options():
    cur = FakeCursor([])
    out = read_changes(
        cur, "slot", ["a", "b"], peek=False, upto_lsn=0x20, upto_nchanges=10, binary=True
    )
    assert out == []
    sql, params = cur.calls[0]
    assert "get_binary_changes" in sql
    assert params[0] == "slot"
    assert params[1] == "0/20"
    assert params[2] == 10
    assert params[3] == [
        "proto_version",
        "1",
        "publication_names",
        '"a","b"',
        "binary",
        "true",
    ]


def test_read_changes_uses_given_decoder():
    dec = Decoder()
    cur = FakeCursor([("0/1", "1", relation_msg())])
    read_changes(cur, "s", "p", decoder=dec)
    assert 16385 in dec.relations
