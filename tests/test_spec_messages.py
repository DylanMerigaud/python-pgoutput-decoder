"""Decoder behaviour checked against bytes built from the message formats documentation."""

from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone

import pytest

from pgoutput.core import (
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
    Origin,
    Prepare,
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

import spec_encoder as enc

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)
BIG_LSN = 2**63 + 12345
MAX_LSN = 2**64 - 1
BIG_XID = 2**31 + 7
MAX_XID = 2**32 - 1
COLS = [(1, "id", 23, -1), (0, "name", 25, 68)]
ROW = [("t", b"7"), ("t", b"bob")]


def when(micros):
    return PG_EPOCH + timedelta(microseconds=micros)


# Transaction messages.


def test_begin():
    m = decode_message(enc.begin(BIG_LSN, 1_000_000, BIG_XID))
    assert m == Begin(BIG_LSN, 1_000_000, BIG_XID)
    assert m.commit_time == PG_EPOCH + timedelta(seconds=1)


def test_begin_max_values():
    m = decode_message(enc.begin(MAX_LSN, 2**62, MAX_XID))
    assert (m.final_lsn, m.xid) == (MAX_LSN, MAX_XID)


def test_begin_negative_timestamp_is_before_2000():
    m = decode_message(enc.begin(1, -86_400_000_000, 1))
    assert m.commit_time == datetime(1999, 12, 31, tzinfo=timezone.utc)


def test_begin_timestamp_zero_is_epoch():
    assert decode_message(enc.begin(1, 0, 1)).commit_time == PG_EPOCH


def test_commit():
    ts = 700_000_000_123_456
    m = decode_message(enc.commit(0, BIG_LSN, MAX_LSN, ts))
    assert m == Commit(0, BIG_LSN, MAX_LSN, ts)
    assert m.commit_time == when(ts)
    assert m.commit_time.tzinfo is not None


def test_commit_flags_kept():
    assert decode_message(enc.commit(0xFF, 1, 2, 3)).flags == 0xFF


def test_origin():
    m = decode_message(enc.origin(BIG_LSN, "node_é"))
    assert m == Origin(BIG_LSN, "node_é")


def test_origin_empty_name():
    assert decode_message(enc.origin(1, "")).name == ""


# Catalog messages.


def test_relation():
    m = decode_message(enc.relation(MAX_XID, "public", "users", "d", COLS))
    assert m.relation_id == MAX_XID
    assert (m.namespace, m.name, m.replica_identity) == ("public", "users", "d")
    assert m.columns == (Column("id", 23, -1, True), Column("name", 25, 68, False))
    assert m.xid is None
    assert m.qualified_name == "public.users"
    assert m.key_columns == (m.columns[0],)


@pytest.mark.parametrize("ident", ["d", "n", "f", "i"])
def test_relation_replica_identity(ident):
    assert decode_message(enc.relation(1, "s", "t", ident, [])).replica_identity == ident


def test_relation_zero_columns():
    assert decode_message(enc.relation(1, "s", "t", "d", [])).columns == ()


def test_relation_unicode_names():
    m = decode_message(enc.relation(1, "schéma", "tâble", "d", [(0, "côl", 25, -1)]))
    assert (m.namespace, m.name, m.columns[0].name) == ("schéma", "tâble", "côl")


def test_relation_key_flag_is_bit_one():
    m = decode_message(enc.relation(1, "s", "t", "d", [(1, "a", 1, 1), (0, "b", 1, 1)]))
    assert [c.is_key for c in m.columns] == [True, False]


def test_relation_unsigned_type_oid_and_signed_typmod():
    m = decode_message(enc.relation(1, "s", "t", "d", [(0, "a", MAX_XID, -1)]))
    assert (m.columns[0].type_oid, m.columns[0].type_modifier) == (MAX_XID, -1)


def test_type():
    m = decode_message(enc.type_msg(MAX_XID, "public", "mood"))
    assert m == Type(MAX_XID, "public", "mood")


# Row messages.


def test_insert_all_tuple_kinds():
    row = [("n",), ("u",), ("t", b"hello"), ("b", b"\x00\xff\x00"), ("t", b"")]
    m = decode_message(enc.insert(BIG_XID, row))
    assert isinstance(m, Insert)
    assert m.relation_id == BIG_XID
    assert m.xid is None
    assert m.relation is None
    assert m.new.values == (
        TupleValue(TupleKind.NULL),
        TupleValue(TupleKind.UNCHANGED_TOAST),
        TupleValue(TupleKind.TEXT, b"hello"),
        TupleValue(TupleKind.BINARY, b"\x00\xff\x00"),
        TupleValue(TupleKind.TEXT, b""),
    )
    assert len(m.new) == 5


def test_zero_length_text_is_empty_not_null():
    m = decode_message(enc.insert(1, [("t", b"")]))
    v = m.new[0]
    assert v.data == b"" and not v.is_null
    assert v.text() == ""


def test_null_and_unchanged_have_no_data():
    m = decode_message(enc.insert(1, [("n",), ("u",)]))
    assert m.new[0].is_null and m.new[0].data is None and m.new[0].text() is None
    assert m.new[1].is_unchanged_toast and m.new[1].data is None


def test_text_of_binary_value_raises():
    m = decode_message(enc.insert(1, [("b", b"ab")]))
    with pytest.raises(ValueError):
        m.new[0].text()


def test_text_value_decodes_utf8():
    m = decode_message(enc.insert(1, [("t", "café".encode())]))
    assert m.new[0].text() == "café"


def test_insert_empty_tuple():
    assert len(decode_message(enc.insert(1, [])).new) == 0


def test_tuple_iteration_and_index():
    m = decode_message(enc.insert(1, ROW))
    assert [v.data for v in m.new] == [b"7", b"bob"]
    assert m.new[1].data == b"bob"


def test_update_without_old_tuple():
    m = decode_message(enc.update(BIG_XID, ROW))
    assert isinstance(m, Update)
    assert m.relation_id == BIG_XID
    assert m.old is None and m.old_kind is None
    assert len(m.new) == 2


def test_update_with_key_tuple():
    m = decode_message(enc.update(5, ROW, [("t", b"7"), ("n",)], "K"))
    assert m.old_kind == "K"
    assert m.old.values == (TupleValue("t", b"7"), TupleValue("n"))
    assert m.new.values[1].data == b"bob"


def test_update_with_old_tuple():
    m = decode_message(enc.update(5, ROW, [("t", b"6"), ("t", b"al")], "O"))
    assert m.old_kind == "O"
    assert m.old.values == (TupleValue("t", b"6"), TupleValue("t", b"al"))


def test_delete_with_key():
    m = decode_message(enc.delete(BIG_XID, [("t", b"7"), ("n",)], "K"))
    assert isinstance(m, Delete)
    assert (m.relation_id, m.old_kind) == (BIG_XID, "K")
    assert m.old.values == (TupleValue("t", b"7"), TupleValue("n"))


def test_delete_with_old_row():
    m = decode_message(enc.delete(5, ROW, "O"))
    assert m.old_kind == "O" and len(m.old) == 2


@pytest.mark.parametrize(
    "options,cascade,restart",
    [(0, False, False), (1, True, False), (2, False, True), (3, True, True)],
)
def test_truncate_options(options, cascade, restart):
    m = decode_message(enc.truncate([1], options))
    assert (m.options, m.cascade, m.restart_identity) == (options, cascade, restart)


@pytest.mark.parametrize("relids", [[], [MAX_XID], [BIG_XID, 2, 3]])
def test_truncate_relations(relids):
    m = decode_message(enc.truncate(relids, 0))
    assert isinstance(m, Truncate)
    assert m.relation_ids == tuple(relids)
    assert m.xid is None and m.relations is None


# Logical message.


def test_logical_message_transactional():
    m = decode_message(enc.logical_message(1, BIG_LSN, "my.prefix", b"payload"))
    assert m == LogicalMessage(1, BIG_LSN, "my.prefix", b"payload")
    assert m.transactional


def test_logical_message_non_transactional():
    m = decode_message(enc.logical_message(0, MAX_LSN, "p", b"x"))
    assert not m.transactional and m.lsn == MAX_LSN


def test_logical_message_binary_content_with_zero_bytes():
    content = b"\x00\x01\x00\x00\xff\x00"
    m = decode_message(enc.logical_message(1, 1, "p", content))
    assert m.content == content


def test_logical_message_empty_content():
    assert decode_message(enc.logical_message(0, 1, "p", b"")).content == b""


def test_logical_message_negative_length_raises():
    raw = b"M" + enc.i8(0) + enc.i64(1) + enc.cstr("p") + enc.i32(-1)
    with pytest.raises(DecodeError):
        decode_message(raw)


# Streaming.


@pytest.mark.parametrize("flag,expected", [(1, True), (0, False)])
def test_stream_start(flag, expected):
    m = decode_message(enc.stream_start(BIG_XID, flag))
    assert m == StreamStart(BIG_XID, expected)


def test_stream_stop():
    assert decode_message(enc.stream_stop()) == StreamStop()


def test_stream_commit():
    ts = 5_000_000
    m = decode_message(enc.stream_commit(MAX_XID, 0, BIG_LSN, MAX_LSN, ts))
    assert m == StreamCommit(MAX_XID, 0, BIG_LSN, MAX_LSN, ts)
    assert m.commit_time == when(ts)


def test_stream_abort_without_protocol_4_fields():
    m = decode_message(enc.stream_abort(BIG_XID, MAX_XID))
    assert m == StreamAbort(BIG_XID, MAX_XID)
    assert m.abort_lsn is None and m.abort_timestamp is None and m.abort_time is None


def test_stream_abort_with_protocol_4_fields():
    m = decode_message(enc.stream_abort(BIG_XID, 3, BIG_LSN, 2_000_000))
    assert m == StreamAbort(BIG_XID, 3, BIG_LSN, 2_000_000)
    assert m.abort_time == when(2_000_000)


def test_stream_abort_partial_protocol_4_fields_raises():
    full = enc.stream_abort(1, 2, 3, 4)
    for cut in range(10, len(full)):
        with pytest.raises(DecodeError):
            decode_message(full[:cut])


# Two phase commit.


def test_begin_prepare():
    m = decode_message(enc.begin_prepare(BIG_LSN, MAX_LSN, 3_000_000, MAX_XID, "gé"))
    assert m == BeginPrepare(BIG_LSN, MAX_LSN, 3_000_000, MAX_XID, "gé")
    assert m.prepare_time == when(3_000_000)


def test_prepare():
    m = decode_message(enc.prepare(0, BIG_LSN, MAX_LSN, 4_000_000, BIG_XID, "gid"))
    assert m == Prepare(0, BIG_LSN, MAX_LSN, 4_000_000, BIG_XID, "gid")
    assert m.prepare_time == when(4_000_000)


def test_commit_prepared():
    m = decode_message(enc.commit_prepared(0, BIG_LSN, MAX_LSN, 6_000_000, MAX_XID, "gid"))
    assert m == CommitPrepared(0, BIG_LSN, MAX_LSN, 6_000_000, MAX_XID, "gid")
    assert m.commit_time == when(6_000_000)


def test_rollback_prepared():
    m = decode_message(
        enc.rollback_prepared(0, BIG_LSN, MAX_LSN, 7_000_000, 8_000_000, BIG_XID, "gid")
    )
    assert m == RollbackPrepared(0, BIG_LSN, MAX_LSN, 7_000_000, 8_000_000, BIG_XID, "gid")
    assert m.rollback_time == when(8_000_000)


def test_stream_prepare():
    m = decode_message(enc.stream_prepare(0, BIG_LSN, MAX_LSN, 9_000_000, MAX_XID, "gid"))
    assert m == StreamPrepare(0, BIG_LSN, MAX_LSN, 9_000_000, MAX_XID, "gid")
    assert m.prepare_time == when(9_000_000)


def test_two_phase_empty_gid():
    assert decode_message(enc.begin_prepare(1, 2, 3, 4, "")).gid == ""


# Xid slot inside a streamed block.


def test_in_stream_relation():
    m = decode_message(enc.relation(7, "s", "t", "d", COLS, xid=BIG_XID), in_stream=True)
    assert m.xid == BIG_XID and m.relation_id == 7 and len(m.columns) == 2


def test_in_stream_type():
    m = decode_message(enc.type_msg(8, "s", "n", xid=BIG_XID), in_stream=True)
    assert m == Type(8, "s", "n", BIG_XID)


def test_in_stream_insert():
    m = decode_message(enc.insert(7, ROW, xid=BIG_XID), in_stream=True)
    assert m.xid == BIG_XID and m.relation_id == 7 and len(m.new) == 2


def test_in_stream_update():
    m = decode_message(enc.update(7, ROW, ROW, "O", xid=MAX_XID), in_stream=True)
    assert m.xid == MAX_XID and m.old_kind == "O" and m.relation_id == 7


def test_in_stream_update_no_old():
    m = decode_message(enc.update(7, ROW, xid=3), in_stream=True)
    assert m.xid == 3 and m.old is None


def test_in_stream_delete():
    m = decode_message(enc.delete(7, ROW, "K", xid=BIG_XID), in_stream=True)
    assert m.xid == BIG_XID and m.old_kind == "K" and m.relation_id == 7


def test_in_stream_truncate():
    m = decode_message(enc.truncate([4, 5], 2, xid=BIG_XID), in_stream=True)
    assert m.xid == BIG_XID and m.relation_ids == (4, 5) and m.restart_identity


def test_in_stream_logical_message():
    m = decode_message(enc.logical_message(1, 9, "p", b"c", xid=BIG_XID), in_stream=True)
    assert m == LogicalMessage(1, 9, "p", b"c", BIG_XID)


def test_xid_slot_absent_outside_stream():
    with pytest.raises(DecodeError):
        decode_message(enc.insert(7, ROW, xid=3))


def test_stream_start_and_commit_do_not_get_extra_xid_in_stream():
    assert decode_message(enc.stream_commit(1, 0, 2, 3, 4), in_stream=True).xid == 1
    assert decode_message(enc.stream_abort(1, 2), in_stream=True).subxid == 2


# TupleData.as_dict.


def test_as_dict_maps_names():
    rel = decode_message(enc.relation(7, "s", "t", "d", COLS))
    ins = decode_message(enc.insert(7, ROW))
    d = ins.new.as_dict(rel)
    assert list(d) == ["id", "name"]
    assert d["id"] == TupleValue("t", b"7") and d["name"].data == b"bob"


def test_as_dict_length_mismatch_raises():
    rel = decode_message(enc.relation(7, "s", "t", "d", COLS))
    with pytest.raises(ValueError):
        TupleData((TupleValue("n"),)).as_dict(rel)


# Decoder.


def _stream_block():
    return [
        enc.stream_start(BIG_XID, 1),
        enc.insert(7, ROW, xid=BIG_XID),
        enc.stream_stop(),
    ]


def test_decoder_tracks_in_stream():
    dec = Decoder()
    dec.decode(enc.relation(7, "s", "t", "d", COLS))
    assert not dec.in_stream
    dec.decode(enc.stream_start(BIG_XID, 1))
    assert dec.in_stream and dec.stream_xid == BIG_XID
    ins = dec.decode(enc.insert(7, ROW, xid=BIG_XID))
    assert ins.xid == BIG_XID
    dec.decode(enc.stream_stop())
    assert not dec.in_stream and dec.stream_xid is None
    plain = dec.decode(enc.insert(7, ROW))
    assert plain.xid is None


def test_decoder_stream_cycle_twice():
    dec = Decoder(strict_relations=False)
    for _ in range(2):
        msgs = dec.decode_all(_stream_block())
        assert [type(m) for m in msgs] == [StreamStart, Insert, StreamStop]
    msg = dec.decode(enc.stream_commit(BIG_XID, 0, 1, 2, 3))
    assert isinstance(msg, StreamCommit)


def test_decoder_attaches_relation():
    dec = Decoder()
    rel = dec.decode(enc.relation(7, "s", "t", "d", COLS))
    ins = dec.decode(enc.insert(7, ROW))
    upd = dec.decode(enc.update(7, ROW))
    dele = dec.decode(enc.delete(7, ROW, "K"))
    for m in (ins, upd, dele):
        assert m.relation is rel
    assert ins.new.as_dict(ins.relation)["name"].data == b"bob"


def test_decoder_attaches_relations_to_truncate():
    dec = Decoder()
    r1 = dec.decode(enc.relation(1, "s", "a", "d", COLS))
    r2 = dec.decode(enc.relation(2, "s", "b", "d", COLS))
    t = dec.decode(enc.truncate([2, 1], 0))
    assert t.relations == (r2, r1)
    assert t.relation_ids == (2, 1)


def test_decoder_truncate_unknown_relation_strict():
    dec = Decoder()
    dec.decode(enc.relation(1, "s", "a", "d", COLS))
    with pytest.raises(UnknownRelationError):
        dec.decode(enc.truncate([1, 99], 0))


def test_decoder_truncate_unknown_relation_lenient():
    dec = Decoder(strict_relations=False)
    t = dec.decode(enc.truncate([99], 0))
    assert t.relations is None


def test_decoder_later_relation_replaces_cached():
    dec = Decoder()
    dec.decode(enc.relation(7, "s", "t", "d", COLS))
    new = dec.decode(enc.relation(7, "s", "t2", "f", [(0, "only", 25, -1)]))
    ins = dec.decode(enc.insert(7, [("t", b"x")]))
    assert ins.relation is new
    assert ins.relation.name == "t2"
    assert dec.relations[7] is new


def test_decoder_relation_in_stream_cached():
    dec = Decoder()
    dec.decode(enc.stream_start(5, 1))
    dec.decode(enc.relation(7, "s", "t", "d", COLS, xid=5))
    ins = dec.decode(enc.insert(7, ROW, xid=5))
    assert ins.relation.name == "t"


def test_decoder_unknown_relation_raises_by_default():
    dec = Decoder()
    for raw in (enc.insert(9, ROW), enc.update(9, ROW), enc.delete(9, ROW, "K")):
        with pytest.raises(UnknownRelationError):
            dec.decode(raw)


def test_decoder_unknown_relation_not_strict():
    dec = Decoder(strict_relations=False)
    for raw in (enc.insert(9, ROW), enc.update(9, ROW), enc.delete(9, ROW, "O")):
        assert dec.decode(raw).relation is None


def test_decoder_tracks_types():
    dec = Decoder()
    t = dec.decode(enc.type_msg(55, "s", "mood"))
    assert dec.types[55] == t


def test_decoder_stream_start_inside_block():
    dec = Decoder()
    dec.decode(enc.stream_start(1, 1))
    with pytest.raises(DecodeError):
        dec.decode(enc.stream_start(2, 1))


def test_decoder_stream_stop_outside_block():
    with pytest.raises(DecodeError):
        Decoder().decode(enc.stream_stop())


def test_decoder_reset():
    dec = Decoder()
    dec.decode(enc.relation(7, "s", "t", "d", COLS))
    dec.decode(enc.type_msg(8, "s", "n"))
    dec.decode(enc.stream_start(1, 1))
    dec.reset()
    assert not dec.relations and not dec.types
    assert not dec.in_stream and dec.stream_xid is None
    with pytest.raises(UnknownRelationError):
        dec.decode(enc.insert(7, ROW))
    # xid slot is no longer expected after reset
    assert dec.decode(enc.stream_start(2, 1)) == StreamStart(2, True)


def test_decoder_error_does_not_change_state():
    dec = Decoder()
    with pytest.raises(DecodeError):
        dec.decode(b"")
    assert not dec.in_stream


def test_decoder_accepts_bytearray_and_memoryview():
    dec = Decoder()
    raw = enc.begin(1, 2, 3)
    assert dec.decode(bytearray(raw)) == dec.decode(memoryview(raw)) == Begin(1, 2, 3)


# Errors.


def test_empty_input():
    with pytest.raises(DecodeError):
        decode_message(b"")


@pytest.mark.parametrize("tag", [b"Z", b"X", b"\x00", b"\xff", b"a", b"0", b"d", b"s"])
def test_unknown_tag(tag):
    with pytest.raises(DecodeError):
        decode_message(tag + b"\x00" * 40)


def test_non_utf8_tag_byte():
    with pytest.raises(DecodeError):
        decode_message(b"\x80")


@pytest.mark.parametrize("in_stream", [False, True])
def test_every_message_truncated_at_every_offset(in_stream):
    checked = 0
    for raw, stream in enc.sample_messages():
        if stream != in_stream and raw[:1] not in (b"B", b"C", b"S", b"E"):
            continue
        kwargs = {"in_stream": stream}
        assert decode_message(raw, **kwargs) is not None
        for cut in range(len(raw)):
            # A protocol 3 Stream Abort is a prefix of the protocol 4 form by design.
            if raw[:1] == b"A" and cut == 9:
                continue
            try:
                decode_message(raw[:cut], **kwargs)
            except DecodeError:
                checked += 1
            else:
                pytest.fail(f"truncated {raw[:1]!r} at {cut} of {len(raw)} decoded")
    assert checked > 100


def test_truncation_of_every_message_never_raises_other_errors():
    for raw, stream in enc.sample_messages():
        for cut in range(len(raw) + 1):
            with contextlib.suppress(DecodeError):
                decode_message(raw[:cut], in_stream=stream)


def test_trailing_bytes_raise():
    for raw, stream in enc.sample_messages():
        for extra in (b"\x00", b"x", b"\x00" * 9):
            with pytest.raises(DecodeError):
                decode_message(raw + extra, in_stream=stream)


def test_trailing_bytes_after_protocol_4_stream_abort():
    with pytest.raises(DecodeError):
        decode_message(enc.stream_abort(1, 2, 3, 4) + b"\x00")


def test_bad_tuple_kind():
    raw = b"I" + enc.i32(1) + b"N" + enc.i16(1) + b"x"
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_bad_tuple_kind_in_second_column():
    raw = b"I" + enc.i32(1) + b"N" + enc.i16(2) + b"n" + b"Q"
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_negative_text_length():
    raw = b"I" + enc.i32(1) + b"N" + enc.i16(1) + b"t" + enc.i32(-1)
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_text_length_beyond_buffer():
    raw = b"I" + enc.i32(1) + b"N" + enc.i16(1) + b"t" + enc.i32(50) + b"abc"
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_negative_tuple_column_count():
    raw = b"I" + enc.i32(1) + b"N" + enc.i16(-1)
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_negative_relation_column_count():
    raw = b"R" + enc.i32(1) + enc.cstr("s") + enc.cstr("t") + b"d" + enc.i16(-1)
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_negative_truncate_count():
    with pytest.raises(DecodeError):
        decode_message(b"T" + enc.i32(-1) + enc.i8(0))


def test_huge_truncate_count_with_short_body():
    with pytest.raises(DecodeError):
        decode_message(b"T" + enc.i32(2**31 - 1) + enc.i8(0) + enc.i32(1))


@pytest.mark.parametrize("marker", [b"X", b"K", b"O", b"n", b"\x00"])
def test_bad_insert_marker(marker):
    raw = b"I" + enc.i32(1) + marker + enc.tuple_data(ROW)
    with pytest.raises(DecodeError):
        decode_message(raw)


@pytest.mark.parametrize("marker", [b"X", b"I", b"n", b"\x00"])
def test_bad_update_marker(marker):
    raw = b"U" + enc.i32(1) + marker + enc.tuple_data(ROW)
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_update_bad_marker_after_old_tuple():
    raw = b"U" + enc.i32(1) + b"K" + enc.tuple_data(ROW) + b"X" + enc.tuple_data(ROW)
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_update_second_old_tuple_rejected():
    raw = b"U" + enc.i32(1) + b"K" + enc.tuple_data(ROW) + b"O" + enc.tuple_data(ROW)
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_update_old_tuple_without_new_tuple():
    with pytest.raises(DecodeError):
        decode_message(b"U" + enc.i32(1) + b"K" + enc.tuple_data(ROW))


@pytest.mark.parametrize("marker", [b"X", b"N", b"n", b"\x00"])
def test_bad_delete_marker(marker):
    raw = b"D" + enc.i32(1) + marker + enc.tuple_data(ROW)
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_string_without_zero_terminator():
    cases = [
        b"O" + enc.i64(1) + b"name",
        b"R" + enc.i32(1) + b"public",
        b"R" + enc.i32(1) + enc.cstr("public") + b"table",
        b"Y" + enc.i32(1) + enc.cstr("s") + b"name",
        b"M" + enc.i8(0) + enc.i64(1) + b"prefix",
        b"b" + enc.i64(1) + enc.i64(2) + enc.i64(3) + enc.i32(4) + b"gid",
        b"P" + enc.i8(0) + enc.i64(1) + enc.i64(2) + enc.i64(3) + enc.i32(4) + b"gid",
        b"K" + enc.i8(0) + enc.i64(1) + enc.i64(2) + enc.i64(3) + enc.i32(4) + b"gid",
        b"r" + enc.i8(0) + enc.i64(1) + enc.i64(2) + enc.i64(3) + enc.i64(4) + enc.i32(5) + b"g",
        b"p" + enc.i8(0) + enc.i64(1) + enc.i64(2) + enc.i64(3) + enc.i32(4) + b"gid",
    ]
    for raw in cases:
        with pytest.raises(DecodeError):
            decode_message(raw)


def test_column_name_without_terminator():
    raw = b"R" + enc.i32(1) + enc.cstr("s") + enc.cstr("t") + b"d" + enc.i16(1) + enc.i8(0) + b"col"
    with pytest.raises(DecodeError):
        decode_message(raw)


def test_invalid_utf8_in_names():
    bad = b"\xff\xfe"
    cases = [
        enc.origin(1, "x")[:-2] + bad + b"\x00",
        b"R" + enc.i32(1) + enc.cstr(bad) + enc.cstr("t") + b"d" + enc.i16(0),
        b"R" + enc.i32(1) + enc.cstr("s") + enc.cstr(bad) + b"d" + enc.i16(0),
        b"R"
        + enc.i32(1)
        + enc.cstr("s")
        + enc.cstr("t")
        + b"d"
        + enc.i16(1)
        + enc.i8(0)
        + enc.cstr(bad)
        + enc.i32(1)
        + enc.i32(1),
        b"Y" + enc.i32(1) + enc.cstr(bad) + enc.cstr("n"),
        b"M" + enc.i8(0) + enc.i64(1) + enc.cstr(bad) + enc.i32(0),
        b"b" + enc.i64(1) + enc.i64(2) + enc.i64(3) + enc.i32(4) + enc.cstr(bad),
    ]
    for raw in cases:
        with pytest.raises(DecodeError):
            decode_message(raw)


def test_invalid_utf8_in_row_data_is_not_a_decode_error():
    m = decode_message(enc.insert(1, [("t", b"\xff")]))
    assert m.new[0].data == b"\xff"


def test_encoding_argument():
    raw = enc.origin(1, "café".encode("latin-1").decode("latin-1"))
    raw = b"O" + enc.i64(1) + "café".encode("latin-1") + b"\x00"
    assert decode_message(raw, encoding="latin-1").name == "café"
    with pytest.raises(DecodeError):
        decode_message(raw)
