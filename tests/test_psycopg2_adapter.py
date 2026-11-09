"""Unit tests for pgoutput.psycopg2 with fake cursors (psycopg2 itself is not imported)."""

import struct
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from pgoutput import psycopg2 as adapter
from pgoutput.core import Begin, Decoder
from pgoutput.psycopg2 import Event, ack, events, start


def begin_payload(final_lsn=0x100, ts=0, xid=7):
    return b"B" + struct.pack(">QqI", final_lsn, ts, xid)


def message(payload, data_start=1, wal_end=2, send_time=None):
    return SimpleNamespace(
        payload=payload,
        data_start=data_start,
        wal_end=wal_end,
        send_time=send_time or datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


class FakeCursor:
    def __init__(self, script=()):
        self.script = list(script)
        self.reads = 0
        self.start_calls = []
        self.feedback_calls = []

    def start_replication(self, **kwargs):
        self.start_calls.append(kwargs)

    def read_message(self):
        self.reads += 1
        if self.script:
            return self.script.pop(0)
        return None

    def send_feedback(self, **kwargs):
        self.feedback_calls.append(kwargs)


@pytest.fixture
def selects(monkeypatch):
    calls = []

    def fake_select(r, w, x, timeout=None):
        calls.append((r, w, x, timeout))
        return [], [], []

    monkeypatch.setattr(adapter.select, "select", fake_select)
    return calls


def test_start_passes_everything_through():
    cur = FakeCursor()
    start(cur, "my_slot", "my_pub")
    assert cur.start_calls == [
        {
            "slot_name": "my_slot",
            "decode": False,
            "start_lsn": 0,
            "status_interval": 10.0,
            "options": {"proto_version": "1", "publication_names": '"my_pub"'},
        }
    ]


def test_start_with_options():
    cur = FakeCursor()
    start(
        cur,
        "s",
        ["a", "b"],
        start_lsn=0x16B374D848,
        status_interval=2.5,
        proto_version=2,
        streaming=True,
        binary=True,
    )
    kw = cur.start_calls[0]
    assert kw["decode"] is False
    assert kw["slot_name"] == "s"
    assert kw["start_lsn"] == 0x16B374D848
    assert kw["status_interval"] == 2.5
    assert kw["options"] == {
        "proto_version": "2",
        "publication_names": '"a","b"',
        "binary": "true",
        "streaming": "on",
    }


def test_start_rejects_bad_options_before_calling_the_cursor():
    cur = FakeCursor()
    with pytest.raises(ValueError):
        start(cur, "s", "p", proto_version=1, streaming=True)
    assert cur.start_calls == []


def test_events_yield_decoded_events(selects):
    sent = datetime(2024, 5, 6, 7, 8, 9, tzinfo=timezone.utc)
    cur = FakeCursor(
        [
            message(begin_payload(xid=7), data_start=10, wal_end=20, send_time=sent),
            message(begin_payload(xid=8), data_start=30, wal_end=40),
        ]
    )
    got = list(events(cur, idle_timeout=0.02, poll_interval=0.0))
    assert len(got) == 2
    assert all(isinstance(e, Event) for e in got)
    assert isinstance(got[0].message, Begin)
    assert got[0].message.xid == 7
    assert got[0].message.final_lsn == 0x100
    assert (got[0].data_start, got[0].wal_end, got[0].send_time) == (10, 20, sent)
    assert got[1].message.xid == 8
    assert (got[1].data_start, got[1].wal_end) == (30, 40)


def test_events_stop_on_idle_timeout_and_poll_the_cursor(selects):
    cur = FakeCursor()
    assert list(events(cur, idle_timeout=0.02, poll_interval=0.25)) == []
    assert cur.reads >= 1
    assert selects
    r, w, x, timeout = selects[0]
    assert r == [cur] and w == [] and x == []
    assert timeout == 0.25


def test_events_idle_gaps_between_messages_do_not_end_the_stream(selects):
    cur = FakeCursor([message(begin_payload()), None, None, message(begin_payload(xid=9))])
    got = list(events(cur, idle_timeout=5.0, poll_interval=0.0))
    assert [e.message.xid for e in got] == [7, 9]


def test_events_use_the_given_decoder(selects):
    dec = Decoder()
    cur = FakeCursor([message(begin_payload())])
    list(events(cur, dec, idle_timeout=0.02))
    assert dec.in_stream is False


def test_events_is_lazy(selects):
    cur = FakeCursor([message(begin_payload(xid=1)), message(begin_payload(xid=2))])
    it = events(cur, idle_timeout=0.02)
    assert cur.reads == 0
    first = next(it)
    assert first.message.xid == 1
    assert cur.reads == 1


def test_event_is_frozen():
    e = Event(message=None, data_start=1, wal_end=2, send_time=datetime(2024, 1, 1))  # type: ignore[arg-type]
    with pytest.raises(AttributeError):
        e.data_start = 5  # type: ignore[misc]


def test_ack_sends_feedback_for_write_and_flush():
    cur = FakeCursor()
    ack(cur, 0x16B374D848)
    assert cur.feedback_calls == [{"write_lsn": 0x16B374D848, "flush_lsn": 0x16B374D848}]


def commit_payload(end_lsn=0x200):
    return b"C" + struct.pack(">BQQq", 0, end_lsn - 8, end_lsn, 0)


def stream_start(xid, first=1):
    return b"S" + struct.pack(">IB", xid, first)


def stream_abort(xid, subxid):
    return b"A" + struct.pack(">II", xid, subxid)


class IdleCursor(FakeCursor):
    """A cursor whose script holds None for an idle poll; wal_end as keepalives set it."""

    wal_end = 0x900

    def read_message(self):
        self.reads += 1
        if not self.script:
            raise StopIteration
        return self.script.pop(0)


def drain(cur, **kw):
    out = []
    try:
        for e in events(cur, **kw):
            out.append(e)
    except RuntimeError:  # StopIteration from the fake cursor inside the generator
        pass
    return out


def test_ack_idle_acknowledges_wal_end_between_transactions(selects):
    cur = IdleCursor([None, message(begin_payload()), None, message(commit_payload()), None])
    drain(cur, ack_idle=True)
    # Idle before the transaction and after its commit, never while it is open.
    assert cur.feedback_calls == [
        {"write_lsn": 0x900, "flush_lsn": 0x900},
        {"write_lsn": 0x900, "flush_lsn": 0x900},
    ]


def test_ack_idle_waits_for_an_open_streamed_transaction(selects):
    cur = IdleCursor(
        [
            message(stream_start(5)),
            message(b"E"),
            None,  # between blocks: the streamed transaction is still open
            message(stream_abort(5, 6)),  # a savepoint only
            None,
            message(stream_abort(5, 5)),  # the whole transaction
            None,
        ]
    )
    drain(cur, ack_idle=True)
    assert cur.feedback_calls == [{"write_lsn": 0x900, "flush_lsn": 0x900}]


def test_no_ack_without_ack_idle(selects):
    cur = IdleCursor([None, None])
    drain(cur)
    assert cur.feedback_calls == []
