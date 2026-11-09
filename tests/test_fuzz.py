"""Seeded random fuzzing: decode_message may return a message or raise DecodeError, nothing else."""

from __future__ import annotations

import contextlib
import random

from pgoutput.core import DecodeError, Decoder, UnknownRelationError, decode_message

import spec_encoder as enc

SEED = 20240611
CASES_PER_KIND = 8000
TAGS = b"BCORYIUDTMSEcAbPKrp"


def _run(data, in_stream):
    with contextlib.suppress(DecodeError):
        decode_message(data, in_stream=in_stream)


def test_random_bytes_with_valid_tag():
    rng = random.Random(SEED)
    for _ in range(CASES_PER_KIND):
        n = rng.randint(0, 80)
        data = bytes([rng.choice(TAGS)]) + bytes(rng.getrandbits(8) for _ in range(n))
        for in_stream in (False, True):
            _run(data, in_stream)


def test_random_bytes_with_structured_bias():
    rng = random.Random(SEED + 1)
    alphabet = b"\x00\x01\x02\x7f\x80\xff NKOnutbIDUT"
    for _ in range(CASES_PER_KIND):
        n = rng.randint(0, 60)
        data = bytes([rng.choice(TAGS)]) + bytes(rng.choice(alphabet) for _ in range(n))
        for in_stream in (False, True):
            _run(data, in_stream)


def test_single_byte_mutations_of_valid_messages():
    rng = random.Random(SEED + 2)
    samples = enc.sample_messages()
    for _ in range(CASES_PER_KIND):
        raw, stream = rng.choice(samples)
        buf = bytearray(raw)
        buf[rng.randrange(len(buf))] = rng.getrandbits(8)
        for in_stream in {stream, not stream}:
            _run(bytes(buf), in_stream)


def test_truncations_and_extensions_of_valid_messages():
    rng = random.Random(SEED + 3)
    samples = enc.sample_messages()
    for _ in range(CASES_PER_KIND):
        raw, stream = rng.choice(samples)
        if rng.random() < 0.5:
            data = raw[: rng.randrange(len(raw) + 1)]
        else:
            data = raw + bytes(rng.getrandbits(8) for _ in range(rng.randint(1, 8)))
        for in_stream in {stream, not stream}:
            _run(data, in_stream)


def test_multi_byte_mutations():
    rng = random.Random(SEED + 4)
    samples = enc.sample_messages()
    for _ in range(CASES_PER_KIND):
        raw, stream = rng.choice(samples)
        buf = bytearray(raw)
        for _ in range(rng.randint(2, 5)):
            buf[rng.randrange(len(buf))] = rng.getrandbits(8)
        _run(bytes(buf), stream)


def test_decoder_survives_garbage_stream():
    rng = random.Random(SEED + 5)
    samples = enc.sample_messages()
    dec = Decoder(strict_relations=False)
    for _ in range(CASES_PER_KIND):
        raw, _stream = rng.choice(samples)
        buf = bytearray(raw)
        if rng.random() < 0.5:
            buf[rng.randrange(len(buf))] = rng.getrandbits(8)
        with contextlib.suppress(DecodeError, UnknownRelationError):
            dec.decode(bytes(buf))
