"""Log sequence numbers: PostgreSQL prints them as two hex halves, ``16/B374D848``."""

from __future__ import annotations

import re

__all__ = ["parse_lsn", "format_lsn"]

# pg_lsn_in: one to eight hex digits on each side of the slash, nothing else.
_LSN = re.compile(r"^([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})$")


def parse_lsn(text: str) -> int:
    """Parse ``"16/B374D848"`` into the 64 bit integer the replication protocol uses."""
    m = _LSN.match(text.strip())
    if m is None:
        raise ValueError(f"not an LSN: {text!r}")
    return (int(m.group(1), 16) << 32) | int(m.group(2), 16)


def format_lsn(lsn: int) -> str:
    """Format a 64 bit LSN the way PostgreSQL prints it (``pg_lsn`` text)."""
    if not 0 <= lsn < 2**64:
        raise ValueError(f"LSN out of range: {lsn}")
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"
