"""Decode pgoutput messages from raw bytes, with no database: a Relation, an Insert and an
Update captured from PostgreSQL 17 (table pgo.full_rows, replica identity full)."""

from pgoutput import Converter, Decoder, Insert, Relation, Update

PAYLOADS = [
    "520000415070676f0066756c6c5f726f7773006600020169640000000017ffffffff016e6f74650000000019ffffffff",
    "49000041504e000274000000013174000000036f6e65",
    "55000041504f000274000000013174000000036f6e654e00027400000001317400000003756e6f",
]

decoder = Decoder()
converter = Converter()
for payload in PAYLOADS:
    message = decoder.decode(bytes.fromhex(payload))
    if isinstance(message, Relation):
        print("relation", message.qualified_name, [c.name for c in message.columns])
    elif isinstance(message, Insert):
        print("insert", converter.row(message.relation, message.new))
    elif isinstance(message, Update):
        print(
            "update",
            converter.row(message.relation, message.old),
            "->",
            converter.row(message.relation, message.new),
        )
