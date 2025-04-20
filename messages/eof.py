from messages.packet_type import PacketType
from messages.serialization import LENGTH_FIELD, encode_packet_type, encode_strings_iterable, decode_strings_set

class EOF:
    def __init__(self, seen_ids=None):
        self.seen_ids = set() if not seen_ids else seen_ids
    
    def __repr__(self):
        return f"EOF(seen_ids={self.seen_ids})"
    
    def add_seen_id(self, id):
        self.seen_ids.add(id)
    
    def serialize(self):
        return encode_packet_type(self.packet_type()) + encode_strings_iterable(self.seen_ids)

    @classmethod
    def deserialize(cls, payload: bytes):
        length_set = int.from_bytes(payload[:LENGTH_FIELD], 'big')
        seen_ids = decode_strings_set(payload[LENGTH_FIELD:LENGTH_FIELD+length_set])
        return cls(seen_ids)
    
    def packet_type(self):
        return PacketType.EOF
    
    def to_csv_line(self):
        return 'EOF'
