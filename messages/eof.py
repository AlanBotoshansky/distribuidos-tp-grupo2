from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.serialization import LENGTH_FIELD, encode_strings_iterable, decode_strings_set

class EOF(BaseMessage):
    def __init__(self, client_id, seen_ids=None):
        super().__init__(client_id)
        self.seen_ids = set() if not seen_ids else seen_ids
    
    def __repr__(self):
        return f"EOF(seen_ids={self.seen_ids})"
    
    def add_seen_id(self, id):
        self.seen_ids.add(id)
    
    def serialize(self):
        payload = b""
        payload += self.serialize_client_id()
        payload += encode_strings_iterable(self.seen_ids)
        
        return payload 

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        client_id, offset = cls.deserialize_client_id(payload, offset)
        
        length_set = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        seen_ids = decode_strings_set(payload[offset:offset+length_set])
        offset += length_set
        
        return cls(client_id, seen_ids)
    
    def packet_type(self):
        return PacketType.EOF
    
    def to_csv_line(self):
        return 'EOF'
