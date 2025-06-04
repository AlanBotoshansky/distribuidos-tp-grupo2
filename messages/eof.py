from messages.base_message import BaseMessage
from messages.packet_type import PacketType

from messages.serialization import (
    encode_string, encode_strings_iterable,
)

class EOF(BaseMessage):
    def __init__(self, client_id, seen_ids=None, message_id=None):
        super().__init__(client_id, message_id)
        self.seen_ids = set() if not seen_ids else seen_ids
    
    def __repr__(self):
        return f"EOF(seen_ids={self.seen_ids})"
    
    def add_seen_id(self, id):
        self.seen_ids.add(id)
    
    def serialize(self):
        payload = b""
        payload += encode_string(self.message_id)
        payload += encode_string(self.client_id)
        payload += encode_strings_iterable(self.seen_ids)
        
        return payload 

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        message_id, offset = cls.deserialize_string(payload, offset)
        client_id, offset = cls.deserialize_string(payload, offset)
        seen_ids, offset = cls.deserialize_strings_set(payload, offset)
        
        return cls(client_id, seen_ids, message_id)
    
    def packet_type(self):
        return PacketType.EOF
    
    def to_csv_lines(self):
        return ['EOF']
