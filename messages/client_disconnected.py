from messages.base_message import BaseMessage
from messages.packet_type import PacketType

from messages.serialization import (
    encode_string,
)

class ClientDisconnected(BaseMessage):
    def __init__(self, client_id, message_id=None):
        super().__init__(client_id, message_id)
        
    def __repr__(self):
        return f"ClientDisconnected(client_id={self.client_id})"
    
    def packet_type(self):
        return PacketType.CLIENT_DISCONNECTED

    def serialize(self):
        payload = b""
        payload += encode_string(self.message_id)
        payload += encode_string(self.client_id)

        return payload

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        message_id, offset = cls.deserialize_string(payload, offset)
        client_id, offset = cls.deserialize_string(payload, offset)

        return cls(client_id, message_id)
