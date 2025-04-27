from messages.base_message import BaseMessage
from messages.packet_type import PacketType

from messages.serialization import (
    encode_string, encode_num,
)

class ActorParticipation(BaseMessage):
    def __init__(self, client_id, actor, participation):
        super().__init__(client_id)
        self.actor = actor
        self.participation = participation
        
    def __repr__(self):
        return f"ActorParticipation(actor={self.actor}, participation={self.participation})"
    
    def packet_type(self):
        return PacketType.ACTOR_PARTICIPATION

    def serialize(self):
        payload = b""
        payload += self.serialize_client_id()
        payload += encode_string(self.actor)
        payload += encode_num(self.participation)

        return payload

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        client_id, offset = cls.deserialize_client_id(payload, offset)
        actor, offset = cls.deserialize_string(payload, offset)
        participation, offset = cls.deserialize_int(payload, offset)

        return cls(client_id, actor, participation)
    
    def to_csv_lines(self):
        return [f"{self.actor},{self.participation}"]