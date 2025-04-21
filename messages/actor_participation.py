from messages.packet_type import PacketType

from messages.serialization import (
    LENGTH_FIELD, 
    encode_string, encode_num,
    decode_string, decode_int,
)

class ActorParticipation:
    def __init__(self, actor, participation):
        self.actor = actor
        self.participation = participation
        
    def __repr__(self):
        return f"ActorParticipation(actor={self.actor}, participation={self.participation})"
    
    def packet_type(self):
        return PacketType.ACTOR_PARTICIPATION

    def serialize(self):
        payload = b""
        payload += encode_string(self.actor)
        payload += encode_num(self.participation)

        return payload

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        length_actor = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        actor = decode_string(payload[offset:offset+length_actor])
        offset += length_actor
        
        length_participation = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        participation = decode_int(payload[offset:offset+length_participation])
        offset += length_participation

        return cls(actor, participation)
    
    def to_csv_line(self):
        return f"{self.actor},{self.participation}"