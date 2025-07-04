from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.credit import Credit
from messages.exceptions import InvalidLineError

from messages.serialization import (
    encode_string,
)

class CreditsBatch(BaseMessage):
    def __init__(self, client_id, credits, message_id=None):
        super().__init__(client_id, message_id)
        self.credits = credits

    def __repr__(self):
        return f"CreditsBatch(amount_credits={len(self.credits)})"
        
    def packet_type(self):
        return PacketType.CREDITS_BATCH
    
    def add_item(self, item: Credit):
        self.credits.append(item)
        
    def get_items(self):
        return self.credits
    
    def serialize(self):
        payload = b""
        payload += encode_string(self.message_id)
        payload += encode_string(self.client_id)
        for credit in self.credits:
            payload += credit.serialize()
        return payload
    
    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        message_id, offset = cls.deserialize_string(payload, offset)
        client_id, offset = cls.deserialize_string(payload, offset)
        
        credits = []
        while offset < len(payload):
            movie_id, offset = cls.deserialize_int(payload, offset)
            cast, offset = cls.deserialize_strings_list(payload, offset)
            
            credits.append(Credit(movie_id, cast))

        return cls(client_id, credits, message_id)
    
    @classmethod
    def from_csv_lines(cls, client_id, lines: list[str]):
        credits = []
        for line in lines:
            try:
                credit = Credit.from_csv_line(line)
                credits.append(credit)
            except InvalidLineError:
                continue
        return cls(client_id, credits)
