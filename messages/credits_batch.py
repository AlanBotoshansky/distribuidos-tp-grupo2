from messages.packet_type import PacketType
from messages.credit import Credit

from messages.serialization import (
    LENGTH_FIELD,
    decode_int, decode_strings_list,
)

class CreditsBatch:
    def __init__(self, credits):
        self.credits = credits

    def __repr__(self):
        return f"CreditsBatch(amount_credits={len(self.credits)})"
        
    def packet_type(self):
        return PacketType.CREDITS_BATCH
    
    def add_credit(self, credit: Credit):
        self.credits.append(credit)
    
    def serialize(self):
        payload = b""
        for credit in self.credits:
            payload += credit.serialize()
        return payload
    
    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        credits = []
        
        while offset < len(payload):
            length_movie_id = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
            offset += LENGTH_FIELD
            movie_id = decode_int(payload[offset:offset+length_movie_id])
            offset += length_movie_id
            
            length_cast = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
            offset += LENGTH_FIELD
            cast = decode_strings_list(payload[offset:offset+length_cast])
            offset += length_cast
            
            credits.append(Credit(movie_id, cast))

        return cls(credits)
    
    @classmethod
    def from_csv_lines(cls, lines: list[str]):
        credits = []
        for line in lines:
            credit = Credit.from_csv_line(line)
            credits.append(credit)
        return cls(credits)
