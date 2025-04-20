from messages.packet_type import PacketType

from messages.serialization import (
    LENGTH_FIELD, 
    encode_string, encode_num,
    decode_string, decode_int,
)

class InvestorCountry:
    def __init__(self, country, investment):
        self.country = country
        self.investment = investment
        
    def __repr__(self):
        return f"InvestorCountry(country={self.country}, investment={self.investment})"
    
    def packet_type(self):
        return PacketType.INVESTOR_COUNTRY

    def serialize(self):
        payload = b""
        payload += encode_string(self.country)
        payload += encode_num(self.investment)

        return payload

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        length_country = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        country = decode_string(payload[offset:offset+length_country])
        offset += length_country
        
        length_investment = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        investment = decode_int(payload[offset:offset+length_investment])
        offset += length_investment

        return cls(country, investment)
    
    def to_csv_line(self):
        return f"{self.country},{self.investment}"