from messages.base_message import BaseMessage
from messages.packet_type import PacketType

from messages.serialization import (
    encode_string, encode_num,
)

class InvestorCountry(BaseMessage):
    def __init__(self, client_id, country, investment):
        super().__init__(client_id)
        self.country = country
        self.investment = investment
        
    def __repr__(self):
        return f"InvestorCountry(country={self.country}, investment={self.investment})"
    
    def packet_type(self):
        return PacketType.INVESTOR_COUNTRY

    def serialize(self):
        payload = b""
        payload += self.serialize_client_id()
        payload += encode_string(self.country)
        payload += encode_num(self.investment)

        return payload

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        client_id, offset = cls.deserialize_client_id(payload, offset)
        country, offset = cls.deserialize_string(payload, offset)
        investment, offset = cls.deserialize_int(payload, offset)

        return cls(client_id, country, investment)
    
    def to_csv_lines(self):
        return [f"{self.country},{self.investment}"]