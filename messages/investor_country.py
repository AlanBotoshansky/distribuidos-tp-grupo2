from enum import IntEnum
from messages.packet_type import PacketType

from messages.serialization import (
    LENGTH_FIELD, 
    encode_packet_type, encode_string, encode_num,
    decode_string, decode_int,
)

LENGTH_FIELD_TYPE = 1

class FieldType(IntEnum):
    COUNTRY = 1
    INVESTMENT = 2

class InvestorCountry:
    def __init__(self, country, investment):
        self.country = country
        self.investment = investment
        
    def __repr__(self):
        return f"InvestorCountry(country={self.country}, investment={self.investment})"
    
    def packet_type(self):
        return PacketType.INVESTOR_COUNTRY

    def serialize(self):
        field_type_map = {
            'country': FieldType.COUNTRY,
            'investment': FieldType.INVESTMENT,
        }

        fields = self.__dict__

        payload = b""

        for field, value in fields.items():
            field_type = field_type_map[field]
            encoded_field_type = field_type.to_bytes(LENGTH_FIELD_TYPE, 'big')
            
            if field_type == FieldType.INVESTMENT:
                encoded_field = encode_num(value)
            elif field_type == FieldType.COUNTRY:
                encoded_field = encode_string(value)

            payload += encoded_field_type + encoded_field

        return encode_packet_type(self.packet_type()) + payload

    @classmethod
    def deserialize(cls, payload: bytes):
        field_name_and_decoder = {
            FieldType.COUNTRY: ('country', decode_string),
            FieldType.INVESTMENT: ('investment', decode_int),
        }

        fields = {}

        offset = 0
        while offset < len(payload):
            field_type = FieldType(payload[offset])
            offset += LENGTH_FIELD_TYPE
            length = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
            offset += LENGTH_FIELD
            field_data = payload[offset:offset+length]
            offset += length

            name, decode = field_name_and_decoder[field_type]
            value = decode(field_data)
            fields[name] = value

        return cls(**fields)
    
    def to_csv_line(self):
        return f"{self.country},{self.investment}"