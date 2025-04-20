from io import StringIO
import csv
from messages.exceptions import InvalidLineError
from messages.packet_type import PacketType

from messages.serialization import (
    LENGTH_FIELD, 
    encode_packet_type, encode_num, encode_string,
    decode_int, decode_string, decode_float,
)

TOTAL_FIELDS_IN_CSV_LINE = 4

class MovieRating:
    def __init__(self, id, title, rating):
        self.id = id
        self.title = title
        self.rating = rating
        
    def __repr__(self):
        return f"MovieRating(id={self.id}, title={self.title}, rating={self.rating})"
    
    def packet_type(self):
        return PacketType.MOVIE_RATING

    def serialize(self):
        payload = b""
        payload += encode_num(self.id)
        payload += encode_string(self.title)
        payload += encode_num(self.rating)

        return encode_packet_type(self.packet_type()) + payload

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        length_id = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        id = decode_int(payload[offset:offset+length_id])
        offset += length_id
        
        length_title = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        title = decode_string(payload[offset:offset+length_title])
        offset += length_title
        
        length_rating = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        rating = decode_float(payload[offset:offset+length_rating])
        offset += length_rating

        return cls(id, title, rating)