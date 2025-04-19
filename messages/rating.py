from io import StringIO
import csv
from messages.exceptions import InvalidLineError
from messages.packet_type import PacketType

from messages.serialization import (
    LENGTH_FIELD, 
    encode_packet_type, encode_num,
    decode_int, decode_float,
)

TOTAL_FIELDS_IN_CSV_LINE = 4

class Rating:
    def __init__(self, movie_id, rating):
        self.movie_id = movie_id
        self.rating = rating
        
    def __repr__(self):
        return f"Rating(movie_id={self.movie_id}, rating={self.rating})"
    
    def packet_type(self):
        return PacketType.RATING

    def serialize(self):
        payload = b""
        payload += encode_num(self.movie_id)
        payload += encode_num(self.rating)

        return encode_packet_type(self.packet_type()) + payload

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        length_movie_id = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        movie_id = decode_int(payload[offset:offset+length_movie_id])
        offset += length_movie_id
        
        length_rating = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        rating = decode_float(payload[offset:offset+length_rating])
        offset += length_rating

        return cls(movie_id, rating)
    
    @classmethod
    def from_csv_line(cls, line: str):
        reader = csv.reader(StringIO(line), quotechar='"', delimiter=',', quoting=csv.QUOTE_MINIMAL)
        fields = next(reader)

        if len(fields) != TOTAL_FIELDS_IN_CSV_LINE:
            raise InvalidLineError(f"Invalid amount of line fields: {len(fields)}")

        movie_id = cls.__parse_movie_id(fields[1])
        rating = cls.__parse_rating(fields[2])

        return cls(movie_id, rating)
    
    @classmethod
    def __parse_movie_id(cls, movie_id_str):
        if not movie_id_str.isdecimal():
            raise InvalidLineError(f"Invalid movie_id: {movie_id_str}")
        return int(movie_id_str)
        
    @classmethod
    def __parse_rating(cls, rating_str):
        if not rating_str.replace('.', '', 1).isdecimal():
            raise InvalidLineError(f"Invalid rating: {rating_str}")
        return float(rating_str)