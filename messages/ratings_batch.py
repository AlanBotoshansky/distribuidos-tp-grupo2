from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.rating import Rating

from messages.serialization import (
    LENGTH_FIELD,
    decode_int, decode_float,
)

class RatingsBatch(BaseMessage):
    def __init__(self, client_id, ratings):
        super().__init__(client_id)
        self.ratings = ratings

    def __repr__(self):
        return f"RatingsBatch(amount_ratings={len(self.ratings)})"
        
    def packet_type(self):
        return PacketType.RATINGS_BATCH
    
    def add_item(self, item: Rating):
        self.ratings.append(item)
        
    def get_items(self):
        return self.ratings
    
    def serialize(self):
        payload = b""
        payload += self.serialize_client_id()
        for rating in self.ratings:
            payload += rating.serialize()
        return payload
    
    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        client_id, offset = cls.deserialize_client_id(payload, offset)
        
        ratings = []
        while offset < len(payload):
            length_movie_id = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
            offset += LENGTH_FIELD
            movie_id = decode_int(payload[offset:offset+length_movie_id])
            offset += length_movie_id
            
            length_rating = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
            offset += LENGTH_FIELD
            rating = decode_float(payload[offset:offset+length_rating])
            offset += length_rating
            
            ratings.append(Rating(movie_id, rating))

        return cls(client_id, ratings)
    
    @classmethod
    def from_csv_lines(cls, client_id, lines: list[str]):
        ratings = []
        for line in lines:
            rating = Rating.from_csv_line(line)
            ratings.append(rating)
        return cls(client_id, ratings)
