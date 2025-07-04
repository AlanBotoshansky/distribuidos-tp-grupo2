from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.rating import Rating
from messages.exceptions import InvalidLineError

from messages.serialization import (
    encode_string,
)

class RatingsBatch(BaseMessage):
    def __init__(self, client_id, ratings, message_id=None):
        super().__init__(client_id, message_id)
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
        payload += encode_string(self.message_id)
        payload += encode_string(self.client_id)
        for rating in self.ratings:
            payload += rating.serialize()
        return payload
    
    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        message_id, offset = cls.deserialize_string(payload, offset)
        client_id, offset = cls.deserialize_string(payload, offset)
        
        ratings = []
        while offset < len(payload):
            movie_id, offset = cls.deserialize_int(payload, offset)
            rating, offset = cls.deserialize_float(payload, offset)
            
            ratings.append(Rating(movie_id, rating))

        return cls(client_id, ratings, message_id)
    
    @classmethod
    def from_csv_lines(cls, client_id, lines: list[str]):
        ratings = []
        for line in lines:
            try:
                rating = Rating.from_csv_line(line)
                ratings.append(rating)
            except InvalidLineError:
                continue
        return cls(client_id, ratings)
