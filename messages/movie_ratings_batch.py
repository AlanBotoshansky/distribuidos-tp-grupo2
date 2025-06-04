from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.movie_rating import MovieRating

from messages.serialization import (
    encode_string,
)

class MovieRatingsBatch(BaseMessage):
    def __init__(self, client_id, movie_ratings, message_id=None):
        super().__init__(client_id, message_id)
        self.movie_ratings = movie_ratings

    def __repr__(self):
        return f"MovieRatingsBatch(amount_movie_ratings={len(self.movie_ratings)})"
        
    def packet_type(self):
        return PacketType.MOVIE_RATINGS_BATCH
    
    def add_item(self, item: MovieRating):
        self.movie_ratings.append(item)
        
    def get_items(self):
        return self.movie_ratings
    
    def serialize(self):
        payload = b""
        payload += encode_string(self.message_id)
        payload += encode_string(self.client_id)
        for movie_rating in self.movie_ratings:
            payload += movie_rating.serialize()
        return payload
    
    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        message_id, offset = cls.deserialize_string(payload, offset)
        client_id, offset = cls.deserialize_string(payload, offset)
        
        movie_ratings = []
        while offset < len(payload):
            id, offset = cls.deserialize_int(payload, offset)
            title, offset = cls.deserialize_string(payload, offset)
            rating, offset = cls.deserialize_float(payload, offset)
            
            movie_ratings.append(MovieRating(id, title, rating))

        return cls(client_id, movie_ratings, message_id)
    
    def to_csv_lines(self):
        lines = []
        for movie_rating in self.movie_ratings:
            line = movie_rating.to_csv_line()
            lines.append(line)
        return lines
