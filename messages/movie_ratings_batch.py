from messages.packet_type import PacketType
from messages.movie_rating import MovieRating

from messages.serialization import (
    LENGTH_FIELD,
    decode_int, decode_string, decode_float,
)

class MovieRatingsBatch:
    def __init__(self, movie_ratings):
        self.movie_ratings = movie_ratings

    def __repr__(self):
        return f"MovieRatingsBatch(amount_movie_ratings={len(self.movie_ratings)})"
        
    def packet_type(self):
        return PacketType.MOVIE_RATINGS_BATCH
    
    def add_movie_rating(self, movie_rating: MovieRating):
        self.movie_ratings.append(movie_rating)
    
    def serialize(self):
        payload = b""
        for movie_rating in self.movie_ratings:
            payload += movie_rating.serialize()
        return payload
    
    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        movie_ratings = []
        
        while offset < len(payload):
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
            
            movie_ratings.append(MovieRating(id, title, rating))

        return cls(movie_ratings)
