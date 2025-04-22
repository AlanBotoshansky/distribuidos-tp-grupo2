from messages.packet_type import PacketType
from messages.analyzed_movie import Sentiment, AnalyzedMovie

from messages.serialization import (
    LENGTH_FIELD,
    decode_float, decode_int,
)

class AnalyzedMoviesBatch:
    def __init__(self, analyzed_movies):
        self.analyzed_movies = analyzed_movies

    def __repr__(self):
        return f"AnalyzedMoviesBatch(amount_analyzed_movies={len(self.analyzed_movies)})"
        
    def packet_type(self):
        return PacketType.ANALYZED_MOVIES_BATCH
    
    def add_item(self, item: AnalyzedMovie):
        self.analyzed_movies.append(item)
        
    def get_items(self):
        return self.analyzed_movies
    
    def serialize(self):
        payload = b""
        for analyzed_movie in self.analyzed_movies:
            payload += analyzed_movie.serialize()
        return payload
    
    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        analyzed_movies = []
        
        while offset < len(payload):
            length_revenue = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
            offset += LENGTH_FIELD
            revenue = decode_float(payload[offset:offset+length_revenue])
            offset += length_revenue
            
            length_budget = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
            offset += LENGTH_FIELD
            budget = decode_int(payload[offset:offset+length_budget])
            offset += length_budget
            
            length_sentiment = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
            offset += LENGTH_FIELD
            sentiment = decode_int(payload[offset:offset+length_sentiment])
            offset += length_sentiment
            
            analyzed_movies.append(AnalyzedMovie(revenue, budget, Sentiment(sentiment)))

        return cls(analyzed_movies)
