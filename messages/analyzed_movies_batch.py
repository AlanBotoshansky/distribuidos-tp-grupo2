from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.analyzed_movie import Sentiment, AnalyzedMovie

from messages.serialization import (
    encode_string,
)

class AnalyzedMoviesBatch(BaseMessage):
    def __init__(self, client_id, analyzed_movies, message_id=None):
        super().__init__(client_id, message_id)
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
        payload += encode_string(self.message_id)
        payload += encode_string(self.client_id)
        for analyzed_movie in self.analyzed_movies:
            payload += analyzed_movie.serialize()
        return payload
    
    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        message_id, offset = cls.deserialize_string(payload, offset)
        client_id, offset = cls.deserialize_string(payload, offset)
        
        analyzed_movies = []
        while offset < len(payload):
            revenue, offset = cls.deserialize_float(payload, offset)
            budget, offset = cls.deserialize_int(payload, offset)
            sentiment, offset = cls.deserialize_int(payload, offset)
            
            analyzed_movies.append(AnalyzedMovie(revenue, budget, Sentiment(sentiment)))

        return cls(client_id, analyzed_movies, message_id)
