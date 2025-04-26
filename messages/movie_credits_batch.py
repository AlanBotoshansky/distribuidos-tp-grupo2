from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.movie_credit import MovieCredit

from messages.serialization import (
    LENGTH_FIELD,
    decode_int, decode_string, decode_strings_list,
)

class MovieCreditsBatch(BaseMessage):
    def __init__(self, client_id, movie_credits):
        super().__init__(client_id)
        self.movie_credits = movie_credits

    def __repr__(self):
        return f"MovieCreditsBatch(amount_movie_credits={len(self.movie_credits)})"
        
    def packet_type(self):
        return PacketType.MOVIE_CREDITS_BATCH
    
    def add_item(self, item: MovieCredit):
        self.movie_credits.append(item)
        
    def get_items(self):
        return self.movie_credits
    
    def serialize(self):
        payload = b""
        payload += self.serialize_client_id()
        for movie_credit in self.movie_credits:
            payload += movie_credit.serialize()
        return payload
    
    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        client_id, offset = cls.deserialize_client_id(payload, offset)
        
        movie_credits = []
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
            cast = decode_strings_list(payload[offset:offset+length_rating])
            offset += length_rating
            
            movie_credits.append(MovieCredit(id, title, cast))

        return cls(client_id, movie_credits)
