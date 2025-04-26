from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.movie_credit import MovieCredit

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
            id, offset = cls.deserialize_int(payload, offset)
            title, offset = cls.deserialize_string(payload, offset)
            cast, offset = cls.deserialize_strings_list(payload, offset)
            
            movie_credits.append(MovieCredit(id, title, cast))

        return cls(client_id, movie_credits)
