from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.rating import Rating
from messages.exceptions import InvalidLineError

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
            movie_id, offset = cls.deserialize_int(payload, offset)
            rating, offset = cls.deserialize_float(payload, offset)
            
            ratings.append(Rating(movie_id, rating))

        return cls(client_id, ratings)
    
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
