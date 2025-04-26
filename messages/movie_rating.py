from messages.packet_type import PacketType

from messages.serialization import (
    encode_num, encode_string,
)

class MovieRating:
    def __init__(self, id, title, rating):
        self.id = id
        self.title = title
        self.rating = rating
        
    def __repr__(self):
        return f"MovieRating(id={self.id}, title={self.title}, rating={self.rating})"

    def serialize(self):
        payload = b""
        payload += encode_num(self.id)
        payload += encode_string(self.title)
        payload += encode_num(self.rating)

        return payload
    
    def to_csv_line(self):
        return f"{self.id},{self.title},{self.rating}"
