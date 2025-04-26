from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.analyzed_movie import Sentiment

from messages.serialization import (
    LENGTH_FIELD, 
    encode_num,
    decode_int, decode_float,
)

class AvgRateRevenueBudget(BaseMessage):
    def __init__(self, client_id, sentiment, avg):
        super().__init__(client_id)
        self.sentiment = sentiment
        self.avg = avg
        
    def __repr__(self):
        return f"AvgRateRevenueBudget(sentiment={self.sentiment}, avg_rate_revenue_budget={self.avg})"
    
    def packet_type(self):
        return PacketType.AVG_RATE_REVENUE_BUDGET

    def serialize(self):
        payload = b""
        payload += self.serialize_client_id()
        payload += encode_num(self.sentiment.value)
        payload += encode_num(self.avg)

        return payload

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        client_id, offset = cls.deserialize_client_id(payload, offset)
        
        length_sentiment = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        sentiment = decode_int(payload[offset:offset+length_sentiment])
        offset += length_sentiment
        
        length_avg = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        avg = decode_float(payload[offset:offset+length_avg])
        offset += length_avg

        return cls(client_id, Sentiment(sentiment), avg)
    
    def to_csv_line(self):
        return f"{self.sentiment},{self.avg}"