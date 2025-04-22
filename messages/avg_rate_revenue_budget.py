from messages.packet_type import PacketType
from messages.analyzed_movie import Sentiment

from messages.serialization import (
    LENGTH_FIELD, 
    encode_num,
    decode_int, decode_float,
)

class AvgRateRevenueBudget:
    def __init__(self, sentiment, avg):
        self.sentiment = sentiment
        self.avg = avg
        
    def __repr__(self):
        return f"AvgRateRevenueBudget(sentiment={self.sentiment}, avg_rate_revenue_budget={self.avg})"
    
    def packet_type(self):
        return PacketType.AVG_RATE_REVENUE_BUDGET

    def serialize(self):
        payload = b""
        payload += encode_num(self.sentiment.value)
        payload += encode_num(self.avg)

        return payload

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        length_sentiment = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        sentiment = decode_int(payload[offset:offset+length_sentiment])
        offset += length_sentiment
        
        length_avg = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        avg = decode_float(payload[offset:offset+length_avg])
        offset += length_avg

        return cls(Sentiment(sentiment), avg)
    
    def to_csv_line(self):
        return f"{self.sentiment},{self.avg}"