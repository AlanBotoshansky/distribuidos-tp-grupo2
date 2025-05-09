from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.analyzed_movie import Sentiment

from messages.serialization import (
    encode_string, encode_num,
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
        payload += encode_string(self.client_id)
        payload += encode_num(self.sentiment.value)
        payload += encode_num(self.avg)

        return payload

    @classmethod
    def deserialize(cls, payload: bytes):
        offset = 0
        
        client_id, offset = cls.deserialize_string(payload, offset)
        sentiment, offset = cls.deserialize_int(payload, offset)
        avg, offset = cls.deserialize_float(payload, offset)

        return cls(client_id, Sentiment(sentiment), avg)
    
    def to_csv_lines(self):
        return [f"{self.sentiment},{self.avg}"]