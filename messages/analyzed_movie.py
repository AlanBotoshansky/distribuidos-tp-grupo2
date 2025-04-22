from enum import IntEnum
from messages.serialization import encode_num

class Sentiment(IntEnum):
    NEGATIVE = 0
    POSITIVE = 1

    @classmethod
    def from_polarity(cls, polarity: float):
        if polarity < -1 or polarity > 1:
            raise ValueError(f"Polarity must be between -1 and 1, got {polarity}")
        if polarity < 0:
            return cls.NEGATIVE
        return cls.POSITIVE
        
    def __repr__(self):
        if self == Sentiment.POSITIVE:
            return "POSITIVE"
        elif self == Sentiment.NEGATIVE:
            return "NEGATIVE"
        else:
            raise ValueError(f"Unexpected sentiment value: {self}")

class AnalyzedMovie:
    def __init__(self, revenue, budget, sentiment):
        self.revenue = revenue
        self.budget = budget
        self.sentiment = sentiment
        
    def __repr__(self):
        return f"AnalyzedMovie(revenue={self.revenue}, budget={self.budget}, sentiment={repr(self.sentiment)})"
    
    def serialize(self):
        payload = b""
        payload += encode_num(self.revenue)
        payload += encode_num(self.budget)
        payload += encode_num(self.sentiment.value)

        return payload
