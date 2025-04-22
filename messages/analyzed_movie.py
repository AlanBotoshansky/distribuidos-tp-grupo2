from enum import IntEnum
from messages.serialization import encode_num

class Sentiment(IntEnum):
    NEGATIVE = 0
    POSITIVE = 1

    @classmethod
    def from_label(cls, label: str):
        normalized = label.upper()
        if normalized == "POSITIVE":
            return cls.POSITIVE
        elif normalized == "NEGATIVE":
            return cls.NEGATIVE
        else:
            raise ValueError(f"Unexpected label: {label}")
        
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
        return f"AnalyzedMovie(revenue={self.revenue}, budget={self.budget}, sentiment={self.sentiment})"
    
    def serialize(self):
        payload = b""
        payload += encode_num(self.revenue)
        payload += encode_num(self.budget)
        payload += encode_num(self.sentiment.value)

        return payload
