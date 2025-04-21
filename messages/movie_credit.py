from messages.serialization import (
    encode_num, encode_string, encode_strings_iterable,
)

class MovieCredit:
    def __init__(self, id, title, cast):
        self.id = id
        self.title = title
        self.cast = cast
        
    def __repr__(self):
        return f"MovieCredit(id={self.id}, title={self.title}, cast={self.cast})"

    def serialize(self):
        payload = b""
        payload += encode_num(self.id)
        payload += encode_string(self.title)
        payload += encode_strings_iterable(self.cast)

        return payload
