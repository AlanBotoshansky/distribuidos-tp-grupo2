import uuid

from messages.serialization import (
    LENGTH_FIELD,
    decode_string, decode_strings_list, decode_date, decode_int, decode_float, decode_strings_set
)

class BaseMessage:
    def __init__(self, client_id, message_id=None):
        self.message_id = message_id or str(uuid.uuid4())
        self.client_id = client_id
    
    @classmethod
    def deserialize_field(cls, payload: bytes, offset, decoder):
        length_field = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        field = decoder(payload[offset:offset+length_field])
        offset += length_field
        
        return field, offset
    
    @classmethod
    def deserialize_string(cls, payload: bytes, offset):
        return cls.deserialize_field(payload, offset, decode_string)

    @classmethod
    def deserialize_strings_list(cls, payload: bytes, offset):
        return cls.deserialize_field(payload, offset, decode_strings_list)

    @classmethod
    def deserialize_date(cls, payload: bytes, offset):
        return cls.deserialize_field(payload, offset, decode_date)

    @classmethod
    def deserialize_int(cls, payload: bytes, offset):
        return cls.deserialize_field(payload, offset, decode_int)

    @classmethod
    def deserialize_float(cls, payload: bytes, offset):
        return cls.deserialize_field(payload, offset, decode_float)

    @classmethod
    def deserialize_strings_set(cls, payload: bytes, offset):
        return cls.deserialize_field(payload, offset, decode_strings_set)
