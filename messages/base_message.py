from messages.serialization import (
    LENGTH_FIELD,
    decode_string, decode_strings_list, decode_date, decode_int, decode_float, decode_strings_set
)

LENGTH_CLIENT_ID = 4

class BaseMessage:
    def __init__(self, client_id):
        self.client_id = client_id
        
    def serialize_client_id(self):
        return self.client_id.to_bytes(LENGTH_CLIENT_ID, byteorder='big')
    
    @classmethod
    def deserialize_client_id(cls, payload: bytes, offset):
        return int.from_bytes(payload[offset:offset + LENGTH_CLIENT_ID], byteorder='big'), offset + LENGTH_CLIENT_ID
    
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
