LENGTH_CLIENT_ID = 4

class BaseMessage:
    def __init__(self, client_id):
        self.client_id = client_id
        
    def serialize_client_id(self):
        return self.client_id.to_bytes(LENGTH_CLIENT_ID, byteorder='big')
    
    @classmethod
    def deserialize_client_id(cls, payload: bytes, offset):
        return int.from_bytes(payload[offset:offset + LENGTH_CLIENT_ID], byteorder='big'), offset + LENGTH_CLIENT_ID
