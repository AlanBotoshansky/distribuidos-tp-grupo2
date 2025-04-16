from messages.packet_type import PacketType
from messages.serialization import encode_packet_type

class EOF:
    def __init__(self):
        pass
    
    def __repr__(self):
        return 'EOF'
    
    def serialize(self):
        return encode_packet_type(PacketType.EOF)
    
    def packet_type(self):
        return PacketType.EOF
    
    def to_csv_line(self):
        return 'EOF'
