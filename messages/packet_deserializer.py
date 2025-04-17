from messages.packet_type import PacketType
from messages.movie import Movie
from messages.eof import EOF

class PacketDeserializer:
    @classmethod
    def deserialize(cls, packet):
        packet_type = PacketType(packet[0])
        payload = packet[1:]
        if packet_type == PacketType.MOVIE:
            return Movie.deserialize(payload)
        elif packet_type == PacketType.EOF:
            return EOF.deserialize(payload)
        else:
            raise ValueError(f"Unknown packet type: {packet_type}")
