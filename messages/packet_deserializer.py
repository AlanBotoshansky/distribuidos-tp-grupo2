from messages.packet_type import PacketType
from messages.movie import Movie
from messages.eof import EOF
from messages.investor_country import InvestorCountry
from messages.rating import Rating

class PacketDeserializer:
    @classmethod
    def deserialize(cls, packet):
        packet_type = PacketType(packet[0])
        payload = packet[1:]
        if packet_type == PacketType.MOVIE:
            return Movie.deserialize(payload)
        elif packet_type == PacketType.EOF:
            return EOF.deserialize(payload)
        elif packet_type == PacketType.INVESTOR_COUNTRY:
            return InvestorCountry.deserialize(payload)
        elif packet_type == PacketType.RATING:
            return Rating.deserialize(payload)
        else:
            raise ValueError(f"Unknown packet type: {packet_type}")
