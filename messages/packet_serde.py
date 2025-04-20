from messages.packet_type import PacketType
from messages.serialization import encode_packet_type
from messages.movie import Movie
from messages.eof import EOF
from messages.investor_country import InvestorCountry
from messages.rating import Rating
from messages.movie_rating import MovieRating
from messages.ratings_batch import RatingsBatch
from messages.movie_ratings_batch import MovieRatingsBatch
from messages.movies_batch import MoviesBatch

class PacketSerde:
    @classmethod
    def deserialize(cls, packet):
        packet_type = PacketType(packet[0])
        payload = packet[1:]
        if packet_type == PacketType.MOVIES_BATCH:
            return MoviesBatch.deserialize(payload)
        elif packet_type == PacketType.EOF:
            return EOF.deserialize(payload)
        elif packet_type == PacketType.INVESTOR_COUNTRY:
            return InvestorCountry.deserialize(payload)
        elif packet_type == PacketType.RATING:
            return Rating.deserialize(payload)
        elif packet_type == PacketType.MOVIE_RATING:
            return MovieRating.deserialize(payload)
        elif packet_type == PacketType.RATINGS_BATCH:
            return RatingsBatch.deserialize(payload)
        elif packet_type == PacketType.MOVIE_RATINGS_BATCH:
            return MovieRatingsBatch.deserialize(payload)
        else:
            raise ValueError(f"Unknown packet type: {packet_type}")
    
    @classmethod
    def serialize(self, msg, fields_subset=None):
        if fields_subset and msg.packet_type() == PacketType.MOVIES_BATCH:
            return encode_packet_type(msg.packet_type()) + msg.serialize(fields_subset=fields_subset)
        return encode_packet_type(msg.packet_type()) + msg.serialize()
