from messages.packet_type import PacketType
from messages.serialization import encode_packet_type
from messages.movies_batch import MoviesBatch
from messages.eof import EOF
from messages.investor_country import InvestorCountry
from messages.ratings_batch import RatingsBatch
from messages.movie_rating import MovieRating
from messages.movie_ratings_batch import MovieRatingsBatch
from messages.credits_batch import CreditsBatch
from messages.movie_credits_batch import MovieCreditsBatch
from messages.actor_participation import ActorParticipation
from messages.analyzed_movies_batch import AnalyzedMoviesBatch

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
        elif packet_type == PacketType.RATINGS_BATCH:
            return RatingsBatch.deserialize(payload)
        elif packet_type == PacketType.MOVIE_RATING:
            return MovieRating.deserialize(payload)
        elif packet_type == PacketType.MOVIE_RATINGS_BATCH:
            return MovieRatingsBatch.deserialize(payload)
        elif packet_type == PacketType.CREDITS_BATCH:
            return CreditsBatch.deserialize(payload)
        elif packet_type == PacketType.MOVIE_CREDITS_BATCH:
            return MovieCreditsBatch.deserialize(payload)
        elif packet_type == PacketType.ACTOR_PARTICIPATION:
            return ActorParticipation.deserialize(payload)
        elif packet_type == PacketType.ANALYZED_MOVIES_BATCH:
            return AnalyzedMoviesBatch.deserialize(payload)
        else:
            raise ValueError(f"Unknown packet type: {packet_type}")
    
    @classmethod
    def serialize(self, msg, fields_subset=None):
        if fields_subset and msg.packet_type() == PacketType.MOVIES_BATCH:
            return encode_packet_type(msg.packet_type()) + msg.serialize(fields_subset=fields_subset)
        return encode_packet_type(msg.packet_type()) + msg.serialize()
