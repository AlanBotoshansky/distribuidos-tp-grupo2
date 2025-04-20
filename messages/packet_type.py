from enum import IntEnum

class PacketType(IntEnum):
    MOVIE = 1
    EOF = 2
    INVESTOR_COUNTRY = 3
    RATING = 4
    MOVIE_RATING = 5
    RATINGS_BATCH = 6
    MOVIE_RATINGS_BATCH = 7
