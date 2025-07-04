from enum import IntEnum

class PacketType(IntEnum):
    MOVIES_BATCH = 1
    EOF = 2
    INVESTOR_COUNTRY = 3
    RATINGS_BATCH = 4
    MOVIE_RATINGS_BATCH = 5
    CREDITS_BATCH = 6
    MOVIE_CREDITS_BATCH = 7
    ACTOR_PARTICIPATION = 8
    ANALYZED_MOVIES_BATCH = 9
    AVG_RATE_REVENUE_BUDGET = 10
    CLIENT_DISCONNECTED = 11

    def __str__(self):
        return self.name
