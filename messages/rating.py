from io import StringIO
import csv
from messages.exceptions import InvalidLineError

from messages.serialization import (
    encode_num,
)

TOTAL_FIELDS_IN_CSV_LINE = 4

class Rating:
    def __init__(self, movie_id, rating):
        self.movie_id = movie_id
        self.rating = rating
        
    def __repr__(self):
        return f"Rating(movie_id={self.movie_id}, rating={self.rating})"

    def serialize(self):
        payload = b""
        payload += encode_num(self.movie_id)
        payload += encode_num(self.rating)

        return payload
    
    @classmethod
    def from_csv_line(cls, line: str):
        reader = csv.reader(StringIO(line), quotechar='"', delimiter=',', quoting=csv.QUOTE_MINIMAL)
        fields = next(reader)

        if len(fields) != TOTAL_FIELDS_IN_CSV_LINE:
            raise InvalidLineError(f"Invalid amount of line fields: {len(fields)}")

        movie_id = cls.__parse_movie_id(fields[1])
        rating = cls.__parse_rating(fields[2])

        return cls(movie_id, rating)
    
    @classmethod
    def __parse_movie_id(cls, movie_id_str):
        if not movie_id_str.isdecimal():
            raise InvalidLineError(f"Invalid movie_id: {movie_id_str}")
        return int(movie_id_str)
        
    @classmethod
    def __parse_rating(cls, rating_str):
        if not rating_str.replace('.', '', 1).isdecimal():
            raise InvalidLineError(f"Invalid rating: {rating_str}")
        return float(rating_str)