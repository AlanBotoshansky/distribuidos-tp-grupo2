from io import StringIO
import csv
import ast
from messages.exceptions import InvalidLineError

from messages.serialization import (
    encode_num, encode_strings_iterable,
)

TOTAL_FIELDS_IN_CSV_LINE = 3

class Credit:
    def __init__(self, movie_id, cast):
        self.movie_id = movie_id
        self.cast = cast
        
    def __repr__(self):
        return f"Credit(movie_id={self.movie_id}, cast={self.cast})"

    def serialize(self):
        payload = b""
        payload += encode_num(self.movie_id)
        payload += encode_strings_iterable(self.cast)

        return payload
    
    @classmethod
    def from_csv_line(cls, line: str):
        reader = csv.reader(StringIO(line), quotechar='"', delimiter=',', quoting=csv.QUOTE_MINIMAL)
        fields = next(reader)

        if len(fields) != TOTAL_FIELDS_IN_CSV_LINE:
            raise InvalidLineError(f"Invalid amount of line fields: {len(fields)}")

        cast = cls.__parse_cast(fields[0])
        movie_id = cls.__parse_movie_id(fields[2])

        return cls(movie_id, cast)
    
    @classmethod
    def __parse_movie_id(cls, movie_id_str):
        if not movie_id_str:
            raise InvalidLineError("Invalid movie_id: empty")
        if not movie_id_str.isdecimal():
            raise InvalidLineError(f"Invalid movie_id: {movie_id_str}")
        return int(movie_id_str)
        
    @classmethod
    def __parse_cast(cls, cast_str):
        if not cast_str:
            raise InvalidLineError("Invalid cast: empty")
        try:
            cast_json = ast.literal_eval(cast_str)
            return [c['name'] for c in cast_json]
        except (ValueError, SyntaxError):
            return []
