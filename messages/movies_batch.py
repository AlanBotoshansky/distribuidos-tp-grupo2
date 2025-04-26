from enum import IntEnum
from messages.base_message import BaseMessage
from messages.packet_type import PacketType
from messages.movie import Movie
from messages.exceptions import InvalidMovieInBatchError

from messages.serialization import (
    encode_string, encode_num, encode_strings_iterable, encode_date,
    decode_string, decode_int, decode_float, decode_strings_list, decode_date
)

LENGTH_MOVIES_AMOUNT = 2
LENGTH_FIELD_TYPE = 1

class FieldType(IntEnum):
    ID = 1
    TITLE = 2
    GENRES = 3
    PRODUCTION_COUNTRIES = 4
    RELEASE_DATE = 5
    BUDGET = 6
    OVERVIEW = 7
    REVENUE = 8

class MoviesBatch(BaseMessage):
    def __init__(self, client_id, movies):
        super().__init__(client_id)
        self.movies = movies

    def __repr__(self):
        return f"MoviesBatch(amount_movies={len(self.movies)})"
        
    def packet_type(self):
        return PacketType.MOVIES_BATCH
    
    def add_item(self, item: Movie):
        self.movies.append(item)
        
    def get_items(self):
        return self.movies
    
    def serialize(self, fields_subset=None):
        field_type_and_encode_map = {
            'id': (FieldType.ID, encode_num),
            'title': (FieldType.TITLE, encode_string),
            'genres': (FieldType.GENRES, encode_strings_iterable),
            'production_countries': (FieldType.PRODUCTION_COUNTRIES, encode_strings_iterable),
            'release_date': (FieldType.RELEASE_DATE, encode_date),
            'budget': (FieldType.BUDGET, encode_num),
            'overview': (FieldType.OVERVIEW, encode_string),
            'revenue': (FieldType.REVENUE, encode_num),
        }

        payload = b""
        payload += self.serialize_client_id()
        
        payload += len(self.movies).to_bytes(LENGTH_MOVIES_AMOUNT, 'big')
        
        if fields_subset is not None:
            fields = fields_subset
            for field in fields:
                if field not in field_type_and_encode_map:
                    continue
                field_type, encode = field_type_and_encode_map[field]
                encoded_field_type = field_type.to_bytes(LENGTH_FIELD_TYPE, 'big')
                
                encoded_fields = b""
                for movie in self.movies:
                    field_value = getattr(movie, field)
                    if field_value is None:
                        raise InvalidMovieInBatchError(f"Movie missing value for field: {field}")
                    encoded_field = encode(field_value)
                    encoded_fields += encoded_field

                payload += encoded_field_type + encoded_fields
        else:
            fields = field_type_and_encode_map.keys()
            for field in fields:
                if field not in field_type_and_encode_map:
                    continue
                field_type, encode = field_type_and_encode_map[field]
                encoded_field_type = field_type.to_bytes(LENGTH_FIELD_TYPE, 'big')
                
                movies_that_have_value = 0
                
                encoded_fields = b""
                for movie in self.movies:
                    field_value = getattr(movie, field)
                    if field_value is None:
                        continue
                    movies_that_have_value += 1
                    encoded_field = encode(field_value)
                    encoded_fields += encoded_field
                    
                if movies_that_have_value == 0:
                    continue
                if movies_that_have_value != len(self.movies):
                    raise InvalidMovieInBatchError(f"Not all movies have value for field: {field}")

                payload += encoded_field_type + encoded_fields

        return payload
    
    @classmethod
    def deserialize(cls, payload: bytes):
        field_name_and_decoder = {
            FieldType.ID: ('id', decode_int),
            FieldType.TITLE: ('title', decode_string),
            FieldType.GENRES: ('genres', decode_strings_list),
            FieldType.PRODUCTION_COUNTRIES: ('production_countries', decode_strings_list),
            FieldType.RELEASE_DATE: ('release_date', decode_date),
            FieldType.BUDGET: ('budget', decode_int),
            FieldType.OVERVIEW: ('overview', decode_string),
            FieldType.REVENUE: ('revenue', decode_float),
        }

        offset = 0
        
        client_id, offset = cls.deserialize_client_id(payload, offset)

        amount_movies = int.from_bytes(payload[offset:offset+LENGTH_MOVIES_AMOUNT], 'big')
        offset += LENGTH_MOVIES_AMOUNT
        
        movies = [Movie() for _ in range(amount_movies)]
        
        while offset < len(payload):
            field_type = FieldType(payload[offset])
            offset += LENGTH_FIELD_TYPE
            
            field, decoder = field_name_and_decoder[field_type]
            
            for movie in movies:
                field_value, offset = cls.deserialize_field(payload, offset, decoder)
                setattr(movie, field, field_value)

        return cls(client_id, movies)
    
    @classmethod
    def from_csv_lines(cls, client_id, lines: list[str]):
        movies = []
        for line in lines:
            movie = Movie.from_csv_line(line)
            movies.append(movie)
        return cls(client_id, movies)
    
    def to_csv_lines(self):
        lines = []
        for movie in self.movies:
            line = movie.to_csv_line()
            lines.append(line)
        return lines
