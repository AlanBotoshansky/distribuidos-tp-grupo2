from enum import IntEnum
from io import StringIO
import csv
import ast
from datetime import datetime

from messages.serialization import (
    LENGTH_FIELD, 
    encode_string, encode_num, encode_list, encode_date,
    decode_string, decode_int, decode_float, decode_list, decode_date
)

LENGTH_FIELD_TYPE = 1

TOTAL_FIELDS_IN_CSV_LINE = 24

class InvalidLineError(Exception):
    pass

class FieldType(IntEnum):
    ID = 1
    TITLE = 2
    GENRES = 3
    PRODUCTION_COUNTRIES = 4
    RELEASE_DATE = 5
    BUDGET = 6
    OVERVIEW = 7
    REVENUE = 8

class Movie:
    def __init__(self, id=None, title=None, genres=None, production_countries=None, release_date=None, budget=None, overview=None, revenue=None):
        self.id = id
        self.title = title
        self.genres = genres
        self.production_countries = production_countries
        self.release_date = release_date
        self.budget = budget
        self.overview = overview
        self.revenue = revenue
        
    def __repr__(self):
        return f"Movie(id={self.id}, title={self.title}, genres={self.genres}, production_countries={self.production_countries}, release_date={self.release_date}, budget={self.budget}, overview={self.overview}, revenue={self.revenue})"

    def serialize(self, fields_subset=None):
        field_type_map = {
            'id': FieldType.ID,
            'title': FieldType.TITLE,
            'genres': FieldType.GENRES,
            'production_countries': FieldType.PRODUCTION_COUNTRIES,
            'release_date': FieldType.RELEASE_DATE,
            'budget': FieldType.BUDGET,
            'overview': FieldType.OVERVIEW,
            'revenue': FieldType.REVENUE,
        }

        fields = self.__dict__ if fields_subset is None else {
            k: getattr(self, k) for k in fields_subset if hasattr(self, k)
        }

        payload = b""

        for field, value in fields.items():
            if value is None:
                continue

            field_type = field_type_map[field]
            encoded_field_type = field_type.to_bytes(LENGTH_FIELD_TYPE, 'big')
            
            if field_type in (FieldType.ID, FieldType.BUDGET, FieldType.REVENUE):
                encoded_field = encode_num(value)
            elif field_type in (FieldType.TITLE, FieldType.OVERVIEW):
                encoded_field = encode_string(value)
            elif field_type in (FieldType.GENRES, FieldType.PRODUCTION_COUNTRIES):
                encoded_field = encode_list(value)
            elif field_type == FieldType.RELEASE_DATE:
                encoded_field = encode_date(value)

            payload += encoded_field_type + encoded_field

        return payload

    @classmethod
    def deserialize(cls, payload: bytes):
        field_name_and_decoder = {
            FieldType.ID: ('id', decode_int),
            FieldType.TITLE: ('title', decode_string),
            FieldType.GENRES: ('genres', decode_list),
            FieldType.PRODUCTION_COUNTRIES: ('production_countries', decode_list),
            FieldType.RELEASE_DATE: ('release_date', decode_date),
            FieldType.BUDGET: ('budget', decode_int),
            FieldType.OVERVIEW: ('overview', decode_string),
            FieldType.REVENUE: ('revenue', decode_float),
        }

        fields = {}

        offset = 0
        while offset < len(payload):
            field_type = FieldType(payload[offset])
            offset += LENGTH_FIELD_TYPE
            length = int.from_bytes(payload[offset:offset+LENGTH_FIELD], 'big')
            offset += LENGTH_FIELD
            field_data = payload[offset:offset+length]
            offset += length

            name, decode = field_name_and_decoder[field_type]
            value = decode(field_data)
            fields[name] = value

        return cls(**fields)
    
    @classmethod
    def from_csv_line(cls, line: str):
        reader = csv.reader(StringIO(line), quotechar='"', delimiter=',', quoting=csv.QUOTE_MINIMAL)
        fields = next(reader)

        if len(fields) != TOTAL_FIELDS_IN_CSV_LINE:
            raise InvalidLineError(f"Invalid amount of line fields: {len(fields)}")

        budget = int(fields[2])
        genres_str = fields[3]
        id = int(fields[5])
        overview = fields[9]
        production_countries_str = fields[13]
        release_date_str = fields[14]
        revenue = float(fields[15])
        title = fields[20]

        genres = []
        if genres_str:
            genres_json = ast.literal_eval(genres_str)
            genres = [g['name'] for g in genres_json]

        production_countries = []
        if production_countries_str:
            countries_json = ast.literal_eval(production_countries_str)
            production_countries = [c['name'] for c in countries_json]

        release_date = None
        if release_date_str:
            release_date = datetime.strptime(release_date_str, '%Y-%m-%d').date()

        return cls(
            id=id,
            title=title,
            genres=genres,
            production_countries=production_countries,
            release_date=release_date,
            budget=budget,
            overview=overview,
            revenue=revenue
        )
            
