from io import StringIO
import csv
import ast
from datetime import datetime
from messages.exceptions import InvalidLineError

TOTAL_FIELDS_IN_CSV_LINE = 24

class Movie:
    def __init__(self, id=None, title='', genres=[], production_countries=[], release_date=None, budget=None, overview='', revenue=None):
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

    @classmethod
    def from_csv_line(cls, line: str):
        reader = csv.reader(StringIO(line), quotechar='"', delimiter=',', quoting=csv.QUOTE_MINIMAL)
        fields = next(reader)

        if len(fields) != TOTAL_FIELDS_IN_CSV_LINE:
            raise InvalidLineError(f"Invalid amount of line fields: {len(fields)}")

        budget = cls.__parse_budget(fields[2])
        genres = cls.__parse_genres(fields[3])
        id = cls.__parse_id(fields[5])
        overview = cls.__parse_overview(fields[9])
        production_countries = cls.__parse_production_countries(fields[13])
        release_date = cls.__parse_release_date(fields[14])
        revenue = cls.__parse_revenue(fields[15])
        title = cls.__parse_title(fields[20])

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

    @classmethod
    def __parse_budget(cls, budget_str):
        if not budget_str:
            raise InvalidLineError("Invalid budget: empty")
        try:
            return int(budget_str)
        except (ValueError, TypeError):
            return None
    
    @classmethod
    def __parse_genres(cls, genres_str):
        if not genres_str:
            raise InvalidLineError("Invalid genres: empty")
        try:
            genres_json = ast.literal_eval(genres_str)
            return [g['name'] for g in genres_json]
        except (ValueError, SyntaxError):
            return []
    
    @classmethod
    def __parse_id(cls, id_str):
        if not id_str:
            raise InvalidLineError("Invalid id: empty")
        if not id_str.isdecimal():
            raise InvalidLineError(f"Invalid id: {id_str}")
        return int(id_str)
    
    @classmethod
    def __parse_overview(cls, overview_str):
        if not overview_str:
            raise InvalidLineError("Invalid overview: empty")
        return overview_str
    
    @classmethod
    def __parse_production_countries(cls, production_countries_str):
        if not production_countries_str:
            raise InvalidLineError("Invalid production_countries: empty")
        try:
            countries_json = ast.literal_eval(production_countries_str)
            return [c['name'] for c in countries_json]
        except (ValueError, SyntaxError):
            return []
    
    @classmethod
    def __parse_release_date(cls, release_date_str):
        if not release_date_str:
            raise InvalidLineError("Invalid release_date: empty")
        try:
            return datetime.strptime(release_date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return None
        
    @classmethod
    def __parse_revenue(cls, revenue_str):
        if not revenue_str:
            raise InvalidLineError("Invalid revenue: empty")
        try:
            return float(revenue_str)
        except (ValueError, TypeError):
            return None
        
    @classmethod
    def __parse_title(cls, title_str):
        if not title_str:
            raise InvalidLineError("Invalid title: empty")
        return title_str
    
    def to_csv_line(self):
        result_line = []
        if self.id is not None:
            result_line.append(str(self.id))
            
        if self.title != '':
            result_line.append(self.title)
            
        if len(self.genres) > 0:
            result_line.append(str(self.genres))
            
        if len(self.production_countries) > 0:
            result_line.append(str(self.production_countries))
        
        if self.release_date is not None:
            result_line.append(self.release_date.strftime('%Y-%m-%d'))
            
        if self.budget is not None:
            result_line.append(str(self.budget))
            
        if self.overview != '':
            result_line.append(self.overview)
            
        if self.revenue is not None:
            result_line.append(str(self.revenue))
            
        return ','.join(result_line)