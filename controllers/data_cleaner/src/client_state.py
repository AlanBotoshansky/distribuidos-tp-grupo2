from enum import IntEnum

class FileType(IntEnum):
    MOVIES = 0
    RATINGS = 1
    CREDITS = 2
    FINISHED = 3
    
    def next(self):
        return FileType((self.value + 1) % len(FileType))

class ClientState:
    def __init__(self):
        self._current_data_file = FileType.MOVIES
        
    def is_sending_movies(self):
        return self._current_data_file == FileType.MOVIES
    
    def is_sending_ratings(self):
        return self._current_data_file == FileType.RATINGS
    
    def is_sending_credits(self):
        return self._current_data_file == FileType.CREDITS
    
    def has_finished_sending(self):
        return self._current_data_file == FileType.FINISHED
    
    def finished_sending_file(self):
        self._current_data_file = self._current_data_file.next()
