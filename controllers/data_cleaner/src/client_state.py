from enum import IntEnum

class FileType(IntEnum):
    MOVIES = 0
    RATINGS = 1
    CREDITS = 2
    
    def next(self):
        return FileType((self.value + 1) % len(FileType))

class ClientState:
    def __init__(self, socket):
        self.socket = socket
        self._current_data_file = FileType.MOVIES
        
    def is_sending_movies(self):
        return self._current_data_file == FileType.MOVIES
    
    def is_sending_ratings(self):
        return self._current_data_file == FileType.RATINGS
    
    def is_sending_credits(self):
        return self._current_data_file == FileType.CREDITS
    
    def finished_sending_file(self):
        self._current_data_file = self._current_data_file.next()
