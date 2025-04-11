import signal
import logging
from middleware.middleware import Middleware
from messages.movie import Movie

PRODUCTION_COUNTRIES_FIELD = 'production_countries'

class MoviesFilter:
    def __init__(self, filter_field, filter_values, output_fields_subset):
        self.filter_field = filter_field
        self.filter_values = filter_values
        self.output_fields_subset = output_fields_subset
        self._shutdown_requested = False
        self._middleware = None
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            logging.info('action: signal_received | result: success | signal: SIGTERM')
            self._shutdown_requested = True
            self.__cleanup()
            
    def __cleanup(self):
        """
        Cleanup resources during shutdown
        """
        pass
        # Shutdown middleware

    def __filter_movie(self, movie):
        if not hasattr(movie, self.filter_field):
            return False
        
        if self.filter_field == PRODUCTION_COUNTRIES_FIELD:
            for country in self.filter_values:
                if country not in movie.production_countries:
                    return False
            return True
        
        return False
    
    def handle_movie(self, movie):
        if self.__filter_movie(movie):
            self.middleware.send_message(movie.serialize(fields_subset=self.output_fields_subset))

    def run(self):
        self._middleware = Middleware()
        self._middleware.run()

        
        