import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_deserializer import PacketDeserializer
from messages.packet_type import PacketType

PRODUCTION_COUNTRIES_FIELD = 'production_countries'
RELEASE_DATE_FIELD = 'release_date'

class MoviesFilter:
    def __init__(self, filter_field, filter_values, output_fields_subset, input_queues, output_exchange, cluster_size, id):
        self._filter_field = filter_field
        self._filter_values = filter_values
        self._output_fields_subset = output_fields_subset
        self._input_queues = input_queues
        self._output_exchange = output_exchange
        self._cluster_size = cluster_size
        self._id = id
        self._middleware = None
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            logging.info('action: signal_received | result: success | signal: SIGTERM')
            self.__cleanup()
            
    def __cleanup(self):
        """
        Cleanup resources during shutdown
        """
        self._middleware.stop_handling_messages()
        self._middleware.close_connection()

    def __filter_movie(self, movie):
        if not hasattr(movie, self._filter_field):
            return False
        
        if self._filter_field == PRODUCTION_COUNTRIES_FIELD:
            if isinstance(self._filter_values, int):
                expected_amount_of_countries = self._filter_values
                if len(movie.production_countries) != expected_amount_of_countries:
                    return False
                return True
            else:
                expected_countries = self._filter_values
                for country in expected_countries:
                    if country not in movie.production_countries:
                        return False
                return True
        
        if self._filter_field == RELEASE_DATE_FIELD:
            if len(self._filter_values) == 2:
                min_year, max_year = self._filter_values
                if movie.release_date.year < min_year or movie.release_date.year > max_year:
                    return False
                return True
            elif len(self._filter_values) == 1:
                min_year = self._filter_values[0]
                if movie.release_date.year < min_year:
                    return False
                return True
        
        return False
    
    def __handle_packet(self, packet):
        msg = PacketDeserializer.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIE:
            movie = msg
            if self.__filter_movie(movie):
                logging.debug(f"action: movie_filtered | result: success | movie_id: {movie.id}") 
                self._middleware.send_message(movie.serialize(fields_subset=self._output_fields_subset))
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            eof.add_seen_id(self._id)
            if len(eof.seen_ids) == self._cluster_size:
                self._middleware.send_message(EOF().serialize())
                logging.info("action: sent_eof | result: success")
            else:
                self._middleware.reenqueue_message(eof.serialize())
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        self._middleware = Middleware(callback_function=self.__handle_packet,
                                      input_queues=self._input_queues,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()

        
        