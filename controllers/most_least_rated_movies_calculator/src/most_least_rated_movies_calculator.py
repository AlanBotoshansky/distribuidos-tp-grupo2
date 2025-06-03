import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.movie_rating import MovieRating
from messages.movie_ratings_batch import MovieRatingsBatch
from common.monitorable import Monitorable
from storage_adapter.storage_adapter import StorageAdapter

MOVIE_RATINGS_FILE_KEY = "movie_ratings"

class MostLeastRatedMoviesCalculator(Monitorable):
    def __init__(self, input_queues, output_exchange, storage_path):
        self._input_queues = input_queues
        self._output_exchange = output_exchange
        self._middleware = None
        self._movie_ratings = {}
        self._storage_adapter = StorageAdapter(storage_path)
        
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
        self._middleware.stop()
        self.stop_receiving_health_checks()
        
    def __load_state_from_storage(self):
        """
        Load persisted state from storage
        """
        movie_ratings = self._storage_adapter.load_data(MOVIE_RATINGS_FILE_KEY)
        if movie_ratings:     
            self._movie_ratings = movie_ratings
    
    def __update_movie_ratings(self, movie_ratings_batch):
        client_id = movie_ratings_batch.client_id
        self._movie_ratings[client_id] = self._movie_ratings.get(client_id, {})
        for movie_rating in movie_ratings_batch.get_items():
            title, sum_ratings, cant_ratings = self._movie_ratings[client_id].get(movie_rating.id, (movie_rating.title, 0, 0))
            sum_ratings += movie_rating.rating
            cant_ratings += 1
            self._movie_ratings[client_id][movie_rating.id] = (title, sum_ratings, cant_ratings)
        self._storage_adapter.update(MOVIE_RATINGS_FILE_KEY, self._movie_ratings[client_id], secondary_file_key=client_id)
    
    def __get_most_least_rated_movies(self, client_id):
        max_id = None
        max_avg_rating = float('-inf')
        min_id = None
        min_avg_rating = float('inf')
        for movie_id, (_, sum_ratings, cant_ratings) in self._movie_ratings[client_id].items():
            avg_rating = sum_ratings / cant_ratings
            if avg_rating > max_avg_rating:
                max_avg_rating = avg_rating
                max_id = movie_id
            if avg_rating < min_avg_rating:
                min_avg_rating = avg_rating
                min_id = movie_id
        most_rated_movie = MovieRating(max_id, self._movie_ratings[client_id][max_id][0], max_avg_rating)
        least_rated_movie = MovieRating(min_id, self._movie_ratings[client_id][min_id][0], min_avg_rating)
        return MovieRatingsBatch(client_id, [most_rated_movie, least_rated_movie])
    
    def __clean_client_state(self, client_id):
        if client_id in self._movie_ratings:
            self._movie_ratings.pop(client_id)
            self._storage_adapter.delete(MOVIE_RATINGS_FILE_KEY, secondary_file_key=client_id)
    
    def __handle_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIE_RATINGS_BATCH:
            movie_ratings_batch = msg
            self.__update_movie_ratings(movie_ratings_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            movie_ratings_batch_result = self.__get_most_least_rated_movies(eof.client_id)
            self._middleware.send_message(PacketSerde.serialize(movie_ratings_batch_result))
            logging.debug(f"action: sent_movie_ratings_batch | result: success | movie_ratings_batch: {movie_ratings_batch_result}")
            self._middleware.send_message(PacketSerde.serialize(EOF(eof.client_id)))
            logging.info("action: sent_eof | result: success")
            self.__clean_client_state(eof.client_id)
        elif msg.packet_type() == PacketType.CLIENT_DISCONNECTED:
            client_disconnected = msg
            logging.debug(f"action: client_disconnected | result: success | client_id: {client_disconnected.client_id}")
            self.__clean_client_state(client_disconnected.client_id)
            self._middleware.send_message(PacketSerde.serialize(client_disconnected))
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        self.start_receiving_health_checks()
        self.__load_state_from_storage()
        input_queues_and_callback_functions = [(input_queue[0], input_queue[1], self.__handle_packet) for input_queue in self._input_queues]
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()
