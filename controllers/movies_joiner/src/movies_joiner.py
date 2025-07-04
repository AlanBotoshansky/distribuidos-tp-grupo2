import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.movie_rating import MovieRating
from messages.movie_ratings_batch import MovieRatingsBatch
from messages.movie_credit import MovieCredit
from messages.movie_credits_batch import MovieCreditsBatch
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from common.monitorable import Monitorable
from storage_adapter.storage_adapter import StorageAdapter
from common.failure_simulation import fail_with_probability

MOVIES_FILE_KEY = "movies"
ALL_MOVIES_RECEIVED_FILE_KEY = "all_movies_received"
SHOULD_REENQUEUE_EOF_FILE_KEY = "should_reenqueue_eof"

class MoviesJoiner(Monitorable):
    def __init__(self, input_queues, output_exchange, failure_probability, cluster_size, id, storage_path):
        self._input_queue_movies = input_queues[0]
        self._input_queue_to_join = input_queues[1]
        self._output_exchange = output_exchange
        self._failure_probability = failure_probability
        self._cluster_size = cluster_size
        self._id = id
        self._middleware = None
        self._movies = {}
        self._all_movies_received_of_clients = set()
        self._should_reenqueue_eof_of_clients = set()
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
        
    def __next_id(self):
        """
        Get the next id in the cluster
        """
        return (int(self._id) % self._cluster_size) + 1
    
    def __load_state_from_storage(self):
        """
        Load persisted state from storage
        """
        movies = self._storage_adapter.load_key_values(MOVIES_FILE_KEY)
        if movies:
            self._movies = movies
            logging.debug(f"action: load_state_from_storage | result: success | movies: {self._movies}")
            
        all_movies_received_of_clients = self._storage_adapter.load_data(ALL_MOVIES_RECEIVED_FILE_KEY)
        if all_movies_received_of_clients:
            self._all_movies_received_of_clients = all_movies_received_of_clients
            logging.debug(f"action: load_state_from_storage | result: success | all_movies_received_of_clients: {self._all_movies_received_of_clients}")
            
        should_reenqueue_eof_of_clients = self._storage_adapter.load_data(SHOULD_REENQUEUE_EOF_FILE_KEY)
        if should_reenqueue_eof_of_clients:
            self._should_reenqueue_eof_of_clients = should_reenqueue_eof_of_clients
            logging.debug(f"action: load_state_from_storage | result: success | should_reenqueue_eof_of_clients: {self._should_reenqueue_eof_of_clients}")
    
    def __store_movies(self, movies_batch):
        client_id = movies_batch.client_id
        self._movies[client_id] = self._movies.get(client_id, {})
        for movie in movies_batch.get_items():
            if movie.id not in self._movies[client_id]:
                self._movies[client_id][movie.id] = movie.title
                self._storage_adapter.append(MOVIES_FILE_KEY, movie.id, value=movie.title, secondary_file_key=client_id)
            
    def __handle_client_disconnected(self, client_disconnected):
        logging.debug(f"action: client_disconnected | result: success | client_id: {client_disconnected.client_id}")
        self.__clean_client_state(client_disconnected.client_id)
        self._middleware.send_message(PacketSerde.serialize(client_disconnected))
    
    def __handle_movies_batch_packet(self, packet):
        fail_with_probability(self._failure_probability, "before handling movies batch packet")
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIES_BATCH:
            movies_batch = msg
            self.__store_movies(movies_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            if eof.client_id not in self._all_movies_received_of_clients:
                self._all_movies_received_of_clients.add(eof.client_id)
                self._storage_adapter.update(ALL_MOVIES_RECEIVED_FILE_KEY, self._all_movies_received_of_clients)
        elif msg.packet_type() == PacketType.CLIENT_DISCONNECTED:
            client_disconnected = msg
            self.__handle_client_disconnected(client_disconnected)
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")
        fail_with_probability(self._failure_probability, f"after handling movies batch packet: {msg.packet_type()}")

    def __reenqueue_batch_to_join(self, batch):
        self._middleware.reenqueue_message(PacketSerde.serialize(batch), queue=self._input_queue_to_join[0])
        if batch.client_id not in self._should_reenqueue_eof_of_clients:
            self._should_reenqueue_eof_of_clients.add(batch.client_id)
            self._storage_adapter.update(SHOULD_REENQUEUE_EOF_FILE_KEY, self._should_reenqueue_eof_of_clients)

    def __join_batch(self, batch, get_movie_id, create_joined_item, joined_batch_class, log_action_prefix):
        """
        Generic method to join any type of batch with movie data
        
        Args:
            batch: The original batch with items to join
            get_movie_id: Function to extract the movie ID from an item
            create_joined_item: Function to create a new item with the joined data
            joined_batch_class: Class to instantiate the new batch with joined items
            log_action_prefix: Prefix for the log message
        """
        client_id = batch.client_id
        if client_id not in self._movies:
            self.__reenqueue_batch_to_join(batch)
            return
        
        joined_batch = joined_batch_class(client_id, [], message_id=batch.message_id)
        
        for item in batch.get_items():
            movie_id = get_movie_id(item)
            if movie_id in self._movies[client_id]:
                movie_title = self._movies[client_id][movie_id]
                joined_item = create_joined_item(movie_id, movie_title, item)
                joined_batch.add_item(joined_item)
            else:
                if client_id in self._all_movies_received_of_clients:
                    continue
                self.__reenqueue_batch_to_join(batch)
                return
            
        if len(joined_batch.get_items()) > 0:
            self._middleware.send_message(PacketSerde.serialize(joined_batch))
            logging.debug(f"action: {log_action_prefix}_joined | result: success | movie_{log_action_prefix}: {joined_batch}")
            
    def __join_ratings(self, ratings_batch):
        self.__join_batch(
            batch=ratings_batch,
            get_movie_id=lambda rating: rating.movie_id,
            create_joined_item=lambda movie_id, movie_title, rating: MovieRating(movie_id, movie_title, rating.rating),
            joined_batch_class=MovieRatingsBatch,
            log_action_prefix="ratings_batch"
        )
            
    def __join_credits(self, credits_batch):
        self.__join_batch(
            batch=credits_batch,
            get_movie_id=lambda credit: credit.movie_id,
            create_joined_item=lambda movie_id, movie_title, credit: MovieCredit(movie_id, movie_title, credit.cast),
            joined_batch_class=MovieCreditsBatch,
            log_action_prefix="credits_batch"
        )
        
    def __clean_client_state(self, client_id):
        if client_id in self._all_movies_received_of_clients:
            self._all_movies_received_of_clients.remove(client_id)
            self._storage_adapter.update(ALL_MOVIES_RECEIVED_FILE_KEY, self._all_movies_received_of_clients)
        if client_id in self._movies:
            self._movies.pop(client_id)
            self._storage_adapter.delete(MOVIES_FILE_KEY, secondary_file_key=client_id)
        if client_id in self._should_reenqueue_eof_of_clients:
            self._should_reenqueue_eof_of_clients.remove(client_id)
            self._storage_adapter.update(SHOULD_REENQUEUE_EOF_FILE_KEY, self._should_reenqueue_eof_of_clients)
        
    def __handle_eof(self, eof):
        client_id = eof.client_id
        if client_id in self._should_reenqueue_eof_of_clients:
            self._middleware.reenqueue_message(PacketSerde.serialize(eof), queue=self._input_queue_to_join[0])
            self._should_reenqueue_eof_of_clients.remove(client_id)
            self._storage_adapter.update(SHOULD_REENQUEUE_EOF_FILE_KEY, self._should_reenqueue_eof_of_clients)
            return
        
        eof.add_seen_id(self._id)
        if len(eof.seen_ids) == self._cluster_size:
            if min(eof.seen_ids) == self._id:
                self._middleware.send_message(PacketSerde.serialize(EOF(eof.client_id, message_id=eof.message_id)))
                logging.info("action: sent_eof | result: success")
            self.__clean_client_state(client_id)
        else:
            exchange = "_".join(self._input_queue_to_join[1].split("_")[:-1] + [str(self.__next_id())])
            self._middleware.send_message(PacketSerde.serialize(eof), exchange=exchange)
    
    def __handle_batch_packet_to_join(self, packet):
        fail_with_probability(self._failure_probability, "before handling batch packet to join")
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.RATINGS_BATCH:
            ratings_batch = msg
            self.__join_ratings(ratings_batch)
        elif msg.packet_type() == PacketType.CREDITS_BATCH:
            credits_batch = msg
            self.__join_credits(credits_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            self.__handle_eof(eof)
        elif msg.packet_type() == PacketType.CLIENT_DISCONNECTED:
            client_disconnected = msg
            self.__handle_client_disconnected(client_disconnected)
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")
        fail_with_probability(self._failure_probability, f"after handling batch packet to join: {msg.packet_type()}")

    def run(self):
        self.start_receiving_health_checks()
        self.__load_state_from_storage()
        input_queues_and_callback_functions = [
            (self._input_queue_movies[0], self._input_queue_movies[1], self.__handle_movies_batch_packet),
            (self._input_queue_to_join[0], self._input_queue_to_join[1], self.__handle_batch_packet_to_join)
            ]
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()
