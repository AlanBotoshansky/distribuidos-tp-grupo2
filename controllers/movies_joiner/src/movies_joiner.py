import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.ratings_batch import RatingsBatch
from messages.movie_rating import MovieRating
from messages.movie_ratings_batch import MovieRatingsBatch
from messages.credits_batch import CreditsBatch
from messages.movie_credit import MovieCredit
from messages.movie_credits_batch import MovieCreditsBatch
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from stateful_controller.stateful_controller import StatefulController

class MoviesJoiner(StatefulController):
    def __init__(self, input_queues, output_exchange, cluster_size, id, control_queue):
        self._input_queue_movies = input_queues[0]
        self._input_queue_to_join = input_queues[1]
        self._output_exchange = output_exchange
        self._cluster_size = cluster_size
        self._id = id
        self._control_queue = control_queue
        self._middleware = None
        self._movies = {}
        self._all_movies_received_of_clients = set()
        self._should_reenqueue_eof_of_clients = set()
        
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
        
    def __next_id(self):
        """
        Get the next id in the cluster
        """
        return (int(self._id) % self._cluster_size) + 1
    
    def __store_movies(self, movies_batch):
        client_id = movies_batch.client_id
        self._movies[client_id] = self._movies.get(client_id, {})
        for movie in movies_batch.get_items():
            self._movies[client_id][movie.id] = movie.title
    
    def __handle_movies_batch_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIES_BATCH:
            movies_batch = msg
            self.__store_movies(movies_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            self._all_movies_received_of_clients.add(eof.client_id)
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def __join_batch(self, batch, get_movie_id, create_joined_item, joined_batch_class, received_batch_class, log_action_prefix):
        """
        Generic method to join any type of batch with movie data
        
        Args:
            batch: The original batch with items to join
            get_movie_id: Function to extract the movie ID from an item
            create_joined_item: Function to create a new item with the joined data
            joined_batch_class: Class to instantiate the new batch with joined items
            received_batch_class: Class to instantiate the received batch
            log_action_prefix: Prefix for the log message
        """
        client_id = batch.client_id
        joined_batch = joined_batch_class(client_id, [])
        batch_to_reenqueue = received_batch_class(client_id, [])
        
        for item in batch.get_items():
            movie_id = get_movie_id(item)
            if movie_id in self._movies[client_id]:
                movie_title = self._movies[client_id][movie_id]
                joined_item = create_joined_item(movie_id, movie_title, item)
                joined_batch.add_item(joined_item)
            else:
                if client_id in self._all_movies_received_of_clients:
                    continue
                batch_to_reenqueue.add_item(item)
                
        if len(batch_to_reenqueue.get_items()) > 0:
            self._middleware.reenqueue_message(PacketSerde.serialize(batch_to_reenqueue), queue=self._input_queue_to_join[0])
            self._should_reenqueue_eof_of_clients.add(client_id)
            
        if len(joined_batch.get_items()) > 0:
            self._middleware.send_message(PacketSerde.serialize(joined_batch))
            logging.debug(f"action: {log_action_prefix}_joined | result: success | movie_{log_action_prefix}: {joined_batch}")
            
    def __join_ratings(self, ratings_batch):
        self.__join_batch(
            batch=ratings_batch,
            get_movie_id=lambda rating: rating.movie_id,
            create_joined_item=lambda movie_id, movie_title, rating: MovieRating(movie_id, movie_title, rating.rating),
            joined_batch_class=MovieRatingsBatch,
            received_batch_class=RatingsBatch,
            log_action_prefix="ratings_batch"
        )
            
    def __join_credits(self, credits_batch):
        self.__join_batch(
            batch=credits_batch,
            get_movie_id=lambda credit: credit.movie_id,
            create_joined_item=lambda movie_id, movie_title, credit: MovieCredit(movie_id, movie_title, credit.cast),
            joined_batch_class=MovieCreditsBatch,
            received_batch_class=CreditsBatch,
            log_action_prefix="credits_batch"
        )
        
    def _clean_client_state(self, client_id):
        if client_id in self._all_movies_received_of_clients:
            self._all_movies_received_of_clients.remove(client_id)
        if client_id in self._movies:
            self._movies.pop(client_id)
        if client_id in self._should_reenqueue_eof_of_clients:
            self._should_reenqueue_eof_of_clients.remove(client_id)
        
    def __handle_eof(self, eof):
        client_id = eof.client_id
        if client_id in self._should_reenqueue_eof_of_clients:
            self._middleware.reenqueue_message(PacketSerde.serialize(eof), queue=self._input_queue_to_join[0])
            self._should_reenqueue_eof_of_clients.remove(client_id)
            return
        
        eof.add_seen_id(self._id)
        if len(eof.seen_ids) == self._cluster_size:
            if min(eof.seen_ids) == self._id:
                self._middleware.send_message(PacketSerde.serialize(EOF(eof.client_id)))
                logging.info("action: sent_eof | result: success")
            self._clean_client_state(client_id)
        else:
            exchange = "_".join(self._input_queue_to_join[1].split("_")[:-1] + [str(self.__next_id())])
            self._middleware.send_message(PacketSerde.serialize(eof), exchange=exchange)
    
    def __handle_batch_packet_to_join(self, packet):
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
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        input_queues_and_callback_functions = [
            (self._input_queue_movies[0], self._input_queue_movies[1], self.__handle_movies_batch_packet),
            (self._input_queue_to_join[0], self._input_queue_to_join[1], self.__handle_batch_packet_to_join)
            ]
        input_queues_and_callback_functions.append((self._control_queue[0], self._control_queue[1], self._handle_control_packet))
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()
