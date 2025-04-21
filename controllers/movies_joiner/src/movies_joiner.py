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

class MoviesJoiner:
    def __init__(self, input_queues, output_exchange, cluster_size, id):
        self._input_queue_movies = input_queues[0]
        self._input_queue_to_join = input_queues[1]
        self._output_exchange = output_exchange
        self._cluster_size = cluster_size
        self._id = id
        self._middleware = None
        self._movies = {}
        
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
        
    def __next_id(self):
        """
        Get the next id in the cluster
        """
        return (int(self._id) % self._cluster_size) + 1
    
    def __handle_movies_batch_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIES_BATCH:
            movies_batch = msg
            for movie in movies_batch.get_items():
                self._movies[movie.id] = movie.title
        elif msg.packet_type() == PacketType.EOF:
            self._middleware.stop_handling_messages()
            self._middleware.close_connection()
            self._middleware = Middleware(callback_function=self.__handle_batch_packet_to_join,
                                          input_queues=[self._input_queue_to_join],
                                          output_exchange=self._output_exchange,
                                         )
            self._middleware.handle_messages()
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def __join_batch(self, batch, get_movie_id, create_joined_item, batch_class, log_action_prefix):
        """
        Generic method to join any type of batch with movie data
        
        Args:
            batch: The original batch with items to join
            get_movie_id: Function to extract the movie ID from an item
            create_joined_item: Function to create a new item with the joined data
            batch_class: Class to instantiate the new batch
            log_action_prefix: Prefix for the log message
        """
        joined_batch = batch_class([])
        
        for item in batch.get_items():
            movie_id = get_movie_id(item)
            if movie_id not in self._movies:
                continue
                
            movie_title = self._movies[movie_id]
            joined_item = create_joined_item(movie_id, movie_title, item)
            joined_batch.add_item(joined_item)
            
        if len(joined_batch.get_items()) == 0:
            return
            
        self._middleware.send_message(PacketSerde.serialize(joined_batch))
        logging.debug(f"action: {log_action_prefix}_joined | result: success | movie_{log_action_prefix}: {joined_batch}")
    
    def __join_ratings(self, ratings_batch):
        self.__join_batch(
            batch=ratings_batch,
            get_movie_id=lambda rating: rating.movie_id,
            create_joined_item=lambda movie_id, movie_title, rating: MovieRating(movie_id, movie_title, rating.rating),
            batch_class=MovieRatingsBatch,
            log_action_prefix="ratings_batch"
        )
            
    def __join_credits(self, credits_batch):
        self.__join_batch(
            batch=credits_batch,
            get_movie_id=lambda credit: credit.movie_id,
            create_joined_item=lambda movie_id, movie_title, credit: MovieCredit(movie_id, movie_title, credit.cast),
            batch_class=MovieCreditsBatch,
            log_action_prefix="credits_batch"
        )
    
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
            eof.add_seen_id(self._id)
            if len(eof.seen_ids) == self._cluster_size:
                if min(eof.seen_ids) == self._id:
                    self._middleware.send_message(PacketSerde.serialize(EOF()))
                    logging.info("action: sent_eof | result: success")
            else:
                exchange = "_".join(self._input_queue_to_join[1].split("_")[:-1] + [str(self.__next_id())])
                self._middleware.send_message(PacketSerde.serialize(eof), exchange=exchange)
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        self._middleware = Middleware(callback_function=self.__handle_movies_batch_packet,
                                      input_queues=[self._input_queue_movies],
                                     )
        self._middleware.handle_messages()
