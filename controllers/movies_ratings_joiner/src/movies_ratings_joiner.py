import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.movie_rating import MovieRating
from messages.movie_ratings_batch import MovieRatingsBatch
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType

class MoviesRatingsJoiner:
    def __init__(self, input_queues, output_exchange, cluster_size, id):
        self._input_queue_movies = input_queues[0]
        self._input_queue_ratings = input_queues[1]
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
            for movie in movies_batch.movies:
                self._movies[movie.id] = movie.title
        elif msg.packet_type() == PacketType.EOF:
            self._middleware.stop_handling_messages()
            self._middleware.close_connection()
            self._middleware = Middleware(callback_function=self.__handle_ratings_batch_packet,
                                          input_queues=[self._input_queue_ratings],
                                          output_exchange=self._output_exchange,
                                         )
            self._middleware.handle_messages()
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")
            
    def __join_ratings(self, ratings_batch):
        movie_ratings_batch = MovieRatingsBatch([])
        for rating in ratings_batch.ratings:
            if rating.movie_id not in self._movies:
                continue
            movie_title = self._movies[rating.movie_id]
            movie_rating = MovieRating(rating.movie_id, movie_title, rating.rating)
            movie_ratings_batch.add_movie_rating(movie_rating)
        self._middleware.send_message(PacketSerde.serialize(movie_ratings_batch))
        logging.debug(f"action: ratings_batch_joined | result: success | movie_ratings_batch: {movie_ratings_batch}") 
    
    def __handle_ratings_batch_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.RATINGS_BATCH:
            ratings_batch = msg
            self.__join_ratings(ratings_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            eof.add_seen_id(self._id)
            if len(eof.seen_ids) == self._cluster_size:
                if min(eof.seen_ids) == self._id:
                    self._middleware.send_message(PacketSerde.serialize(EOF()))
                    logging.info("action: sent_eof | result: success")
            else:
                exchange = "_".join(self._input_queue_ratings[1].split("_")[:-1] + [str(self.__next_id())])
                self._middleware.send_message(PacketSerde.serialize(eof), exchange=exchange)
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        self._middleware = Middleware(callback_function=self.__handle_movies_batch_packet,
                                      input_queues=[self._input_queue_movies],
                                     )
        self._middleware.handle_messages()

        
        