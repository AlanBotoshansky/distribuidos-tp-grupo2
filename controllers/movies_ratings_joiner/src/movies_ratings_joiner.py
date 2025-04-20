import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.movie_rating import MovieRating
from messages.packet_deserializer import PacketDeserializer
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
    
    def __handle_movie_packet(self, packet):
        msg = PacketDeserializer.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIE:
            movie = msg
            self._movies[movie.id] = movie.title
        elif msg.packet_type() == PacketType.EOF:
            self._middleware.stop_handling_messages()
            self._middleware.close_connection()
            self._middleware = Middleware(callback_function=self.__handle_rating_packet,
                                          input_queues=[self._input_queue_ratings],
                                          output_exchange=self._output_exchange,
                                         )
            self._middleware.handle_messages()
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")
            
    def __handle_rating_packet(self, packet):
        msg = PacketDeserializer.deserialize(packet)
        if msg.packet_type() == PacketType.RATING:
            rating = msg
            if rating.movie_id not in self._movies:
                return
            movie_title = self._movies[rating.movie_id]
            movie_rating = MovieRating(rating.movie_id, movie_title, rating.rating)
            logging.debug(f"action: rating_joined | result: success | movie_id: {rating.movie_id}") 
            self._middleware.send_message(movie_rating.serialize())
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            eof.add_seen_id(self._id)
            if len(eof.seen_ids) == self._cluster_size:
                if min(eof.seen_ids) == self._id:
                    self._middleware.send_message(EOF().serialize())
                    logging.info("action: sent_eof | result: success")
            else:
                exchange = "_".join(self._input_queue_ratings[1].split("_")[:-1] + [str(self.__next_id())])
                self._middleware.send_message(eof.serialize(), exchange=exchange)
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        self._middleware = Middleware(callback_function=self.__handle_movie_packet,
                                      input_queues=[self._input_queue_movies],
                                     )
        self._middleware.handle_messages()

        
        