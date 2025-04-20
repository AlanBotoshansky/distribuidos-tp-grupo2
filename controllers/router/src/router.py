import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.movies_batch import MoviesBatch
from messages.ratings_batch import RatingsBatch

class Router:
    def __init__(self, destination_nodes_amount, input_queues, output_exchange_prefix, cluster_size, id):
        self.destination_nodes_amount = destination_nodes_amount
        self._input_queues = input_queues
        self._output_exchange_prefix = output_exchange_prefix
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
    
    def __hash_id(self, id):
        return (id % self.destination_nodes_amount) + 1
    
    def __route_movies(self, movies_batch):
        movies_batches = {}
        for movie in movies_batch.movies:
            destination_id = self.__hash_id(movie.id)
            destination_movies_batch = movies_batches.get(destination_id, MoviesBatch([]))
            destination_movies_batch.add_movie(movie)
            movies_batches[destination_id] = destination_movies_batch
        
        for destination_id, dest_movies_batch in movies_batches.items():
            output_exchange = f"{self._output_exchange_prefix}_{destination_id}"
            self._middleware.send_message(PacketSerde.serialize(dest_movies_batch), exchange=output_exchange)
            logging.debug(f"action: movies_batch_routed | result: success | movies_batch: {dest_movies_batch} | destination_id: {destination_id}")
        
    def __route_ratings(self, ratings_batch):
        ratings_batches = {}
        for rating in ratings_batch.ratings:
            destination_id = self.__hash_id(rating.movie_id)
            destination_ratings_batch = ratings_batches.get(destination_id, RatingsBatch([]))
            destination_ratings_batch.add_rating(rating)
            ratings_batches[destination_id] = destination_ratings_batch

        for destination_id, dest_ratings_batch in ratings_batches.items():
            output_exchange = f"{self._output_exchange_prefix}_{destination_id}"
            self._middleware.send_message(PacketSerde.serialize(dest_ratings_batch), exchange=output_exchange)
            logging.debug(f"action: ratings_batch_routed | result: success | ratings_batch: {dest_ratings_batch} | destination_id: {destination_id}")
        
    def __send_eof_to_all_destination_nodes(self):
        for i in range(1, self.destination_nodes_amount + 1):
            output_exchange = f"{self._output_exchange_prefix}_{i}"
            self._middleware.send_message(PacketSerde.serialize(EOF()), exchange=output_exchange)
            logging.info(f"action: sent_eof | result: success | destination_id: {i}")
    
    def __handle_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIES_BATCH:
            movies_batch = msg
            self.__route_movies(movies_batch)
        elif msg.packet_type() == PacketType.RATINGS_BATCH:
            ratings_batch = msg
            self.__route_ratings(ratings_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            eof.add_seen_id(self._id)
            if len(eof.seen_ids) == self._cluster_size:
                self.__send_eof_to_all_destination_nodes()
            else:
                self._middleware.reenqueue_message(PacketSerde.serialize(eof))
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        self._middleware = Middleware(callback_function=self.__handle_packet,
                                      input_queues=self._input_queues,
                                     )
        self._middleware.handle_messages()

        
        