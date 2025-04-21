import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.movies_batch import MoviesBatch
from messages.ratings_batch import RatingsBatch
from messages.credits_batch import CreditsBatch

class Router:
    def __init__(self, destination_nodes_amount, input_queues, output_exchange_prefixes, cluster_size, id):
        self.destination_nodes_amount = destination_nodes_amount
        self._input_queues = input_queues
        self._output_exchange_prefixes = output_exchange_prefixes
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
    
    def __route_batch(self, batch, get_hash_id, batch_class, log_action_prefix):
        """
        Generic method to route any type of batch to its destination nodes
        
        Args:
            batch: The original batch of items
            get_hash_id: Function to extract the ID to hash from an item
            batch_class: Class to instantiate for creating new batches
            log_action_prefix: Prefix for the log action message
        """
        batches = {}
        
        for item in batch.get_items():
            destination_id = self.__hash_id(get_hash_id(item))
            destination_batch = batches.get(destination_id, batch_class([]))
            destination_batch.add_item(item)
            batches[destination_id] = destination_batch
        
        for output_exchange_prefix in self._output_exchange_prefixes:
            for destination_id, dest_batch in batches.items():
                output_exchange = f"{output_exchange_prefix}_{destination_id}"
                self._middleware.send_message(PacketSerde.serialize(dest_batch), exchange=output_exchange)
                logging.debug(f"action: {log_action_prefix}_routed | result: success | {log_action_prefix}: {dest_batch} | destination_id: {destination_id}")
    
    def __route_movies(self, movies_batch):
        self.__route_batch(
            batch=movies_batch,
            get_hash_id=lambda movie: movie.id,
            batch_class=MoviesBatch,
            log_action_prefix="movies_batch"
        )
        
    def __route_ratings(self, ratings_batch):
        self.__route_batch(
            batch=ratings_batch,
            get_hash_id=lambda rating: rating.movie_id,
            batch_class=RatingsBatch,
            log_action_prefix="ratings_batch"
        )
            
    def __route_credits(self, credits_batch):
        self.__route_batch(
            batch=credits_batch,
            get_hash_id=lambda credit: credit.movie_id,
            batch_class=CreditsBatch,
            log_action_prefix="credits_batch"
        )
        
    def __send_eof_to_all_destination_nodes(self):
        for output_exchange_prefix in self._output_exchange_prefixes:
            for i in range(1, self.destination_nodes_amount + 1):
                output_exchange = f"{output_exchange_prefix}_{i}"
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
        elif msg.packet_type() == PacketType.CREDITS_BATCH:
            credits_batch = msg
            self.__route_credits(credits_batch)
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

        
        