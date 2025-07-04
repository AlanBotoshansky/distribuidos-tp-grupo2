import signal
import logging
import uuid
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.movies_batch import MoviesBatch
from messages.ratings_batch import RatingsBatch
from messages.credits_batch import CreditsBatch
from common.monitorable import Monitorable
from common.failure_simulation import fail_with_probability

class Router(Monitorable):
    def __init__(self, input_queues, output_exchange_prefixes_and_dest_nodes_amount, failure_probability, cluster_size, id):
        self._input_queues = input_queues
        self._output_exchange_prefixes_and_dest_nodes_amount = output_exchange_prefixes_and_dest_nodes_amount
        self._failure_probability = failure_probability
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
        self._middleware.stop()
        self.stop_receiving_health_checks()
    
    def __hash_id(self, id, dest_nodes_amount):
        return (id % dest_nodes_amount) + 1
    
    def __generate_deterministic_uuid(self, message_id, destination_id):
        """
        Generate a deterministic UUID based on the message ID and destination ID.
        """
        return str(uuid.uuid5(uuid.UUID(message_id), str(destination_id)))
    
    def __route_batch(self, batch, get_hash_id, batch_class, log_action_prefix):
        """
        Generic method to route any type of batch to its destination nodes
        
        Args:
            batch: The original batch of items
            get_hash_id: Function to extract the ID to hash from an item
            batch_class: Class to instantiate for creating new batches
            log_action_prefix: Prefix for the log action message
        """
        for output_exchange_prefix, dest_nodes_amount in self._output_exchange_prefixes_and_dest_nodes_amount:
            batches = {}
            for item in batch.get_items():
                destination_id = self.__hash_id(get_hash_id(item), dest_nodes_amount)
                new_message_id = self.__generate_deterministic_uuid(batch.message_id, destination_id)
                destination_batch = batches.get(destination_id, batch_class(batch.client_id, [], message_id=new_message_id))
                destination_batch.add_item(item)
                batches[destination_id] = destination_batch
        
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
        
    def __send_eof_to_all_destination_nodes(self, received_eof):
        for output_exchange_prefix, dest_nodes_amount in self._output_exchange_prefixes_and_dest_nodes_amount:
            for i in range(1, dest_nodes_amount + 1):
                output_exchange = f"{output_exchange_prefix}_{i}"
                new_message_id = self.__generate_deterministic_uuid(received_eof.message_id, i)
                self._middleware.send_message(PacketSerde.serialize(EOF(received_eof.client_id, message_id=new_message_id)), exchange=output_exchange)
                logging.info(f"action: sent_eof | result: success | destination_id: {i}")
        
    def __send_message_to_all_destination_nodes(self, msg):
        for output_exchange_prefix, dest_nodes_amount in self._output_exchange_prefixes_and_dest_nodes_amount:
            for i in range(1, dest_nodes_amount + 1):
                output_exchange = f"{output_exchange_prefix}_{i}"
                self._middleware.send_message(PacketSerde.serialize(msg), exchange=output_exchange)
                logging.info(f"action: sent_msg | result: success | destination_id: {i}")
    
    def __handle_packet(self, packet):
        fail_with_probability(self._failure_probability, "before sending message")
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
                self.__send_eof_to_all_destination_nodes(eof)
            else:
                self._middleware.reenqueue_message(PacketSerde.serialize(eof))
        elif msg.packet_type() == PacketType.CLIENT_DISCONNECTED:
            client_disconnected = msg
            logging.debug(f"action: client_disconnected | result: success | client_id: {client_disconnected.client_id}")
            self.__send_message_to_all_destination_nodes(client_disconnected)
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")
        fail_with_probability(self._failure_probability, "after sending message")

    def run(self):
        self.start_receiving_health_checks()
        input_queues_and_callback_functions = [(input_queue[0], input_queue[1], self.__handle_packet) for input_queue in self._input_queues]
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions)
        self._middleware.handle_messages()
