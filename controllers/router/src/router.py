import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType

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
    
    def __route_movie(self, movie):
        destination_id = self.__hash_id(movie.id)
        output_exchange = f"{self._output_exchange_prefix}_{destination_id}"
        self._middleware.send_message(PacketSerde.serialize(movie), exchange=output_exchange)
        logging.debug(f"action: movie_routed | result: success | movie_id: {movie.id} | destination_id: {destination_id}")
        
    def __route_rating(self, rating):
        destination_id = self.__hash_id(rating.movie_id)
        output_exchange = f"{self._output_exchange_prefix}_{destination_id}"
        self._middleware.send_message(PacketSerde.serialize(rating), exchange=output_exchange)
        logging.debug(f"action: rating_routed | result: success | rating_movie_id: {rating.movie_id} | destination_id: {destination_id}")
        
    def __send_eof_to_all_destination_nodes(self):
        for i in range(1, self.destination_nodes_amount + 1):
            output_exchange = f"{self._output_exchange_prefix}_{i}"
            self._middleware.send_message(PacketSerde.serialize(EOF()), exchange=output_exchange)
            logging.info(f"action: sent_eof | result: success | destination_id: {i}")
    
    def __handle_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIE:
            movie = msg
            self.__route_movie(movie)
        elif msg.packet_type() == PacketType.RATING:
            rating = msg
            self.__route_rating(rating)
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

        
        