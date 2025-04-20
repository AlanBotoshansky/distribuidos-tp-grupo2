import signal
import logging
from middleware.middleware import Middleware
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType

class QueryResultsHandler:
    def __init__(self, num_query, input_queues, results_queue):
        self._num_query = num_query
        self._results_queue = results_queue
        self._middleware = Middleware(callback_function=self.__handle_result_packet, input_queues=input_queues)
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        if signalnum == signal.SIGTERM:
            logging.info(f"action: signal_received_query_handler_{self._num_query} | result: success | signal: SIGTERM")
            self._middleware.stop_handling_messages()
            self._middleware.close_connection()
            
    def __handle_result_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        result = [self._num_query]
        if msg.packet_type() == PacketType.MOVIES_BATCH:
            query_result = msg.to_csv_lines()
        else:
            query_result = [msg.to_csv_line()]
        result.extend(query_result)
        self._results_queue.put(result)
        
    def run(self):
        self._middleware.handle_messages()
        