import signal
import logging
from middleware.middleware import Middleware
from messages.packet_serde import PacketSerde

class QueryResultsHandler:
    def __init__(self, num_query, input_queues, results_queue):
        self._num_query = num_query
        self._results_queue = results_queue
        input_queues_and_callback_functions = [(input_queue[0], input_queue[1], self.__handle_result_packet) for input_queue in input_queues]
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions)
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        if signalnum == signal.SIGTERM:
            logging.info(f"action: signal_received_query_handler_{self._num_query} | result: success | signal: SIGTERM")
            self._middleware.stop()
            
    def __handle_result_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        result = [self._num_query]
        query_result = msg.to_csv_lines()
        result.extend(query_result)
        self._results_queue.put((msg.client_id, result))
        
    def run(self):
        self._middleware.handle_messages()
        