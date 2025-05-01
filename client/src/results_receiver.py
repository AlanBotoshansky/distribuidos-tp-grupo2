import logging
import socket
import signal
import communication.communication as communication
from datetime import datetime
import os

QUERY_RESULTS_HEADERS = [
    "id,title,genres",
    "country,investment",
    "id,title,avg_rating",
    "actor,participation",
    "sentiment,avg_rate_revenue_budget",
]

class ResultsReceiver:
    def __init__(self, results_dir, results_socket):
        self._results_dir = results_dir
        self._results_socket = results_socket
        self._result_files = {}
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            self.__shutdown()
            
    def __close_socket(self, socket_to_close, socket_name):
        try:
            logging.info(f'action: close_{socket_name} | result: in_progress')
            socket_to_close.shutdown(socket.SHUT_RDWR)
            socket_to_close.close()
            logging.info(f'action: close_{socket_name} | result: success')
        except OSError:
            logging.error(f"action: close_{socket_name} | result: fail | socket already closed")
            
    def __shutdown(self):
        self.__close_socket(self._results_socket, "results_socket")
        self.__close_all_result_files()

    def __create_results_dir(self):
        os.makedirs(self._results_dir, exist_ok=True)
        
    def __get_result_file(self, num_query):
        if num_query not in self._result_files:
            file_path = os.path.join(self._results_dir, f"query_{num_query}_results.csv")
            self._result_files[num_query] = open(file_path, 'w')
            self._result_files[num_query].write(f"{QUERY_RESULTS_HEADERS[num_query - 1]}\n")
        return self._result_files[num_query]

    def __write_result_to_file(self, num_query, result):
        file_handle = self.__get_result_file(num_query)
        file_handle.write(f"{result}\n")
        file_handle.flush()

    def __close_result_file(self, num_query):
        if num_query in self._result_files:
            self._result_files[num_query].close()

    def __close_all_result_files(self):
        for num_query in self._result_files:
            self.__close_result_file(num_query)
    
    def receive_results(self):
        self.__create_results_dir()
        amount_queries_resolved = 0
        
        start_time = datetime.now()
        while True:
            try:
                message = communication.receive_lines_message(self._results_socket)
                num_query, results = int(message[0]), message[1:]
                for result in results:
                    if result == communication.EOF:
                        logging.info(f"Query {num_query} resolved in {(datetime.now() - start_time).total_seconds()} seconds")
                        self.__close_result_file(num_query)
                        amount_queries_resolved += 1
                        if amount_queries_resolved == len(QUERY_RESULTS_HEADERS):
                            logging.info("action: all_queries_resolved | result: success")
                            return
                    else:
                        self.__write_result_to_file(num_query, result)
                        logging.info(f"Received result from query {num_query}: {result}")
            except OSError as e:
                logging.error(f"Error while receiving results: {e}")
                return