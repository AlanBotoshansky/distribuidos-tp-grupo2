import logging
import signal
import os
from datetime import datetime
from src.utils import connect_to_server
from utils.utils import close_socket
import communication.communication as communication

QUERY_RESULTS_HEADERS = [
    "id,title,genres",
    "country,investment",
    "id,title,avg_rating",
    "actor,participation",
    "sentiment,avg_rate_revenue_budget",
]

RESULTS_SOCKET_NAME = "results_socket"

class ResultsReceiver:
    def __init__(self, id, results_dir, server_ip_results, server_port_results, server_results_disconnected, ready_to_receive_results):
        self._id = id
        self._results_dir = results_dir
        self._server_ip_results = server_ip_results
        self._server_port_results = server_port_results
        self._result_files = {}
        self._shutdown_requested = False
        self._server_results_disconnected = server_results_disconnected
        self._ready_to_receive_results = ready_to_receive_results
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            self._shutdown_requested = True
            self.__shutdown()
            
    def __shutdown(self):
        close_socket(self._results_socket, RESULTS_SOCKET_NAME)
        self.__close_all_result_files()
    
    def __send_id(self):
        logging.info("action: send_id | result: in_progress")
        communication.send_message(self._results_socket, self._id)
        logging.info(f"action: send_id | result: success | id: {self._id}")

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
    
    def __receive_results(self):
        self.__create_results_dir()
        amount_queries_resolved = 0
        
        start_time = datetime.now()
        self._ready_to_receive_results.set()
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
                if not self._shutdown_requested:
                    self._server_results_disconnected.set()
                return
            
    def run(self):
        self._results_socket = connect_to_server(self._server_ip_results, self._server_port_results, RESULTS_SOCKET_NAME)
        if not self._results_socket:
            return
        
        try:
            self.__send_id()
        except OSError as e:
            logging.error(f"action: send_id | result: fail | error: {e}")
            close_socket(self._results_socket, RESULTS_SOCKET_NAME)
            return
        
        self.__receive_results()
        if not self._shutdown_requested:
            close_socket(self._results_socket, RESULTS_SOCKET_NAME)
