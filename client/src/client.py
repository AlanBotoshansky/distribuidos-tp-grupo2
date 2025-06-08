import logging
import signal
import multiprocessing as mp
import time
from src.results_receiver import ResultsReceiver
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

WAIT_TIME_RESTART = 5

DATA_SOCKET_NAME = "data_socket"

class ServerResultsDisconnectedError(Exception):
    pass

class Client:
    
    def __init__(self, server_ip_data, server_port_data, server_ip_results, server_port_results, movies_path, ratings_path, credits_path, movies_batch_max_size, ratings_batch_max_size, credits_batch_max_size, results_dir):
        self._server_ip_data = server_ip_data
        self._server_port_data = server_port_data
        self._server_ip_results = server_ip_results
        self._server_port_results = server_port_results
        self._movies_path = movies_path
        self._ratings_path = ratings_path
        self._credits_path = credits_path
        self._movies_batch_max_size = movies_batch_max_size
        self._ratings_batch_max_size = ratings_batch_max_size
        self._credits_batch_max_size = credits_batch_max_size
        self._results_dir = results_dir
        self._result_files = {}
        self._data_socket = None
        self._results_receiver = None
        self._id = None
        self._shutdown_requested = False
        self._manager = mp.Manager()
        self._server_results_disconnected = self._manager.Event()
        self._ready_to_receive_results = self._manager.Event()
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            logging.info('action: signal_received | result: success | signal: SIGTERM')
            self._shutdown_requested = True
            self.__shutdown()
    
    def __shutdown(self):
        if self._results_receiver:
            self._results_receiver.terminate()
            self._results_receiver.join()
            logging.info("action: results_receiver_terminated | result: success")
        
        close_socket(self._data_socket, DATA_SOCKET_NAME)
    
    def __receive_id(self):
        logging.info("action: receive_id | result: in_progress")
        id = communication.receive_message(self._data_socket)
        logging.info(f"action: receive_id | result: success | id: {id}")
        return id
    
    def __send_file(self, file_path, batch_max_size=1):
        batch = []
        with open(file_path) as file:
            next(file)
            for line in file:
                line = line.rstrip()
                batch.append(line)
                if len(batch) == batch_max_size:
                    if self._server_results_disconnected.is_set():
                        raise ServerResultsDisconnectedError
                    communication.send_lines(self._data_socket, batch)
                    batch = []
        if batch:
            communication.send_lines(self._data_socket, batch)

    def __send_data(self):
        self.__send_file(self._movies_path, batch_max_size=self._movies_batch_max_size)
        communication.send_message(self._data_socket, communication.EOF)
        logging.info(f"action: finished_sending_file | result: success | file: {self._movies_path}")
        self.__send_file(self._ratings_path, batch_max_size=self._ratings_batch_max_size)
        communication.send_message(self._data_socket, communication.EOF)
        logging.info(f"action: finished_sending_file | result: success | file: {self._ratings_path}")
        self.__send_file(self._credits_path, batch_max_size=self._credits_batch_max_size)
        communication.send_message(self._data_socket, communication.EOF)
        logging.info(f"action: finished_sending_file | result: success | file: {self._credits_path}")
    
    def __receive_results(self, id, results_dir, server_ip_results, server_port_results, server_results_disconnected, ready_to_receive_results):
        results_receiver = ResultsReceiver(id, results_dir, server_ip_results, server_port_results, server_results_disconnected, ready_to_receive_results)
        results_receiver.run()
        
    def run(self):        
        self._data_socket = connect_to_server(self._server_ip_data, self._server_port_data, DATA_SOCKET_NAME)
        if not self._data_socket:
            return
        
        try:
            id = self.__receive_id()
        except OSError as e:
            logging.error(f"action: receive_id | result: fail | error: {e}")
            close_socket(self._data_socket, DATA_SOCKET_NAME)
            return
    
        self._results_receiver = mp.Process(target=self.__receive_results, args=(id, self._results_dir, self._server_ip_results, self._server_port_results, self._server_results_disconnected, self._ready_to_receive_results))
        self._results_receiver.start()
        
        self._ready_to_receive_results.wait()
        try:
            self.__send_data()
        except OSError as e:
            logging.error(f"Error while sending data: {e}")
            if not self._shutdown_requested:  
                self._results_receiver.terminate()
                self._results_receiver.join()
                time.sleep(WAIT_TIME_RESTART)
                self.run()
        except ServerResultsDisconnectedError:
            logging.error("Server results disconnected while sending data")
            
        if not self._shutdown_requested:
            close_socket(self._data_socket, DATA_SOCKET_NAME)
        
        self._results_receiver.join()
        if self._server_results_disconnected.is_set():
            self._ready_to_receive_results.clear()
            self._server_results_disconnected.clear()
            time.sleep(WAIT_TIME_RESTART)
            self.run()
