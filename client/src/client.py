import logging
import socket
import signal
import multiprocessing as mp
import communication.communication as communication
from datetime import datetime

class Client:
    
    def __init__(self, server_ip_data, server_port_data, server_ip_results, server_port_results, movies_path, ratings_path, credits_path, movies_batch_max_size, ratings_batch_max_size, credits_batch_max_size):
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
        self.data_socket = None
        self.results_socket = None
        self.results_receiver = None
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            logging.info('action: signal_received | result: success | signal: SIGTERM')
            self._shutdown()
            
    def __close_socket(self, socket_to_close, socket_name):
        try:
            logging.info(f'action: close_{socket_name} | result: in_progress')
            socket_to_close.shutdown(socket.SHUT_RDWR)
            socket_to_close.close()
            logging.info(f'action: close_{socket_name} | result: success')
        except OSError:
            logging.error(f"action: close_{socket_name} | result: fail | socket already closed")
    
    def _shutdown(self):
        self.__close_socket(self.data_socket, "data_socket")
        self.__close_socket(self.results_socket, "results_socket")
            
        self.results_receiver.join()
    
    def _send_file(self, file_path, batch_max_size=1):
        batch = []
        with open(file_path) as file:
            next(file)
            for line in file:
                line = line.rstrip()
                batch.append(line)
                if len(batch) == batch_max_size:
                    communication.send_lines(self.data_socket, batch)
                    batch = []
        if batch:
            communication.send_lines(self.data_socket, batch)

    def _send_data(self):
        self._send_file(self._movies_path, batch_max_size=self._movies_batch_max_size)
        communication.send_message(self.data_socket, communication.EOF)
        logging.info(f"action: finished_sending_file | result: success | file: {self._movies_path}")
        self._send_file(self._ratings_path, batch_max_size=self._ratings_batch_max_size)
        communication.send_message(self.data_socket, communication.EOF)
        logging.info(f"action: finished_sending_file | result: success | file: {self._ratings_path}")
        self._send_file(self._credits_path, batch_max_size=self._credits_batch_max_size)
        communication.send_message(self.data_socket, communication.EOF)
        logging.info(f"action: finished_sending_file | result: success | file: {self._credits_path}")
        
    def _receive_results(self, results_socket): 
        start_time = datetime.now()
        while True:
            try:
                message = communication.receive_lines_message(results_socket)
                num_query, results = message[0], message[1:]
                for result in results:
                    if result == communication.EOF:
                        logging.info(f"Query {num_query} resolved in {(datetime.now() - start_time).total_seconds()} seconds")
                        continue
                    logging.info(f"Received result from query {num_query}: {result}")
            except OSError as e:
                logging.error(f"Error while receiving results: {e}")
                return
        
    def run(self):
        logging.info(f"Connecting to server at {self._server_ip_results}:{self._server_port_results}")
        self.results_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.results_socket.connect((self._server_ip_results, self._server_port_results))
        except OSError as e:
            logging.error(f"Error while connecting to results socket: {e}")
            return
        self.results_receiver = mp.Process(target=self._receive_results, args=(self.results_socket,))
        self.results_receiver.start()
        
        logging.info(f"Connecting to server at {self._server_ip_data}:{self._server_port_data}")
        self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.data_socket.connect((self._server_ip_data, self._server_port_data))
        except OSError as e:
            logging.error(f"Error while connecting to data socket: {e}")
        
        try:    
            self._send_data()
            self.__close_socket(self.data_socket, "data_socket")
        except Exception as e:
            logging.error(f"Error while sending data: {e}")
        
        self.results_receiver.join()