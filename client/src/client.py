import logging
import socket
import signal
import src.communication as communication

class Client:
    
    def __init__(self, server_ip, server_port, movies_path, ratings_path, credits_path):
        self._server_ip = server_ip
        self._server_port = server_port
        self._movies_path = movies_path
        self._ratings_path = ratings_path
        self._credits_path = credits_path
        self.socket = None
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            logging.info('action: signal_received | result: success | signal: SIGTERM')
            self._shutdown()
    
    def _shutdown(self):
        try:
            logging.info('action: close_client_socket | result: in_progress')
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
            logging.info('action: close_client_socket | result: success')
        except OSError as e:
            logging.error(f"action: close_client_socket | result: fail | error: {e}")
    
    def _send_file(self, file_path):
        with open(file_path) as file:
            next(file)
            for line in file:
                communication.send_message(self.socket, line.rstrip()) 

    def _send_data(self):
        self._send_file(self._movies_path)
        self._send_file(self._ratings_path)
        self._send_file(self._credits_path)
        
    def run(self):
        logging.info(f"Connecting to server at {self._server_ip}:{self._server_port}")
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self._server_ip, self._server_port))
            self.socket = client_socket
            logging.info("Connection established successfully.")
            self._send_data()
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()
        except OSError as e:
            logging.error(f"Error while executing the client: {e}")
            