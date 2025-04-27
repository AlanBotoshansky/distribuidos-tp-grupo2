import socket
import logging
import signal
from enum import IntEnum
import multiprocessing as mp
import communication.communication as communication
from messages.exceptions import InvalidLineError
from messages.movies_batch import MoviesBatch
from messages.ratings_batch import RatingsBatch
from messages.credits_batch import CreditsBatch
from messages.eof import EOF
from src.data_sender import DataSender

DATA_QUEUE_SIZE = 10000

class FileType(IntEnum):
    MOVIES = 0
    RATINGS = 1
    CREDITS = 2
    
    def next(self):
        return FileType((self.value + 1) % len(FileType))
    
    def reset(self):
        return FileType.MOVIES

class DataCleaner:
    def __init__(self, port, listen_backlog, movies_exchange, ratings_exchange, credits_exchange):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._shutdown_requested = False
        self._client_sock = None
        self._cleaning_file = FileType.MOVIES
        self._data_queue = mp.Queue(maxsize=DATA_QUEUE_SIZE)
        self._movies_exchange = movies_exchange
        self._ratings_exchange = ratings_exchange
        self._credits_exchange = credits_exchange
        self._sender_process = None
        self._next_client_id = 1
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            logging.info('action: signal_received | result: success | signal: SIGTERM')
            self._shutdown_requested = True
            self.__cleanup()
            
    def __cleanup(self):
        """
        Cleanup server resources during shutdown
        """
        try:
            logging.info('action: close_server_socket | result: in_progress')
            self._server_socket.shutdown(socket.SHUT_RDWR)
            self._server_socket.close()
            logging.info('action: close_server_socket | result: success')
        except OSError as e:
            logging.error(f"action: close_server_socket | result: fail | error: {e}")   
        
        try:    
            logging.info("action: close_client_socket | result: in_progress") 
            self._client_sock.shutdown(socket.SHUT_RDWR)
            self._client_sock.close()
            logging.info('action: close_client_socket | result: success')
        except OSError as e:
            logging.error(f"action: close_client_socket | result: fail | error: {e}")
        
        if self._sender_process:
            self._sender_process.terminate()
            self._sender_process.join()
            logging.info("action: sender_process_terminated | result: success")

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        The connection created and the client id are returned
        """
        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        self._client_sock = c
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        client_id = self._next_client_id
        self._next_client_id += 1
        return c, client_id
    
    def __handle_client_connection(self, client_sock, client_id):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        while not self._shutdown_requested:
            try:
                msg = communication.receive_message(client_sock)
                self.__handle_client_message(client_id, msg)
            except (OSError, ConnectionError) as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                self._cleaning_file.reset()
                break
            
    def __handle_client_message(self, client_id, msg):
        if msg == communication.EOF:
            self._data_queue.put(EOF(client_id))
            self._cleaning_file = self._cleaning_file.next()
            return
        if self._cleaning_file == FileType.MOVIES:
            try:
                movies_csv_lines = communication.parse_lines_message(msg)
                movies_batch = MoviesBatch.from_csv_lines(client_id, movies_csv_lines)
                self._data_queue.put(movies_batch)
            except InvalidLineError as e:
                # logging.error(f"action: handle_message | result: fail | error: {e}")
                return
        elif self._cleaning_file == FileType.RATINGS:
            try:
                ratings_csv_lines = communication.parse_lines_message(msg)
                ratings_batch = RatingsBatch.from_csv_lines(client_id, ratings_csv_lines)
                self._data_queue.put(ratings_batch)
            except InvalidLineError as e:
                # logging.error(f"action: handle_message | result: fail | error: {e}")
                return
        elif self._cleaning_file == FileType.CREDITS:
            try:
                credits_csv_lines = communication.parse_lines_message(msg)
                credits_batch = CreditsBatch.from_csv_lines(client_id, credits_csv_lines)
                self._data_queue.put(credits_batch)
            except InvalidLineError as e:
                logging.error(f"action: handle_message | result: fail | error: {e}")
                return

    def __send_data(self):
        exchanges = [self._movies_exchange, self._ratings_exchange, self._credits_exchange]
        data_sender = DataSender(self._data_queue, exchanges)
        data_sender.send_data()
    
    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again.
        The loop will continue until a SIGTERM signal is received.
        """
        self._sender_process = mp.Process(target=self.__send_data)
        self._sender_process.start()
        while not self._shutdown_requested:
            try:
                client_sock, client_id = self.__accept_new_connection()
            except OSError as e:
                if self._shutdown_requested:
                    break
                logging.error(f"action: accept_connection | result: fail | error: {e}")
            self.__handle_client_connection(client_sock, client_id)
