import socket
import logging
import signal
from enum import IntEnum
import multiprocessing as mp
import communication.communication as communication
from messages.exceptions import InvalidLineError
from messages.movie import Movie
from messages.ratings_batch import RatingsBatch
from messages.eof import EOF
from src.data_sender import DataSender

DATA_QUEUE_SIZE = 10000

class FileType(IntEnum):
    MOVIES = 0
    RATINGS = 1
    CREDITS = 2
    
    def next(self):
        if self == FileType.CREDITS:
            return FileType.CREDITS
        return FileType(self.value + 1)

class DataCleaner:
    def __init__(self, port, listen_backlog, movies_exchange, ratings_exchange):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._shutdown_requested = False
        self._client_sock = None
        self._cleaning_file = FileType.MOVIES
        self._data_queue = mp.Queue(maxsize=DATA_QUEUE_SIZE)
        self._movies_exchange = movies_exchange
        self._ratings_exchange = ratings_exchange
        self._sender_process = None
        
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
        Then connection created is printed and returned
        """
        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        self._client_sock = c
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
    
    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        while not self._shutdown_requested:
            try:
                msg = communication.receive_message(client_sock)
                self.__handle_client_message(msg)
            except (OSError, ConnectionError) as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                break
            
    def __handle_client_message(self, msg):
        if msg == communication.EOF:
            self._data_queue.put(EOF())
            self._cleaning_file = self._cleaning_file.next()
            return
        if self._cleaning_file == FileType.MOVIES:
            try:
                movie = Movie.from_csv_line(msg)
                self._data_queue.put(movie)
            except InvalidLineError as e:
                # logging.error(f"action: handle_message | result: fail | error: {e}")
                return
        elif self._cleaning_file == FileType.RATINGS:
            try:
                ratings_csv_lines = communication.parse_batch_message(msg)
                ratings_batch = RatingsBatch.from_ratings_csv_lines(ratings_csv_lines)
                self._data_queue.put(ratings_batch)
            except InvalidLineError as e:
                # logging.error(f"action: handle_message | result: fail | error: {e}")
                return

    def __send_data(self):
        exchanges = [self._movies_exchange, self._ratings_exchange]
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
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
            except OSError as e:
                if self._shutdown_requested:
                    break
                logging.error(f"action: accept_connection | result: fail | error: {e}")
