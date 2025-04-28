import socket
import logging
import signal
import multiprocessing as mp
import communication.communication as communication
from messages.exceptions import InvalidLineError
from messages.movies_batch import MoviesBatch
from messages.ratings_batch import RatingsBatch
from messages.credits_batch import CreditsBatch
from messages.eof import EOF
from src.data_sender import DataSender
from src.client_state import ClientState

DATA_QUEUE_SIZE = 10000

class DataCleaner:
    def __init__(self, port, listen_backlog, movies_exchange, ratings_exchange, credits_exchange):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._shutdown_requested = False
        self._manager = mp.Manager()
        self._client_states = self._manager.dict()
        self._data_queue = mp.Queue(maxsize=DATA_QUEUE_SIZE)
        self._movies_exchange = movies_exchange
        self._ratings_exchange = ratings_exchange
        self._credits_exchange = credits_exchange
        self._sender_process = None
        self._receiver_processes = []
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
            
    def __close_socket(self, socket_to_close, socket_name):
        try:
            logging.info(f'action: close_{socket_name} | result: in_progress')
            socket_to_close.shutdown(socket.SHUT_RDWR)
            socket_to_close.close()
            logging.info(f'action: close_{socket_name} | result: success')
        except OSError:
            logging.error(f"action: close_{socket_name} | result: fail | socket already closed")
            
    def __cleanup(self):
        """
        Cleanup server resources during shutdown
        """
        self.__close_socket(self._server_socket, "server_socket")
        
        for client_id, client_state in self._client_states.items():
            self.__close_socket(client_state.socket, f"client_{client_id}_socket") 
        
        if self._sender_process:
            self._sender_process.terminate()
            self._sender_process.join()
            logging.info("action: sender_process_terminated | result: success")

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        The client id is returned
        """
        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        client_sock, addr = self._server_socket.accept()
        client_id = self._next_client_id
        self._client_states[client_id] = ClientState(client_sock)
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        self._next_client_id += 1
        return client_id
    
    def __handle_client_connection(self, client_id):
        client_sock = self._client_states[client_id].socket
        communication.send_message(client_sock, str(client_id))
        
        while not self._shutdown_requested:
            try:
                msg = communication.receive_message(client_sock)
                self.__handle_client_message(client_id, msg)
            except (OSError, ConnectionError) as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                self._client_states.pop(client_id)
                self.__close_socket(client_sock, f"client_{client_id}_socket")
                break
            
    def __handle_client_message(self, client_id, msg):
        client_state = self._client_states[client_id]
        if msg == communication.EOF:
            self._data_queue.put(EOF(client_id))
            client_state.finished_sending_file()
            self._client_states[client_id] = client_state
            return
        if client_state.is_sending_movies():
            try:
                movies_csv_lines = communication.parse_lines_message(msg)
                movies_batch = MoviesBatch.from_csv_lines(client_id, movies_csv_lines)
                self._data_queue.put(movies_batch)
            except InvalidLineError as e:
                logging.debug(f"action: handle_message | result: fail | error: {e}")
                return
        elif client_state.is_sending_ratings():
            try:
                ratings_csv_lines = communication.parse_lines_message(msg)
                ratings_batch = RatingsBatch.from_csv_lines(client_id, ratings_csv_lines)
                self._data_queue.put(ratings_batch)
            except InvalidLineError as e:
                logging.debug(f"action: handle_message | result: fail | error: {e}")
                return
        elif client_state.is_sending_credits():
            try:
                credits_csv_lines = communication.parse_lines_message(msg)
                credits_batch = CreditsBatch.from_csv_lines(client_id, credits_csv_lines)
                self._data_queue.put(credits_batch)
            except InvalidLineError as e:
                logging.debug(f"action: handle_message | result: fail | error: {e}")
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
                client_id = self.__accept_new_connection()
            except OSError as e:
                if self._shutdown_requested:
                    break
                logging.error(f"action: accept_connection | result: fail | error: {e}")
            receiver_process = mp.Process(target=self.__handle_client_connection, args=(client_id,))
            self._receiver_processes.append(receiver_process)
            receiver_process.start()
            
            for process in self._receiver_processes:
                if not process.is_alive():
                    process.join()
                    self._receiver_processes.remove(process)
