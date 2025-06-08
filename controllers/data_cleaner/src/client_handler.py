import socket
import logging
import signal
import communication.communication as communication
from utils.utils import close_socket
from messages.movies_batch import MoviesBatch
from messages.ratings_batch import RatingsBatch
from messages.credits_batch import CreditsBatch
from messages.eof import EOF
from messages.client_disconnected import ClientDisconnected
from src.client_state import ClientState

CLIENT_DISCONNECT_TIMEOUT = 1
CONNECTED_CLIENTS_FILE_KEY = "connected_clients"

class ClientHandler:
    def __init__(self, client_id, client_sock, messages_queue, receiver_pool_semaphore, connected_clients, connected_clients_update_lock, storage_adapter):
        self._client_id = client_id
        self._client_sock = client_sock
        self._messages_queue = messages_queue
        self._receiver_pool_semaphore = receiver_pool_semaphore
        self._client_state = ClientState()
        self._shutdown_requested = False
        self._connected_clients = connected_clients
        self._connected_clients_update_lock = connected_clients_update_lock
        self._storage_adapter = storage_adapter
      
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        if signalnum == signal.SIGTERM:
            self._shutdown_requested = True
            self.__cleanup()
            
    def __cleanup(self):
        close_socket(self._client_sock, f"client_{self._client_id}_socket")
    
    def handle_client(self):
        communication.send_message(self._client_sock, self._client_id)
        
        self._client_sock.settimeout(CLIENT_DISCONNECT_TIMEOUT)
        while not self._shutdown_requested:
            try:
                msg = communication.receive_message(self._client_sock)
                self.__handle_client_message(msg)
            except (ConnectionError, socket.timeout):
                logging.info(f"action: client_disconnected | client_id: {self._client_id}")
                if not self._client_state.has_finished_sending():
                    self._messages_queue.put(ClientDisconnected(self._client_id))
                break
            except OSError:
                logging.info(f"action: terminating_client_handler | client_id: {self._client_id}")
                break
        if not self._shutdown_requested:
            close_socket(self._client_sock, f"client_{self._client_id}_socket")
        with self._connected_clients_update_lock:
            self._connected_clients.pop(self._client_id)
            self._storage_adapter.update(CONNECTED_CLIENTS_FILE_KEY, self._connected_clients)
        self._receiver_pool_semaphore.release()
        
    def __handle_client_message(self, msg):
        if msg == communication.EOF:
            self._messages_queue.put(EOF(self._client_id))
            self._client_state.finished_sending_file()
            return
        if self._client_state.is_sending_movies():
            movies_csv_lines = communication.parse_lines_message(msg)
            movies_batch = MoviesBatch.from_csv_lines(self._client_id, movies_csv_lines)
            self._messages_queue.put(movies_batch)
        elif self._client_state.is_sending_ratings():
            ratings_csv_lines = communication.parse_lines_message(msg)
            ratings_batch = RatingsBatch.from_csv_lines(self._client_id, ratings_csv_lines)
            self._messages_queue.put(ratings_batch)
        elif self._client_state.is_sending_credits():
            credits_csv_lines = communication.parse_lines_message(msg)
            credits_batch = CreditsBatch.from_csv_lines(self._client_id, credits_csv_lines)
            self._messages_queue.put(credits_batch)
