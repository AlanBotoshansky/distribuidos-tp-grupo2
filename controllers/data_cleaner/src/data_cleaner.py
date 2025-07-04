import socket
import logging
import signal
import multiprocessing as mp
import uuid
from utils.utils import close_socket
from src.messages_sender import MessagesSender
from src.client_handler import ClientHandler, CONNECTED_CLIENTS_FILE_KEY
from common.monitorable import Monitorable
from storage_adapter.storage_adapter import StorageAdapter
from messages.client_disconnected import ClientDisconnected

MESSAGES_QUEUE_SIZE = 10000

class DataCleaner(Monitorable):
    def __init__(self, port, listen_backlog, movies_exchange, ratings_exchange, credits_exchange, max_concurrent_clients, storage_path):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._movies_exchange = movies_exchange
        self._ratings_exchange = ratings_exchange
        self._credits_exchange = credits_exchange
        self._max_concurrent_clients = max_concurrent_clients
        self._shutdown_requested = False
        self._manager = mp.Manager()
        self._messages_queue = self._manager.Queue(maxsize=MESSAGES_QUEUE_SIZE)
        self._sender_process = None
        self._receiver_processes = []
        self._receiver_pool_semaphore = self._manager.BoundedSemaphore(max_concurrent_clients)
        self._connected_clients = self._manager.dict()
        self._connected_clients_update_lock = self._manager.Lock()
        self._storage_adapter = StorageAdapter(storage_path)
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            logging.info('action: signal_received | result: success | signal: SIGTERM')
            self._shutdown_requested = True
            self.__cleanup()
            if len(self._receiver_processes) == self._max_concurrent_clients:
                self._receiver_pool_semaphore.release()
            
    def __cleanup(self):
        """
        Cleanup server resources during shutdown
        """
        close_socket(self._server_socket, "server_socket")
        
        for receiver_process in self._receiver_processes:
            receiver_process.terminate()
            receiver_process.join()
            logging.info("action: receiver_process_terminated | result: success")
            
        self._messages_queue.put(None)
        if self._sender_process:
            self._sender_process.terminate()
            self._sender_process.join()
            logging.info("action: sender_process_terminated | result: success")
            
        self.stop_receiving_health_checks()
        
    def __notify_disconnection_of_previous_clients(self):
        previous_connected_clients = self._storage_adapter.load_data(CONNECTED_CLIENTS_FILE_KEY)
        if previous_connected_clients:
            for client_id in previous_connected_clients:
                self._messages_queue.put(ClientDisconnected(client_id))
                logging.info(f"action: notify_disconnection_of_previous_client | client_id: {client_id}")
            self._storage_adapter.delete(CONNECTED_CLIENTS_FILE_KEY)

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        The client id is returned
        """
        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        client_sock, addr = self._server_socket.accept()
        client_id = str(uuid.uuid4())
        with self._connected_clients_update_lock:
            self._connected_clients[client_id] = True
            self._storage_adapter.update(CONNECTED_CLIENTS_FILE_KEY, self._connected_clients)
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return client_id, client_sock

    def __handle_client(self, client_id, client_sock, messages_queue, receiver_pool_semaphore, connected_clients, connected_clients_update_lock, storage_adapter):
        client_handler = ClientHandler(client_id, client_sock, messages_queue, receiver_pool_semaphore, connected_clients, connected_clients_update_lock, storage_adapter)
        client_handler.handle_client()

    def __send_messages(self, messages_queue, data_exchanges):
        messages_sender = MessagesSender(messages_queue, data_exchanges)
        messages_sender.send_messages()
    
    def run(self):
        self.start_receiving_health_checks()
        self.__notify_disconnection_of_previous_clients()
        data_exchanges = [self._movies_exchange, self._ratings_exchange, self._credits_exchange]
        self._sender_process = mp.Process(target=self.__send_messages, args=(self._messages_queue, data_exchanges))
        self._sender_process.start()
        while not self._shutdown_requested:
            self._receiver_pool_semaphore.acquire()
            try:
                client_id, client_sock = self.__accept_new_connection()
            except OSError as e:
                if self._shutdown_requested:
                    break
                logging.error(f"action: accept_connection | result: fail | error: {e}")
            
            client_handler = mp.Process(target=self.__handle_client, args=(client_id, client_sock, self._messages_queue, self._receiver_pool_semaphore, self._connected_clients, self._connected_clients_update_lock, self._storage_adapter))
            self._receiver_processes.append(client_handler)
            client_handler.start()

            for process in self._receiver_processes:
                if not process.is_alive():
                    process.join()
                    self._receiver_processes.remove(process)
