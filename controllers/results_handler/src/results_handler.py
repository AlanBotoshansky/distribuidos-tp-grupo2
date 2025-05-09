import socket
import logging
import signal
import multiprocessing as mp
import communication.communication as communication
from utils.utils import close_socket
from src.query_results_handler import QueryResultsHandler

class ResultsHandler:
    def __init__(self, port, listen_backlog, input_queues):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._shutdown_requested = False
        self._manager = mp.Manager()
        self._client_socks = self._manager.dict()
        self._results_queue = self._manager.Queue()
        self._input_queues = input_queues
        self._sender_process = None
        self._query_results_handlers = []
        
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
        self._results_queue.put(None)
        for client_id, client_sock in self._client_socks.items():
            close_socket(client_sock, f"client_{client_id}_socket") 
                
        if self._sender_process:
            self._sender_process.join()
            logging.info("action: sender_process_finished | result: success")
        
        for query_results_handler in self._query_results_handlers:
            query_results_handler.terminate()
            query_results_handler.join()
            logging.info("action: query_results_handler_terminated | result: success")

        close_socket(self._server_socket, "server_socket")

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
    
    def __send_results(self, client_socks, results_queue):
        while True:
            client_result = results_queue.get()
            if not client_result:
                logging.info("action: stop_sending | result: success")
                return
            client_id, result = client_result
            try:
                if client_id not in client_socks:
                    logging.debug(f"action: send_results | result: fail | error: client {client_id} not found")
                    continue
                client_sock = client_socks[client_id]
                communication.send_lines(client_sock, result)
            except OSError:
                logging.error(f"action: client_disconnected | client_id: {client_id}")
                client_socks.pop(client_id)
                close_socket(client_sock, f"client_{client_id}_socket")
    
    def __start_sender_process(self):
        self._sender_process = mp.Process(target=self.__send_results, args=(self._client_socks, self._results_queue))
        self._sender_process.start()
    
    def __handle_query(self, num_query, input_queues, results_queue):
        query_results_handler = QueryResultsHandler(num_query, input_queues, results_queue)
        query_results_handler.run()
        
    def __start_query_results_handlers(self):
        for i, input_queue in enumerate(self._input_queues):
            num_query = i + 1
            process = mp.Process(target=self.__handle_query, args=(num_query, [input_queue], self._results_queue))
            process.start()
            self._query_results_handlers.append(process)
    
    def __handle_client_connection(self, client_sock):
        client_id = communication.receive_message(client_sock)
        self._client_socks[client_id] = client_sock
    
    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again.
        The loop will continue until a SIGTERM signal is received.
        """
        self.__start_sender_process()
        self.__start_query_results_handlers()
        
        while not self._shutdown_requested:
            try:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
            except OSError as e:
                if self._shutdown_requested:
                    break
                logging.error(f"action: accept_connection | result: fail | error: {e}")
