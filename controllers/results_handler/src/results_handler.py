import socket
import logging
import signal
import multiprocessing as mp
import communication.communication as communication
from src.query_results_handler import QueryResultsHandler

class ResultsHandler:
    def __init__(self, port, listen_backlog, input_queues):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._shutdown_requested = False
        self._client_sock = None
        self._results_queue = mp.Queue()
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
        try:    
            logging.info("action: close_client_socket | result: in_progress") 
            self._client_sock.shutdown(socket.SHUT_RDWR)
            self._client_sock.close()
            logging.info('action: close_client_socket | result: success')
        except OSError as e:
            logging.error(f"action: close_client_socket | result: fail | error: {e}")  
        if self._sender_process:
            self._sender_process.join()
            logging.info("action: sender_process_finished | result: success")
        
        for query_results_handler in self._query_results_handlers:
            query_results_handler.terminate()
            query_results_handler.join()
            logging.info("action: query_results_handler_terminated | result: success")
        
        try:
            logging.info('action: close_server_socket | result: in_progress')
            self._server_socket.shutdown(socket.SHUT_RDWR)
            self._server_socket.close()
            logging.info('action: close_server_socket | result: success')
        except OSError as e:
            logging.error(f"action: close_server_socket | result: fail | error: {e}")

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
    
    def __send_results(self, client_sock):
        while True:
            result = self._results_queue.get()
            if not result:
                logging.info("action: stop_sending | result: success")
                return
            try:
                communication.send_lines(client_sock, result)
            except OSError as e:
                logging.error(f"action: send_results | result: fail | error: {e}")
                break
    
    def __handle_query(self, num_query, input_queues, results_queue):
        query_results_handler = QueryResultsHandler(num_query, input_queues, results_queue)
        query_results_handler.run()
    
    def __handle_client_connection(self, client_sock):
        self._sender_process = mp.Process(target=self.__send_results, args=(client_sock,), daemon=True)
        self._sender_process.start()
        for i, input_queue in enumerate(self._input_queues):
            num_query = i + 1
            process = mp.Process(target=self.__handle_query, args=(num_query, [input_queue], self._results_queue), daemon=True)
            process.start()
            self._query_results_handlers.append(process)
    
    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again.
        The loop will continue until a SIGTERM signal is received.
        """
        while not self._shutdown_requested:
            try:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
            except OSError as e:
                if self._shutdown_requested:
                    break
                logging.error(f"action: accept_connection | result: fail | error: {e}")
