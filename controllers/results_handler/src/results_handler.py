import socket
import logging
import signal
import multiprocessing as mp
import communication.communication as communication
from middleware.middleware import Middleware
from messages.packet_deserializer import PacketDeserializer

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
        self._results_handler_processes = []
        self._middlewares = []
        
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
        if self._sender_process:
            self._sender_process.join()
            logging.info("action: sender_process_finished | result: success")
            
        for middleware in self._middlewares:
            middleware.close_connection()
            logging.info("action: middleware_closed | result: success")
            self._middlewares.remove(middleware)
            
        for process in self._results_handler_processes:
            process.join()
            logging.info("action: results_handler_process_finished | result: success")
            self._results_handler_processes.remove(process)
        
        try:    
            logging.info("action: close_client_socket | result: in_progress") 
            self._client_sock.shutdown(socket.SHUT_RDWR)
            self._client_sock.close()
            logging.info('action: close_client_socket | result: success')
        except OSError as e:
            logging.error(f"action: close_client_socket | result: fail | error: {e}")
        
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
        while not self._shutdown_requested:
            result_csv_line = self._results_queue.get()
            if not result_csv_line:
                logging.info("action: stop_sending | result: success")
                return
            communication.send_message(client_sock, result_csv_line)
    
    def __handle_result_packet(self, packet, num_query):
        msg = PacketDeserializer.deserialize(packet)
        msg_csv_line = msg.to_csv_line()
        result_csv_line = f"{num_query},{msg_csv_line}"
        self._results_queue.put(result_csv_line)
    
    def __handle_results(self, middleware):
        middleware.handle_messages()
    
    def __handle_client_connection(self, client_sock):
        self._sender_process = mp.Process(target=self.__send_results, args=(client_sock,), daemon=True)
        self._sender_process.start()
        for i, input_queue in enumerate(self._input_queues):
            num_query = i + 1
            middleware = Middleware(callback_function=self.__handle_result_packet, callback_args=(num_query,), input_queues=[input_queue])
            process = mp.Process(target=self.__handle_results, args=(middleware,), daemon=True)
            process.start()
            self._results_handler_processes.append(process)
            self._middlewares.append(middleware)
    
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
