import socket
import logging
import signal
from utils.utils import close_socket

PORT = 9911
LISTEN_BACKLOG = 5

class HealthChecksReceiver:
    def __init__(self, port=PORT, listen_backlog=LISTEN_BACKLOG):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(('', port))
        self._socket.listen(listen_backlog)
        self._shutdown_requested = False
      
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        if signalnum == signal.SIGTERM:
            self._shutdown_requested = True
            self.__cleanup()
            
    def __cleanup(self):
        close_socket(self._socket, f"health_checks_socket")
    
    def start(self):
        while not self._shutdown_requested:
            try:
                s, addr = self._socket.accept()
                logging.info(f"action: received_health_check | result: success | from: {addr}")
            except OSError as e:
                if self._shutdown_requested:
                    break
                logging.error(f"action: accept_connection | result: fail | error: {e}")
            try:
                s.shutdown(socket.SHUT_RDWR)
                s.close()
            except OSError:
                continue
