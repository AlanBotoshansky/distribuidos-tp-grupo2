import signal
import logging
from middleware.middleware import Middleware

class DataSender:
    def __init__(self, data_queue, output_exchange):
        self._data_queue = data_queue
        self._middleware = Middleware(output_exchange=output_exchange)
        self._shutdown_requested = False
      
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        if signalnum == signal.SIGTERM:
            self._shutdown_requested = True
            self._data_queue.put(None)
    
    def send_data(self):
        while not self._shutdown_requested:
            msg = self._data_queue.get()
            if not msg:
                logging.info("action: stop_sending | result: success")
                break
            self._middleware.send_message(msg.serialize())
        self._middleware.close_connection()