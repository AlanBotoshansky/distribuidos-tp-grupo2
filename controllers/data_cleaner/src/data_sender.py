import signal
import logging
from middleware.middleware import Middleware
from messages.packet_type import PacketType

class DataSender:
    def __init__(self, data_queue, output_exchanges):
        self._data_queue = data_queue
        self._output_exchanges = output_exchanges
        self._current_exchange_i = 0
        self._middleware = Middleware()
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
            self._middleware.send_message(msg.serialize(), exchange=self._output_exchanges[self._current_exchange_i])    
            if msg.packet_type() == PacketType.EOF:
                self._current_exchange_i += 1
                if self._current_exchange_i >= len(self._output_exchanges):
                    logging.info("action: stop_sending | result: success")
                    break
        self._middleware.close_connection()