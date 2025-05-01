import signal
import logging
from middleware.middleware import Middleware
from messages.packet_type import PacketType
from messages.packet_serde import PacketSerde
import logging

class MessagesSender:
    def __init__(self, messages_queue, data_exchanges, control_exchange):
        self._messages_queue = messages_queue
        self._data_exchanges = data_exchanges
        self._control_exchange = control_exchange
        self._current_data_exchange_i = {}
        self._middleware = Middleware()
        self._shutdown_requested = False
      
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        if signalnum == signal.SIGTERM:
            self._shutdown_requested = True
            self._messages_queue.put(None)
    
    def send_messages(self):
        while not self._shutdown_requested:
            msg = self._messages_queue.get()
            if msg.packet_type() == PacketType.CLIENT_DISCONNECTED:
                self._middleware.send_message(PacketSerde.serialize(msg), exchange=self._control_exchange)
                continue
            self._current_data_exchange_i[msg.client_id] = self._current_data_exchange_i.get(msg.client_id, 0)
            if not msg:
                logging.info("action: stop_sending | result: success")
                break
            self._middleware.send_message(PacketSerde.serialize(msg), exchange=self._data_exchanges[self._current_data_exchange_i[msg.client_id]])
            if msg.packet_type() == PacketType.EOF:
                self._current_data_exchange_i[msg.client_id] += 1
                if self._current_data_exchange_i[msg.client_id] >= len(self._data_exchanges):
                    logging.info("action: stop_sending | result: success")
                    self._current_data_exchange_i.pop(msg.client_id)
        self._middleware.stop()
