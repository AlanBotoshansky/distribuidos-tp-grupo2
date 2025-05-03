import signal
import logging
from middleware.middleware import Middleware
from messages.packet_type import PacketType
from messages.packet_serde import PacketSerde
import logging

class MessagesSender:
    def __init__(self, messages_queue, exchanges):
        self._messages_queue = messages_queue
        self._exchanges = exchanges
        self._current_exchange_i = {}
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
            if not msg:
                logging.info("action: stop_sending | result: success")
                break
            if msg.packet_type() == PacketType.CLIENT_DISCONNECTED:
                for exchange in self._exchanges:
                    self._middleware.send_message(PacketSerde.serialize(msg), exchange=exchange)
                self._middleware.send_message(PacketSerde.serialize(msg))
                continue
            self._current_exchange_i[msg.client_id] = self._current_exchange_i.get(msg.client_id, 0)
            self._middleware.send_message(PacketSerde.serialize(msg), exchange=self._exchanges[self._current_exchange_i[msg.client_id]])
            if msg.packet_type() == PacketType.EOF:
                self._current_exchange_i[msg.client_id] += 1
                if self._current_exchange_i[msg.client_id] >= len(self._exchanges):
                    logging.info("action: stop_sending | result: success")
                    self._current_exchange_i.pop(msg.client_id)
        self._middleware.stop()
