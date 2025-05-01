from abc import ABC, abstractmethod
import logging
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
import logging

class StatefulController(ABC):
    @abstractmethod
    def _clean_client_state(self):
        """
        Cleans the state of the client.
        This method should be implemented by subclasses to define how to clean the client state.
        """
        pass
    
    def _handle_control_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.CLIENT_DISCONNECTED:
            client_disconnected = msg
            self._clean_client_state(client_disconnected.client_id)
            logging.debug(f"action: client_disconnected | result: success | client_id: {client_disconnected.client_id}")
