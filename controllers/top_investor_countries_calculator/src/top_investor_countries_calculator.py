import signal
import logging
import uuid
import time
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.investor_country import InvestorCountry
from common.monitorable import Monitorable
from storage_adapter.storage_adapter import StorageAdapter
from common.failure_simulation import fail_with_probability

STATE_FILE_KEY = "state"
INVESTMENT_BY_COUNTRY = "investment_by_country"
PROCESSED_MESSAGE_IDS= "processed_message_ids"
MAX_PROCESSED_MESSAGE_IDS = 500

class TopInvestorCountriesCalculator(Monitorable):
    def __init__(self, top_n_investor_countries, input_queues, output_exchange, failure_probability, storage_path):
        self._top_n_investor_countries = top_n_investor_countries
        self._input_queues = input_queues
        self._output_exchange = output_exchange
        self._failure_probability = failure_probability
        self._middleware = None
        self._state = {}
        self._storage_adapter = StorageAdapter(storage_path)
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            logging.info('action: signal_received | result: success | signal: SIGTERM')
            self.__cleanup()
            
    def __cleanup(self):
        """
        Cleanup resources during shutdown
        """
        self._middleware.stop()
        self.stop_receiving_health_checks()

    def __load_state_from_storage(self):
        """
        Load persisted state from storage
        """
        state = self._storage_adapter.load_data(STATE_FILE_KEY)
        if state:
            self._state = state
            logging.debug(f"action: load_state_from_storage | result: success | state: {self._state}")
            
    def __generate_deterministic_uuid(self, message_id, country):
        """
        Generate a deterministic UUID based on the message ID and country.
        """
        return str(uuid.uuid5(uuid.UUID(message_id), country))
    
    def __save_processed_message_id(self, client_id, message_id):
        self._state[client_id][PROCESSED_MESSAGE_IDS][message_id] = time.time_ns()
        if len(self._state[client_id][PROCESSED_MESSAGE_IDS]) > MAX_PROCESSED_MESSAGE_IDS:
            oldest_message_id = min(self._state[client_id][PROCESSED_MESSAGE_IDS], key=self._state[client_id][PROCESSED_MESSAGE_IDS].get)
            self._state[client_id][PROCESSED_MESSAGE_IDS].pop(oldest_message_id)
    
    def __update_investments(self, movies_batch):
        client_id = movies_batch.client_id
        self._state[client_id] = self._state.get(client_id, {INVESTMENT_BY_COUNTRY: {}, PROCESSED_MESSAGE_IDS: {}})
        if movies_batch.message_id in self._state[client_id][PROCESSED_MESSAGE_IDS]:
            return
        for movie in movies_batch.get_items():
            for country in movie.production_countries:
                self._state[client_id][INVESTMENT_BY_COUNTRY][country] = self._state[client_id][INVESTMENT_BY_COUNTRY].get(country, 0) + movie.budget
        self.__save_processed_message_id(client_id, movies_batch.message_id)
        self._storage_adapter.update(STATE_FILE_KEY, self._state[client_id], secondary_file_key=client_id)
    
    def __get_top_investor_countries(self, client_id):
        if client_id not in self._state:
            return []
        sorted_investments = sorted(self._state[client_id][INVESTMENT_BY_COUNTRY].items(), key=lambda x: x[1], reverse=True)
        top_investor_countries = sorted_investments[:self._top_n_investor_countries]
        return top_investor_countries
    
    def __clean_client_state(self, client_id):
        if client_id in self._state:
            self._state.pop(client_id)
            self._storage_adapter.delete(STATE_FILE_KEY, secondary_file_key=client_id)
    
    def __handle_packet(self, packet):
        fail_with_probability(self._failure_probability, "before handling packet")
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIES_BATCH:
            movies_batch = msg
            self.__update_investments(movies_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            for country, investment in self.__get_top_investor_countries(eof.client_id):
                new_message_id = self.__generate_deterministic_uuid(eof.message_id, country)
                investor_country = InvestorCountry(eof.client_id, country, investment, message_id=new_message_id)
                self._middleware.send_message(PacketSerde.serialize(investor_country))
            self._middleware.send_message(PacketSerde.serialize(EOF(eof.client_id, message_id=eof.message_id)))
            logging.info("action: sent_eof | result: success")
            fail_with_probability(self._failure_probability, "after sending results and eof, before cleaning client state")
            self.__clean_client_state(eof.client_id)
        elif msg.packet_type() == PacketType.CLIENT_DISCONNECTED:
            client_disconnected = msg
            logging.debug(f"action: client_disconnected | result: success | client_id: {client_disconnected.client_id}")
            self.__clean_client_state(client_disconnected.client_id)
            self._middleware.send_message(PacketSerde.serialize(client_disconnected))
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")
        fail_with_probability(self._failure_probability, f"after handling packet: {msg.packet_type()}")

    def run(self):
        self.start_receiving_health_checks()
        self.__load_state_from_storage()
        input_queues_and_callback_functions = [(input_queue[0], input_queue[1], self.__handle_packet) for input_queue in self._input_queues]
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()
