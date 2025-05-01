import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.investor_country import InvestorCountry
from stateful_controller.stateful_controller import StatefulController

class TopInvestorCountriesCalculator(StatefulController):
    def __init__(self, top_n_investor_countries, input_queues, output_exchange, control_queue):
        self._top_n_investor_countries = top_n_investor_countries
        self._input_queues = input_queues
        self._output_exchange = output_exchange
        self._control_queue = control_queue
        self._middleware = None
        self._investment_by_country = {}
        
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
    
    def __update_investments(self, movies_batch):
        client_id = movies_batch.client_id
        self._investment_by_country[client_id] = self._investment_by_country.get(client_id, {})
        for movie in movies_batch.get_items():
            for country in movie.production_countries:
                self._investment_by_country[client_id][country] = self._investment_by_country[client_id].get(country, 0) + movie.budget
    
    def __get_top_investor_countries(self, client_id):
        sorted_investments = sorted(self._investment_by_country[client_id].items(), key=lambda x: x[1], reverse=True)
        top_investor_countries = sorted_investments[:self._top_n_investor_countries]
        return top_investor_countries
    
    def _clean_client_state(self, client_id):
        if client_id in self._investment_by_country:
            self._investment_by_country.pop(client_id)
    
    def __handle_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIES_BATCH:
            movies_batch = msg
            self.__update_investments(movies_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            for country, investment in self.__get_top_investor_countries(eof.client_id):
                investor_country = InvestorCountry(eof.client_id, country, investment)
                self._middleware.send_message(PacketSerde.serialize(investor_country))
            self._middleware.send_message(PacketSerde.serialize(EOF(eof.client_id)))
            logging.info("action: sent_eof | result: success")
            self._clean_client_state(eof.client_id)
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        input_queues_and_callback_functions = [(input_queue[0], input_queue[1], self.__handle_packet) for input_queue in self._input_queues]
        input_queues_and_callback_functions.append((self._control_queue[0], self._control_queue[1], self._handle_control_packet))
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()

        
        