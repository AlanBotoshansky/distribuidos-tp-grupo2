import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_deserializer import PacketDeserializer
from messages.packet_type import PacketType
from messages.investor_country import InvestorCountry

class TopInvestorCountriesCalculator:
    def __init__(self, top_n_investor_countries, input_queues, output_exchange):
        self._top_n_investor_countries = top_n_investor_countries
        self._input_queues = input_queues
        self._output_exchange = output_exchange
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
        self._middleware.stop_handling_messages()
        self._middleware.close_connection()
    
    def __update_investments(self, movie):
        for country in movie.production_countries:
            self._investment_by_country[country] = self._investment_by_country.get(country, 0) + movie.budget
    
    def __get_top_investor_countries(self):
        sorted_investments = sorted(self._investment_by_country.items(), key=lambda x: x[1], reverse=True)
        top_investor_countries = sorted_investments[:self._top_n_investor_countries]
        return top_investor_countries
    
    def __handle_packet(self, packet):
        msg = PacketDeserializer.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIE:
            movie = msg
            self.__update_investments(movie)
        elif msg.packet_type() == PacketType.EOF:            
            for country, investment in self.__get_top_investor_countries():
                investor_country = InvestorCountry(country, investment)
                self._middleware.send_message(investor_country.serialize())
            self._middleware.send_message(EOF().serialize())
            logging.info("action: sent_eof | result: success")
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        self._middleware = Middleware(callback_function=self.__handle_packet,
                                      input_queues=self._input_queues,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()

        
        