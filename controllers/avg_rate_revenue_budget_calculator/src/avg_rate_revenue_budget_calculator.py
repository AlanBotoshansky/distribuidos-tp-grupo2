import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.avg_rate_revenue_budget import AvgRateRevenueBudget
from common.monitorable import Monitorable

class AvgRateRevenueBudgetCalculator(Monitorable):
    def __init__(self, input_queues, output_exchange):
        self._input_queues = input_queues
        self._output_exchange = output_exchange
        self._middleware = None
        self._revenue_budget_by_sentiment = {}
        
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
    
    def __update_revenues_budgets(self, analyzed_movies_batch):
        client_id = analyzed_movies_batch.client_id
        self._revenue_budget_by_sentiment[client_id] = self._revenue_budget_by_sentiment.get(client_id, {})
        for analyzed_movie in analyzed_movies_batch.get_items():
            revenue, budget, sentiment = analyzed_movie.revenue, analyzed_movie.budget, analyzed_movie.sentiment
            if revenue == 0 or budget == 0:
                continue
            revenue_sum, budget_sum = self._revenue_budget_by_sentiment[client_id].get(sentiment, (0, 0))
            revenue_sum += revenue
            budget_sum += budget
            self._revenue_budget_by_sentiment[client_id][sentiment] = (revenue_sum, budget_sum)
    
    def __get_avgs_rate_revenue_budget_by_sentiment(self, client_id):
        avgs_rate_revenue_budget = []
        for sentiment, (revenue, budget) in self._revenue_budget_by_sentiment[client_id].items():
            avg_rate_revenue_budget = AvgRateRevenueBudget(client_id, sentiment, revenue/budget)
            avgs_rate_revenue_budget.append(avg_rate_revenue_budget)
        return avgs_rate_revenue_budget
    
    def __clean_client_state(self, client_id):
        if client_id in self._revenue_budget_by_sentiment:
            self._revenue_budget_by_sentiment.pop(client_id)
    
    def __handle_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.ANALYZED_MOVIES_BATCH:
            analyzed_movies_batch = msg
            self.__update_revenues_budgets(analyzed_movies_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            for avg_rate_revenue_budget in self.__get_avgs_rate_revenue_budget_by_sentiment(eof.client_id):
                self._middleware.send_message(PacketSerde.serialize(avg_rate_revenue_budget))
                logging.debug(f"action: sent_avg_rate_revenue_budget | result: success | avg_rate_revenue_budget: {avg_rate_revenue_budget}")
            self._middleware.send_message(PacketSerde.serialize(EOF(eof.client_id)))
            logging.info("action: sent_eof | result: success")
            self.__clean_client_state(eof.client_id)
        elif msg.packet_type() == PacketType.CLIENT_DISCONNECTED:
            client_disconnected = msg
            logging.debug(f"action: client_disconnected | result: success | client_id: {client_disconnected.client_id}")
            self.__clean_client_state(client_disconnected.client_id)
            self._middleware.send_message(PacketSerde.serialize(client_disconnected))
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        self.start_receiving_health_checks()
        input_queues_and_callback_functions = [(input_queue[0], input_queue[1], self.__handle_packet) for input_queue in self._input_queues]
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()
