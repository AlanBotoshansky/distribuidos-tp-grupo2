import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.avg_rate_revenue_budget import AvgRateRevenueBudget

class AvgRateRevenueBudgetCalculator:
    def __init__(self, input_queues, output_exchange):
        self._input_queues = input_queues
        self._output_exchange = output_exchange
        self._middleware = None
        self.revenue_budget_by_sentiment = {}
        
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
    
    def __update_revenues_budgets(self, analyzed_movies_batch):
        for analyzed_movie in analyzed_movies_batch.get_items():
            revenue, budget, sentiment = analyzed_movie.revenue, analyzed_movie.budget, analyzed_movie.sentiment
            if revenue == 0 or budget == 0:
                continue
            revenue_sum, budget_sum = self.revenue_budget_by_sentiment.get(sentiment, (0, 0))
            revenue_sum += revenue
            budget_sum += budget
            self.revenue_budget_by_sentiment[sentiment] = (revenue_sum, budget_sum)
    
    def __get_avgs_rate_revenue_budget_by_sentiment(self):
        avgs_rate_revenue_budget = []
        for sentiment, (revenue, budget) in self.revenue_budget_by_sentiment.items():
            avg_rate_revenue_budget = AvgRateRevenueBudget(sentiment, revenue/budget)
            avgs_rate_revenue_budget.append(avg_rate_revenue_budget)
        return avgs_rate_revenue_budget
    
    def __handle_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.ANALYZED_MOVIES_BATCH:
            analyzed_movies_batch = msg
            self.__update_revenues_budgets(analyzed_movies_batch)
        elif msg.packet_type() == PacketType.EOF:            
            for avg_rate_revenue_budget in self.__get_avgs_rate_revenue_budget_by_sentiment():
                self._middleware.send_message(PacketSerde.serialize(avg_rate_revenue_budget))
                logging.debug(f"action: sent_avg_rate_revenue_budget | result: success | avg_rate_revenue_budget: {avg_rate_revenue_budget}")
            self._middleware.send_message(PacketSerde.serialize(EOF()))
            logging.info("action: sent_eof | result: success")
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        self._middleware = Middleware(callback_function=self.__handle_packet,
                                      input_queues=self._input_queues,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()

        
        