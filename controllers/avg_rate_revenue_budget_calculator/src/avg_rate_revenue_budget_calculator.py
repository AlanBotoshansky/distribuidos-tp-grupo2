import signal
import logging
import uuid
import time
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.analyzed_movie import Sentiment
from messages.avg_rate_revenue_budget import AvgRateRevenueBudget
from common.monitorable import Monitorable
from storage_adapter.storage_adapter import StorageAdapter
from common.failure_simulation import fail_with_probability

STATE_FILE_KEY = "state"
REVENUE_BUDGET_BY_SENTIMENT = "revenue_budget_by_sentiment"
PROCESSED_MESSAGE_IDS= "processed_message_ids"
MAX_PROCESSED_MESSAGE_IDS = 500

class AvgRateRevenueBudgetCalculator(Monitorable):
    def __init__(self, input_queues, output_exchange, failure_probability, storage_path):
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
            
    def __generate_deterministic_uuid(self, message_id, sentiment_value):
        """
        Generate a deterministic UUID based on the message ID and sentiment_value.
        """
        return str(uuid.uuid5(uuid.UUID(message_id), str(sentiment_value)))
    
    def __save_processed_message_id(self, client_id, message_id):
        self._state[client_id][PROCESSED_MESSAGE_IDS][message_id] = time.time_ns()
        if len(self._state[client_id][PROCESSED_MESSAGE_IDS]) > MAX_PROCESSED_MESSAGE_IDS:
            oldest_message_id = min(self._state[client_id][PROCESSED_MESSAGE_IDS], key=self._state[client_id][PROCESSED_MESSAGE_IDS].get)
            self._state[client_id][PROCESSED_MESSAGE_IDS].pop(oldest_message_id)
    
    def __update_revenues_budgets(self, analyzed_movies_batch):
        client_id = analyzed_movies_batch.client_id
        self._state[client_id] = self._state.get(client_id, {REVENUE_BUDGET_BY_SENTIMENT: {}, PROCESSED_MESSAGE_IDS: {}})
        if analyzed_movies_batch.message_id in self._state[client_id][PROCESSED_MESSAGE_IDS]:
            return
        for analyzed_movie in analyzed_movies_batch.get_items():
            revenue, budget, sentiment_value = analyzed_movie.revenue, analyzed_movie.budget, analyzed_movie.sentiment.value
            if revenue == 0 or budget == 0:
                continue
            revenue_sum, budget_sum = self._state[client_id][REVENUE_BUDGET_BY_SENTIMENT].get(sentiment_value, (0, 0))
            revenue_sum += revenue
            budget_sum += budget
            self._state[client_id][REVENUE_BUDGET_BY_SENTIMENT][sentiment_value] = (revenue_sum, budget_sum)
        self.__save_processed_message_id(client_id, analyzed_movies_batch.message_id)
        self._storage_adapter.update(STATE_FILE_KEY, self._state[client_id], secondary_file_key=client_id)
    
    def __get_avgs_rate_revenue_budget_by_sentiment(self, eof):
        client_id = eof.client_id
        if client_id not in self._state:
            return []
        avgs_rate_revenue_budget = []
        for sentiment_value, (revenue, budget) in self._state[client_id][REVENUE_BUDGET_BY_SENTIMENT].items():
            new_message_id = self.__generate_deterministic_uuid(eof.message_id, sentiment_value)
            avg_rate_revenue_budget = AvgRateRevenueBudget(client_id, Sentiment(sentiment_value), revenue/budget, message_id=new_message_id)
            avgs_rate_revenue_budget.append(avg_rate_revenue_budget)
        return avgs_rate_revenue_budget
    
    def __clean_client_state(self, client_id):
        if client_id in self._state:
            self._state.pop(client_id)
            self._storage_adapter.delete(STATE_FILE_KEY, secondary_file_key=client_id)
    
    def __handle_packet(self, packet):
        fail_with_probability(self._failure_probability, "before handling packet")
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.ANALYZED_MOVIES_BATCH:
            analyzed_movies_batch = msg
            self.__update_revenues_budgets(analyzed_movies_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            for avg_rate_revenue_budget in self.__get_avgs_rate_revenue_budget_by_sentiment(eof):
                self._middleware.send_message(PacketSerde.serialize(avg_rate_revenue_budget))
                logging.debug(f"action: sent_avg_rate_revenue_budget | result: success | avg_rate_revenue_budget: {avg_rate_revenue_budget}")
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
