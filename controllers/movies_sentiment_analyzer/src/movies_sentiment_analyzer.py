import signal
import logging
from textblob import TextBlob
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.analyzed_movie import Sentiment, AnalyzedMovie
from messages.analyzed_movies_batch import AnalyzedMoviesBatch
from common.monitorable import Monitorable
from common.failure_simulation import fail_with_probability

ANALYSIS_TYPE = "sentiment-analysis"
OVERVIEW_FIELD = 'overview'

class MoviesSentimentAnalyzer(Monitorable):
    def __init__(self, field_to_analyze, input_queues, output_exchange, failure_probability, cluster_size, id):
        self._field_to_analyze = field_to_analyze
        self._input_queues = input_queues
        self._output_exchange = output_exchange
        self._failure_probability = failure_probability
        self._cluster_size = cluster_size
        self._id = id
        self._analyzer = None
        self._middleware = None
        
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
    
    def __analyze_movies(self, movies_batch):
        analyzed_movies = []
        if self._field_to_analyze == OVERVIEW_FIELD:
            get_text = lambda movie: movie.overview
        else:
            raise ValueError(f"Unknown field to analyze: {self._field_to_analyze}")
        
        for movie in movies_batch.get_items():
            text = get_text(movie)
            polarity = TextBlob(text).sentiment.polarity
            sentiment = Sentiment.from_polarity(polarity)
            analyzed_movie = AnalyzedMovie(movie.revenue, movie.budget, sentiment)
            analyzed_movies.append(analyzed_movie)
        
        return analyzed_movies
    
    def __analyze_movies_sentiment(self, movies_batch):
        analyzed_movies = self.__analyze_movies(movies_batch)
        analyzed_movies_batch = AnalyzedMoviesBatch(movies_batch.client_id, analyzed_movies, message_id=movies_batch.message_id)
        self._middleware.send_message(PacketSerde.serialize(analyzed_movies_batch))
        logging.debug(f"action: movies_batch_analyzed | result: success | analyzed_movies_batch: {analyzed_movies_batch}")
    
    def __handle_packet(self, packet):
        fail_with_probability(self._failure_probability, "before sending message")
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIES_BATCH:
            movies_batch = msg
            self.__analyze_movies_sentiment(movies_batch)
        elif msg.packet_type() == PacketType.EOF:
            eof = msg
            eof.add_seen_id(self._id)
            if len(eof.seen_ids) == self._cluster_size:
                self._middleware.send_message(PacketSerde.serialize(EOF(eof.client_id, message_id=eof.message_id)))
                logging.info("action: sent_eof | result: success")
            else:
                self._middleware.reenqueue_message(PacketSerde.serialize(eof))
        elif msg.packet_type() == PacketType.CLIENT_DISCONNECTED:
            client_disconnected = msg
            logging.debug(f"action: client_disconnected | result: success | client_id: {client_disconnected.client_id}")
            self._middleware.send_message(PacketSerde.serialize(client_disconnected))
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")
        fail_with_probability(self._failure_probability, "after sending message")

    def run(self):
        self.start_receiving_health_checks()
        input_queues_and_callback_functions = [(input_queue[0], input_queue[1], self.__handle_packet) for input_queue in self._input_queues]
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()
