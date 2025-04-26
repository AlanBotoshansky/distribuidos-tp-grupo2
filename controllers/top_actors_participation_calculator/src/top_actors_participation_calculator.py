import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.actor_participation import ActorParticipation

class TopActorsParticipationCalculator:
    def __init__(self, top_n_actors_participation, input_queues, output_exchange):
        self._top_n_actors_participation = top_n_actors_participation
        self._input_queues = input_queues
        self._output_exchange = output_exchange
        self._middleware = None
        self._actors_participation = {}
        
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
    
    def __update_actors_participation(self, movies_credits_batch):
        for movie_credit in movies_credits_batch.get_items():
            for actor in movie_credit.cast:
                self._actors_participation[actor] = self._actors_participation.get(actor, 0) + 1
    
    def __get_top_actors_participations(self):
        sorted_actors_participations = sorted(self._actors_participation.items(), key=lambda x: x[1], reverse=True)
        top_actors_participations = sorted_actors_participations[:self._top_n_actors_participation]
        return top_actors_participations
    
    def __handle_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIE_CREDITS_BATCH:
            movies_credits_batch = msg
            self.__update_actors_participation(movies_credits_batch)
        elif msg.packet_type() == PacketType.EOF:  
            eof = msg          
            for actor, participation in self.__get_top_actors_participations():
                actor_participation = ActorParticipation(eof.client_id, actor, participation)
                self._middleware.send_message(PacketSerde.serialize(actor_participation))
            self._middleware.send_message(PacketSerde.serialize(EOF(eof.client_id)))
            logging.info("action: sent_eof | result: success")
        else:
            logging.error(f"action: unexpected_packet_type | result: fail | packet_type: {msg.packet_type()}")

    def run(self):
        input_queues_and_callback_functions = [(input_queue[0], input_queue[1], self.__handle_packet) for input_queue in self._input_queues]
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()

        
        