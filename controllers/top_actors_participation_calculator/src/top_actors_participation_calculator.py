import signal
import logging
from middleware.middleware import Middleware
from messages.eof import EOF
from messages.packet_serde import PacketSerde
from messages.packet_type import PacketType
from messages.actor_participation import ActorParticipation
from common.monitorable import Monitorable
from storage_adapter.storage_adapter import StorageAdapter

ACTORS_PARTICIPATION_FILE_KEY = "actors_participation"

class TopActorsParticipationCalculator(Monitorable):
    def __init__(self, top_n_actors_participation, input_queues, output_exchange, storage_path):
        self._top_n_actors_participation = top_n_actors_participation
        self._input_queues = input_queues
        self._output_exchange = output_exchange
        self._middleware = None
        self._actors_participation = {}
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
        actors_participation = self._storage_adapter.load_data(ACTORS_PARTICIPATION_FILE_KEY)
        if actors_participation:
            self._actors_participation = actors_participation
            logging.debug(f"action: load_state_from_storage | result: success | actors_participation: {self._actors_participation}")
    
    def __update_actors_participation(self, movies_credits_batch):
        client_id = movies_credits_batch.client_id
        self._actors_participation[client_id] = self._actors_participation.get(client_id, {})
        for movie_credit in movies_credits_batch.get_items():
            for actor in movie_credit.cast:
                self._actors_participation[client_id][actor] = self._actors_participation[client_id].get(actor, 0) + 1
        self._storage_adapter.update(ACTORS_PARTICIPATION_FILE_KEY, self._actors_participation[client_id], secondary_file_key=client_id)
    
    def __get_top_actors_participations(self, client_id):
        sorted_actors_participations = sorted(self._actors_participation[client_id].items(), key=lambda x: x[1], reverse=True)
        top_actors_participations = sorted_actors_participations[:self._top_n_actors_participation]
        return top_actors_participations
    
    def __clean_client_state(self, client_id):
        if client_id in self._actors_participation:
            self._actors_participation.pop(client_id)
            self._storage_adapter.delete(ACTORS_PARTICIPATION_FILE_KEY, secondary_file_key=client_id)
    
    def __handle_packet(self, packet):
        msg = PacketSerde.deserialize(packet)
        if msg.packet_type() == PacketType.MOVIE_CREDITS_BATCH:
            movies_credits_batch = msg
            self.__update_actors_participation(movies_credits_batch)
        elif msg.packet_type() == PacketType.EOF:  
            eof = msg
            for actor, participation in self.__get_top_actors_participations(eof.client_id):
                actor_participation = ActorParticipation(eof.client_id, actor, participation)
                self._middleware.send_message(PacketSerde.serialize(actor_participation))
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
        self.__load_state_from_storage()
        input_queues_and_callback_functions = [(input_queue[0], input_queue[1], self.__handle_packet) for input_queue in self._input_queues]
        self._middleware = Middleware(input_queues_and_callback_functions=input_queues_and_callback_functions,
                                      output_exchange=self._output_exchange,
                                     )
        self._middleware.handle_messages()
