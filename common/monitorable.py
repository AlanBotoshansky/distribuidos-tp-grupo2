import logging
import multiprocessing as mp
from common.health_checks_receiver import HealthChecksReceiver

class Monitorable:
    def start_receiving_health_checks(self):
        self._health_checks_receiver_process = mp.Process(target=self.__receive_health_checks)
        self._health_checks_receiver_process.start()
        logging.info("action: health_checks_receiver_started | result: success")
        
    def __receive_health_checks(self):
        health_checks_receiver = HealthChecksReceiver()
        health_checks_receiver.start()
        
    def stop_receiving_health_checks(self):
        p = getattr(self, '_health_checks_receiver_process', None)
        if p:
            p.terminate()
            p.join()
            logging.info("action: health_checks_receiver_terminated | result: success")
            