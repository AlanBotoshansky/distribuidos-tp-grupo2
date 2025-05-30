import socket
import signal
import logging
import docker
import time
from common.monitorable import Monitorable

HEALTH_CHECK_TIMEOUT = 5

class HealthGuard(Monitorable):
    def __init__(self, health_check_port, seconds_between_health_checks, compose_project_name, dont_guard_containers, service_prefix, cluster_size, id):
        self._health_check_port = health_check_port
        self._seconds_between_health_checks = seconds_between_health_checks
        self._compose_project_name = compose_project_name
        self._dont_guard_containers = dont_guard_containers
        self._service_prefix = service_prefix
        self._cluster_size = cluster_size
        self._id = id
        self._shutdown_requested = False
        self._docker_client = docker.from_env()
        self._health_check_socket = None
        
        signal.signal(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self, signalnum, frame):
        """
        Signal handler for graceful shutdown
        """
        if signalnum == signal.SIGTERM:
            logging.info('action: signal_received | result: success | signal: SIGTERM')
            self._shutdown_requested = True
            self.__cleanup()
            
    def __cleanup(self):
        """
        Cleanup resources during shutdown
        """
        self._health_check_socket.close()
        self.stop_receiving_health_checks()
    
    def _should_guard(self, container):
        if container.name.startswith(self._service_prefix):
            health_guard_id = int(container.name.split("_")[-1])
            return health_guard_id == self._id % self._cluster_size + 1
        for dgc in self._dont_guard_containers:
            if container.name.startswith(dgc):
                return False
        return hash(container.name) % self._cluster_size + 1 == self._id
    
    def _guard(self, container):
        """
        Perform health check on the container. Revive if necessary.
        """
        try:
            logging.debug(f"action: health_check | result: in_progress | container: {container.name}")
            self._health_check_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._health_check_socket.settimeout(HEALTH_CHECK_TIMEOUT)
            self._health_check_socket.connect((container.name, self._health_check_port))
            self._health_check_socket.close()
            logging.debug(f"action: health_check | result: success | container: {container.name}")
        except (socket.error, socket.timeout) as e:
            if self._shutdown_requested:
                return
            logging.error(f"action: health_check | result: failure | container: {container.name} | error: {e}")
            logging.info(f"action: revive | result: in_progress | container: {container.name}")
            try:
                container.restart()
                logging.info(f"action: revive | result: success | container: {container.name}")
            except docker.errors.APIError as e:
                logging.error(f"action: revive | result: failure | container: {container.name} | error: {e}")

    def run(self):
        self.start_receiving_health_checks()
        list_filters = {
            "label": f"com.docker.compose.project={self._compose_project_name}",
        }
        while not self._shutdown_requested:
            containers = self._docker_client.containers.list(all=True, filters=list_filters)
            for container in containers:
                if self._shutdown_requested:
                    return
                if self._should_guard(container):
                    self._guard(container)
                    time.sleep(self._seconds_between_health_checks)
