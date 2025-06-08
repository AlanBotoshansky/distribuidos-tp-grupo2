import socket
import logging
import time

ATTEMPTS_TO_CONNECT_TO_SERVER = 5

def connect_to_server(server_ip, server_port, socket_name):
        attempts = 0
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while True:
            try:
                logging.info(f"Connecting to server at {server_ip}:{server_port} | attempt: {attempts + 1}")
                sock.connect((server_ip, server_port))
                logging.info(f"action: connect_to_{socket_name} | result: success")
                return sock
            except OSError as e:
                attempts += 1
                if attempts == ATTEMPTS_TO_CONNECT_TO_SERVER:
                    logging.error(f"action: connect_to_{socket_name} | result: fail | error: {e} | attempts: {ATTEMPTS_TO_CONNECT_TO_SERVER}")
                    return
                retry_wait_time = 2 ** attempts
                time.sleep(retry_wait_time)