import socket
import logging

def close_socket(socket_to_close, socket_name):
    try:
        logging.info(f'action: close_{socket_name} | result: in_progress')
        socket_to_close.shutdown(socket.SHUT_RDWR)
        socket_to_close.close()
        logging.info(f'action: close_{socket_name} | result: success')
    except OSError:
        logging.error(f"action: close_{socket_name} | result: fail | socket already closed")
