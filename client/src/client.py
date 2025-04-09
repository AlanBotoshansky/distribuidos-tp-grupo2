import logging
import socket

class Client:
    
    def __init__(self, server_ip, server_port, movies_path, ratings_path, credits_path):
        self._server_ip = server_ip
        self._server_port = server_port
        self._movies_path = movies_path
        self._ratings_path = ratings_path
        self._credits_path = credits_path
        
    def run(self):
        logging.info(f"Connecting to server at {self._server_ip}:{self._server_port}")
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self._server_ip, self._server_port))
            logging.info("Connection established successfully.")
            client_socket.sendall(b"Hello, server!")
            response = client_socket.recv(1024)
            logging.info(f"Received response from server: {response.decode('utf-8')}")
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            