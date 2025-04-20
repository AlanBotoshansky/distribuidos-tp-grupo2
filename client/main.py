#!/usr/bin/env python3

from configparser import ConfigParser
from src.client import Client
import logging
import os
from datetime import datetime


def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["server_ip_data"] = os.getenv('SERVER_IP_DATA', config["DEFAULT"]["SERVER_IP_DATA"])
        config_params["server_port_data"] = int(os.getenv('SERVER_PORT_DATA', config["DEFAULT"]["SERVER_PORT_DATA"]))
        config_params["server_ip_results"] = os.getenv('SERVER_IP_RESULTS', config["DEFAULT"]["SERVER_IP_RESULTS"])
        config_params["server_port_results"] = int(os.getenv('SERVER_PORT_RESULTS', config["DEFAULT"]["SERVER_PORT_RESULTS"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["movies_path"] = os.getenv('MOVIES_PATH', config["DEFAULT"]["MOVIES_PATH"])
        config_params["ratings_path"] = os.getenv('RATINGS_PATH', config["DEFAULT"]["RATINGS_PATH"])
        config_params["credits_path"] = os.getenv('CREDITS_PATH', config["DEFAULT"]["CREDITS_PATH"])
        config_params["ratings_batch_max_size"] = int(os.getenv('RATINGS_BATCH_MAX_SIZE', config["DEFAULT"]["RATINGS_BATCH_MAX_SIZE"]))
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

def main():
    config_params = initialize_config()
    server_ip_data = config_params["server_ip_data"]
    server_port_data = config_params["server_port_data"]
    server_ip_results = config_params["server_ip_results"]
    server_port_results = config_params["server_port_results"]
    logging_level = config_params["logging_level"]
    movies_path = config_params["movies_path"]
    ratings_path = config_params["ratings_path"]
    credits_path = config_params["credits_path"]
    ratings_batch_max_size = config_params["ratings_batch_max_size"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | server_ip_data: {server_ip_data} | server_port_data: {server_port_data} | "
                  f"server_ip_results: {server_ip_results} | server_port_results: {server_port_results} | logging_level: {logging_level} | "
                  f"movies_path: {movies_path} | ratings_path: {ratings_path} | credits_path: {credits_path} | ratings_batch_max_size: {ratings_batch_max_size}")
    
    # Initialize client
    client = Client(server_ip_data, server_port_data, server_ip_results, server_port_results, movies_path, ratings_path, credits_path, ratings_batch_max_size)
    now = datetime.now()
    client.run()
    elapsed_time = datetime.now() - now
    logging.info(f"action: client | result: success | elapsed_time: {elapsed_time.total_seconds()} seconds")    

if __name__ == "__main__":
    main()
