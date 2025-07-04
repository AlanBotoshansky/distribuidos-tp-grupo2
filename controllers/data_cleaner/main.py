#!/usr/bin/env python3

from configparser import ConfigParser
from src.data_cleaner import DataCleaner
import logging
import os

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
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["listen_backlog"] = int(os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SERVER_LISTEN_BACKLOG"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["movies_exchange"] = os.getenv('MOVIES_EXCHANGE')
        config_params["ratings_exchange"] = os.getenv('RATINGS_EXCHANGE')
        config_params["credits_exchange"] = os.getenv('CREDITS_EXCHANGE')
        config_params["max_concurrent_clients"] = int(os.getenv('MAX_CONCURRENT_CLIENTS', config["DEFAULT"]["MAX_CONCURRENT_CLIENTS"]))
        config_params["storage_path"] = os.getenv('STORAGE_PATH')
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

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
    port = config_params["port"]
    listen_backlog = config_params["listen_backlog"]
    logging_level = config_params["logging_level"]
    movies_exchange = config_params["movies_exchange"]
    ratings_exchange = config_params["ratings_exchange"]
    credits_exchange = config_params["credits_exchange"]
    max_concurrent_clients = config_params["max_concurrent_clients"]
    storage_path = config_params["storage_path"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | port: {port} | "
                  f"listen_backlog: {listen_backlog} | logging_level: {logging_level} | movies_exchange: {movies_exchange} | ratings_exchange: {ratings_exchange} | credits_exchange: {credits_exchange} | max_concurrent_clients: {max_concurrent_clients} | storage_path: {storage_path}")

    data_cleaner = DataCleaner(port, listen_backlog, movies_exchange, ratings_exchange, credits_exchange, max_concurrent_clients, storage_path)
    data_cleaner.run()

if __name__ == "__main__":
    main()
