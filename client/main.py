#!/usr/bin/env python3

from configparser import ConfigParser
from src.client import Client
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
        config_params["server_ip"] = os.getenv('SERVER_IP', config["DEFAULT"]["SERVER_IP"])
        config_params["server_port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["movies_path"] = os.getenv('MOVIES_PATH', config["DEFAULT"]["MOVIES_PATH"])
        config_params["ratings_path"] = os.getenv('RATINGS_PATH', config["DEFAULT"]["RATINGS_PATH"])
        config_params["credits_path"] = os.getenv('CREDITS_PATH', config["DEFAULT"]["CREDITS_PATH"])
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
    server_ip = config_params["server_ip"]
    server_port = config_params["server_port"]
    logging_level = config_params["logging_level"]
    movies_path = config_params["movies_path"]
    ratings_path = config_params["ratings_path"]
    credits_path = config_params["credits_path"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | server_ip: {server_ip} | server_port: {server_port} | "
                  f"logging_level: {logging_level}")
    
    # Initialize client
    client = Client(server_ip, server_port, movies_path, ratings_path, credits_path)
    client.run()

if __name__ == "__main__":
    main()
