#!/usr/bin/env python3

from configparser import ConfigParser
from src.top_actors_participation_calculator import TopActorsParticipationCalculator
import logging
import os
import ast

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
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["top_n_actors_participation"] = int(os.getenv('TOP_N_ACTORS_PARTICIPATION'))
        config_params["input_queues"] = ast.literal_eval(os.getenv('INPUT_QUEUES'))
        config_params["output_exchange"] = os.getenv('OUTPUT_EXCHANGE')
        config_params["failure_probability"] = float(os.getenv('FAILURE_PROBABILITY'))
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
    logging_level = config_params["logging_level"]
    top_n_actors_participation = config_params["top_n_actors_participation"]
    input_queues = config_params["input_queues"]
    output_exchange = config_params["output_exchange"]
    failure_probability = config_params["failure_probability"]
    storage_path = config_params["storage_path"]
    
    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | logging_level: {logging_level} | top_n_actors_participation: {top_n_actors_participation} | input_queues: {input_queues} | output_exchange: {output_exchange} | failure_probability: {failure_probability} | storage_path: {storage_path}")

    top_actors_participation_calculator = TopActorsParticipationCalculator(top_n_actors_participation, input_queues, output_exchange, failure_probability, storage_path)
    top_actors_participation_calculator.run()

if __name__ == "__main__":
    main()
