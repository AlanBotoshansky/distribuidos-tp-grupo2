#!/usr/bin/env python3

from configparser import ConfigParser
from src.movies_filter import MoviesFilter
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
        config_params["filter_field"] = os.getenv('FILTER_FIELD')
        config_params["filter_values"] = ast.literal_eval(os.getenv('FILTER_VALUES'))
        config_params["output_fields_subset"] = ast.literal_eval(os.getenv('OUTPUT_FIELDS_SUBSET'))
        config_params["input_queues"] = ast.literal_eval(os.getenv('INPUT_QUEUES'))
        config_params["output_exchange"] = os.getenv('OUTPUT_EXCHANGE')
        config_params["cluster_size"] = int(os.getenv('CLUSTER_SIZE'))
        config_params["id"] = os.getenv('ID')
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
    filter_field = config_params["filter_field"]
    filter_values = config_params["filter_values"]
    output_fields_subset = config_params["output_fields_subset"]
    input_queues = config_params["input_queues"]
    output_exchange = config_params["output_exchange"]
    cluster_size = config_params["cluster_size"]
    id = config_params["id"]
    
    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | logging_level: {logging_level} | filter_field: {filter_field} | filter_values: {filter_values} | output_fields_subset: {output_fields_subset} | input_queues: {input_queues} | output_exchange: {output_exchange}")

    movies_filter = MoviesFilter(filter_field, filter_values, output_fields_subset, input_queues, output_exchange, cluster_size, id)
    movies_filter.run()

if __name__ == "__main__":
    main()
