from configparser import ConfigParser
import os
import yaml
import sys
from src.generate_compose import generate_docker_compose

def initialize_config():
    config = ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
    config.read(config_path)

    config_params = {}
    try:
        config_params["movies_filter_produced_in_argentina_and_spain"] = int(config["CLUSTER_SIZES"]["MOVIES_FILTER_PRODUCED_IN_ARGENTINA_AND_SPAIN"])
        config_params["movies_filter_released_between_2000_2009"] = int(config["CLUSTER_SIZES"]["MOVIES_FILTER_RELEASED_BETWEEN_2000_AND_2009"])
        config_params["movies_filter_by_one_production_country"] = int(config["CLUSTER_SIZES"]["MOVIES_FILTER_BY_ONE_PRODUCTION_COUNTRY"])
        config_params["movies_filter_produced_in_argentina"] = int(config["CLUSTER_SIZES"]["MOVIES_FILTER_PRODUCED_IN_ARGENTINA"])
        config_params["movies_filter_released_after_2000"] = int(config["CLUSTER_SIZES"]["MOVIES_FILTER_RELEASED_AFTER_2000"])
        config_params["movies_router_by_id"] = int(config["CLUSTER_SIZES"]["MOVIES_ROUTER_BY_ID"])
        config_params["movies_ratings_joiner"] = int(config["CLUSTER_SIZES"]["MOVIES_RATINGS_JOINER"])
        config_params["ratings_router_by_movie_id"] = int(config["CLUSTER_SIZES"]["RATINGS_ROUTER_BY_MOVIE_ID"])
        config_params["credits_router_by_movie_id"] = int(config["CLUSTER_SIZES"]["CREDITS_ROUTER_BY_MOVIE_ID"])
        config_params["movies_credits_joiner"] = int(config["CLUSTER_SIZES"]["MOVIES_CREDITS_JOINER"])
        config_params["movies_sentiment_analyzer"] = int(config["CLUSTER_SIZES"]["MOVIES_SENTIMENT_ANALYZER"])
    except KeyError as e:
        raise KeyError(f"Key was not found. Error: {e}")
    except ValueError as e:
        raise ValueError(f"Key could not be parsed. Error: {e}")

    return config_params

def main():
    config_params = initialize_config()
    try:
        docker_compose = generate_docker_compose(config_params)
        with open("docker-compose-dev.yaml", "w") as f:
            yaml.dump(docker_compose, f, sort_keys=False, default_flow_style=False)
    except Exception:
        sys.exit(1)

if __name__ == "__main__":
    main()
