from configparser import ConfigParser
import os
import yaml
import sys
from src.generate_compose import generate_docker_compose

class InvalidProbabilityError(Exception):
    pass

def parse_failure_probabilities(failure_probabilities):
    parsed_failure_probabilities = {}
    for controller, probability_str in failure_probabilities.items():
        try:
            probability = float(probability_str)
            if not (0 <= probability <= 1):
                raise InvalidProbabilityError(f"Probability for {controller} must be between 0 and 1.")
            parsed_failure_probabilities[controller] = probability
        except ValueError:
            raise ValueError(f"Invalid probability value for {controller}: {probability_str}")
    return parsed_failure_probabilities
        

def initialize_config():
    config = ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
    config.read(config_path)
    
    failure_probabilities = parse_failure_probabilities(dict(config["FAILURE_PROBABILITIES"]))
    config_params = {}
    try:
        config_params["movies_filter_produced_in_argentina_and_spain"] = int(config["CLUSTER_SIZES"]["MOVIES_FILTER_PRODUCED_IN_ARGENTINA_AND_SPAIN"])
        config_params["movies_filter_released_between_2000_2009"] = int(config["CLUSTER_SIZES"]["MOVIES_FILTER_RELEASED_BETWEEN_2000_2009"])
        config_params["movies_filter_by_one_production_country"] = int(config["CLUSTER_SIZES"]["MOVIES_FILTER_BY_ONE_PRODUCTION_COUNTRY"])
        config_params["movies_filter_produced_in_argentina"] = int(config["CLUSTER_SIZES"]["MOVIES_FILTER_PRODUCED_IN_ARGENTINA"])
        config_params["movies_filter_released_after_2000"] = int(config["CLUSTER_SIZES"]["MOVIES_FILTER_RELEASED_AFTER_2000"])
        config_params["movies_router_by_id"] = int(config["CLUSTER_SIZES"]["MOVIES_ROUTER_BY_ID"])
        config_params["movies_ratings_joiner"] = int(config["CLUSTER_SIZES"]["MOVIES_RATINGS_JOINER"])
        config_params["ratings_router_by_movie_id"] = int(config["CLUSTER_SIZES"]["RATINGS_ROUTER_BY_MOVIE_ID"])
        config_params["credits_router_by_movie_id"] = int(config["CLUSTER_SIZES"]["CREDITS_ROUTER_BY_MOVIE_ID"])
        config_params["movies_credits_joiner"] = int(config["CLUSTER_SIZES"]["MOVIES_CREDITS_JOINER"])
        config_params["movies_sentiment_analyzer"] = int(config["CLUSTER_SIZES"]["MOVIES_SENTIMENT_ANALYZER"])
        config_params["health_guard"] = int(config["CLUSTER_SIZES"]["HEALTH_GUARD"])
        
        config_params["clients"] = int(config["CLIENTS"]["CLIENTS"])
        
        config_params["failure_probabilities"] = {}
        config_params["failure_probabilities"]["movies_filter_produced_in_argentina_and_spain"] = failure_probabilities.get("movies_filter_produced_in_argentina_and_spain", 0.0)
        config_params["failure_probabilities"]["movies_filter_released_between_2000_2009"] = failure_probabilities.get("movies_filter_released_between_2000_2009", 0.0)
        config_params["failure_probabilities"]["movies_filter_by_one_production_country"] = failure_probabilities.get("movies_filter_by_one_production_country", 0.0)
        config_params["failure_probabilities"]["movies_filter_produced_in_argentina"] = failure_probabilities.get("movies_filter_produced_in_argentina", 0.0)
        config_params["failure_probabilities"]["movies_filter_released_after_2000"] = failure_probabilities.get("movies_filter_released_after_2000", 0.0)
        config_params["failure_probabilities"]["movies_sentiment_analyzer"] = failure_probabilities.get("movies_sentiment_analyzer", 0.0)
        config_params["failure_probabilities"]["movies_router_by_id"] = failure_probabilities.get("movies_router_by_id", 0.0)
        config_params["failure_probabilities"]["ratings_router_by_movie_id"] = failure_probabilities.get("ratings_router_by_movie_id", 0.0)
        config_params["failure_probabilities"]["credits_router_by_movie_id"] = failure_probabilities.get("credits_router_by_movie_id", 0.0)
        config_params["failure_probabilities"]["movies_ratings_joiner"] = failure_probabilities.get("movies_ratings_joiner", 0.0)
        config_params["failure_probabilities"]["movies_credits_joiner"] = failure_probabilities.get("movies_credits_joiner", 0.0)
        config_params["failure_probabilities"]["top_investor_countries_calculator"] = failure_probabilities.get("top_investor_countries_calculator", 0.0)
        config_params["failure_probabilities"]["most_least_rated_movies_calculator"] = failure_probabilities.get("most_least_rated_movies_calculator", 0.0)
        config_params["failure_probabilities"]["top_actors_participation_calculator"] = failure_probabilities.get("top_actors_participation_calculator", 0.0)
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
