COMPOSE_PROJECT_NAME = 'tp'
NETWORK_NAME = 'testing_net'
STORAGE_PATH = '/storage'

def generate_service(name, image, container_name=None, environment=None, volumes=None, networks=None, depends_on=None):
    """
    Generic function to generate a service configuration for docker-compose.
    
    Args:
        name: Name of the service
        image: Docker image name
        container_name: Container name (defaults to name if not provided)
        environment: List of environment variables
        volumes: List of volume mappings
        networks: List of network names
        depends_on: List of services this service depends on
    
    Returns:
        Dictionary with service configuration
    """
    service = {
        "container_name": container_name if container_name else name,
        "image": f"{image}:latest"
    }
    
    if environment:
        service["environment"] = environment
    
    if volumes:
        service["volumes"] = volumes
    
    if networks:
        service["networks"] = networks
    
    if depends_on:
        service["depends_on"] = depends_on
    
    return service

def generate_cluster(cluster_size, service_prefix, image, container_prefix=None, environment=[], volumes=None, networks=None, depends_on=None):
    """
    Generic function to generate a cluster of services
    
    Args:
        cluster_size: Number of instances to create
        service_prefix: Prefix for the service names
        image: Docker image name
        container_prefix: Prefix for the container names
        environment: List of environment variables
        volumes: List of volume mappings
        networks: List of network names
        depends_on: List of services this service depends on
        
    Returns:
        Dictionary mapping service names to their configurations
    """
    services = {}
    
    for i in range(1, cluster_size + 1):
        service_name = f"{service_prefix}_{i}"
        container_name = f"{container_prefix}_{i}" if container_prefix else service_name
        service_environment = environment.copy()
        service_environment.append(f"CLUSTER_SIZE={cluster_size}")
        service_environment.append(f"ID={i}")
        services[service_name] = generate_service(
            name=service_name,
            image=image,
            container_name=container_name,
            environment=service_environment,
            volumes=volumes,
            networks=networks,
            depends_on=depends_on
        )
    
    return services

def generate_filter_cluster(cluster_size, service_prefix, filter_field, filter_values, output_fields_subset, input_queues, output_exchange, failure_probability):
    """
    Generic function to generate a cluster of filter services
    
    Args:
        cluster_size: Number of instances to create
        service_prefix: Prefix for the service names
        filter_field: Field to filter on
        filter_values: Values to filter for
        output_fields_subset: Fields to include in output
        input_queues: Input queues configuration
        output_exchange: Output exchange name
        failure_probability: Probability of service failure
        
    Returns:
        Dictionary mapping service names to their configurations
    """    
    return generate_cluster(
        cluster_size=cluster_size,
        service_prefix=service_prefix,
        image="movies_filter",
        environment=[
            "PYTHONUNBUFFERED=1",
            f"FILTER_FIELD={filter_field}",
            f"FILTER_VALUES={filter_values}",
            f"OUTPUT_FIELDS_SUBSET={output_fields_subset}",
            f"INPUT_QUEUES={input_queues}",
            f"OUTPUT_EXCHANGE={output_exchange}",
            f"FAILURE_PROBABILITY={failure_probability}",
        ],
        volumes=[
            "./controllers/movies_filter/config.ini:/config.ini"
        ],
        networks=[
            NETWORK_NAME
        ]
    )

def generate_routing_cluster(cluster_size, service_prefix, input_queues, output_exchange_prefixes_and_dest_nodes_amount, failure_probability):
    """
    Generic function to generate a cluster of routing services
    
    Args:
        cluster_size: Number of instances to create
        destination_nodes_amount: Number of destination nodes
        service_prefix: Prefix for the service names
        input_queues: Input queues configuration
        output_exchange_prefixes_and_dest_nodes_amount: Output exchange prefixes and their destination nodes amount
        failure_probability: Probability of service failure
        
    Returns:
        Dictionary mapping service names to their configurations
    """    
    return generate_cluster(
        cluster_size=cluster_size,
        service_prefix=service_prefix,
        image="router",
        environment=[
            "PYTHONUNBUFFERED=1",
            f"INPUT_QUEUES={input_queues}",
            f"OUTPUT_EXCHANGE_PREFIXES_AND_DEST_NODES_AMOUNT={output_exchange_prefixes_and_dest_nodes_amount}",
            f"FAILURE_PROBABILITY={failure_probability}",
        ],
        volumes=[
            "./controllers/router/config.ini:/config.ini"
        ],
        networks=[
            NETWORK_NAME
        ]
    )

def generate_movies_joiner_cluster(cluster_size, service_prefix, input_queues_prefixes, output_exchange, failure_probability):
    """
    Generic function to generate a cluster of joiner services
    
    Args:
        cluster_size: Number of instances to create
        service_prefix: Prefix for the service names
        input_queues_prefixes: List of input queues prefixes
        output_exchange: Output exchange name
        failure_probability: Probability of service failure
        
    Returns:
        Dictionary mapping service names to their configurations
    """
    services = {}
    cluster_size = int(cluster_size)
    
    for i in range(1, cluster_size + 1):
        service_name = f"{service_prefix}_{i}"
        input_queues = f"[{', '.join([f'("{prefix}_{i}", "{prefix}_{i}")' for prefix in input_queues_prefixes])}]"
        services[service_name] = generate_service(
            name=service_name,
            image='movies_joiner',
            environment=[
                "PYTHONUNBUFFERED=1",
                f"INPUT_QUEUES={input_queues}",
                f"OUTPUT_EXCHANGE={output_exchange}",
                f"FAILURE_PROBABILITY={failure_probability}",
                f"CLUSTER_SIZE={cluster_size}",
                f"ID={i}",
                f"STORAGE_PATH={STORAGE_PATH}"
            ],
            volumes=[
                "./controllers/movies_joiner/config.ini:/config.ini",
                f"{service_name}_storage:{STORAGE_PATH}"
            ],
            networks=[
                NETWORK_NAME
            ]
        )
    
    return services

def generate_movies_sentiment_analyzer_cluster(cluster_size, service_prefix, field_to_analyze, input_queues, output_exchange, failure_probability):
    """
    Generic function to generate a cluster of sentiment analyzer services
    
    Args:
        cluster_size: Number of instances to create
        service_prefix: Prefix for the service names
        field_to_analyze: Field to analyze
        input_queues: Input queues configuration
        output_exchange: Output exchange name
        failure_probability: Probability of service failure
        
    Returns:
        Dictionary mapping service names to their configurations
    """
    return generate_cluster(
        cluster_size=cluster_size,
        service_prefix=service_prefix,
        image="movies_sentiment_analyzer",
        environment=[
            "PYTHONUNBUFFERED=1",
            f"FIELD_TO_ANALYZE={field_to_analyze}",
            f"INPUT_QUEUES={input_queues}",
            f"OUTPUT_EXCHANGE={output_exchange}",
            f"FAILURE_PROBABILITY={failure_probability}",
        ],
        volumes=[
            "./controllers/movies_sentiment_analyzer/config.ini:/config.ini"
        ],
        networks=[
            NETWORK_NAME
        ]
    )

def generate_data_cleaner():
    """Generate data_cleaner service configuration"""
    service_name = "data_cleaner"
    return generate_service(
        name=service_name,
        image="data_cleaner",
        environment=[
            "PYTHONUNBUFFERED=1",
            "MOVIES_EXCHANGE=movies",
            "RATINGS_EXCHANGE=ratings",
            "CREDITS_EXCHANGE=credits",
            f"STORAGE_PATH={STORAGE_PATH}"
        ],
        volumes=[
            "./controllers/data_cleaner/config.ini:/config.ini",
            f"{service_name}_storage:{STORAGE_PATH}"
        ],
        networks=[
            NETWORK_NAME
        ]
    )

def generate_results_handler():
    """Generate results_handler service configuration"""
    return generate_service(
        name="results_handler",
        image="results_handler",
        environment=[
            "PYTHONUNBUFFERED=1",
            'INPUT_QUEUES=[("movies_produced_in_argentina_and_spain_released_between_2000_2009", "movies_produced_in_argentina_and_spain_released_between_2000_2009"), ("top_investor_countries", "top_investor_countries"), ("most_least_rated_movies_produced_in_argentina_released_after_2000", "most_least_rated_movies_produced_in_argentina_released_after_2000"), ("top_actors_participation_movies_produced_in_argentina_released_after_2000", "top_actors_participation_movies_produced_in_argentina_released_after_2000"), ("avg_rate_revenue_budget_by_sentiment", "avg_rate_revenue_budget_by_sentiment")]'
        ],
        volumes=[
            "./controllers/results_handler/config.ini:/config.ini"
        ],
        networks=[
            NETWORK_NAME
        ]
    )

def generate_clients(n):
    """Generate client service configuration for multiple clients"""
    clients = {}
    n = int(n)
    
    for i in range(1, n + 1):
        client_name = f"client_{i}"
        clients[client_name] = generate_service(
            name=client_name,
            image="client",
            environment=[
                "PYTHONUNBUFFERED=1",
                f"RESULTS_DIR=/results/client_{i}",
            ],
            volumes=[
                "./client/config.ini:/config.ini",
                "./datasets/movies_metadata.csv:/datasets/movies_metadata.csv",
                "./datasets/ratings.csv:/datasets/ratings.csv",
                "./datasets/credits.csv:/datasets/credits.csv",
                f"./results/client_{i}:/results/client_{i}"
            ],
            networks=[
                NETWORK_NAME
            ],
            depends_on=[
                "data_cleaner",
                "results_handler"
            ]
        )
    
    return clients

def generate_movies_filter_argentina_spain_cluster(cluster_size, failure_probability):
    """Generate the movies filter services for Argentina and Spain filtering"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_produced_in_argentina_and_spain",
        filter_field="production_countries",
        filter_values='["Argentina", "Spain"]',
        output_fields_subset='["id", "title", "genres", "release_date"]',
        input_queues='[("movies_q1", "movies")]',
        output_exchange="movies_produced_in_argentina_and_spain",
        failure_probability=failure_probability
    )

def generate_movies_filter_date_2000_2009_cluster(cluster_size, failure_probability):
    """Generate the movies filter services for filtering by release date between 2000 and 2009"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_released_between_2000_2009",
        filter_field="release_date",
        filter_values="(2000, 2009)",
        output_fields_subset='["id", "title", "genres"]',
        input_queues='[("movies_produced_in_argentina_and_spain", "movies_produced_in_argentina_and_spain")]',
        output_exchange="movies_produced_in_argentina_and_spain_released_between_2000_2009",
        failure_probability=failure_probability
    )

def generate_movies_filter_by_one_country_cluster(cluster_size, failure_probability):
    """Generate the movies filter services for one production country filtering"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_by_one_production_country",
        filter_field="production_countries",
        filter_values='1',
        output_fields_subset='["production_countries", "budget"]',
        input_queues='[("movies_q2", "movies")]',
        output_exchange="movies_produced_by_one_country",
        failure_probability=failure_probability
    )
    
def generate_top_investor_countries_calculator(failure_probability):
    """Generate the movies top investor countries calculator service configuration"""
    service_name = "top_investor_countries_calculator"
    return generate_service(
        name=service_name,
        image="top_investor_countries_calculator",
        environment=[
            "PYTHONUNBUFFERED=1",
            "TOP_N_INVESTOR_COUNTRIES=5",
            "INPUT_QUEUES=[('movies_produced_by_one_country', 'movies_produced_by_one_country')]",
            "OUTPUT_EXCHANGE=top_investor_countries",
            f"FAILURE_PROBABILITY={failure_probability}",
            f"STORAGE_PATH={STORAGE_PATH}"
        ],
        volumes=[
            "./controllers/top_investor_countries_calculator/config.ini:/config.ini",
            f"{service_name}_storage:{STORAGE_PATH}"
        ],
        networks=[
            NETWORK_NAME
        ]
    )
    
def generate_movies_filter_argentina_cluster(cluster_size, failure_probability):
    """Generate the movies filter services for Argentina filtering"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_produced_in_argentina",
        filter_field="production_countries",
        filter_values='["Argentina"]',
        output_fields_subset='["id", "title", "release_date"]',
        input_queues='[("movies_q3_q4", "movies")]',
        output_exchange="movies_produced_in_argentina",
        failure_probability=failure_probability
    )

def generate_movies_filter_date_after_2000_cluster(cluster_size, failure_probability):
    """Generate the movies filter services for filtering by release date after 2000"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_released_after_2000",
        filter_field="release_date",
        filter_values="(2000,)",
        output_fields_subset='["id", "title"]',
        input_queues='[("movies_produced_in_argentina", "movies_produced_in_argentina")]',
        output_exchange="movies_produced_in_argentina_released_after_2000",
        failure_probability=failure_probability
    )
    
def generate_movies_router_by_id_cluster(cluster_size, movies_ratings_joiner_amount, movies_credits_joiner_amount, failure_probability):
    """Generate the movies router services for routing by ID"""    
    return generate_routing_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_router_by_id",
        input_queues='[("movies_produced_in_argentina_released_after_2000", "movies_produced_in_argentina_released_after_2000")]',
        output_exchange_prefixes_and_dest_nodes_amount=f'[("movies_produced_in_argentina_released_after_2000_q3", {movies_ratings_joiner_amount}), ("movies_produced_in_argentina_released_after_2000_q4", {movies_credits_joiner_amount})]',
        failure_probability=failure_probability
    ) 
    
def generate_ratings_router_by_movie_id_cluster(cluster_size, destination_nodes_amount, failure_probability):
    """Generate the ratings router services for routing by movie ID"""
    return generate_routing_cluster(
        cluster_size=cluster_size,
        service_prefix="ratings_router_by_movie_id",
        input_queues='[("ratings", "ratings")]',
        output_exchange_prefixes_and_dest_nodes_amount=f'[("ratings", {destination_nodes_amount})]',
        failure_probability=failure_probability
    )
    
def generate_movies_ratings_joiner_cluster(cluster_size, failure_probability):
    """Generate the movies ratings joiner services for joining movies and ratings"""
    return generate_movies_joiner_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_ratings_joiner",
        input_queues_prefixes=["movies_produced_in_argentina_released_after_2000_q3", "ratings"],
        output_exchange="ratings_movies_produced_in_argentina_released_after_2000",
        failure_probability=failure_probability
    )
    
def generate_most_least_rated_movies_calculator(failure_probability):
    """Generate the most and least rated movies calculator service configuration"""
    service_name = "most_least_rated_movies_calculator"
    return generate_service(
        name=service_name,
        image="most_least_rated_movies_calculator",
        environment=[
            "PYTHONUNBUFFERED=1",
            "INPUT_QUEUES=[('ratings_movies_produced_in_argentina_released_after_2000', 'ratings_movies_produced_in_argentina_released_after_2000')]",
            "OUTPUT_EXCHANGE=most_least_rated_movies_produced_in_argentina_released_after_2000",
            f"FAILURE_PROBABILITY={failure_probability}",
            f"STORAGE_PATH={STORAGE_PATH}"
        ],
        volumes=[
            "./controllers/most_least_rated_movies_calculator/config.ini:/config.ini",
            f"{service_name}_storage:{STORAGE_PATH}"
        ],
        networks=[
            NETWORK_NAME
        ]
    )
    
def generate_credits_router_by_movie_id_cluster(cluster_size, destination_nodes_amount, failure_probability):
    """Generate the credits router services for routing by movie ID"""
    return generate_routing_cluster(
        cluster_size=cluster_size,
        service_prefix="credits_router_by_movie_id",
        input_queues='[("credits", "credits")]',
        output_exchange_prefixes_and_dest_nodes_amount=f'[("credits", {destination_nodes_amount})]',
        failure_probability=failure_probability
    )
    
def generate_movies_credits_joiner_cluster(cluster_size, failure_probability):
    """Generate the movies credits joiner services for joining movies and credits"""
    return generate_movies_joiner_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_credits_joiner",
        input_queues_prefixes=["movies_produced_in_argentina_released_after_2000_q4", "credits"],
        output_exchange="credits_movies_produced_in_argentina_released_after_2000",
        failure_probability=failure_probability
    )
    
def generate_top_actors_participation_calculator(failure_probability):
    """Generate the top actors participation calculator service configuration"""
    service_name = "top_actors_participation_calculator"
    return generate_service(
        name=service_name,
        image="top_actors_participation_calculator",
        environment=[
            "PYTHONUNBUFFERED=1",
            "TOP_N_ACTORS_PARTICIPATION=10",
            "INPUT_QUEUES=[('credits_movies_produced_in_argentina_released_after_2000', 'credits_movies_produced_in_argentina_released_after_2000')]",
            "OUTPUT_EXCHANGE=top_actors_participation_movies_produced_in_argentina_released_after_2000",
            f"FAILURE_PROBABILITY={failure_probability}",
            f"STORAGE_PATH={STORAGE_PATH}"
        ],
        volumes=[
            "./controllers/top_actors_participation_calculator/config.ini:/config.ini",
            f"{service_name}_storage:{STORAGE_PATH}"
        ],
        networks=[
            NETWORK_NAME
        ]
    )
    
def generate_movies_sentiment_analyzer_by_overview_cluster(cluster_size, failure_probability):
    """Generate the movies sentiment analyzer services for analyzing movies overview"""
    return generate_movies_sentiment_analyzer_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_sentiment_analyzer",
        field_to_analyze="overview",
        input_queues='[("movies_q5", "movies")]',
        output_exchange="movies_sentiment_analyzed",
        failure_probability=failure_probability
    )
    
def generate_avg_rate_revenue_budget_calculator(failure_probability):
    """Generate the average rate revenue budget calculator service configuration"""
    service_name = "avg_rate_revenue_budget_calculator"
    return generate_service(
        name=service_name,
        image="avg_rate_revenue_budget_calculator",
        environment=[
            "PYTHONUNBUFFERED=1",
            "INPUT_QUEUES=[('movies_sentiment_analyzed', 'movies_sentiment_analyzed')]",
            "OUTPUT_EXCHANGE=avg_rate_revenue_budget_by_sentiment",
            f"FAILURE_PROBABILITY={failure_probability}",
            f"STORAGE_PATH={STORAGE_PATH}"
        ],
        volumes=[
            "./controllers/avg_rate_revenue_budget_calculator/config.ini:/config.ini",
            f"{service_name}_storage:{STORAGE_PATH}"
        ],
        networks=[
            NETWORK_NAME
        ]
    )
    
def generate_health_guard_cluster(cluster_size):
    """
    Generic function to generate a cluster of health guard services
    
    Args:
        cluster_size: Number of instances to create
        
    Returns:
        Dictionary mapping service names to their configurations
    """
    service_prefix = "health_guard"
    return generate_cluster(
        cluster_size=cluster_size,
        service_prefix=service_prefix,
        image="health_guard",
        environment=[
            "PYTHONUNBUFFERED=1",
            "PYTHONHASHSEED=0",
            f"COMPOSE_PROJECT_NAME={COMPOSE_PROJECT_NAME}",
            'DONT_GUARD_CONTAINERS=["rabbitmq", "client"]',
            f"SERVICE_PREFIX={service_prefix}",
        ],
        volumes=[
            "/var/run/docker.sock:/var/run/docker.sock",
            "./health_guard/config.ini:/config.ini"
        ],
        networks=[
            NETWORK_NAME
        ]
    )

def generate_network_config():
    """Generate the network configuration for the docker-compose file"""
    return {
        NETWORK_NAME: {
            "ipam": {
                "driver": "default",
                "config": [
                    {"subnet": "172.25.125.0/24"}
                ]
            }
        }
    }

def generate_storage_volumes_config(config_params):
    """Generate the storage volumes configuration for the docker-compose file"""
    volumes = {}
    
    for i in range(1, config_params["movies_ratings_joiner"] + 1):
        volume_name = f"movies_ratings_joiner_{i}_storage"
        volumes[volume_name] = None
    
    for i in range(1, config_params["movies_credits_joiner"] + 1):
        volume_name = f"movies_credits_joiner_{i}_storage"
        volumes[volume_name] = None
        
    volumes["top_investor_countries_calculator_storage"] = None
    
    volumes["most_least_rated_movies_calculator_storage"] = None
    
    volumes["top_actors_participation_calculator_storage"] = None
    
    volumes["avg_rate_revenue_budget_calculator_storage"] = None
    
    volumes["data_cleaner_storage"] = None
    
    return volumes

def generate_docker_compose(config_params):
    """
    Generate the complete docker-compose configuration based on provided parameters
    
    Args:
        config_params: Dictionary with configuration parameters
        
    Returns:
        Dictionary with complete docker-compose configuration
    """
    docker_compose = {
        "name": COMPOSE_PROJECT_NAME,
        "services": {},
        "networks": generate_network_config(),
        "volumes": generate_storage_volumes_config(config_params)
    }
    
    docker_compose["services"]["data_cleaner"] = generate_data_cleaner()
    docker_compose["services"]["results_handler"] = generate_results_handler()
    clients = generate_clients(config_params["clients"])
    docker_compose["services"].update(clients)

    # Query 1
    movies_filter_argentina_spain_cluster = generate_movies_filter_argentina_spain_cluster(
        config_params["movies_filter_produced_in_argentina_and_spain"],
        config_params["failure_probabilities"]["movies_filter_produced_in_argentina_and_spain"]
    )
    docker_compose["services"].update(movies_filter_argentina_spain_cluster)
    movies_filter_date_2000_2009_cluster = generate_movies_filter_date_2000_2009_cluster(
        config_params["movies_filter_released_between_2000_2009"],
        config_params["failure_probabilities"]["movies_filter_released_between_2000_2009"]
    )
    docker_compose["services"].update(movies_filter_date_2000_2009_cluster)
    
    # Query 2
    movies_filter_by_one_country_cluster = generate_movies_filter_by_one_country_cluster(
        config_params["movies_filter_by_one_production_country"],
        config_params["failure_probabilities"]["movies_filter_by_one_production_country"]
    )
    docker_compose["services"].update(movies_filter_by_one_country_cluster)
    docker_compose["services"]["top_investor_countries_calculator"] = generate_top_investor_countries_calculator(
        config_params["failure_probabilities"]["top_investor_countries_calculator"]
    )
    
    # Queries 3 and 4
    movies_filter_argentina_cluster = generate_movies_filter_argentina_cluster(
        config_params["movies_filter_produced_in_argentina"],
        config_params["failure_probabilities"]["movies_filter_produced_in_argentina"]
    )
    docker_compose["services"].update(movies_filter_argentina_cluster)
    movies_filter_date_after_2000_cluster = generate_movies_filter_date_after_2000_cluster(
        config_params["movies_filter_released_after_2000"],
        config_params["failure_probabilities"]["movies_filter_released_after_2000"]
    )
    docker_compose["services"].update(movies_filter_date_after_2000_cluster)
    movies_router_by_id_cluster = generate_movies_router_by_id_cluster(
        config_params["movies_router_by_id"],
        config_params["movies_ratings_joiner"],
        config_params["movies_credits_joiner"],
        config_params["failure_probabilities"]["movies_router_by_id"]
    )
    docker_compose["services"].update(movies_router_by_id_cluster)
    
    # Query 3
    ratings_router_by_movie_id_cluster = generate_ratings_router_by_movie_id_cluster(
        config_params["ratings_router_by_movie_id"],
        config_params["movies_ratings_joiner"],
        config_params["failure_probabilities"]["ratings_router_by_movie_id"]
    )
    docker_compose["services"].update(ratings_router_by_movie_id_cluster)
    movies_ratings_joiner_cluster = generate_movies_ratings_joiner_cluster(
        config_params["movies_ratings_joiner"],
        config_params["failure_probabilities"]["movies_ratings_joiner"]
    )
    docker_compose["services"].update(movies_ratings_joiner_cluster)
    docker_compose["services"]["most_least_rated_movies_calculator"] = generate_most_least_rated_movies_calculator(
        config_params["failure_probabilities"]["most_least_rated_movies_calculator"]
    )
    
    # Query 4
    credits_router_by_movie_id_cluster = generate_credits_router_by_movie_id_cluster(
        config_params["credits_router_by_movie_id"],
        config_params["movies_credits_joiner"],
        config_params["failure_probabilities"]["credits_router_by_movie_id"]
    )
    docker_compose["services"].update(credits_router_by_movie_id_cluster)
    movies_credits_joiner_cluster = generate_movies_credits_joiner_cluster(
        config_params["movies_credits_joiner"],
        config_params["failure_probabilities"]["movies_credits_joiner"]
    )
    docker_compose["services"].update(movies_credits_joiner_cluster)
    docker_compose["services"]["top_actors_participation_calculator"] = generate_top_actors_participation_calculator(
        config_params["failure_probabilities"]["top_actors_participation_calculator"]
    )

    # Query 5
    movies_sentiment_analyzer_cluster = generate_movies_sentiment_analyzer_by_overview_cluster(
        config_params["movies_sentiment_analyzer"],
        config_params["failure_probabilities"]["movies_sentiment_analyzer"]
    )
    docker_compose["services"].update(movies_sentiment_analyzer_cluster)
    docker_compose["services"]["avg_rate_revenue_budget_calculator"] = generate_avg_rate_revenue_budget_calculator(
        config_params["failure_probabilities"]["avg_rate_revenue_budget_calculator"]
    )
    
    # Health guards
    health_guard_cluster = generate_health_guard_cluster(
        config_params["health_guard"]
    )
    docker_compose["services"].update(health_guard_cluster)
    
    return docker_compose
