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

def generate_filter_cluster(cluster_size, service_prefix, filter_field, filter_values, output_fields_subset, input_queues, output_exchange):
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
        
    Returns:
        Dictionary mapping service names to their configurations
    """
    services = {}
    cluster_size = int(cluster_size)
    
    for i in range(1, cluster_size + 1):
        service_name = f"{service_prefix}_{i}"
        services[service_name] = generate_service(
            name=service_name,
            image="movies_filter",
            environment=[
                "PYTHONUNBUFFERED=1",
                f"FILTER_FIELD={filter_field}",
                f"FILTER_VALUES={filter_values}",
                f"OUTPUT_FIELDS_SUBSET={output_fields_subset}",
                f"INPUT_QUEUES={input_queues}",
                f"OUTPUT_EXCHANGE={output_exchange}",
                f"CLUSTER_SIZE={cluster_size}",
                f"ID={i}"
            ],
            volumes=[
                "./controllers/movies_filter/config.ini:/config.ini"
            ],
            networks=[
                "testing_net"
            ]
        )
    
    return services

def generate_routing_cluster(cluster_size, destination_nodes_amount, service_prefix, input_queues, output_exchange_prefixes):
    """
    Generic function to generate a cluster of routing services
    
    Args:
        cluster_size: Number of instances to create
        destination_nodes_amount: Number of destination nodes
        service_prefix: Prefix for the service names
        input_queues: Input queues configuration
        output_exchange_prefix: Output exchange prefix
        
    Returns:
        Dictionary mapping service names to their configurations
    """
    services = {}
    cluster_size = int(cluster_size)
    
    for i in range(1, cluster_size + 1):
        service_name = f"{service_prefix}_{i}"
        services[service_name] = generate_service(
            name=service_name,
            image="router",
            environment=[
                "PYTHONUNBUFFERED=1",
                f"DESTINATION_NODES_AMOUNT={destination_nodes_amount}",
                f"INPUT_QUEUES={input_queues}",
                f"OUTPUT_EXCHANGE_PREFIXES={output_exchange_prefixes}",
                f"CLUSTER_SIZE={cluster_size}",
                f"ID={i}"
            ],
            volumes=[
                "./controllers/router/config.ini:/config.ini"
            ],
            networks=[
                "testing_net"
            ]
        )
    
    return services

def generate_movies_joiner_cluster(cluster_size, service_prefix, input_queues_prefixes, output_exchange):
    """
    Generic function to generate a cluster of joiner services
    
    Args:
        cluster_size: Number of instances to create
        service_prefix: Prefix for the service names
        input_queues_prefixes: List of input queues prefixes
        output_exchange: Output exchange name
        
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
                f"CLUSTER_SIZE={cluster_size}",
                f"ID={i}"
            ],
            volumes=[
                f"./controllers/movies_joiner/config.ini:/config.ini"
            ],
            networks=[
                "testing_net"
            ]
        )
    
    return services

def generate_movies_sentiment_analyzer_cluster(cluster_size, service_prefix, field_to_analyze, input_queues, output_exchange):
    """
    Generic function to generate a cluster of sentiment analyzer services
    
    Args:
        cluster_size: Number of instances to create
        service_prefix: Prefix for the service names
        field_to_analyze: Field to analyze
        input_queues: Input queues configuration
        output_exchange: Output exchange name
        
    Returns:
        Dictionary mapping service names to their configurations
    """
    services = {}
    cluster_size = int(cluster_size)
    
    for i in range(1, cluster_size + 1):
        service_name = f"{service_prefix}_{i}"
        services[service_name] = generate_service(
            name=service_name,
            image="movies_sentiment_analyzer",
            environment=[
                "PYTHONUNBUFFERED=1",
                f"FIELD_TO_ANALYZE={field_to_analyze}",
                f"INPUT_QUEUES={input_queues}",
                f"OUTPUT_EXCHANGE={output_exchange}",
                f"CLUSTER_SIZE={cluster_size}",
                f"ID={i}"
            ],
            volumes=[
                "./controllers/movies_sentiment_analyzer/config.ini:/config.ini"
            ],
            networks=[
                "testing_net"
            ]
        )
    
    return services

def generate_data_cleaner():
    """Generate data_cleaner service configuration"""
    return generate_service(
        name="data_cleaner",
        image="data_cleaner",
        environment=[
            "PYTHONUNBUFFERED=1",
            "MOVIES_EXCHANGE=movies",
            "RATINGS_EXCHANGE=ratings",
            "CREDITS_EXCHANGE=credits",
        ],
        volumes=[
            "./controllers/data_cleaner/config.ini:/config.ini"
        ],
        networks=[
            "testing_net"
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
            "testing_net"
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
                "RESULTS_DIR=/results",
            ],
            volumes=[
                "./client/config.ini:/config.ini",
                "./datasets/movies_metadata.csv:/datasets/movies_metadata.csv",
                "./datasets/ratings.csv:/datasets/ratings.csv",
                "./datasets/credits.csv:/datasets/credits.csv",
                "./results:/results"
            ],
            networks=[
                "testing_net"
            ],
            depends_on=[
                "data_cleaner",
                "results_handler"
            ]
        )
    
    return clients

def generate_movies_filter_argentina_spain_cluster(cluster_size):
    """Generate the movies filter services for Argentina and Spain filtering"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_produced_in_argentina_and_spain",
        filter_field="production_countries",
        filter_values='["Argentina", "Spain"]',
        output_fields_subset='["id", "title", "genres", "release_date"]',
        input_queues='[("movies_q1", "movies")]',
        output_exchange="movies_produced_in_argentina_and_spain"
    )

def generate_movies_filter_date_2000_2009_cluster(cluster_size):
    """Generate the movies filter services for filtering by release date between 2000 and 2009"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_released_between_2000_2009",
        filter_field="release_date",
        filter_values="(2000, 2009)",
        output_fields_subset='["id", "title", "genres"]',
        input_queues='[("movies_produced_in_argentina_and_spain", "movies_produced_in_argentina_and_spain")]',
        output_exchange="movies_produced_in_argentina_and_spain_released_between_2000_2009"
    )

def generate_movies_filter_by_one_country_cluster(cluster_size):
    """Generate the movies filter services for one production country filtering"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_by_one_production_country",
        filter_field="production_countries",
        filter_values='1',
        output_fields_subset='["production_countries", "budget"]',
        input_queues='[("movies_q2", "movies")]',
        output_exchange="movies_produced_by_one_country"
    )
    
def generate_top_investor_countries_calculator():
    """Generate the movies top investor countries calculator service configuration"""
    return generate_service(
        name="top_investor_countries_calculator",
        image="top_investor_countries_calculator",
        environment=[
            "PYTHONUNBUFFERED=1",
            "TOP_N_INVESTOR_COUNTRIES=5",
            "INPUT_QUEUES=[('movies_produced_by_one_country', 'movies_produced_by_one_country')]",
            "OUTPUT_EXCHANGE=top_investor_countries"
        ],
        volumes=[
            "./controllers/top_investor_countries_calculator/config.ini:/config.ini"
        ],
        networks=[
            "testing_net"
        ]
    )
    
def generate_movies_filter_argentina_cluster(cluster_size):
    """Generate the movies filter services for Argentina filtering"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_produced_in_argentina",
        filter_field="production_countries",
        filter_values='["Argentina"]',
        output_fields_subset='["id", "title", "release_date"]',
        input_queues='[("movies_q3_q4", "movies")]',
        output_exchange="movies_produced_in_argentina"
    )

def generate_movies_filter_date_after_2000_cluster(cluster_size):
    """Generate the movies filter services for filtering by release date after 2000"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_released_after_2000",
        filter_field="release_date",
        filter_values="(2000,)",
        output_fields_subset='["id", "title"]',
        input_queues='[("movies_produced_in_argentina", "movies_produced_in_argentina")]',
        output_exchange="movies_produced_in_argentina_released_after_2000"
    )
    
def generate_movies_router_by_id_cluster(cluster_size, destination_nodes_amount):
    """Generate the movies router services for routing by ID"""
    return generate_routing_cluster(
        cluster_size=cluster_size,
        destination_nodes_amount=destination_nodes_amount,
        service_prefix="movies_router_by_id",
        input_queues='[("movies_produced_in_argentina_released_after_2000", "movies_produced_in_argentina_released_after_2000")]',
        output_exchange_prefixes='["movies_produced_in_argentina_released_after_2000_q3", "movies_produced_in_argentina_released_after_2000_q4"]'
    )
    
def generate_ratings_router_by_movie_id_cluster(cluster_size, destination_nodes_amount):
    """Generate the ratings router services for routing by movie ID"""
    return generate_routing_cluster(
        cluster_size=cluster_size,
        destination_nodes_amount=destination_nodes_amount,
        service_prefix="ratings_router_by_movie_id",
        input_queues='[("ratings", "ratings")]',
        output_exchange_prefixes='["ratings"]'
    )
    
def generate_movies_ratings_joiner_cluster(cluster_size):
    """Generate the movies ratings joiner services for joining movies and ratings"""
    return generate_movies_joiner_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_ratings_joiner",
        input_queues_prefixes=["movies_produced_in_argentina_released_after_2000_q3", "ratings"],
        output_exchange="ratings_movies_produced_in_argentina_released_after_2000"
    )
    
def generate_most_least_rated_movies_calculator():
    """Generate the most and least rated movies calculator service configuration"""
    return generate_service(
        name="most_least_rated_movies_calculator",
        image="most_least_rated_movies_calculator",
        environment=[
            "PYTHONUNBUFFERED=1",
            "INPUT_QUEUES=[('ratings_movies_produced_in_argentina_released_after_2000', 'ratings_movies_produced_in_argentina_released_after_2000')]",
            "OUTPUT_EXCHANGE=most_least_rated_movies_produced_in_argentina_released_after_2000"
        ],
        volumes=[
            "./controllers/most_least_rated_movies_calculator/config.ini:/config.ini"
        ],
        networks=[
            "testing_net"
        ]
    )
    
def generate_credits_router_by_movie_id_cluster(cluster_size, destination_nodes_amount):
    """Generate the credits router services for routing by movie ID"""
    return generate_routing_cluster(
        cluster_size=cluster_size,
        destination_nodes_amount=destination_nodes_amount,
        service_prefix="credits_router_by_movie_id",
        input_queues='[("credits", "credits")]',
        output_exchange_prefixes='["credits"]'
    )
    
def generate_movies_credits_joiner_cluster(cluster_size):
    """Generate the movies credits joiner services for joining movies and credits"""
    return generate_movies_joiner_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_credits_joiner",
        input_queues_prefixes=["movies_produced_in_argentina_released_after_2000_q4", "credits"],
        output_exchange="credits_movies_produced_in_argentina_released_after_2000"
    )
    
def generate_top_actors_participation_calculator():
    """Generate the top actors participation calculator service configuration"""
    return generate_service(
        name="top_actors_participation_calculator",
        image="top_actors_participation_calculator",
        environment=[
            "PYTHONUNBUFFERED=1",
            "TOP_N_ACTORS_PARTICIPATION=10",
            "INPUT_QUEUES=[('credits_movies_produced_in_argentina_released_after_2000', 'credits_movies_produced_in_argentina_released_after_2000')]",
            "OUTPUT_EXCHANGE=top_actors_participation_movies_produced_in_argentina_released_after_2000"
        ],
        volumes=[
            "./controllers/top_actors_participation_calculator/config.ini:/config.ini"
        ],
        networks=[
            "testing_net"
        ]
    )
    
def generate_movies_sentiment_analyzer_by_overview_cluster(cluster_size):
    """Generate the movies sentiment analyzer services for analyzing movies overview"""
    return generate_movies_sentiment_analyzer_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_sentiment_analyzer",
        field_to_analyze="overview",
        input_queues='[("movies_q5", "movies")]',
        output_exchange="movies_sentiment_analyzed"
    )
    
def generate_avg_rate_revenue_budget_calculator():
    """Generate the average rate revenue budget calculator service configuration"""
    return generate_service(
        name="avg_rate_revenue_budget_calculator",
        image="avg_rate_revenue_budget_calculator",
        environment=[
            "PYTHONUNBUFFERED=1",
            "INPUT_QUEUES=[('movies_sentiment_analyzed', 'movies_sentiment_analyzed')]",
            "OUTPUT_EXCHANGE=avg_rate_revenue_budget_by_sentiment"
        ],
        volumes=[
            "./controllers/avg_rate_revenue_budget_calculator/config.ini:/config.ini"
        ],
        networks=[
            "testing_net"
        ]
    )

def generate_network_config():
    """Generate the network configuration for the docker-compose file"""
    return {
        "testing_net": {
            "ipam": {
                "driver": "default",
                "config": [
                    {"subnet": "172.25.125.0/24"}
                ]
            }
        }
    }

def generate_docker_compose(config_params):
    """
    Generate the complete docker-compose configuration based on provided parameters
    
    Args:
        config_params: Dictionary with configuration parameters
        
    Returns:
        Dictionary with complete docker-compose configuration
    """
    docker_compose = {
        "name": "tp",
        "services": {},
        "networks": generate_network_config()
    }
    
    docker_compose["services"]["data_cleaner"] = generate_data_cleaner()
    docker_compose["services"]["results_handler"] = generate_results_handler()
    clients = generate_clients(config_params["clients"])
    docker_compose["services"].update(clients)

    # Query 1
    movies_filter_argentina_spain_cluster = generate_movies_filter_argentina_spain_cluster(
        config_params["movies_filter_produced_in_argentina_and_spain"]
    )
    docker_compose["services"].update(movies_filter_argentina_spain_cluster)
    movies_filter_date_2000_2009_cluster = generate_movies_filter_date_2000_2009_cluster(
        config_params["movies_filter_released_between_2000_2009"]
    )
    docker_compose["services"].update(movies_filter_date_2000_2009_cluster)
    
    # Query 2
    movies_filter_by_one_country_cluster = generate_movies_filter_by_one_country_cluster(
        config_params["movies_filter_by_one_production_country"]
    )
    docker_compose["services"].update(movies_filter_by_one_country_cluster)
    docker_compose["services"]["top_investor_countries_calculator"] = generate_top_investor_countries_calculator()
    
    # Queries 3 and 4
    movies_filter_argentina_cluster = generate_movies_filter_argentina_cluster(
        config_params["movies_filter_produced_in_argentina"]
    )
    docker_compose["services"].update(movies_filter_argentina_cluster)
    movies_filter_date_after_2000_cluster = generate_movies_filter_date_after_2000_cluster(
        config_params["movies_filter_released_after_2000"]
    )
    docker_compose["services"].update(movies_filter_date_after_2000_cluster)
    movies_router_by_id_cluster = generate_movies_router_by_id_cluster(
        config_params["movies_router_by_id"],
        config_params["movies_ratings_joiner"]
    )
    docker_compose["services"].update(movies_router_by_id_cluster)
    
    # Query 3
    ratings_router_by_movie_id_cluster = generate_ratings_router_by_movie_id_cluster(
        config_params["ratings_router_by_movie_id"],
        config_params["movies_ratings_joiner"]
    )
    docker_compose["services"].update(ratings_router_by_movie_id_cluster)
    movies_ratings_joiner_cluster = generate_movies_ratings_joiner_cluster(
        config_params["movies_ratings_joiner"]
    )
    docker_compose["services"].update(movies_ratings_joiner_cluster)
    docker_compose["services"]["most_least_rated_movies_calculator"] = generate_most_least_rated_movies_calculator()
    
    # Query 4
    credits_router_by_movie_id_cluster = generate_credits_router_by_movie_id_cluster(
        config_params["credits_router_by_movie_id"],
        config_params["movies_ratings_joiner"]
    )
    docker_compose["services"].update(credits_router_by_movie_id_cluster)
    movies_credits_joiner_cluster = generate_movies_credits_joiner_cluster(
        config_params["movies_credits_joiner"]
    )
    docker_compose["services"].update(movies_credits_joiner_cluster)
    docker_compose["services"]["top_actors_participation_calculator"] = generate_top_actors_participation_calculator()

    # Query 5
    movies_sentiment_analyzer_cluster = generate_movies_sentiment_analyzer_by_overview_cluster(
        config_params["movies_sentiment_analyzer"]
    )
    docker_compose["services"].update(movies_sentiment_analyzer_cluster)
    docker_compose["services"]["avg_rate_revenue_budget_calculator"] = generate_avg_rate_revenue_budget_calculator()
    
    return docker_compose
