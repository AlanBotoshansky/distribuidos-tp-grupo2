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
            "MOVIES_EXCHANGE=movies"
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
            'INPUT_QUEUES=[("movies_produced_in_argentina_and_spain_released_between_2000_2009", "movies_produced_in_argentina_and_spain_released_between_2000_2009")]'
        ],
        volumes=[
            "./controllers/results_handler/config.ini:/config.ini"
        ],
        networks=[
            "testing_net"
        ]
    )

def generate_client():
    """Generate client service configuration"""
    return generate_service(
        name="client",
        image="client",
        environment=[
            "PYTHONUNBUFFERED=1"
        ],
        volumes=[
            "./client/config.ini:/config.ini",
            "./datasets/movies_metadata.csv:/datasets/movies_metadata.csv",
            "./datasets/ratings.csv:/datasets/ratings.csv",
            "./datasets/credits.csv:/datasets/credits.csv"
        ],
        networks=[
            "testing_net"
        ],
        depends_on=[
            "data_cleaner",
            "results_handler"
        ]
    )

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

def generate_movies_filter_date_cluster(cluster_size):
    """Generate the movies filter services for date range filtering"""
    return generate_filter_cluster(
        cluster_size=cluster_size,
        service_prefix="movies_filter_released_between_2000_2009",
        filter_field="release_date",
        filter_values="(2000, 2009)",
        output_fields_subset='["id", "title", "genres"]',
        input_queues='[("movies_produced_in_argentina_and_spain", "movies_produced_in_argentina_and_spain")]',
        output_exchange="movies_produced_in_argentina_and_spain_released_between_2000_2009"
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
    docker_compose["services"]["client"] = generate_client()
    
    movies_filter_argentina_spain_cluster = generate_movies_filter_argentina_spain_cluster(
        config_params["movies_filter_produced_in_argentina_and_spain"]
    )
    movies_filter_date_cluster = generate_movies_filter_date_cluster(
        config_params["movies_filter_released_between_2000_2009"]
    )
    
    docker_compose["services"].update(movies_filter_argentina_spain_cluster)
    docker_compose["services"].update(movies_filter_date_cluster)
    
    return docker_compose
