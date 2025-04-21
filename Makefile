SHELL := /bin/bash
PWD := $(shell pwd)

default: build

docker-image:
	docker build -f ./controllers/data_cleaner/Dockerfile -t "data_cleaner:latest" .
	docker build -f ./controllers/results_handler/Dockerfile -t "results_handler:latest" .
	docker build -f ./controllers/movies_filter/Dockerfile -t "movies_filter:latest" .
	docker build -f ./controllers/top_investor_countries_calculator/Dockerfile -t "top_investor_countries_calculator:latest" .
	docker build -f ./controllers/router/Dockerfile -t "router:latest" .
	docker build -f ./controllers/movies_joiner/Dockerfile -t "movies_joiner:latest" .
	docker build -f ./controllers/most_least_rated_movies_calculator/Dockerfile -t "most_least_rated_movies_calculator:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

rabbitmq-image:
	docker build -f ./rabbitmq/Dockerfile -t "rabbitmq:latest" .
.PHONY: rabbitmq-image

rabbitmq-up: rabbitmq-image
	docker compose -f docker-compose-rabbitmq.yaml up -d --build
.PHONY: rabbitmq-up

rabbitmq-down:
	docker compose -f docker-compose-rabbitmq.yaml stop -t 1
	docker compose -f docker-compose-rabbitmq.yaml down
.PHONY: rabbitmq-down

rabbitmq-logs:
	docker compose -f docker-compose-rabbitmq.yaml logs -f
.PHONY: rabbitmq-logs

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
