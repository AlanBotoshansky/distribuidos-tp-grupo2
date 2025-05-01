FROM python:3.13.3-slim
RUN pip install pika
COPY controllers/most_least_rated_movies_calculator /
COPY controllers/stateful_controller /stateful_controller
COPY /messages /messages
COPY /middleware /middleware
WORKDIR /
ENTRYPOINT ["python3", "main.py"]