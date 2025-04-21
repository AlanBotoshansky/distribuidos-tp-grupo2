FROM python:3.13.3-slim
RUN pip install pika
COPY controllers/top_actors_participation_calculator /
COPY /messages /messages
COPY /middleware /middleware
WORKDIR /
ENTRYPOINT ["python3", "main.py"]