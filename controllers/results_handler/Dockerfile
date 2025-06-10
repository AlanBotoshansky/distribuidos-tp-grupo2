FROM python:3.13.3-slim
RUN pip install pika
COPY controllers/results_handler /
COPY /communication /communication
COPY /utils /utils
COPY /messages /messages
COPY /middleware /middleware
COPY /common /common
WORKDIR /
ENTRYPOINT ["python3", "main.py"]