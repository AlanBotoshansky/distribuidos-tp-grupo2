FROM python:3.13.3-slim
RUN pip install pika
COPY controllers/router /
COPY /messages /messages
COPY /middleware /middleware
WORKDIR /
ENTRYPOINT ["python3", "main.py"]