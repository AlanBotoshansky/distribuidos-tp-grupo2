FROM python:3.13.3-slim
RUN pip install pika
COPY controllers/movies_joiner /
COPY /messages /messages
COPY /middleware /middleware
COPY /common /common
COPY /utils /utils
COPY /storage_adapter /storage_adapter
WORKDIR /
ENTRYPOINT ["python3", "main.py"]