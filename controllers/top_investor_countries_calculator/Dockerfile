FROM python:3.13.3-slim
RUN pip install pika
COPY controllers/top_investor_countries_calculator /
COPY /messages /messages
COPY /middleware /middleware
WORKDIR /
ENTRYPOINT ["python3", "main.py"]