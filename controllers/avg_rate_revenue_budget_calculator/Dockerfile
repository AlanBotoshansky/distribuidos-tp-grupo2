FROM python:3.13.3-slim
RUN pip install pika
COPY controllers/avg_rate_revenue_budget_calculator /
COPY /messages /messages
COPY /middleware /middleware
WORKDIR /
ENTRYPOINT ["python3", "main.py"]