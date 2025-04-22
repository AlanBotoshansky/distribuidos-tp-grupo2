FROM python:3.13.3-slim
RUN pip install pika
RUN pip install 'transformers[torch]'
RUN pip install 'huggingface_hub[hf_xet]'
COPY controllers/movies_sentiment_analyzer /
COPY /messages /messages
COPY /middleware /middleware
WORKDIR /
ENTRYPOINT ["python3", "main.py"]