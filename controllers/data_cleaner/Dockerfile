FROM python:3.13.3-slim
COPY controllers/data_cleaner /
COPY /messages /messages
WORKDIR /
ENTRYPOINT ["python3", "main.py"]