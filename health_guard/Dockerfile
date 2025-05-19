FROM python:3.13.3-slim
RUN pip install docker
COPY health_guard /
COPY /common /common
COPY /utils /utils
WORKDIR /
ENTRYPOINT ["python3", "main.py"]