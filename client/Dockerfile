FROM python:3.13.3-slim
COPY client /
COPY /communication /communication
COPY /utils /utils
WORKDIR /
ENTRYPOINT ["python3", "main.py"]