FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip install kafka-python

CMD ["python", "data_consumer.py"]
