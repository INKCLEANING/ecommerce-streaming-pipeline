FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    confluent-kafka==2.4.0 \
    boto3==1.34.110 \
    prometheus-client==0.20.0 \
    python-dotenv==1.0.1

COPY consumer.py ./

CMD ["python", "consumer.py"]
