FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    faker==24.11.0 \
    pydantic==2.7.1 \
    confluent-kafka==2.4.0 \
    prometheus-client==0.20.0 \
    python-dotenv==1.0.1

COPY schemas.py producer.py ./

CMD ["python", "producer.py"]
