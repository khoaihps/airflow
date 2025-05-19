import json
from kafka import KafkaProducer

class DataKafkaProducer:
    def __init__(self, bootstrap_servers="kafka:9092", topic="onboard"):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

    def send(self, message: dict):
        self.producer.send(self.topic, value=message)

    def flush(self):
        self.producer.flush()
