"""Simulate user events and publish to Kafka."""
import json
import time
import random
from kafka import KafkaProducer
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

# Sample data
user_ids = [f"user_{i}" for i in range(1, 101)]
event_types = ["click", "view", "purchase", "scroll"]

def generate_event():
    return {
        "user_id": random.choice(user_ids),
        "timestamp": int(time.time()),
        "event_type": random.choice(event_types),
        "metadata": {
            "page": random.choice(["home", "product", "cart", "checkout"]),
            "duration": random.randint(1, 300)
        }
    }

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Producing events to topic {KAFKA_TOPIC}...")
try:
    while True:
        event = generate_event()
        producer.send(KAFKA_TOPIC, value=event)
        print(f"Sent: {event}")
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopped.")
finally:
    producer.close()