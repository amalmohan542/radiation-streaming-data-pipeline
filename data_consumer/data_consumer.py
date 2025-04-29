from kafka import KafkaConsumer
import json
import os

def main():
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "radiation-data")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='radiation-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )

    print(f"Listening for messages on topic '{topic}'...")
    for message in consumer:
        print(f"Received message: {message.value}")

if __name__ == "__main__":
    main()
