import asyncio
import json 
import requests
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# kafka-topics.sh --bootstrap-server localhost:9092 --topic scada_data_topic --create
KAFKA_TOPIC = "scada_data_topic"
SCADA_DATA_STREAM_URL = "http://localhost:8000/generator/stream"

# Kafka producer configuration
producer_config = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "client_id": "scada_data_producer",
    "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
}

async def fetch_scada_data_and_produce():
    producer = KafkaProducer(**producer_config)

    response = requests.get(SCADA_DATA_STREAM_URL, stream=True)
    for line in response.iter_lines():
        if line:
            scada_data = json.loads(line.decode("utf-8"))
            producer.send(KAFKA_TOPIC, value=scada_data)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_scada_data_and_produce())


#kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic scada_data_topic
