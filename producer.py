import json
import os

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()

max_messages = int(os.getenv("MAX_MESSAGES", "10"))

consumer = Consumer({
    "bootstrap.servers": os.environ["KAFKA_SOURCE_BOOTSTRAP_SERVERS"],
    "security.protocol": os.environ["KAFKA_SOURCE_SECURITY_PROTOCOL"],
    "sasl.mechanisms": os.environ["KAFKA_SOURCE_SASL_MECHANISMS"],
    "sasl.username": os.environ["KAFKA_SOURCE_USERNAME"],
    "sasl.password": os.environ["KAFKA_SOURCE_PASSWORD"],
    "group.id": os.environ["KAFKA_SOURCE_GROUP_ID"],
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([os.environ["KAFKA_SOURCE_TOPIC"]])

producer = Producer({
    "bootstrap.servers": os.environ["KAFKA_LOCAL_BOOTSTRAP_SERVERS"],
    "acks": "all",
})

print("waiting for messages...")
print("max messages:", max_messages)

count = 0

try:
    while count < max_messages:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("error:", msg.error())
            continue

        key = msg.key().decode("utf-8") if msg.key() else None
        raw_value = msg.value().decode("utf-8")
        value = json.loads(raw_value)

        print("key:", key)
        print("value:", value)
        print("source partition:", msg.partition())
        print("source offset:", msg.offset())

        producer.produce(
            topic=os.environ["KAFKA_LOCAL_TOPIC"],
            key=key,
            value=json.dumps(value),
        )

        producer.flush()
        consumer.commit(message=msg, asynchronous=False)

        count += 1
        print("produced to local topic:", os.environ["KAFKA_LOCAL_TOPIC"])
        print("count:", count)
finally:
    consumer.close()

print("done")
