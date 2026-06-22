import json
import os

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
from mongo import close_mongo, insert_product_view

load_dotenv()

max_messages = int(os.getenv("MAX_MESSAGES", "10"))

consumer = Consumer({
    "bootstrap.servers": os.environ["KAFKA_LOCAL_BOOTSTRAP_SERVERS"],
    "group.id": os.getenv("KAFKA_LOCAL_GROUP_ID", "product-view-loader"),
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([os.environ["KAFKA_LOCAL_TOPIC"]])

print("waiting for local messages...")
print("max messages:", max_messages)

count = 0

try:
    while count < max_messages:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        key = msg.key().decode("utf-8") if msg.key() else None
        raw_value = msg.value().decode("utf-8")
        value = json.loads(raw_value)

        document = {
            "kafka_key": key,
            "kafka_topic": msg.topic(),
            "kafka_partition": msg.partition(),
            "kafka_offset": msg.offset(),
            "value": value,
        }

        insert_product_view(document)
        consumer.commit(message=msg, asynchronous=False)

        count += 1
        print("inserted:", document["_id"])
        print("count:", count)
finally:
    consumer.close()
    close_mongo()

print("done")
