# Product View Pipeline Guide

Goal:

```text
external Kafka product_view -> local Kafka product_view_local -> MongoDB
```

Use these files:

```text
producer.py = external Kafka -> local Kafka
consumer.py = local Kafka -> MongoDB
mongo.py    = MongoDB helper
.env        = secrets/config
```

## Step 1: Build `producer.py` As Source Reader

Goal:

```text
read 1 message from external Kafka topic product_view and print it
```

Do not write to local Kafka yet.

Build order:

1. Import tools:

```text
os
load_dotenv from dotenv
Consumer and KafkaException from confluent_kafka
```

2. Load `.env`:

```text
load_dotenv()
```

3. Read source Kafka env vars:

```text
KAFKA_SOURCE_BOOTSTRAP_SERVERS
KAFKA_SOURCE_SECURITY_PROTOCOL
KAFKA_SOURCE_SASL_MECHANISMS
KAFKA_SOURCE_USERNAME
KAFKA_SOURCE_PASSWORD
KAFKA_SOURCE_TOPIC
KAFKA_SOURCE_GROUP_ID
```

4. Create Kafka `Consumer` config:

```text
bootstrap.servers
security.protocol
sasl.mechanisms
sasl.username
sasl.password
group.id
auto.offset.reset = earliest
```

5. Subscribe to topic:

```text
product_view
```

6. Poll messages:

```text
poll 1 second
if no message -> continue
if message has error -> handle error
if valid message -> print key, value, partition, offset
break
```

7. Close consumer:

```text
consumer.close()
```

Checkpoint:

```bash
uv run producer.py
```

Expected:

```text
prints 1 message from product_view
```

## Step 2: Extend `producer.py` To Produce Locally

Goal:

```text
external Kafka product_view -> local Kafka product_view_local
```

Add local Kafka env vars:

```text
KAFKA_LOCAL_BOOTSTRAP_SERVERS
KAFKA_LOCAL_TOPIC
```

Add Kafka `Producer` config:

```text
bootstrap.servers = localhost:9094
acks = all
```

Flow:

```text
consume message from source
keep same key
keep same value
produce to local topic
flush producer
commit source offset
```

Checkpoint:

```bash
uv run producer.py
```

Then verify local Kafka:

```bash
docker exec kafka-0 kafka-console-consumer --bootstrap-server kafka-0:29092 --topic product_view_local --from-beginning --property print.key=true --property print.partition=true --property print.offset=true --timeout-ms 5000
```

Expected:

```text
message appears in product_view_local
```

## Step 3: Build `mongo.py`

Goal:

```text
small helper for MongoDB insert
```

Env vars:

```text
MONGO_URI
MONGO_DATABASE
MONGO_COLLECTION
```

Functions:

```text
get_collection()
insert_product_view(document)
```

Keep it boring. No Kafka code in `mongo.py`.

## Step 4: Build `consumer.py`

Goal:

```text
local Kafka product_view_local -> MongoDB
```

Build order:

1. Load `.env`
2. Create Kafka `Consumer`
3. Subscribe to `product_view_local`
4. Poll messages
5. Convert Kafka message to Mongo document:

```text
kafka_key
kafka_topic
kafka_partition
kafka_offset
value
```

6. Insert document with `mongo.py`
7. Commit Kafka offset after Mongo insert succeeds
8. Close consumer

Checkpoint:

```bash
uv run consumer.py
```

Expected:

```text
messages from product_view_local saved to MongoDB
```

## Step 5: Final Test

1. Start local Kafka.
2. Start MongoDB.
3. Create local topic if needed:

```bash
docker exec kafka-0 kafka-topics --bootstrap-server kafka-0:29092 --create --topic product_view_local --partitions 3 --replication-factor 3
```

4. Run producer:

```bash
MAX_MESSAGES=10 uv run producer.py
```

5. Run consumer:

```bash
MAX_MESSAGES=10 uv run consumer.py
```

Final success:

```text
10 messages copied from external Kafka to local Kafka
10 messages loaded from local Kafka to MongoDB
```

## Important Rules

Do not commit `.env`.

Commit source offset only after local Kafka produce succeeds.

Commit local Kafka offset only after MongoDB insert succeeds.

Use small `MAX_MESSAGES` while testing:

```text
1
5
10
```
