# Product View Pipeline

Simple Kafka pipeline:

```text
external Kafka product_view -> local Kafka product_view_local -> MongoDB
```

## Files

```text
producer.py  external Kafka -> local Kafka
consumer.py  local Kafka -> MongoDB
mongo.py     MongoDB helper
.env         local config/secrets
```

## Setup

Start services:

```bash
docker compose up -d
```

Create local topic:

```bash
docker exec kafka-0 kafka-topics --bootstrap-server kafka-0:29092 --create --topic product_view_local --partitions 3 --replication-factor 3
```

If topic already exists, ignore the error.

## Run

Set message limit in `.env`:

```env
MAX_MESSAGES=10
```

Copy external Kafka messages to local Kafka:

```bash
uv run producer.py
```

Load local Kafka messages to MongoDB:

```bash
uv run consumer.py
```

## Verify

```bash
docker exec -it mongodb mongosh
```

```javascript
use product_view_pipeline
db.product_views.countDocuments()
db.product_views.findOne()
```
