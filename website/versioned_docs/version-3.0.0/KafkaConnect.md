---
id: version-3.0.0-kafka-connect
title: Kafka Connect
original_id: kafka-connect
---

# Kafka Connect

## Overview

[Kafka Connect](https://kafka.apache.org/documentation/#connect) is a framework for streaming data between Apache Kafka and other systems. It runs connectors — plugins that move data in (source connectors) or out (sink connectors) of Kafka.

KafkaJS does not include a Kafka Connect management API. Connect is managed through its own [REST API](https://kafka.apache.org/documentation/#connect_rest). However, KafkaJS is commonly used alongside Kafka Connect to:

- **Consume CDC events** produced by source connectors like [Debezium](https://debezium.io/)
- **Produce messages** to topics consumed by sink connectors
- **Monitor connector status** by consuming from Connect's internal topics

## Managing Connectors via REST API

Kafka Connect exposes a REST API (default port `8083`) for managing connectors:

### List Connectors

```bash
curl http://localhost:8083/connectors
```

### Create a Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "my-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "localhost",
      "database.port": "5432",
      "database.user": "debezium",
      "database.password": "dbz",
      "database.dbname": "inventory",
      "topic.prefix": "dbserver1",
      "schema.include.list": "public",
      "plugin.name": "pgoutput"
    }
  }'
```

### Check Connector Status

```bash
curl http://localhost:8083/connectors/my-connector/status
```

### Delete a Connector

```bash
curl -X DELETE http://localhost:8083/connectors/my-connector
```

## Consuming from Source Connectors

Source connectors produce messages to Kafka topics. Use a standard KafkaJS consumer to process them:

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })
const consumer = kafka.consumer({ groupId: 'connect-consumer' })

const run = async () => {
  await consumer.connect()
  // Debezium topics: {prefix}.{schema}.{table}
  await consumer.subscribe({ topics: [/dbserver1\..*/], fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = JSON.parse(message.value.toString())
      console.log(`[${topic}]`, value.op, value.after)
    },
  })
}

run()
```

For CDC-specific event parsing, see [CDC with Debezium](CDCDebezium.md).

## Producing to Sink Connectors

Sink connectors consume messages from Kafka topics and write them to external systems. Use a standard KafkaJS producer to write messages to topics that sink connectors monitor:

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })
const producer = kafka.producer()

const run = async () => {
  await producer.connect()

  // Produce to a topic consumed by an Elasticsearch sink connector
  await producer.send({
    topic: 'elasticsearch-sink-topic',
    messages: [
      {
        key: 'user-123',
        value: JSON.stringify({
          id: 123,
          name: 'Alice',
          email: 'alice@example.com',
          timestamp: Date.now(),
        }),
      },
    ],
  })

  await producer.disconnect()
}

run()
```

## Docker Compose Setup

A minimal setup with Kafka Connect for local development:

```yaml
version: '3.8'
services:
  kafka:
    image: apache/kafka:4.2.0
    ports: ['9092:9092']
    environment:
      KAFKA_NODE_ID: '1'
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://localhost:9092'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_LOG_DIRS: '/tmp/kraft-logs'
      CLUSTER_ID: 'kafkajs-dev-cluster-id-0001'

  connect:
    image: debezium/connect:2.5
    depends_on: [kafka]
    ports: ['8083:8083']
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: 1
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
```

## Common Connector Types

| Connector | Type | Description |
|-----------|------|-------------|
| [Debezium PostgreSQL](https://debezium.io/documentation/reference/connectors/postgresql.html) | Source | CDC from PostgreSQL WAL |
| [Debezium MySQL](https://debezium.io/documentation/reference/connectors/mysql.html) | Source | CDC from MySQL binlog |
| [Debezium MongoDB](https://debezium.io/documentation/reference/connectors/mongodb.html) | Source | CDC from MongoDB change streams |
| [Elasticsearch Sink](https://docs.confluent.io/kafka-connectors/elasticsearch/current/overview.html) | Sink | Index messages into Elasticsearch |
| [S3 Sink](https://docs.confluent.io/kafka-connectors/s3-sink/current/overview.html) | Sink | Write messages to Amazon S3 |
| [JDBC Source/Sink](https://docs.confluent.io/kafka-connectors/jdbc/current/overview.html) | Both | SQL database integration |

## See Also

- [CDC with Debezium](CDCDebezium.md) — Detailed guide for consuming Debezium CDC events
- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
