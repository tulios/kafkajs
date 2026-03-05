---
id: version-3.0.0-cdc-debezium
title: CDC with Debezium
original_id: cdc-debezium
---

# Change Data Capture (CDC) with Debezium

[Debezium](https://debezium.io/) is an open-source distributed platform for Change Data Capture. It captures row-level changes in your databases and streams them as events to Kafka topics. KafkaJS can consume these CDC events to build real-time data pipelines, sync data across services, maintain search indexes, update caches, and more.

## Architecture Overview

```
 Database (source)
       |
       v
  Debezium Connector (runs on Kafka Connect)
       |
       v
  Kafka Topics (one per table)
       |
       v
  KafkaJS Consumer (your application)
       |
       v
  Target (data warehouse, search index, cache, etc.)
```

Debezium runs as a Kafka Connect connector. It reads the database's change log (WAL, binlog, oplog) and produces CDC events to Kafka topics. Your KafkaJS consumer subscribes to these topics and processes the change events.

## Debezium Event Envelope

All Debezium connectors produce events with a common envelope structure:

```json
{
  "before": { "id": 1, "name": "Alice" },
  "after":  { "id": 1, "name": "Alice Smith" },
  "source": {
    "connector": "postgresql",
    "db": "inventory",
    "schema": "public",
    "table": "customers",
    "lsn": 33692784,
    "ts_ms": 1709654400000
  },
  "op": "u",
  "ts_ms": 1709654400123
}
```

| Field | Description |
|-------|-------------|
| `before` | Row state before the change (`null` for inserts) |
| `after` | Row state after the change (`null` for deletes) |
| `source` | Metadata about the source database, table, and position |
| `op` | Operation type: `c` (create), `u` (update), `d` (delete), `r` (snapshot read) |
| `ts_ms` | Timestamp when Debezium processed the event |

A `null` value (tombstone) indicates that Kafka log compaction should remove the record.

## Parsing CDC Events

A reusable helper for parsing Debezium events:

```javascript
const parseCDCEvent = (message) => {
  const value = message.value ? JSON.parse(message.value.toString()) : null
  const key = message.key ? JSON.parse(message.key.toString()) : null

  if (!value) {
    return { operation: 'tombstone', key, before: null, after: null, source: null }
  }

  const operationMap = { c: 'CREATE', u: 'UPDATE', d: 'DELETE', r: 'READ' }

  return {
    operation: operationMap[value.op] || value.op,
    before: value.before,
    after: value.after,
    source: value.source,
    timestamp: value.ts_ms,
    key,
  }
}
```

## PostgreSQL

### Connector Setup

PostgreSQL requires logical replication (`wal_level = logical`). Register the Debezium connector:

```json
{
  "name": "postgres-connector",
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
}
```

Topics follow the pattern: `{topic.prefix}.{schema}.{table}` (e.g., `dbserver1.public.customers`).

### KafkaJS Consumer

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })
const consumer = kafka.consumer({ groupId: 'cdc-postgres-handler' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({
    topics: ['dbserver1.public.customers', 'dbserver1.public.orders'],
    fromBeginning: true,
  })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = parseCDCEvent(message)
      const table = topic.split('.').pop()

      switch (event.operation) {
        case 'CREATE':
        case 'READ':
          console.log(`[${table}] INSERT:`, event.after)
          break
        case 'UPDATE':
          console.log(`[${table}] UPDATE:`, { before: event.before, after: event.after })
          break
        case 'DELETE':
          console.log(`[${table}] DELETE:`, event.before)
          break
      }
    },
  })
}

run()
```

### PostgreSQL-Specific Notes

- The `source.lsn` field contains the PostgreSQL Log Sequence Number, useful for tracking replication positions.
- Use `plugin.name: "pgoutput"` (built-in) or `"decoderbufs"` depending on your PostgreSQL setup.
- TOAST columns: Large columns stored externally will show `"__debezium_unavailable_value"` if unchanged during an update.

## MySQL

### Connector Setup

MySQL requires row-based binary logging (`binlog_format = ROW`). Register the connector:

```json
{
  "name": "mysql-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "localhost",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "184054",
    "topic.prefix": "mysqldb",
    "database.include.list": "inventory",
    "schema.history.internal.kafka.bootstrap.servers": "localhost:9092",
    "schema.history.internal.kafka.topic": "schema-changes.inventory"
  }
}
```

Topics follow the pattern: `{topic.prefix}.{database}.{table}` (e.g., `mysqldb.inventory.customers`).

### KafkaJS Consumer

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })
const consumer = kafka.consumer({ groupId: 'cdc-mysql-handler' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({
    topics: ['mysqldb.inventory.customers', 'mysqldb.inventory.orders'],
    fromBeginning: true,
  })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = parseCDCEvent(message)
      const [, database, table] = topic.split('.')

      switch (event.operation) {
        case 'CREATE':
        case 'READ':
          console.log(`[${database}.${table}] INSERT:`, event.after)
          break
        case 'UPDATE':
          console.log(`[${database}.${table}] UPDATE:`, { before: event.before, after: event.after })
          break
        case 'DELETE':
          console.log(`[${database}.${table}] DELETE:`, event.before)
          break
      }
    },
  })
}

run()
```

### MySQL-Specific Notes

- `source.file` and `source.pos` contain the binlog file name and position.
- MySQL requires a unique `database.server.id` for each connector (mimics a MySQL replica).
- Schema history is tracked in a separate internal Kafka topic (`schema.history.internal.kafka.topic`).
- `DATETIME` and `TIMESTAMP` columns are represented as epoch milliseconds by default.

## MongoDB

### Connector Setup

MongoDB requires a replica set (for change streams). Register the connector:

```json
{
  "name": "mongodb-connector",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string": "mongodb://localhost:27017/?replicaSet=rs0",
    "topic.prefix": "mongodb",
    "collection.include.list": "inventory.customers,inventory.orders",
    "capture.mode": "change_streams_update_full"
  }
}
```

Topics follow the pattern: `{topic.prefix}.{database}.{collection}` (e.g., `mongodb.inventory.customers`).

### KafkaJS Consumer

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })
const consumer = kafka.consumer({ groupId: 'cdc-mongodb-handler' })

// MongoDB documents are JSON-encoded strings in Debezium events
const parseDocument = (doc) => {
  if (!doc) return null
  return typeof doc === 'string' ? JSON.parse(doc) : doc
}

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({
    topics: ['mongodb.inventory.customers'],
    fromBeginning: true,
  })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value ? JSON.parse(message.value.toString()) : null
      if (!value) return // tombstone

      const after = parseDocument(value.after)
      const before = parseDocument(value.before)

      switch (value.op) {
        case 'c':
        case 'r':
          console.log('INSERT:', after)
          break
        case 'u':
          console.log('UPDATE:', {
            document: after,
            updatedFields: value.updateDescription?.updatedFields,
            removedFields: value.updateDescription?.removedFields,
          })
          break
        case 'd':
          console.log('DELETE:', before)
          break
      }
    },
  })
}

run()
```

### MongoDB-Specific Notes

- Documents in `before` and `after` are JSON-encoded strings, not objects. Parse them with `JSON.parse()`.
- Use `capture.mode: "change_streams_update_full"` to get the full document on updates (otherwise only the `_id` is included).
- The `updateDescription` field provides `updatedFields` and `removedFields` for partial update tracking.
- MongoDB requires a replica set even for single-node development (use `rs.initiate()` in mongosh).

## Using with Dead Letter Queue

Combine CDC consumption with the KafkaJS Dead Letter Queue for robust error handling:

```javascript
const { Kafka, deadLetterQueue } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })
const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'cdc-handler' })

const withDLQ = deadLetterQueue({
  producer,
  topic: 'cdc-events.dlq',
  maxRetries: 3,
  onOriginalMessageFailed: async ({ topic, partition, message }, error) => {
    console.error('CDC event processing failed', { topic, offset: message.offset, error: error.message })
  },
})

const processCDCEvent = async ({ topic, partition, message }) => {
  const event = parseCDCEvent(message)
  // Your processing logic - will retry 3 times, then send to DLQ
  await syncToDataWarehouse(event)
}

const run = async () => {
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topics: ['dbserver1.public.customers'], fromBeginning: true })
  await consumer.run({
    eachMessage: withDLQ(processCDCEvent),
  })
}

run()
```

## Docker Compose for Local Development

A minimal Docker Compose setup for testing CDC locally:

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports: ['9092:9092']
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  postgres:
    image: postgres:16
    ports: ['5432:5432']
    environment:
      POSTGRES_USER: debezium
      POSTGRES_PASSWORD: dbz
      POSTGRES_DB: inventory
    command: ['postgres', '-c', 'wal_level=logical']

  connect:
    image: debezium/connect:2.5
    depends_on: [kafka, postgres]
    ports: ['8083:8083']
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: 1
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
```

Start with `docker compose up -d`, then register the connector:

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
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

## Examples

Full working examples are available in the repository:

- [`examples/cdc-debezium-postgres.js`](../examples/cdc-debezium-postgres.js) - PostgreSQL CDC consumer
- [`examples/cdc-debezium-mysql.js`](../examples/cdc-debezium-mysql.js) - MySQL CDC consumer
- [`examples/cdc-debezium-mongodb.js`](../examples/cdc-debezium-mongodb.js) - MongoDB CDC consumer
