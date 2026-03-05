---
id: version-3.0.0-kafka-streams
title: Kafka Streams
original_id: kafka-streams
---

# Kafka Streams

## Overview

[Kafka Streams](https://kafka.apache.org/documentation/streams/) is a Java library for building stream processing applications. It provides high-level abstractions like KStream, KTable, and windowed aggregations.

> **Note:** Kafka Streams is a Java-only library and is **not** part of KafkaJS. KafkaJS is a Node.js client that provides producer, consumer, and admin functionality.

If you need Kafka Streams-style processing in Node.js, you have several options:

## Option 1: Stream Processing with KafkaJS

You can build stream processing patterns using KafkaJS consumers and producers directly:

### Transform and Forward

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })
const consumer = kafka.consumer({ groupId: 'stream-processor' })
const producer = kafka.producer()

const run = async () => {
  await consumer.connect()
  await producer.connect()
  await consumer.subscribe({ topics: ['raw-events'], fromBeginning: false })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString())

      // Transform
      const enriched = {
        ...event,
        processedAt: new Date().toISOString(),
        source: topic,
      }

      // Forward to output topic
      await producer.send({
        topic: 'enriched-events',
        messages: [{ key: message.key, value: JSON.stringify(enriched) }],
      })
    },
  })
}

run()
```

### Filter Pattern

```javascript
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const event = JSON.parse(message.value.toString())

    // Filter: only forward events matching criteria
    if (event.type === 'ORDER' && event.amount > 100) {
      await producer.send({
        topic: 'high-value-orders',
        messages: [{ key: message.key, value: message.value }],
      })
    }
  },
})
```

### Fan-Out Pattern

```javascript
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const event = JSON.parse(message.value.toString())

    // Route to different topics based on content
    const targetTopic = `events-${event.region}`
    await producer.send({
      topic: targetTopic,
      messages: [{ key: message.key, value: message.value }],
    })
  },
})
```

### Aggregation with External State

```javascript
// Use an external store (Redis, database) for aggregation state
const Redis = require('ioredis')
const redis = new Redis()

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const event = JSON.parse(message.value.toString())

    // Increment counter in Redis
    const count = await redis.incr(`count:${event.userId}`)

    // Emit aggregated result
    if (count % 100 === 0) {
      await producer.send({
        topic: 'user-activity-summary',
        messages: [{
          key: event.userId,
          value: JSON.stringify({ userId: event.userId, eventCount: count }),
        }],
      })
    }
  },
})
```

## Option 2: Node.js Stream Processing Libraries

For more advanced stream processing in Node.js, consider these libraries that work with Kafka:

| Library | Description |
|---------|-------------|
| [kafka-streams](https://www.npmjs.com/package/kafka-streams) | KStream and KTable implementation for Node.js |
| [Apache Flink](https://flink.apache.org/) | Distributed stream processing (supports Kafka sources/sinks) |
| [Benthos](https://www.benthos.dev/) | Stream processor with Kafka input/output |

## Option 3: Use Kafka Streams (Java) Alongside KafkaJS

A common architecture uses Kafka Streams (Java) for complex stream processing and KafkaJS (Node.js) for:

- **Producing events** from Node.js web services
- **Consuming results** from Kafka Streams applications
- **Admin operations** on topics used by stream processors

```
  Node.js Service             Java Service              Node.js Service
  (KafkaJS Producer)      (Kafka Streams App)          (KafkaJS Consumer)
       |                        |                           |
       v                        v                           v
  [raw-events] -----> [Stream Processing] -----> [processed-events]
                     (filter, join, aggregate)
```

## Dead Letter Queue for Stream Processing

When building stream processors with KafkaJS, use the built-in Dead Letter Queue for error handling:

```javascript
const { Kafka, deadLetterQueue } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })
const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'stream-processor' })

const withDLQ = deadLetterQueue({
  producer,
  topic: 'stream-processing.dlq',
  maxRetries: 3,
})

const run = async () => {
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topics: ['input-topic'] })

  await consumer.run({
    eachMessage: withDLQ(async ({ message }) => {
      const event = JSON.parse(message.value.toString())
      const result = await processEvent(event)
      await producer.send({
        topic: 'output-topic',
        messages: [{ key: message.key, value: JSON.stringify(result) }],
      })
    }),
  })
}

run()
```

## See Also

- [Consuming Messages](Consuming.md) — Consumer API reference
- [Producing Messages](Producing.md) — Producer API reference
- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
