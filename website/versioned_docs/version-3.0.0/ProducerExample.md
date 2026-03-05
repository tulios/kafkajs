---
id: version-3.0.0-producer-example
title: Producer
original_id: producer-example
---

## Basic Producer Example

```javascript
const { Kafka, CompressionTypes, logLevel } = require('kafkajs')

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: ['localhost:9092'],
  clientId: 'example-producer',
})

const topic = 'topic-test'
const producer = kafka.producer()

const getRandomNumber = () => Math.round(Math.random() * 1000)
const createMessage = num => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
  headers: {
    'correlation-id': `${num}-${Date.now()}`,
  },
})

let msgNumber = 0
const sendMessage = () => {
  const messages = Array(getRandomNumber())
    .fill()
    .map(_ => createMessage(getRandomNumber()))

  msgNumber += messages.length
  console.log(`Sending ${messages.length} messages...`)
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages,
    })
    .then(response => {
      console.log(`Messages sent`, { response, msgNumber })
    })
    .catch(e => console.error(`[example/producer] ${e.message}`, e))
}

const run = async () => {
  await producer.connect()
  setInterval(sendMessage, 3000)
}

run().catch(e => console.error(`[example/producer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e.message, e)
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    console.log('[example/producer] disconnecting')
    await producer.disconnect()
  })
})
```

## KRaft Mode (Kafka 4.0+)

The following example demonstrates producing messages to a Kafka 4.0+ KRaft-native cluster (no ZooKeeper needed). It uses the `DefaultPartitioner` for Java-compatible partitioning and enables idempotent production.

```javascript
const { Kafka, CompressionTypes, logLevel, Partitioners } = require('kafkajs')

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',')
const topic = process.env.KAFKA_TOPIC || 'kraft-test-topic'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers,
  clientId: 'kafkajs-kraft-producer',
})

// Use DefaultPartitioner (Java-compatible) for Kafka 4.2+
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
  idempotent: true,
  maxInFlightRequests: 5,
})

const createMessage = num => ({
  key: `key-${num}`,
  value: JSON.stringify({
    id: num,
    timestamp: new Date().toISOString(),
    message: 'Hello from KafkaJS on Kafka 4.2 KRaft!',
  }),
  headers: {
    'correlation-id': `${num}-${Date.now()}`,
    source: 'kafkajs-kraft-example',
  },
})

let msgCount = 0

const run = async () => {
  await producer.connect()
  console.log('Producer connected to KRaft cluster', { brokers })

  setInterval(async () => {
    const count = Math.floor(Math.random() * 10) + 1
    const messages = Array(count)
      .fill()
      .map(() => createMessage(msgCount++))

    try {
      const result = await producer.send({
        topic,
        compression: CompressionTypes.GZIP,
        messages,
      })
      console.log('Messages sent', { partitions: result.map(r => r.partition), totalSent: msgCount })
    } catch (error) {
      console.error(`Failed to send: ${error.message}`)
    }
  }, 3000)
}

run().catch(e => {
  console.error(`[kraft-producer] ${e.message}`, e)
  process.exit(1)
})

process.on('SIGTERM', async () => {
  await producer.disconnect()
})
process.on('SIGINT', async () => {
  await producer.disconnect()
})
```

## SSL & SASL Authentication

See the [Consumer Example](ConsumerExample.md#ssl-and-sasl-authentication).
