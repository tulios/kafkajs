---
id: version-3.0.0-consumer-example
title: Consumer
original_id: consumer-example
---

The following example assumes that you are using the local Kafka configuration described in [Running Kafka in Development](DockerLocal.md).

```javascript
const { Kafka, logLevel } = require('kafkajs')

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: ['localhost:9092'],
  clientId: 'example-consumer',
})

const topic = 'topic-test'
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topics: [topic], fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
    },
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    await consumer.disconnect()
  })
})
```

## KRaft Mode (Kafka 4.0+)

```javascript
const { Kafka, logLevel } = require('kafkajs')

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',')
const topic = process.env.KAFKA_TOPIC || 'kraft-test-topic'
const groupId = process.env.KAFKA_GROUP_ID || 'kraft-test-group'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers,
  clientId: 'kafkajs-kraft-consumer',
})

const consumer = kafka.consumer({
  groupId,
  // Rack-aware consumption for follower fetching
  rackId: process.env.KAFKA_RACK_ID || '',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
})

let messageCount = 0

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topics: [topic], fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      messageCount++
      console.log('Message received', {
        topic,
        partition,
        offset: message.offset,
        key: message.key ? message.key.toString() : null,
        value: message.value.toString(),
        messageCount,
      })
    },
  })
}

run().catch(e => {
  console.error(`[kraft-consumer] ${e.message}`, e)
  process.exit(1)
})

process.on('SIGTERM', async () => {
  await consumer.disconnect()
})
process.on('SIGINT', async () => {
  await consumer.disconnect()
})
```

## Static Membership

Static membership (KIP-345) reduces unnecessary rebalances during consumer restarts. By assigning a unique `groupInstanceId` to each consumer instance, the broker treats it as a "static" member of the consumer group.

When a consumer with a `groupInstanceId` disconnects, the broker does not immediately trigger a rebalance. Instead, it waits for the duration of `session.timeout.ms` before removing the member. If the consumer reconnects with the same `groupInstanceId` before the session timeout expires, it resumes its previous partition assignments without triggering a rebalance.

This is especially useful in environments where consumers are frequently restarted, such as during rolling deployments.

```javascript
// Static membership reduces rebalances during consumer restarts
const consumer = kafka.consumer({
  groupId: 'my-group',
  groupInstanceId: 'instance-1', // Unique per consumer instance
})
```

## Dead Letter Queue

```javascript
const { Kafka, deadLetterQueue } = require('kafkajs')

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'example-dlq',
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'order-processing-group' })

const withDLQ = deadLetterQueue({
  producer,
  topic: 'orders.dlq',
  maxRetries: 3,
  onOriginalMessageFailed: async ({ topic, partition, message }, error) => {
    console.warn('Message sent to DLQ', {
      topic,
      partition,
      offset: message.offset,
      error: error.message,
    })
  },
})

const run = async () => {
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topics: ['orders'], fromBeginning: true })
  await consumer.run({
    eachMessage: withDLQ(async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString())
      if (!order.id) {
        throw new Error('Order is missing an ID')
      }
      console.log('Order processed', { orderId: order.id })
    }),
  })
}

run().catch(e => console.error(`[example/dlq] ${e.message}`, e))
```

## <a name="ssl-and-sasl-authentication"></a> SSL & SASL Authentication

```javascript
const { Kafka, logLevel } = require('kafkajs')

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: ['localhost:9094'],
  clientId: 'example-consumer',
  ssl: {
    rejectUnauthorized: true
  },
  sasl: {
    mechanism: 'scram-sha-256',
    username: 'test',
    password: 'testtest',
  },
})

const topic = 'topic-test'
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topics: [topic], fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
    },
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))
```
