---
id: consumer-example
title: Consumer
---

The following example assumes that you are using the local Kafka configuration described in [Running Kafka in Development](DockerLocal.md).

```javascript
const ip = require('ip')

const { Kafka, logLevel } = require('kafkajs')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:9092`],
  clientId: 'example-consumer',
})

const topic = 'topic-test'
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
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
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
```

## <a name="typescript-consumer-example"></a> TypeScript Example

A similar example in TypeScript

```typescript
import { Consumer, ConsumerSubscribeTopic, EachBatchPayload, Kafka, EachMessagePayload } from 'kafkajs'

// ExampleMessageProcessor is a placeholder for the logic applied to each 
// message or message batch and is not provided by kafkajs
import ExampleMessageProcessor from './ExampleMessageProcessor'

export default class ExampleConsumer {
  private kafkaConsumer: Consumer
  private messageProcessor: ExampleMessageProcessor

  public constructor(messageProcessor: ExampleMessageProcessor) {
    this.messageProcessor = messageProcessor
    this.kafkaConsumer = this.createKafkaConsumer()
  }

  public async startConsumer(): Promise<void> {
    const topic: ConsumerSubscribeTopic = {
      topic: 'example-topic',
      fromBeginning: false
    }

    try {
      await this.kafkaConsumer.connect()
      await this.kafkaConsumer.subscribe(topic)

      await this.kafkaConsumer.run({
        eachMessage: async (message: EachMessagePayload) => {
          await this.messageProcessor.processMessage(message)
        }
      })
    } catch (error) {
      console.log('Error: ', error)
    }
  }

  public async startBatchConsumer(): Promise<void> {
    const topic: ConsumerSubscribeTopic = {
      topic: 'example-topic',
      fromBeginning: false
    }

    try {
      await this.kafkaConsumer.connect()
      await this.kafkaConsumer.subscribe(topic)
      await this.kafkaConsumer.run({
        eachBatch: async (eatchBatchPayload: EachBatchPayload) => {
          await this.messageProcessor.processInBatch(eatchBatchPayload)
        }
      })
    } catch (error) {
      console.log('Error: ', error)
    }
  }

  public async shutdown(): Promise<void> {
    await this.kafkaConsumer.disconnect()
  }

  private createKafkaConsumer(): Consumer {
    const kafka = new Kafka({ 
      clientId: 'client-id',
      brokers: ['example.kafka.broker:9092']
    })
    const consumer = kafka.consumer({ groupId: 'consumer-group' })
    return consumer
  }
}
```

## <a name="ssl-and-sasl-authentication"></a> SSL & SASL Authentication

The following example assumes a valid SSL certificate and SASL authentication using the `scram-sha-256` mechanism. Other mechanisms are also available (see [Client Configuration](Configuration.md#sasl)).

```javascript
const ip = require('ip')

const { Kafka, logLevel } = require('../index')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:9094`],
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
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
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
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
```
