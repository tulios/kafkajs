---
id: dead-letter-queue
title: Dead Letter Queue
---

# Dead Letter Queue (DLQ)

A Dead Letter Queue is a common pattern for handling messages that cannot be processed successfully. Instead of blocking the consumer or losing the message, failed messages are forwarded to a separate "dead letter" topic for later inspection, debugging, or reprocessing.

KafkaJS provides a built-in `deadLetterQueue` utility that wraps your `eachMessage` handler with retry and DLQ logic.

## Usage

```javascript
const { Kafka, deadLetterQueue } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })
const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'my-group' })

const withDLQ = deadLetterQueue({
  producer,
  topic: 'my-topic.dlq',
  maxRetries: 3,
})

const run = async () => {
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topics: ['my-topic'], fromBeginning: true })

  await consumer.run({
    eachMessage: withDLQ(async ({ topic, partition, message }) => {
      // Your message processing logic
      const data = JSON.parse(message.value.toString())
      await processData(data)
    }),
  })
}

run()
```

## How It Works

1. Your `eachMessage` handler is called as usual.
2. If the handler throws an error, it is retried up to `maxRetries` times (default: 3).
3. If all retries are exhausted, the original message is produced to the configured DLQ topic with additional headers containing error metadata.
4. The consumer continues processing the next message without crashing.

## Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `producer` | `Producer` | *required* | A KafkaJS producer instance (must be connected before consumer starts) |
| `topic` | `string` | *required* | The dead letter queue topic name |
| `maxRetries` | `number` | `3` | Number of times to retry the handler before sending to DLQ |
| `onOriginalMessageFailed` | `function` | `undefined` | Callback invoked after a message is sent to the DLQ |
| `createDLQMessage` | `function` | `undefined` | Custom function to build the DLQ message |

## DLQ Message Headers

When a message is sent to the DLQ, the following headers are added automatically:

| Header | Description |
|--------|-------------|
| `dlq.original.topic` | The original topic the message was consumed from |
| `dlq.original.partition` | The original partition number |
| `dlq.original.offset` | The original message offset |
| `dlq.original.timestamp` | The original message timestamp |
| `dlq.error.message` | The error message from the last failed attempt |
| `dlq.error.name` | The error class name |

The original message key, value, and headers are preserved.

## Callbacks

### `onOriginalMessageFailed`

Called after a message has been sent to the DLQ. Useful for logging, metrics, or alerting.

```javascript
const withDLQ = deadLetterQueue({
  producer,
  topic: 'orders.dlq',
  onOriginalMessageFailed: async ({ topic, partition, message }, error) => {
    console.error(`Message failed after all retries`, {
      topic,
      partition,
      offset: message.offset,
      error: error.message,
    })
    metrics.increment('dlq.messages.count', { topic })
  },
})
```

### `createDLQMessage`

Customize the message that is produced to the DLQ topic. This gives you full control over the key, value, and headers.

```javascript
const withDLQ = deadLetterQueue({
  producer,
  topic: 'orders.dlq',
  createDLQMessage: (error, { topic, partition, message }) => ({
    key: message.key,
    value: JSON.stringify({
      originalValue: message.value.toString(),
      error: {
        message: error.message,
        stack: error.stack,
      },
      metadata: {
        originalTopic: topic,
        originalPartition: partition,
        originalOffset: message.offset,
        failedAt: new Date().toISOString(),
      },
    }),
    headers: {
      'dlq.error.name': error.name,
    },
  }),
})
```

## Full Example

```javascript
const { Kafka, deadLetterQueue } = require('kafkajs')

const kafka = new Kafka({ brokers: ['localhost:9092'] })
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

const processOrder = async ({ topic, partition, message }) => {
  const order = JSON.parse(message.value.toString())

  if (!order.id) {
    throw new Error('Order is missing an ID')
  }

  await saveOrder(order)
}

const run = async () => {
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topics: ['orders'], fromBeginning: true })

  await consumer.run({
    eachMessage: withDLQ(processOrder),
  })
}

run()
```

## Notes

- The producer must be connected before the consumer starts processing messages.
- The DLQ topic must exist (or auto-creation must be enabled on the broker).
- If producing to the DLQ topic itself fails, the error will propagate and the consumer's normal error handling (retry/crash) will apply. This ensures no silent message loss.
- The DLQ utility works with the `eachMessage` handler. For `eachBatch`, you can implement similar logic within your batch handler using the same `producer.send()` pattern.
- Retries within the DLQ wrapper are immediate (no backoff). For more sophisticated retry strategies, consider combining with the consumer-level retry configuration or implementing custom backoff logic in `eachMessage`.
