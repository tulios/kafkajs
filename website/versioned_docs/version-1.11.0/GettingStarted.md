---
id: version-1.11.0-getting-started
title: Getting Started
sidebar_label: Getting Started
original_id: getting-started
---

Install KafkaJS using [`yarn`](https://yarnpkg.com/en/package/kafkajs):

```bash
yarn add kafkajs
```

Or [`npm`](https://www.npmjs.com/package/kafkajs):

```bash
npm install kafkajs
```

Let's start by instantiating the KafkaJS client by pointing it towards at least one broker:

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092']
})
```

Now to produce a message to a topic, we'll create a producer using our client:

```javascript
const producer = kafka.producer()

await producer.connect()
await producer.send({
  topic: 'test-topic',
  messages: [
    { value: 'Hello KafkaJS user!' },
  ],
})

await producer.disconnect()
```

Finally, to verify that our message has indeed been produced to the topic, let's create a consumer to consume our message:

```javascript
const consumer = kafka.consumer({ groupId: 'test-group' })

await consumer.connect()
await consumer.subscribe({ topic: 'test-topic' })

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({
      value: message.value.toString(),
    })
  },
})
```

**Congratulations, you just produced and consumed your first Kafka message!**

> Run into issues? Be sure to take a look at the [FAQ](FAQ.md).

