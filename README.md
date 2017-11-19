[![Build Status](https://travis-ci.org/tulios/kafkajs.svg?branch=master)](https://travis-ci.org/tulios/kafkajs)

# KafkaJS

A modern Apache Kafka client for node.js

__In active development - early alpha__

- Fully working producer compatible with 0.10.x (0.9.x will be possible soon)
- Fully working consumer groups compatible with 0.10.x (0.9.x will be possible soon)
- GZIP compression
- Plain, SSL and SASL_SSL implementations

## Usage

### Setting up the Client

```javascript
const { Kafka } = require('kafkajs')

// Create the client with broker list
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092']
})
```

### Producing Messages to Kafka

```javascript
const producer = kafka.producer()

async () => {
  await producer.connect()
  await producer.send({
    topic: 'topic-name',
    messages: [
      { key: 'key1', value: 'hello world' },
      { key: 'key2', value: 'hey hey!' }
    ],
  })

  // before you exit your app
  await producer.disconnect()
}
```

### Consuming messages with consumer groups

```javascript
const consumer = kafka.consumer({ groupId: 'my-group' })

async () => {
  await consumer.connect()

  // Subscribe can be called several times
  await consumer.subscribe({ topic: 'topic-name' })

  // It's possible to start from the beginning:
  // await consumer.subscribe({ topic: 'topic-name', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        key: message.key.toString(),
        value: message.value.toString()
      })
    },
  })

  // before you exit your app
  await consumer.disconnect()
}
```

it's also possible to consume the batch instead of each message, example:

```javascript
// create consumer, connect and subscribe ...

await consumer.run({
  eachBatch: async ({ batch }) => {
    console.log({
      topic: batch.topic
      partition: batch.partition
      highWatermark: batch.highWatermark
      messages: batch.messages
    })
  },
})

// remember to close your consumer when you leave
```

### Configure SSL and SASL

```javascript
const fs = require('fs')
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092']
  ssl: {
    cert: fs.readFileSync('<path/to>/client_cert.pem', 'utf-8'),
    key: fs.readFileSync('<path/to>/client_key.pem', 'utf-8'),
    ca: [fs.readFileSync('<path/to>/ca_cert.pem', 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'my-username',
    password: 'my-password',
  },
})
```

## Development

https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol  
http://kafka.apache.org/protocol.html

```sh
yarn test
```

or

```sh
# This will run a kafka cluster configured with your current IP
./scripts/dockerComposeUp.sh
yarn test:local
```

Password for test keystore and certificates: `testtest`  
Password for SASL `test:testtest`
