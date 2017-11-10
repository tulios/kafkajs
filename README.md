[![Build Status](https://travis-ci.org/tulios/kafkajs.svg?branch=master)](https://travis-ci.org/tulios/kafkajs)

# KafkaJS

__In active development - early alpha__

- Fully working producer compatible with 0.10.x (0.9.x is possible)
- GZIP compression
- Plain, SSL and SASL_SSL implementations

## Usage

### Setting up the Client

```javascript
const Kafka = require('kafkajs')

// Create the client with the host and port of your
// seed broker
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
}
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
