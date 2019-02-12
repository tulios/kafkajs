[![KafkaJS](https://raw.githubusercontent.com/tulios/kafkajs/master/logo.png)](https://kafka.js.org)

# [KafkaJS](https://kafka.js.org)

[![Build Status](https://travis-ci.org/tulios/kafkajs.svg?branch=master)](https://travis-ci.org/tulios/kafkajs)
[![npm version](https://badge.fury.io/js/kafkajs.svg)](https://badge.fury.io/js/kafkajs)
[![Slack Channel](https://kafkajs-slackin.herokuapp.com/badge.svg)](https://kafkajs-slackin.herokuapp.com/)


A modern Apache Kafka client for node.js. This library is compatible with Kafka `0.10+`.  
Native support for Kafka `0.11` features.

KafkaJS is battle-tested and ready for production.

## Features

- Producer
- Consumer groups with pause, resume, and seek
- Message headers
- GZIP compression
- Snappy and LZ4 compression through plugins
- Plain, SSL and SASL_SSL implementations
- Support for SCRAM-SHA-256 and SCRAM-SHA-512
- Admin client

## <a name="getting-started"></a> Getting Started

```sh
npm install kafkajs
# yarn add kafkajs
```

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092']
})

// Producing
const producer = kafka.producer()

await producer.connect()
await producer.send({
  topic: 'test-topic',
  messages: [
    { value: 'Hello KafkaJS user!' },
  ],
})

// Consuming
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

## Documentation

Learn more about using [KafkaJS on the official site!](https://kafka.js.org)

- [Getting Started](https://kafka.js.org/docs/getting-started)
- [A Brief Intro to Kafka](https://kafka.js.org/docs/introduction)
- [Configuring KafkaJS](https://kafka.js.org/docs/configuration)
- [Example Producer](https://kafka.js.org/docs/producer-example)
- [Example Consumer](https://kafka.js.org/docs/consumer-example)

## <a name="contributing"></a> Contributing

KafkaJS is an open-source project where development takes place in the open on GitHub. Although the project is maintained by a small group of dedicated volunteers, we are grateful to the community for bugfixes, feature development and other contributions.

### <a name="contributing-resources"></a> Resources

https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

https://kafka.apache.org/protocol.html


### <a name="contributing-testing"></a> Testing

```sh
yarn test
```

or

```sh
# This will run a kafka cluster configured with your current IP
./scripts/dockerComposeUp.sh
./scripts/createScramCredentials.sh
yarn test:local

# To run with logs
# KAFKAJS_LOG_LEVEL=debug yarn test:local
```

Password for test keystore and certificates: `testtest`
Password for SASL `test:testtest`

### <a name="environment-variables"></a> Environment variables

| variable                       | description                              | default |
| ------------------------------ | ---------------------------------------- | ------- |
| KAFKAJS_DEBUG_PROTOCOL_BUFFERS | Output raw protocol buffers in debug log | 0       |

## Acknowledgements

Thanks to [Sebastian Norde](https://github.com/sebastiannorde) for the logo ❤️

## License

See [LICENSE](https://github.com/tulios/kafkajs/blob/master/LICENSE) for more details.
