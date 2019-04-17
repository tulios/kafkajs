# <a href='https://kafka.js.org'><img src='https://raw.githubusercontent.com/tulios/kafkajs/master/logoV2.png' height='60' alt='KafkaJS' aria-label='kafka.js.org' /></a>

A modern Apache Kafka client for node.js. This library is compatible with Kafka `0.10+`.  
Native support for Kafka `0.11` features.

KafkaJS is battle-tested and ready for production.

[![Build Status](https://dev.azure.com/tulios/kafkajs/_apis/build/status/tulios.kafkajs?branchName=master)](https://dev.azure.com/tulios/kafkajs/_build/latest?definitionId=1&branchName=master)
[![npm version](https://badge.fury.io/js/kafkajs.svg)](https://badge.fury.io/js/kafkajs)
[![Slack Channel](https://kafkajs-slackin.herokuapp.com/badge.svg)](https://kafkajs-slackin.herokuapp.com/)

## Features

- Producer
- Consumer groups with pause, resume, and seek
- Transactional support for producers and consumers
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

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  // Producing
  await producer.connect()
  await producer.send({
    topic: 'test-topic',
    messages: [
      { value: 'Hello KafkaJS user!' },
    ],
  })

  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
    },
  })
}

run().catch(console.error)
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

See [Developing KafkaJS](https://kafka.js.org/docs/contribution-guide) for information on how to run and develop KafkaJS.

## Acknowledgements

Thanks to [Sebastian Norde](https://github.com/sebastiannorde) for the V1 logo ❤️

Thanks to [Tracy (Tan Yun)](https://medium.com/@tanyuntracy) for the V2 logo ❤️

### Sponsored by:

<a href="https://www.digitalocean.com/?refcode=9ee868b06152&utm_campaign=Referral_Invite&utm_medium=opensource&utm_source=kafkajs">
  <img src="https://opensource.nyc3.cdn.digitaloceanspaces.com/attribution/assets/SVG/DO_Logo_horizontal_blue.svg" width="201px">
</a>

## License

See [LICENSE](https://github.com/tulios/kafkajs/blob/master/LICENSE) for more details.
