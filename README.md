[![npm version](https://img.shields.io/npm/v/kafkajs?color=%2344cc11&label=stable)](https://www.npmjs.com/package/kafkajs) [![npm pre-release version](https://img.shields.io/npm/v/kafkajs/beta?label=pre-release)](https://www.npmjs.com/package/kafkajs) [![Build Status](https://dev.azure.com/tulios/kafkajs/_apis/build/status/tulios.kafkajs?branchName=master)](https://dev.azure.com/tulios/kafkajs/_build/latest?definitionId=2&branchName=master) [![Slack Channel](https://kafkajs-slackin.herokuapp.com/badge.svg)](https://kafkajs-slackin.herokuapp.com/)
<br />
<p align="center">
  <a href="https://kafka.js.org">
      <img src="https://raw.githubusercontent.com/tulios/kafkajs/master/logo/v2/kafkajs_circle.svg" alt="Logo" width="125" height="125">
  </a>

  <h3 align="center">KafkaJS</h3>

  <p align="center">
    A modern Apache Kafka® client for Node.js
    <br />
    <a href="https://kafka.js.org/"><strong>Get Started »</strong></a>
    <br />
    <br />
    <a href="https://kafka.js.org/docs/getting-started" target="_blank">Read the Docs</a>
    ·
    <a href="https://github.com/tulios/kafkajs/issues/new?assignees=&labels=&template=bug_report.md&title=">Report Bug</a>
    ·
    <a href="https://github.com/tulios/kafkajs/issues/new?assignees=&labels=&template=feature_request.md&title=">Request Feature</a>
  </p>
</p>

## Table of Contents

- [About the project](#about)
  - [Features](#features)
  - [Getting Started](#getting-started)
    - [Usage](#usage)
- [Contributing](#contributing)
  - [Help Wanted](#help-wanted)
  - [Contact](#contact)
- [Sponsors](#sponsors)
- [License](#license)
  - [Acknowledgements](#acknowledgements)

## <a name="about"></a> About the Project

KafkaJS is a modern [Apache Kafka](https://kafka.apache.org/) client for Node.js. It is compatible with Kafka 0.10+ and offers native support for 0.11 features.

### <a name="features"></a> Features

* Producer
* Consumer groups with pause, resume, and seek
* Transactional support for producers and consumers
* Message headers
* GZIP compression
  * Snappy, LZ4 and ZSTD compression through pluggable codecs
* Plain, SSL and SASL_SSL implementations
* Support for SCRAM-SHA-256 and SCRAM-SHA-512
* Support for AWS IAM authentication
* Admin client

### <a name="getting-started"></a> Getting Started

```sh
npm install kafkajs
# yarn add kafkajs
```

#### <a name="usage"></a> Usage
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

Learn more about using [KafkaJS on the official site!](https://kafka.js.org)

- [Getting Started](https://kafka.js.org/docs/getting-started)
- [A Brief Intro to Kafka](https://kafka.js.org/docs/introduction)
- [Configuring KafkaJS](https://kafka.js.org/docs/configuration)
- [Example Producer](https://kafka.js.org/docs/producer-example)
- [Example Consumer](https://kafka.js.org/docs/consumer-example)

> _Read something on the website that didn't work with the latest stable version?_  
[Check the pre-release versions](https://kafka.js.org/docs/pre-releases) - the website is updated on every merge to master.

## <a name="contributing"></a> Contributing

KafkaJS is an open-source project where development takes place in the open on GitHub. Although the project is maintained by a small group of dedicated volunteers, we are grateful to the community for bug fixes, feature development and other contributions.

See [Developing KafkaJS](https://kafka.js.org/docs/contribution-guide) for information on how to run and develop KafkaJS.

### <a name="help-wanted"></a> Help wanted 🤝

We welcome contributions to KafkaJS, but we also want to see a thriving third-party ecosystem. If you would like to create an open-source project that builds on top of KafkaJS, [please get in touch](https://kafkajs-slackin.herokuapp.com/) and we'd be happy to provide feedback and support.

Here are some projects that we would like to build, but haven't yet been able to prioritize:

* [Dead Letter Queue](https://eng.uber.com/reliable-reprocessing/) - Automatically reprocess messages
* ✅ [Schema Registry](https://www.confluent.io/confluent-schema-registry/) - **[Now available!](https://www.npmjs.com/package/@kafkajs/confluent-schema-registry)** thanks to [@erikengervall](https://github.com/erikengervall)
* [Metrics](https://prometheus.io/) - Integrate with the [instrumentation events](https://kafka.js.org/docs/instrumentation-events) to expose commonly used metrics

### <a name="contact"></a> Contact 💬

[Join our Slack community](https://kafkajs-slackin.herokuapp.com/)

## <a name="sponsors"></a> Sponsors ❤️

*To become a sponsor, [reach out in our Slack community](https://kafkajs-slackin.herokuapp.com/) to get in touch with one of the maintainers. Also consider becoming a Github Sponsor by following any of the links under "Sponsor this project" in the sidebar.*

<a href="https://www.confluent.io/confluent-cloud/?utm_source=kafkajs&utm_medium=opensource&utm_campaign=referral">
  <img src="https://raw.githubusercontent.com/tulios/kafkajs/master/logo/confluent/logo.png" width="830px">
</a>

## <a name="license"></a> License

See [LICENSE](https://github.com/tulios/kafkajs/blob/master/LICENSE) for more details.

### <a name="acknowledgements"></a> Acknowledgements

* Thanks to [Sebastian Norde](https://github.com/sebastiannorde) for the V1 logo ❤️
* Thanks to [Tracy (Tan Yun)](https://medium.com/@tanyuntracy) for the V2 logo ❤️

<small>Apache Kafka and Kafka are either registered trademarks or trademarks of The Apache Software Foundation in the United States and other countries. KafkaJS has no affiliation with the Apache Software Foundation.</small>
