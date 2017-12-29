[![Build Status](https://travis-ci.org/tulios/kafkajs.svg?branch=master)](https://travis-ci.org/tulios/kafkajs)
[![npm version](https://badge.fury.io/js/kafkajs.svg)](https://badge.fury.io/js/kafkajs)

# KafkaJS

A modern Apache Kafka client for node.js. This library is compatible with Kafka `0.10+`.

__In active development - alpha__

## Features

- Producer
- Consumer groups
- GZIP compression
- Plain, SSL and SASL_SSL implementations

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
  - [Setting up the Client](#setup-client)
    - [SSL](#setup-client-ssl)
    - [SASL](#setup-client-sasl)
    - [Connection timeout](#setup-client-connection-timeout)
    - [Default retry](#setup-client-default-retry)
    - [Logger](#setup-client-logger)
  - [Producing Messages to Kafka](#producing-messages)
    - [Custom partitioner](#producing-messages-custom-partitioner)
    - [Retry](#producing-messages-retry)
  - [Consuming messages from Kafka](consuming-messages)
    - [Options](consuming-messages-options)
    - [Custom assigner](#consuming-messages-custom-assigner)
  - [Instrumentation](#instrumentation)
  - [Development](#development)

## <a name="installation"></a> Installation

```sh
npm install kafkajs
# yarn add kafkajs
```

## <a name="usage"></a> Usage

### <a name="setup-client"></a> Setting up the Client

The client must be configured with at least one broker. The brokers on the list are considered seed brokers and are only used to bootstrap the client and load initial metadata.

```javascript
const { Kafka } = require('kafkajs')

// Create the client with the broker list
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092']
})
```

#### <a name="setup-client-ssl"></a> SSL

The `ssl` option can be used to configure the TLS sockets. The options are passed directly to `tls.connect` and used to create the TLS Secure Context, all options are accepted.

```javascript
const fs = require('fs')

new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  ssl: {
    rejectUnauthorized: false,
    ca: [fs.readFileSync('/my/custom/ca.crt', 'utf-8')],
    key: fs.readFileSync('/my/custom/client-key.pem', 'utf-8'),
    cert: fs.readFileSync('/my/custom/client-cert.pem', 'utf-8')
  },
})
```

Take a look at [TLS create secure context](https://nodejs.org/dist/latest-v8.x/docs/api/tls.html#tls_tls_createsecurecontext_options) for more information.

#### <a name="setup-client-sasl"></a> SASL

Kafka has support for using SASL to authenticate clients. The `sasl` option can be used to configure the authentication mechanism. Currently, KafkaJS only supports the `PLAIN` mechanism.

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  sasl: {
    mechanism: 'plain',
    username: 'my-username',
    password: 'my-password'
  },
})
```

It is __highly recommended__ that you use SSL for encryption when using `PLAIN`.

#### <a name="setup-client-connection-timeout"></a> Connection Timeout

Time in milliseconds to wait for a successful connection. The default value is: `1000`.

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  connectionTimeout: 3000
})
```

#### <a name="setup-client-default-retry"></a> Default Retry

The `retry` option can be used to set the default configuration. The retry mechanism uses a randomization function that grows exponentially.

Available options:

* __maxRetryTime__ - Maximum wait time for a retry in milliseconds. Default: `30000`
* __initialRetryTime__ - Initial value used to calculate the retry in milliseconds (This is still randomized following the randomization factor). Default: `300`
* __factor__ - Randomization factor. Default: `0.2`
* __multiplier__ - Exponential factor. Default: `2`
* __retries__ - Max number of retries per call. Default: `5`

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
})
```

#### <a name="setup-client-logger"></a> Logger

KafkaJS has a built-in `STDOUT` logger which outputs JSON. It also accepts a custom log creator which allows you to integrate your favorite logger library.
There are 5 log levels available: `NOTHING`, `ERROR`, `WARN`, `INFO`, and `DEBUG`. `INFO` is configured by default.

__How to configure the log level?__

```javascript
const { Kafka, logLevel } = require('kafkajs')

// Create the client with the broker list
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  logLevel: logLevel.ERROR
})
```

The environment variable `KAFKAJS_LOG_LEVEL` can also be used and it has precedence over the configuration in code, example:

```sh
KAFKAJS_LOG_LEVEL=info node code.js
```

__How to create a log creator?___

A log creator is a function which receives a log level and returns a log function. The log function receives two arguments: namespace and log.

`namespace` identifies the component which is performing the log, for example, connection or consumer.

`log` is an object with the following keys: `level`, `timestamp`, 'logger', and `message`

```javascript
{
  level: 'INFO', // NOTHING, ERROR, WARN, INFO, or DEBUG
  timestamp: '2017-12-29T13:39:54.575Z',
  logger: 'kafkajs',
  message: 'Started',
  // ... any other extra key provided to the log function
}
```

The general structure looks like this:

```javascript
const MyLogCreator = logLevel => (namespace, log) => {
  // Example:
  // const { level, timestamp, logger, message, ...others } = log
  // console.log(`${level} [${namespace}] ${message} ${JSON.stringify(others)}`)
}
```

Example using [winston](https://github.com/winstonjs/winston):

```javascript
const winstom = require('winston')
const toWinstomLogLevel = level => switch(level) {
  case 'NOTHING':
    return 'error'
  default:
    return level.toLowerCase()
}

const WinstomLogCreator = logLevel => {
  const logger = winston.createLogger({
    level: toWinstomLogLevel(logLevel),
    transports: [
      new winston.transports.Console(),
      new winston.transports.File({ filename: 'myapp.log' })
    ]
  })

  return (namespace, { level, message, ...extra }) => {
    logger.log({
      level: toWinstomLogLevel(level),
      message,
      extra
    })
  }
}
```

Once you have your log creator you can use the `logCreator` option to configure the client:

```javascript
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  logLevel: logLevel.ERROR,
  logCreator: WinstomLogCreator
})
```

### <a name="producing-messages"></a> Producing Messages to Kafka

To publish messages to Kafka you have to create a producer. By default the producer is configured to distribute the messages with the following logic:

- If a partition is specified in the message, use it
- If no partition is specified but a key is present choose a partition based on a hash (murmur2) of the key
- If no partition or key is present choose a partition in a round-robin fashion

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

#### <a name="producing-messages-custom-partitioner"></a> Custom partitioner

It's possible to assign a custom partitioner to the consumer. A partitioner is a function which returns another function responsible for the partition selection, something like this:

```javascript
const MyPartitioner = () => {
  // some initialization
  return ({ topic, partitionMetadata, message }) => {
    // select a partition based on some logic
    // return the partition number
    return 0
  }
}
```

`partitionMetadata` is an array of partitions with the following structure:

```javascript
[
  { partitionId: 1, leader: 1 },
  { partitionId: 2, leader: 2 },
  { partitionId: 0, leader: 0 }
]
```

To Configure your partitioner use the option `createPartitioner`.

```javascript
kafka.producer({ createPartitioner: MyPartitioner })
```

#### <a name="producing-messages-retry"></a> Retry

The option `retry` can be used to customize the configurations for the producer.

Take a look at [Retry](#setup-client-default-retry) for more information.

### <a name="consuming-messages"></a> Consuming messages from Kafka

KafkaJS only supports consumer groups.

Consumer groups allow a group of machines or processes to coordinate access to a list of topics, distributing the load among the consumers. When a consumer fails the load is automatically distributed to other members of the group. Consumer groups must have unique group ids.

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
  eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
    for (let message of batch.messages) {
      console.log({
        topic: batch.topic,
        partition: batch.partition,
        highWatermark: batch.highWatermark,
        message: {
          offset: message.offset,
          key: message.key.toString(),
          value: message.value.toString()
        }
      })

      await resolveOffset(message.offset)
      await heartbeat()
    }
  },
})

// remember to close your consumer when you leave
await consumer.disconnect()
```

#### <a name="consuming-messages-options"></a> Options

- __createPartitionAssigner__ - default: `round robin`
- __sessionTimeout__ - Timeout in milliseconds used to detect failures. The consumer sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, then the broker will remove this consumer from the group and initiate a rebalance. default: `30000`
- __heartbeatInterval__ - The expected time in milliseconds between heartbeats to the consumer coordinator. Heartbeats are used to ensure that the consumer's session stays active. The value must be set lower than session timeout. default: `3000`
- __maxBytesPerPartition__ - The maximum amount of data per-partition the server will return. This size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large message on a certain partition. default: `1048576` (1MB)
- __minBytes__ - Minimum amount of data the server should return for a fetch request, otherwise wait up to `maxWaitTimeInMs` for more data to accumulate. default: `1`
- __maxBytes__ - default: `10485760` (10MB)
- __maxWaitTimeInMs__ - The maximum amount of time in milliseconds the server will block before answering the fetch request if there isn’t sufficient data to immediately satisfy the requirement given by `minBytes`. default: `5000`,
- __retry__ - default: `{ retries: 10 }`

#### <a name="consuming-messages-custom-assigner"></a> Custom assigner

TODO: write

## <a name="instrumentation"></a> Instrumentation

TODO: write

## <a name="development"></a> Development

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
