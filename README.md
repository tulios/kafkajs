[![KafkaJS](https://raw.githubusercontent.com/tulios/kafkajs/master/logo.png)](https://github.com/tulios/kafkajs)

# KafkaJS

[![Build Status](https://travis-ci.org/tulios/kafkajs.svg?branch=master)](https://travis-ci.org/tulios/kafkajs)
[![npm version](https://badge.fury.io/js/kafkajs.svg)](https://badge.fury.io/js/kafkajs)

A modern Apache Kafka client for node.js. This library is compatible with Kafka `0.10+`.

KafkaJS is battle-tested and ready for production.

## Features

- Producer
- Consumer groups with pause, resume, and seek
- GZIP compression
- Plain, SSL and SASL_SSL implementations
- Support for SCRAM-SHA-256 and SCRAM-SHA-512

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
  - [SSL](#configuration-ssl)
  - [SASL](#configuration-sasl)
  - [Connection timeout](#configuration-connection-timeout)
  - [Default retry](#configuration-default-retry)
  - [Logging](#configuration-logging)
- [Producing Messages](#producing-messages)
  - [Producing to multiple topics](#producing-messages-to-multiple-topics)
  - [Custom partitioner](#producing-messages-custom-partitioner)
  - [Retry](#producing-messages-retry)
  - [Compression](#producing-messages-compression)
    - [GZIP](#producing-messages-compression-gzip)
    - [Other](#producing-messages-compression-other)
- [Consuming messages](#consuming-messages)
  - [eachMessage](#consuming-messages-each-message)
  - [eachBatch](#consuming-messages-each-batch)
  - [Options](#consuming-messages-options)
  - [Pause, Resume, & Seek](#consuming-messages-pause-resume)
  - [Seek](#consuming-messages-seek)
  - [Custom partition assigner](#consuming-messages-custom-partition-assigner)
  - [Describe group](#consuming-messages-describe-group)
  - [Compression](#consuming-messages-compression)
- [Admin](#admin)
  - [Create Topics](#admin-create-topics)
- [Instrumentation](#instrumentation)
- [Custom logging](#custom-logging)
- [Retry (detailed)](#configuration-default-retry-detailed)
- [Development](#development)

## <a name="installation"></a> Installation

```sh
npm install kafkajs
# yarn add kafkajs
```

## <a name="setup-client"></a> Configuration

The client must be configured with at least one broker. The brokers on the list are considered seed brokers and are only used to bootstrap the client and load initial metadata.

```javascript
const { Kafka } = require('kafkajs')

// Create the client with the broker list
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092']
})
```

### <a name="configuration-ssl"></a> SSL

The `ssl` option can be used to configure the TLS sockets. The options are passed directly to [`tls.connect`](https://nodejs.org/api/tls.html#tls_tls_connect_options_callback) and used to create the TLS Secure Context, all options are accepted.

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

Refer to [TLS create secure context](https://nodejs.org/dist/latest-v8.x/docs/api/tls.html#tls_tls_createsecurecontext_options) for more information. `NODE_EXTRA_CA_CERTS` can be used to add custom CAs. Use `ssl: true` if you don't have any extra configurations and want to enable SSL.

### <a name="configuration-sasl"></a> SASL

Kafka has support for using SASL to authenticate clients. The `sasl` option can be used to configure the authentication mechanism. Currently, KafkaJS supports `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512` mechanisms.

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  // authenticationTimeout: 1000,
  sasl: {
    mechanism: 'plain', // scram-sha-256 or scram-sha-512
    username: 'my-username',
    password: 'my-password'
  },
})
```

It is __highly recommended__ that you use SSL for encryption when using `PLAIN`.

### <a name="configuration-connection-timeout"></a> Connection Timeout

Time in milliseconds to wait for a successful connection. The default value is: `1000`.

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
  connectionTimeout: 3000
})
```

### <a name="configuration-default-retry"></a> Default Retry

The `retry` option can be used to set the configuration of the retry mechanism, which is be used to retry connections and API calls to Kafka (when using producers or consumers).

The retry mechanism uses a randomization function that grows exponentially.
[Detailed example](#configuration-default-retry-detailed)

If the max number of retries is exceeded the retrier will throw `KafkaJSNumberOfRetriesExceeded` and interrupt. Producers will bubble up the error to the user code; Consumers will wait the retry time attached to the exception (it will be based on the number of attempts) and perform a full restart.

__Available options:__

| option           | description                                                                                                             | default |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------- | ------- |
| maxRetryTime     | Maximum wait time for a retry in milliseconds                                                                           | `30000` |
| initialRetryTime | Initial value used to calculate the retry in milliseconds (This is still randomized following the randomization factor) | `300`   |
| factor           | Randomization factor                                                                                                    | `0.2`   |
| multiplier       | Exponential factor                                                                                                      | `2`     |
| retries          | Max number of retries per call                                                                                          | `5`     |

Example:

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

### <a name="configuration-logging"></a> Logging

KafkaJS has a built-in `STDOUT` logger which outputs JSON. It also accepts a custom log creator which allows you to integrate your favorite logger library. There are 5 log levels available: `NOTHING`, `ERROR`, `WARN`, `INFO`, and `DEBUG`. `INFO` is configured by default.

##### Log level

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

NOTE: for more information on how to customize your logs, take a look at [Custom logging](#custom-logging)

## <a name="producing-messages"></a> Producing Messages

To publish messages to Kafka you have to create a producer. Simply call the `producer` function of the client to create it:

```javascript
const producer = kafka.producer()
```

The method `send` is used to publish messages to the Kafka cluster.

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

Example with a defined partition:

```javascript
// ...require and connect...
async () => {
  await producer.send({
    topic: 'topic-name',
    messages: [
      { key: 'key1', value: 'hello world', partition: 0 },
      { key: 'key2', value: 'hey hey!', partition: 1 }
    ],
  })
}
```

The method `send` has the following signature:

```javascript
await producer.send({
  topic: <String>,
  messages: <Message[]>,
  acks: <Number>,
  timeout: <Number>,
  compression: <CompressionTypes>,
})
```

| property    | description                                                                                                                                                                        |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| topic       | topic name                                                                                                                                                                         |
| messages    | An array of objects with "key" and "value", example: <br> `[{ key: 'my-key', value: 'my-value'}]`                                                                                  |
| acks        | Control the number of required acks. <br> __-1__ = all replicas must acknowledge _(default)_ <br> __0__ = no acknowledgments <br> __1__ = only waits for the leader to acknowledge |
| timeout     | The time to await a response in ms. Default value _30000_                                                                                                                          |
| compression | Compression codec. Default value `CompressionTypes.None`                                                                                                                           |

By default, the producer is configured to distribute the messages with the following logic:

- If a partition is specified in the message, use it
- If no partition is specified but a key is present choose a partition based on a hash (murmur2) of the key
- If no partition or key is present choose a partition in a round-robin fashion

### <a name="producing-messages-to-multiple-topics"></a> Producing to multiple topics

To produce to multiple topics at the same time, use `sendBatch`. This can be useful, for example, when migrating between two topics.

```javascript
const topicMessages = [
  {
    topic: 'topic-a',
    messages: [{ key: 'key', value: 'hello topic-a' }],
  },
  {
    topic: 'topic-b',
    messages: [{ key: 'key', value: 'hello topic-b' }],
  }
]
await producer.sendBatch({ topicMessages })
```

`sendBatch` has the same signature as `send`, except `topic` and `messages` are replaced with `topicMessages`:

```javascript
await producer.sendBatch({
  topicMessages: <TopicMessages[]>,
  acks: <Number>,
  timeout: <Number>,
  compression: <CompressionTypes>,
})
```

| property      | description                                                                                                |
| ------------- | ---------------------------------------------------------------------------------------------------------- |
| topicMessages | An array of objects with `topic` and `messages`.<br>`messages` is an array of the same type as for `send`. |

### <a name="producing-messages-custom-partitioner"></a> Custom partitioner

It's possible to assign a custom partitioner to the producer. A partitioner is a function which returns another function responsible for the partition selection, something like this:

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

`{ partitionId: <NodeId>, leader: <NodeId> }`

Example:

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

### <a name="producing-messages-retry"></a> Retry

The option `retry` can be used to customize the configuration for the producer.

Take a look at [Retry](#configuration-default-retry) for more information.

### <a name="producing-messages-compression"></a> Compression

Since KafkaJS aims to have as small footprint and as little dependencies as possible, only GZIP codec is part of the core functionality. Providing plugins supporting other codecs might be considered in the future.

#### <a name="producing-messages-compression-gzip"></a> GZIP

```javascript
const { CompressionTypes } = require('kafkajs')

async () => {
  await producer.send({
    topic: 'topic-name',
    compression: CompressionTypes.GZIP,
    messages: [
      { key: 'key1', value: 'hello world' },
      { key: 'key2', value: 'hey hey!' }
    ],
  })
}
```

The consumers know how to decompress GZIP, so no further work is necessary.

#### <a name="producing-messages-compression-other"></a> Other

Any other codec than GZIP can be easily implemented using existing libraries.

This is an example of how one would go about in order to add the Snappy codec.

First of all, a codec is an object with two `async` functions: `compress` and `decompress`. Import the libraries and define the codec object:

```javascript
const { promisify } = require('util')
const snappy = require('snappy')

const snappyCompress = promisify(snappy.compress)
const snappyDecompress = promisify(snappy.uncompress)

const SnappyCodec = {
  async compress(encoder) {
    return snappyCompress(encoder.buffer)
  },

  async decompress(buffer) {
    return snappyDecompress(buffer)
  }
}
```

Now we that have the codec object, we can add it to the implementation:

```javascript
const { CompressionTypes, CompressionCodecs } = require('kafkajs')
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec
```

The new codec can now be used with the `send` method, example:

```javascript
async () => {
  await producer.send({
    topic: 'topic-name',
    compression: CompressionTypes.Snappy,
    messages: [
      { key: 'key1', value: 'hello world' },
      { key: 'key2', value: 'hey hey!' }
    ],
  })
}
```

## <a name="consuming-messages"></a> Consuming messages from Kafka

Consumer groups allow a group of machines or processes to coordinate access to a list of topics, distributing the load among the consumers. When a consumer fails the load is automatically distributed to other members of the group. Consumer groups must have unique group ids within the cluster, from a kafka broker perspective.

Creating the consumer:

```javascript
const consumer = kafka.consumer({ groupId: 'my-group' })
```

Subscribing to some topics:

```javascript
async () => {
  await consumer.connect()

  // Subscribe can be called several times
  await consumer.subscribe({ topic: 'topic-A' })
  await consumer.subscribe({ topic: 'topic-B' })

  // It's possible to start from the beginning:
  // await consumer.subscribe({ topic: 'topic-C', fromBeginning: true })
}
```

KafkaJS offers you two ways to process your data: `eachMessage` and `eachBatch`

### <a name="consuming-messages-each-message"></a> eachMessage

The `eachMessage` handler provides a convenient and easy to use API, feeding your function one message at a time. It is implemented on top of `eachBatch`, and it will automatically commit your offsets and heartbeat at the configured interval for you. If you are just looking to get started with Kafka consumers this a good place to start.

```javascript
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

### <a name="consuming-messages-each-batch"></a> eachBatch

Some use cases require dealing with batches directly. This handler will feed your function batches and provide some utility functions to give your code more flexibility: `resolveOffset`, `heartbeat`, and `isRunning`. All resolved offsets will be automatically committed after the function is executed.

Be aware that using `eachBatch` directly is considered a more advanced use case as compared to using `eachMessage`, since you will have to understand how session timeouts and heartbeats are connected.

```javascript
// create consumer, connect and subscribe ...

await consumer.run({
  eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning }) => {
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
```

> `batch.highWatermark` is the last committed offset within the topic partition. It can be useful for calculating lag.

> `eachBatchAutoResolve` configures auto-resolve of batch processing. If set to true, KafkaJS will automatically commit the last offset of the batch if `eachBatch` doesn't throw an error. Default: true.

> `resolveOffset()` is used to mark a message in the batch as processed. In case of errors, the consumer will automatically commit the resolved offsets.

Example:

```javascript
consumer.run({
  eachBatchAutoResolve: false,
  eachBatch: ({ batch, resolveOffset, heartbeat, isRunning }) => {
    for (let message of batch.messages) {
      if (!isRunning()) break
      await processMessage(message)
      await resolveOffset(message.offset)
      await heartbeat()
    }
  }
})
```

In the example above, if the consumer is shutting down in the middle of the batch, the remaining messages won't be resolved and therefore not committed. This way, you can quickly shut down the consumer without losing/skipping any messages.

### <a name="consuming-messages-options"></a> Options

```javascript
kafka.consumer({
  groupId: <String>,
  partitionAssigners: <Array>,
  sessionTimeout: <Number>,
  heartbeatInterval: <Number>,
  maxBytesPerPartition: <Number>,
  minBytes: <Number>,
  maxBytes: <Number>,
  maxWaitTimeInMs: <Number>,
  retry: <Object>,
})
```

| option               | description                                                                                                                                                                                                                                                                                                                                        | default                           |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| partitionAssigners   | List of partition assigners                                                                                                                                                                                                                                                                                                                        | `[PartitionAssigners.roundRobin]` |
| sessionTimeout       | Timeout in milliseconds used to detect failures. The consumer sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, then the broker will remove this consumer from the group and initiate a rebalance                                       | `30000`                           |
| heartbeatInterval    | The expected time in milliseconds between heartbeats to the consumer coordinator. Heartbeats are used to ensure that the consumer's session stays active. The value must be set lower than session timeout                                                                                                                                         | `3000`                            |
| maxBytesPerPartition | The maximum amount of data per-partition the server will return. This size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large message on a certain partition | `1048576` (1MB)                   |
| minBytes             | Minimum amount of data the server should return for a fetch request, otherwise wait up to `maxWaitTimeInMs` for more data to accumulate. default: `1`                                                                                                                                                                                              |
| maxBytes             | Maximum amount of bytes to accumulate in the response. Supported by Kafka >= `0.10.1.0`                                                                                                                                                                                                                                                            | `10485760` (10MB)                 |
| maxWaitTimeInMs      | The maximum amount of time in milliseconds the server will block before answering the fetch request if there isn’t sufficient data to immediately satisfy the requirement given by `minBytes`                                                                                                                                                     | `5000`                            |
| retry                | See [retry](#configuration-default-retry) for more information                                                                                                                                                                                                                                                                                     | `{ retries: 10 }`                 |

### <a name="consuming-messages-pause-resume"></a> Pause & Resume

In order to pause and resume consuming from one or more topics, the `Consumer` provides the methods `pause` and `resume`. Note that pausing a topic means that it won't be fetched in the next cycle. You may still receive messages for the topic within the current batch.

Calling `pause` with a topic that the consumer is not subscribed to is a no-op, calling `resume` with a topic that is not paused is also a no-op.

Example: A situation where this could be useful is when an external dependency used by the consumer is under too much load. Here we want to `pause` consumption from a topic when this happens, and after a predefined interval we `resume` again:

```javascript
await consumer.connect()
await consumer.subscribe({ topic: 'jobs' })

await consumer.run({ eachMessage: async ({ topic, message }) => {
  try {
    await sendToDependency(message)
  } catch (e) {
    if (e instanceof TooManyRequestsError) {
      consumer.pause([{ topic }])
      setTimeout(() => consumer.resume([{ topic }]), e.retryAfter * 1000)
    }

    throw e
  }
}})
```

### <a name="consuming-messages-seek"></a> Seek

To move the offset position in a topic/partition the `Consumer` provides the method `seek`. This method has to be called after the consumer is initialized and is running (after consumer#run).

```javascript
await consumer.connect()
await consumer.subscribe({ topic: 'example' })

// you don't need to await consumer#run
consumer.run({ eachMessage: async ({ topic, message }) => true })
consumer.seek({ topic: 'example', partition: 0, offset: 12384 })
```

### <a name="consuming-messages-custom-partition-assigner"></a> Custom partition assigner

It's possible to configure the strategy the consumer will use to distribute partitions amongst the consumer group. KafkaJS has a round robin assigner configured by default.

A partition assigner is a function which returns an object with the following interface:

```javascript
const MyPartitionAssigner = ({ cluster }) => ({
  name: 'MyPartitionAssigner',
  version: 1,
  async assign({ members, topics }) {},
  protocol({ topics }) {}
})
```

The method `assign` has to return an assignment plan with partitions per topic. A partition plan consists of a list of `memberId` and `memberAssignment`. The member assignment has to be encoded, use the `MemberAssignment` utility for that. Example:

```javascript
const { AssignerProtocol: { MemberAssignment } } = require('kafkajs')

const MyPartitionAssigner = ({ cluster }) => ({
  // ...
  version: 1,
  async assign({ members, topics }) {
    // perform assignment
    return myCustomAssignmentArray.map(memberId => ({
      memberId,
      memberAssignment: MemberAssignment.encode({
        version: this.version,
        assignment: assignment[memberId],
      })
    }))
  }
  // ...
})
```

The method `protocol` has to return `name` and `metadata`. Metadata has to be encoded, use the `MemberMetadata` utility for that. Example:

```javascript
const { AssignerProtocol: { MemberMetadata } } = require('kafkajs')

const MyPartitionAssigner = ({ cluster }) => ({
  name: 'MyPartitionAssigner',
  version: 1,
  protocol({ topics }) {
    return {
      name: this.name,
      metadata: MemberMetadata.encode({
        version: this.version,
        topics,
      }),
    }
  }
  // ...
})
```

Your `protocol` method will probably look like the example, but it's not implemented by default because extra data can be included as `userData`. Take a look at the `MemberMetadata#encode` for more information.

Once your assigner is done, add it to the list of assigners. It's important to keep the default assigner there to allow the old consumers to have a common ground with the new consumers when deploying.

```javascript
const { PartitionAssigners: { roundRobin } } = require('kafkajs')

kafka.consumer({
  groupId: 'my-group',
  partitionAssigners: [
    MyPartitionAssigner,
    roundRobin
  ]
})
```

### <a name="consuming-messages-describe-group"></a> Describe group

> Experimental - This feature may be removed or changed in new versions of KafkaJS

Returns metadata for the configured consumer group, example:

```javascript
const data = await consumer.describeGroup()
// {
//  errorCode: 0,
//  groupId: 'consumer-group-id-f104efb0e1044702e5f6',
//  members: [
//    {
//      clientHost: '/172.19.0.1',
//      clientId: 'test-3e93246fe1f4efa7380a',
//      memberAssignment: Buffer,
//      memberId: 'test-3e93246fe1f4efa7380a-ff87d06d-5c87-49b8-a1f1-c4f8e3ffe7eb',
//      memberMetadata: Buffer,
//    },
//  ],
//  protocol: 'RoundRobinAssigner',
//  protocolType: 'consumer',
//  state: 'Stable',
// },
```

### <a name="consuming-messages-compression"></a> Compression

KafkaJS only support GZIP natively, but [other codecs can be supported](#producing-messages-compression-other).

## <a name="admin"></a> Admin

The admin client will host all the cluster operations, such as: `createTopics`, `createPartitions`, etc. Currently, only `createTopics` is available.

```javascript
const kafka = new Kafka(...)
const admin = kafka.admin() // kafka.admin({ retry: { retries: 2 } })

// remember to connect/disconnect the client
await admin.connect()
await admin.disconnect()
```

The option `retry` can be used to customize the configuration for the admin.

Take a look at [Retry](#configuration-default-retry) for more information.

### <a name="admin-create-topics"></a> Create topics

`createTopics` will resolve to `true` if the topic was created successfully or `false` if it already exists. The method will throw exceptions in case of errors.

```javascript
await admin.createTopics({
  validateOnly: <boolean>,
  waitForLeaders: <boolean>
  timeout: <Number>,
  topics: <Topic[]>,
})
```

`Topic` structure:

```javascript
{
  topic: <String>,
  numPartitions: <Number>,     // default: 1
  replicationFactor: <Number>, // default: 1
  replicaAssignment: <Array>,  // Example: [{ partition: 0, replicas: [0,1,2] }] - default: []
  configEntries: <Array>       // Example: [{ name: 'cleanup.policy', value: 'compact' }] - default: []
}
```

| property       | description                                                                                           | default |
| -------------- | ----------------------------------------------------------------------------------------------------- | ------- |
| topics         | Topic definition                                                                                      |         |
| validateOnly   | If this is `true`, the request will be validated, but the topic won't be created.                     | false   |
| timeout        | The time in ms to wait for a topic to be completely created on the controller node                    | 5000    |
| waitForLeaders | If this is `true` it will wait until metadata for the new topics doesn't throw `LEADER_NOT_AVAILABLE` | true    |

## <a name="instrumentation"></a> Instrumentation

> Experimental - This feature may be removed or changed in new versions of KafkaJS

Some operations are instrumented using the `EventEmitter`. To receive the events use the method `consumer#on`, example:

```javascript
const { HEARTBEAT } = consumer.events
const removeListener = consumer.on(HEARTBEAT, e => console.log(`heartbeat at ${e.timestamp}`))
// removeListener()
```

The listeners are always async, even when using regular functions. The consumer will never block when executing your listeners. Errors in the listeners won't affect the consumer.

List of available events:

* consumer.events.HEARTBEAT
* consumer.events.COMMIT_OFFSETS

Instrumentation Event:

```javascript
{
  id: <Number>,
  type: <String>,
  timestamp: <Number>,
  payload: <Object>
}
```

## <a name="custom-logging"></a> Custom logging

The logger is customized using log creators. A log creator is a function which receives a log level and returns a log function. The log function receives namespace, level, label, and log.

- `namespace` identifies the component which is performing the log, for example, connection or consumer.
- `level` is the log level of the log entry.
- `label` is a text representation of the log level, example: 'INFO'.
- `log` is an object with the following keys: `timestamp`, `logger`, `message`, and the extra keys given by the user. (`logger.info('test', { extra_data: true })`)

```javascript
{
  level: 4,
  label: 'INFO', // NOTHING, ERROR, WARN, INFO, or DEBUG
  timestamp: '2017-12-29T13:39:54.575Z',
  logger: 'kafkajs',
  message: 'Started',
  // ... any other extra key provided to the log function
}
```

The general structure looks like this:

```javascript
const MyLogCreator = logLevel => ({ namespace, level, label, log }) => {
  // Example:
  // const { timestamp, logger, message, ...others } = log
  // console.log(`${label} [${namespace}] ${message} ${JSON.stringify(others)}`)
}
```

Example using [Winston](https://github.com/winstonjs/winston):

```javascript
const { logLevel } = require('kafkajs')
const winston = require('winston')
const toWinstonLogLevel = level => switch(level) {
  case logLevel.ERROR:
  case logLevel.NOTHING:
    return 'error'
  case logLevel.WARN:
    return 'warn'
  case logLevel.INFO:
    return 'info'
  case logLevel.DEBUG:
    return 'debug'
}

const WinstonLogCreator = logLevel => {
  const logger = winston.createLogger({
    level: toWinstonLogLevel(logLevel),
    transports: [
      new winston.transports.Console(),
      new winston.transports.File({ filename: 'myapp.log' })
    ]
  })

  return ({ namespace, level, { message, ...extra } }) => {
    logger.log({
      level: toWinstonLogLevel(level),
      message,
      extra,
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
  logCreator: WinstonLogCreator
})
```

## <a name="configuration-default-retry-detailed"></a> Retry (detailed)

The retry mechanism uses a randomization function that grows exponentially. This formula and how the default values affect it is best described by the example below:

- 1st retry:
  - Always a flat `initialRetryTime` ms
  - Default: `300ms`
- Nth retry:
  - Formula: `Random(previousRetryTime * (1 - factor), previousRetryTime * (1 + factor)) * multiplier`
  - N = 1:
    - Since `previousRetryTime == initialRetryTime` just plug the values in the formula:
    - Random(300 * (1 - 0.2), 300 * (1 + 0.2)) * 2 => Random(240, 360) * 2 => (480, 720) ms
    - Hence, somewhere between `480ms` to `720ms`
  - N = 2:
    - Since `previousRetryTime` from N = 1 was in a range between 480ms and 720ms, the retry for this step will be in the range of:
    - `previousRetryTime = 480ms` => Random(480 * (1 - 0.2), 480 * (1 + 0.2)) * 2 => Random(384, 576) * 2 => (768, 1152) ms
    - `previousRetryTime = 720ms` => Random(720 * (1 - 0.2), 720 * (1 + 0.2)) * 2 => Random(576, 864) * 2 => (1152, 1728) ms
    - Hence, somewhere between `768ms` to `1728ms`
  - And so on...

Table of retry times for default values:

| Retry # | min (ms) | max (ms) |
| ------- | -------- | -------- |
| 1       | 300      | 300      |
| 2       | 480      | 720      |
| 3       | 768      | 1728     |
| 4       | 1229     | 4147     |
| 5       | 1966     | 9953     |

## <a name="development"></a> Development

https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

https://kafka.apache.org/protocol.html

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

## Acknowledgements

Thanks to [Sebastian Norde](https://github.com/sebastiannorde) for the logo ❤️

## License

See [LICENSE](https://github.com/tulios/kafkajs/blob/master/LICENSE) for more details.
