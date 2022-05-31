---
id: version-1.11.0-producing
title: Producing Messages
original_id: producing
---

To publish messages to Kafka you have to create a producer. Simply call the `producer` function of the client to create it:

```javascript
const producer = kafka.producer()
```

## Options

| option                 | description                                                                                                                                                                                  | default              |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------- |
| createPartitioner      | Take a look at [Custom Partitioner](#custom-partitioner) for more information                                                                                                                | `null`               |
| retry                  | Take a look at [Producer Retry](#retry) for more information                                                                                                                                 | `null`               |
| metadataMaxAge         | The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions | `300000` - 5 minutes |
| allowAutoTopicCreation | Allow topic creation when querying metadata for non-existent topics                                                                                                                          | `true`               |
| transactionTimeout | The maximum amount of time in ms that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction. If this value is larger than the `transaction.max.timeout.ms` setting in the __broker__, the request will fail with a `InvalidTransactionTimeout` error | `60000`                            |
| idempotent         | _Experimental._ If enabled producer will ensure each message is written exactly once. Acks _must_ be set to -1 ("all"). Retries will default to MAX_SAFE_INTEGER.                                                                                                                                                                          | `false`                            |

The method `send` is used to publish messages to the Kafka cluster.

```javascript
const producer = kafka.producer()

await producer.connect()
await producer.send({
    topic: 'topic-name',
    messages: [
        { key: 'key1', value: 'hello world' },
        { key: 'key2', value: 'hey hey!' }
    ],
})
```

Example with a defined partition:

```javascript
const producer = kafka.producer()

await producer.connect()
await producer.send({
    topic: 'topic-name',
    messages: [
        { key: 'key1', value: 'hello world', partition: 0 },
        { key: 'key2', value: 'hey hey!', partition: 1 }
    ],
})
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

| property           | description                                                                                                                                                                                                                                                                                                                                | default                            |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------- |
| topic              | topic name                                                                                                                                                                                                                                                                                                                                 | `null`                             |
| messages           | An array of objects with "key" (optional), "value" (required), "partition" (optional), "timestamp" (optional), example: <br> `[{ key: 'my-key', value: 'my-value'}]`                                                                                                                                                                                                                                          | `null`                             |
| acks               | Control the number of required acks. <br> __-1__ = all replicas must acknowledge _(default)_ <br> __0__ = no acknowledgments <br> __1__ = only waits for the leader to acknowledge                                                                                                                                                         | `-1` all replicas must acknowledge |
| timeout            | The time to await a response in ms                                                                                                                                                                                                                                                                                                         | `30000`                            |
| compression        | Compression codec                                                                                                                                                                                                                                                                                                                          | `CompressionTypes.None`            |

By default, the producer is configured to distribute the messages with the following logic:

- If a partition is specified in the message, use it
- If no partition is specified but a key is present choose a partition based on a hash (murmur2) of the key
- If no partition or key is present choose a partition in a round-robin fashion

## Message Headers

Kafka v0.11 introduces record headers, which allows your messages to carry extra metadata. To send headers with your message, include the key `headers` with the values. Example:

```javascript
await producer.send({
    topic: 'topic-name',
    messages: [{
        key: 'key1',
        value: 'hello world',
        headers: {
            'correlation-id': '2bfb68bb-893a-423b-a7fa-7b568cad5b67',
            'system-id': 'my-system'
        }
    }]
})
```

## Producing to multiple topics

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
  },
  {
    topic: 'topic-c',
    messages: [
      {
        key: 'key',
        value: 'hello topic-c',
        headers: {
          'correlation-id': '2bfb68bb-893a-423b-a7fa-7b568cad5b67',
        },
      }
    ],
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

## <a name="custom-partitioner"></a> Custom partitioner

It's possible to assign a custom partitioner to the producer. A partitioner is a function which returns another function responsible for the partition selection, something like this:

```javascript
const MyPartitioner = () => {
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

To use your custom partitioner, use the option `createPartitioner` when creating the producer.

```javascript
kafka.producer({ createPartitioner: MyPartitioner })
```

### Default Partitioners

KafkaJS ships with 2 partitioners: `DefaultPartitioner` and `JavaCompatiblePartitioner`.

The `JavaCompatiblePartitioner` should be compatible with the default partitioner that ships with the Java Kafka client. This can be important to meet the [co-partitioning requirement](https://docs.confluent.io/current/ksql/docs/developer-guide/partition-data.html#co-partitioning-requirements) when joining multiple topics.

Use the `JavaCompatiblePartitioner` by importing it and providing it to the Producer constructor:

```javascript
const { Partitioners } = require('kafkajs')
kafka.producer({ createPartitioner: Partitioners.JavaCompatiblePartitioner })
```

## <a name="retry"></a> Retry

The option `retry` can be used to customize the configuration for the producer.

Take a look at [Retry](Configuration.md#retry) for more information.

## <a name="compression"></a> Compression

Since KafkaJS aims to have as small footprint and as few dependencies as possible, only the GZIP codec is part of the core functionality. Providing plugins supporting other codecs might be considered in the future.

### <a name="compression-gzip"></a> GZIP

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

### <a name="compression-snappy"></a> Snappy

Snappy support is provided by the package `kafkajs-snappy`

```sh
npm install kafkajs-snappy
# yarn add kafkajs-snappy
```

```javascript
const {  CompressionTypes, CompressionCodecs } = require('kafkajs')
const SnappyCodec = require('kafkajs-snappy')

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec
```

Take a look at the official [readme](https://github.com/tulios/kafkajs-snappy) for more information

### <a name="compression-lz4"></a> LZ4

LZ4 support is provided by the package `kafkajs-lz4`

```sh
npm install kafkajs-lz4
# yarn add kafkajs-lz4
```

```javascript
const { CompressionTypes, CompressionCodecs } = require('kafkajs')
const LZ4 = require('kafkajs-lz4')

CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec
```

The package also accepts options to granularly control LZ4 compression & decompression. Take a look at the official [readme](https://github.com/indix/kafkajs-lz4) for more information.

### <a name="compression-other"></a> Other

Any other codec than GZIP can be easily implemented using existing libraries.

A codec is an object with two `async` functions: `compress` and `decompress`. Import the libraries and define the codec object:

```javascript
const MyCustomSnappyCodec = {
    async compress(encoder) {
        return someCompressFunction(encoder.buffer)
    },

    async decompress(buffer) {
        return someDecompressFunction(buffer)
    }
}
```

Now that we have the codec object, we can add it to the implementation:

```javascript
const { CompressionTypes, CompressionCodecs } = require('kafkajs')
CompressionCodecs[CompressionTypes.Snappy] = MyCustomSnappyCodec
```

The new codec can now be used with the `send` method, example:

```javascript
await producer.send({
    topic: 'topic-name',
    compression: CompressionTypes.Snappy,
    messages: [
        { key: 'key1', value: 'hello world' },
        { key: 'key2', value: 'hey hey!' }
    ],
})
```
