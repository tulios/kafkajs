---
id: consuming
title: Consuming Messages
---

Consumer groups allow a group of machines or processes to coordinate access to a list of topics, distributing the load among the consumers. When a consumer fails the load is automatically distributed to other members of the group. Consumer groups __must have__ unique group ids within the cluster, from a kafka broker perspective.

Creating the consumer:

```javascript
const consumer = kafka.consumer({ groupId: 'my-group' })
```

Subscribing to some topics:

```javascript
await consumer.connect()

// Subscribe can be called several times
await consumer.subscribe({ topic: 'topic-A' })
await consumer.subscribe({ topic: 'topic-B' })

// It's possible to start from the beginning:
// await consumer.subscribe({ topic: 'topic-C', fromBeginning: true })
```

KafkaJS offers you two ways to process your data: `eachMessage` and `eachBatch`

## <a name="each-message"></a> eachMessage

The `eachMessage` handler provides a convenient and easy to use API, feeding your function one message at a time. It is implemented on top of `eachBatch`, and it will automatically commit your offsets and heartbeat at the configured interval for you. If you are just looking to get started with Kafka consumers this a good place to start.

```javascript
await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log({
            key: message.key.toString(),
            value: message.value.toString(),
            headers: message.headers,
        })
    },
})
```

## <a name="each-batch"></a> eachBatch

Some use cases require dealing with batches directly. This handler will feed your function batches and provide some utility functions to give your code more flexibility: `resolveOffset`, `heartbeat`, `isRunning`, and `commitOffsetsIfNecessary`. All resolved offsets will be automatically committed after the function is executed.

> Note: Be aware that using `eachBatch` directly is considered a more advanced use case as compared to using `eachMessage`, since you will have to understand how session timeouts and heartbeats are connected.

```javascript
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
                    value: message.value.toString(),
                    headers: message.headers,
                }
            })

            await resolveOffset(message.offset)
            await heartbeat()
        }
    },
})
```

* `batch.highWatermark` is the last committed offset within the topic partition. It can be useful for calculating lag.
* `eachBatchAutoResolve` configures auto-resolve of batch processing. If set to true, KafkaJS will automatically commit the last offset of the batch if `eachBatch` doesn't throw an error. Default: true.
* `resolveOffset()` is used to mark a message in the batch as processed. In case of errors, the consumer will automatically commit the resolved offsets.
* `commitOffsetsIfNecessary(offsets?)` is used to commit offsets based on the autoCommit configurations (`autoCommitInterval` and `autoCommitThreshold`). Note that auto commit won't happen in `eachBatch` if `commitOffsetsIfNecessary` is not invoked. Take a look at [autoCommit](#auto-commit) for more information.
* `uncommittedOffsets()` returns all offsets by topic-partition which have not yet been committed.

### Example

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

## <a name="auto-commit"></a> autoCommit

The messages are always fetched in batches from Kafka, even when using the `eachMessage` handler. All resolved offsets will be committed to Kafka after processing the whole batch.

Committing offsets periodically during a batch allows the consumer to recover from group rebalances, stale metadata and other issues before it has completed the entire batch. However, committing more often increases network traffic and slows down processing. Auto-commit offers more flexibility when committing offsets; there are two flavors available:

`autoCommitInterval`: The consumer will commit offsets after a given period, for example, five seconds. Value in milliseconds. Default: `null`

```javascript
consumer.run({
  autoCommitInterval: 5000,
  // ...
})
```

`autoCommitThreshold`: The consumer will commit offsets after resolving a given number of messages, for example, a hundred messages. Default: `null`

```javascript
consumer.run({
  autoCommitThreshold: 100,
  // ...
})
```

Having both flavors at the same time is also possible, the consumer will commit the offsets if any of the use cases (interval or number of messages) happens.

`autoCommit`: Advanced option to disable auto committing altogether. If auto committing is disabled you must manually commit message offsets, either by using the `commitOffsetsIfNecessary` method available in the `eachBatch` callback, or by [sending message offsets in a transaction](Transactions.md#offsets). The `commitOffsetsIfNecessary` method will still respect the other autoCommit options if set. Default: `true`

## <a name="from-beginning"></a> fromBeginning

The consumer group will use the latest committed offset when fetching messages. If the offset is invalid or not defined, `fromBeginning` defines the behavior of the consumer group. This can be configured when subscribing to a topic:

```javascript
await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })
await consumer.subscribe({ topic: 'other-topic', fromBeginning: false })
```

When `fromBeginning` is `true`, the group will use the earliest offset. If set to `false`, it will use the latest offset. The default is `false`.

## <a name="options"></a> Options

```javascript
kafka.consumer({
  groupId: <String>,
  partitionAssigners: <Array>,
  sessionTimeout: <Number>,
  heartbeatInterval: <Number>,
  metadataMaxAge: <Number>,
  allowAutoTopicCreation: <Boolean>,
  maxBytesPerPartition: <Number>,
  minBytes: <Number>,
  maxBytes: <Number>,
  maxWaitTimeInMs: <Number>,
  retry: <Object>,
})
```

| option                 | description                                                                                                                                                                                                                                                                                                                                        | default                           |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| partitionAssigners     | List of partition assigners                                                                                                                                                                                                                                                                                                                        | `[PartitionAssigners.roundRobin]` |
| sessionTimeout         | Timeout in milliseconds used to detect failures. The consumer sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, then the broker will remove this consumer from the group and initiate a rebalance                                       | `30000`                           |
| heartbeatInterval      | The expected time in milliseconds between heartbeats to the consumer coordinator. Heartbeats are used to ensure that the consumer's session stays active. The value must be set lower than session timeout                                                                                                                                         | `3000`                            |
| metadataMaxAge         | The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions                                                                                                                                                       | `300000` (5 minutes)              |
| allowAutoTopicCreation | Allow topic creation when querying metadata for non-existent topics                                                                                                                                                                                                                                                                                | `true`                            |
| maxBytesPerPartition   | The maximum amount of data per-partition the server will return. This size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large message on a certain partition | `1048576` (1MB)                   |
| minBytes               | Minimum amount of data the server should return for a fetch request, otherwise wait up to `maxWaitTimeInMs` for more data to accumulate. default: `1`                                                                                                                                                                                              |
| maxBytes               | Maximum amount of bytes to accumulate in the response. Supported by Kafka >= `0.10.1.0`                                                                                                                                                                                                                                                            | `10485760` (10MB)                 |
| maxWaitTimeInMs        | The maximum amount of time in milliseconds the server will block before answering the fetch request if there isn’t sufficient data to immediately satisfy the requirement given by `minBytes`                                                                                                                                                     | `5000`                            |
| retry                  | See [retry](Configuration.md#retry) for more information                                                                                                                                                                                                                                                                                           | `{ retries: 10 }`                 |
| readUncommitted        | Configures the consumer isolation level. If `false` (default), the consumer will not return any transactional messages which were not committed.                                                                                                                                                                                                   | `false`                           |

## <a name="pause-resume"></a> Pause & Resume

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

## <a name="seek"></a> Seek

To move the offset position in a topic/partition the `Consumer` provides the method `seek`. This method has to be called after the consumer is initialized and is running (after consumer#run).

```javascript
await consumer.connect()
await consumer.subscribe({ topic: 'example' })

// you don't need to await consumer#run
consumer.run({ eachMessage: async ({ topic, message }) => true })
consumer.seek({ topic: 'example', partition: 0, offset: 12384 })
```

## <a name="custom-partition-assigner"></a> Custom partition assigner

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

## <a name="describe-group"></a> Describe group

> **Experimental** - This feature may be removed or changed in new versions of KafkaJS

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

## <a name="compression"></a> Compression

KafkaJS only support GZIP natively, but [other codecs can be supported](Producing.md#compression-other).