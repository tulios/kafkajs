---
id: version-3.0.0-consuming
title: Consuming Messages
original_id: consuming
---

Consumer groups allow a group of machines or processes to coordinate access to a list of topics, distributing the load among the consumers. When a consumer fails the load is automatically distributed to other members of the group. Consumer groups __must have__ unique group ids within the cluster, from a kafka broker perspective.

Creating the consumer:

```javascript
const consumer = kafka.consumer({ groupId: 'my-group' })
```

## <a name="options"></a> Options

```javascript
kafka.consumer({
  groupId: <String>,
  groupInstanceId: <String>,
  partitionAssigners: <Array>,
  sessionTimeout: <Number>,
  rebalanceTimeout: <Number>,
  heartbeatInterval: <Number>,
  metadataMaxAge: <Number>,
  allowAutoTopicCreation: <Boolean>,
  maxBytesPerPartition: <Number>,
  minBytes: <Number>,
  maxBytes: <Number>,
  maxWaitTimeInMs: <Number>,
  retry: <Object>,
  readUncommitted: <Boolean>,
  maxInFlightRequests: <Number>,
  rackId: <String>
})
```

| option                 | description                                                                                                                                                                                                                                                                                                                                        | default                           | type       | required |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- | ---------- | -------- |
| groupId                | Consumer group ID. Must be unique within the cluster                                                                                                                                                                                                                                                                                               |                                   | `String`   | **Yes** |
| groupInstanceId        | Static group instance ID for static membership (KIP-345). See [Static Membership](#static-membership)                                                                                                                                                                                                                                             | `undefined`                       | `String`   | No |
| partitionAssigners     | List of partition assigners                                                                                                                                                                                                                                                                                                                        | `[PartitionAssigners.roundRobin]` | `Array`    | No |
| sessionTimeout         | Timeout in milliseconds used to detect failures. The consumer sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, then the broker will remove this consumer from the group and initiate a rebalance                                       | `30000`                           | `Number`   | No |
| rebalanceTimeout       | The maximum time that the coordinator will wait for each member to rejoin when rebalancing the group                                                                                                                                                                                                                                               | `60000`                           | `Number`   | No |
| heartbeatInterval      | The expected time in milliseconds between heartbeats to the consumer coordinator. Heartbeats are used to ensure that the consumer's session stays active. The value must be set lower than session timeout                                                                                                                                         | `3000`                            | `Number`   | No |
| metadataMaxAge         | The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions                                                                                                                                                       | `300000` (5 min)                  | `Number`   | No |
| allowAutoTopicCreation | Allow topic creation when querying metadata for non-existent topics                                                                                                                                                                                                                                                                                | `true`                            | `Boolean`  | No |
| maxBytesPerPartition   | The maximum amount of data per-partition the server will return. This size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large message on a certain partition | `1048576` (1MB)                   | `Number`   | No |
| minBytes               | Minimum amount of data the server should return for a fetch request, otherwise wait up to `maxWaitTimeInMs` for more data to accumulate                                                                                                                                                                                                           | `1`                               | `Number`   | No |
| maxBytes               | Maximum amount of bytes to accumulate in the response. Supported by Kafka >= `0.10.1.0`                                                                                                                                                                                                                                                            | `10485760` (10MB)                 | `Number`   | No |
| maxWaitTimeInMs        | The maximum amount of time in milliseconds the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement given by `minBytes`                                                                                                                                                      | `5000`                            | `Number`   | No |
| retry                  | Retry configuration. Also supports consumer-specific `restartOnFailure`. See [retry](Configuration.md#default-retry)                                                                                                                                                                                                                              | `{ retries: 5 }`                 | `Object`   | No |
| readUncommitted        | Configures the consumer isolation level. If `false` (default), the consumer will not return any transactional messages which were not committed                                                                                                                                                                                                   | `false`                           | `Boolean`  | No |
| maxInFlightRequests    | Max number of requests that may be in progress at any time. If falsey then no limit                                                                                                                                                                                                                                                                | `null` _(no limit)_              | `Number`   | No |
| rackId                 | Configure the "rack" in which the consumer resides to enable [follower fetching](#follower-fetching)                                                                                                                                                                                                                                               | `null` _(fetch from leader)_     | `String`   | No |

### Static Membership

Static membership (KIP-345) allows consumers to maintain their group assignment across restarts by providing a `groupInstanceId`. This reduces unnecessary rebalances when consumers restart:

```javascript
const consumer = kafka.consumer({
  groupId: 'my-group',
  groupInstanceId: 'instance-1',  // Unique per consumer instance
})
```

When a consumer with a `groupInstanceId` disconnects, the broker waits for `session.timeout.ms` before triggering a rebalance. If the consumer reconnects with the same `groupInstanceId` within that window, it resumes its previous assignment without rebalancing.

| option          | description                                                                 | type     | required |
|-----------------|-----------------------------------------------------------------------------|----------|----------|
| groupInstanceId | Unique identifier for static group membership. Must be unique per instance  | `String` | No |

## Subscribing to topics

```javascript
await consumer.connect()

await consumer.subscribe({ topics: ['topic-A'] })

// You can subscribe to multiple topics at once
await consumer.subscribe({ topics: ['topic-B', 'topic-C'] })

// It's possible to start from the beginning of the topic
await consumer.subscribe({ topics: ['topic-D'], fromBeginning: true })
```

Alternatively, you can subscribe to any topic that matches a regular expression:

```javascript
await consumer.connect()
await consumer.subscribe({ topics: [/topic-(eu|us)-.*/i] })
```

When suppling a regular expression, the consumer will not match topics created after the subscription. If your broker has `topic-A` and `topic-B`, you subscribe to `/topic-.*/`, then `topic-C` is created, your consumer would not be automatically subscribed to `topic-C`.

| property      | description                                                                                     | default | type       | required |
|---------------|-------------------------------------------------------------------------------------------------|---------|------------|----------|
| topics        | Array of topic names or RegExp patterns to subscribe to                                          |         | `(String \| RegExp)[]` | **Yes** |
| fromBeginning | When `true`, start from earliest offset. When `false`, start from latest                        | `false` | `Boolean`  | No |

> **Deprecated:** `consumer.subscribe({ topic: 'topic-A' })` (single `topic` string) is deprecated. Use `topics` array instead: `consumer.subscribe({ topics: ['topic-A'] })`.

KafkaJS offers you two ways to process your data: `eachMessage` and `eachBatch`

## <a name="each-message"></a> eachMessage

The `eachMessage` handler provides a convenient and easy to use API, feeding your function one message at a time. It is implemented on top of `eachBatch`, and it will automatically commit your offsets and heartbeat at the configured interval for you. If you are just looking to get started with Kafka consumers this a good place to start.

```javascript
await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
        console.log({
            key: message.key.toString(),
            value: message.value.toString(),
            headers: message.headers,
        })
    },
})
```

### `eachMessage` payload

| property  | description                                                                         | type       |
|-----------|-------------------------------------------------------------------------------------|------------|
| topic     | The topic name                                                                       | `String`   |
| partition | The partition number                                                                 | `Number`   |
| message   | The Kafka message. See [KafkaMessage](#kafka-message-type) below                     | `KafkaMessage` |
| heartbeat | Async function to send heartbeat to broker                                           | `() => Promise<void>` |
| pause     | Convenience function to pause the current topic-partition. Returns a resume function | `() => () => void` |

Be aware that the `eachMessage` handler should not block for longer than the configured [session timeout](#options) or else the consumer will be removed from the group. If your workload involves very slow processing times for individual messages then you should either increase the session timeout or make periodic use of the `heartbeat` function exposed in the handler payload.
The `pause` function is a convenience for `consumer.pause({ topic, partitions: [partition] })`. It will pause the current topic-partition and returns a function that allows you to resume consuming later.

### <a name="kafka-message-type"></a> KafkaMessage type

Messages received by the consumer have the following properties:

| property   | description                                                | type |
|------------|------------------------------------------------------------|------|
| key        | Message key (may be `null`)                                | `Buffer \| null` |
| value      | Message value (may be `null` for tombstones)               | `Buffer \| null` |
| timestamp  | Message timestamp as string                                | `String` |
| offset     | Message offset as string                                   | `String` |
| headers    | Message headers (only present in RecordBatch format)       | `Object` |
| attributes | Message attributes bitmask                                 | `Number` |

## <a name="each-batch"></a> eachBatch

Some use cases require dealing with batches directly. This handler will feed your function batches and provide some utility functions to give your code more flexibility: `resolveOffset`, `heartbeat`, `commitOffsetsIfNecessary`, `uncommittedOffsets`, `isRunning`, `isStale`, and `pause`. All resolved offsets will be automatically committed after the function is executed.

> Note: Be aware that using `eachBatch` directly is considered a more advanced use case as compared to using `eachMessage`, since you will have to understand how session timeouts and heartbeats are connected.

```javascript
await consumer.run({
    eachBatchAutoResolve: true,
    eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
        uncommittedOffsets,
        isRunning,
        isStale,
        pause,
    }) => {
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

            resolveOffset(message.offset)
            await heartbeat()
        }
    },
})
```

### `eachBatch` payload

| property                    | description                                                                                                                     | type       |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------|------------|
| batch                       | The batch object containing `topic`, `partition`, `highWatermark`, and `messages`                                                | `Batch`    |
| resolveOffset(offset)       | Mark a message offset as processed. On errors, the consumer will commit resolved offsets                                         | `Function` |
| heartbeat()                 | Send heartbeat to broker. Respects `heartbeatInterval` — calls sooner than the interval are ignored                              | `() => Promise<void>` |
| commitOffsetsIfNecessary(offsets?) | Commit offsets based on `autoCommitInterval` and `autoCommitThreshold`. **Required** for auto-commit in `eachBatch`        | `(offsets?) => Promise<void>` |
| uncommittedOffsets()        | Returns all offsets by topic-partition which have not yet been committed                                                          | `() => OffsetsByTopicPartition` |
| isRunning()                 | Returns `true` if consumer is in running state                                                                                   | `() => Boolean` |
| isStale()                   | Returns whether messages have been rendered stale (e.g., after `consumer.seek`)                                                  | `() => Boolean` |
| pause()                     | Pause the current topic-partition. Returns a resume function                                                                      | `() => () => void` |

### Batch Object

| property      | description                                                    | type |
|---------------|----------------------------------------------------------------|------|
| topic         | Topic name                                                      | `String` |
| partition     | Partition number                                                | `Number` |
| highWatermark | The last committed offset within the topic partition (for lag calculation) | `String` |
| messages      | Array of `KafkaMessage`                                         | `KafkaMessage[]` |
| isEmpty()     | Returns whether the batch has no messages                       | `Boolean` |
| firstOffset() | Returns the first offset or `null`                              | `String \| null` |
| lastOffset()  | Returns the last offset                                         | `String` |
| offsetLag()   | Returns the offset lag (distance from high watermark)           | `String` |
| offsetLagLow()| Returns the low offset lag                                      | `String` |

### Example

```javascript
consumer.run({
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        for (let message of batch.messages) {
            if (!isRunning() || isStale()) break
            await processMessage(message)
            resolveOffset(message.offset)
            await heartbeat()
        }
    }
})
```

In the example above, if the consumer is shutting down in the middle of the batch, the remaining messages won't be resolved and therefore not committed. This way, you can quickly shut down the consumer without losing/skipping any messages. If the batch goes stale for some other reason (like calling `consumer.seek`) none of the remaining messages are processed either.

## <a name="run-config"></a> consumer.run() Configuration

| option                        | description                                                                                     | default | type       | required |
|-------------------------------|-------------------------------------------------------------------------------------------------|---------|------------|----------|
| autoCommit                    | Enable/disable automatic offset committing. See [autoCommit](#auto-commit)                      | `true`  | `Boolean`  | No |
| autoCommitInterval            | Commit offsets after this interval in ms. See [autoCommit](#auto-commit)                        | `null`  | `Number`   | No |
| autoCommitThreshold           | Commit offsets after resolving this many messages. See [autoCommit](#auto-commit)                | `null`  | `Number`   | No |
| eachBatchAutoResolve          | Auto-resolve batch offsets on success                                                            | `true`  | `Boolean`  | No |
| partitionsConsumedConcurrently| Number of partitions processed concurrently. See [Concurrency](#concurrent-processing)           | `1`     | `Number`   | No |
| eachBatch                     | Batch handler function                                                                           |         | `Function` | One of eachBatch/eachMessage |
| eachMessage                   | Message handler function                                                                         |         | `Function` | One of eachBatch/eachMessage |

## <a name="concurrent-processing"></a> Partition-aware concurrency

By default, [`eachMessage`](Consuming.md#each-message) is invoked sequentially for each message in each partition. In order to concurrently process several messages per once, you can increase the `partitionsConsumedConcurrently` option:

```javascript
consumer.run({
    partitionsConsumedConcurrently: 3, // Default: 1
    eachMessage: async ({ topic, partition, message }) => {
        // This will be called up to 3 times concurrently
    },
})
```

Messages in the same partition are still guaranteed to be processed in order, but messages from multiple partitions can be processed at the same time. If `eachMessage` consists of asynchronous work, such as network requests or other I/O, this can improve performance. If `eachMessage` is entirely synchronous, this will make no difference.

The same thing applies if you are using [`eachBatch`](Consuming.md#each-batch). Given `partitionsConsumedConcurrently > 1`, you will be able to process multiple batches concurrently.

A guideline for setting `partitionsConsumedConcurrently` would be that it should not be larger than the number of partitions consumed. Depending on whether or not your workload is CPU bound, it may also not benefit you to set it to a higher number than the number of logical CPU cores. A recommendation is to start with a low number and measure if increasing leads to higher throughput.

## <a name="auto-commit"></a> autoCommit

The messages are always fetched in batches from Kafka, even when using the `eachMessage` handler. All resolved offsets will be committed to Kafka after processing the whole batch.

Committing offsets periodically during a batch allows the consumer to recover from group rebalancing, stale metadata and other issues before it has completed the entire batch. However, committing more often increases network traffic and slows down processing. Auto-commit offers more flexibility when committing offsets; there are two flavors available:

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

`autoCommit`: Advanced option to disable auto committing altogether. Instead, you can [manually commit offsets](#manual-commits). Default: `true`

## <a name="manual-commits"></a> Manual committing

When disabling [`autoCommit`](#auto-commit) you can still manually commit message offsets, in a couple of different ways:

- By using the `commitOffsetsIfNecessary` method available in the `eachBatch` callback. The `commitOffsetsIfNecessary` method will still respect the other autoCommit options if set.
- By [sending message offsets in a transaction](Transactions.md#offsets).
- By using the `commitOffsets` method of the consumer (see below).

The `consumer.commitOffsets` is the lowest-level option and will ignore all other auto commit settings, but in doing so allows the committed offset to be set to any offset and committing various offsets at once. This can be useful, for example, for building a processing reset tool. It can only be called after `consumer.run`. Committing offsets does not change what message we'll consume next once we've started consuming, but instead is only used to determine **from which place to start**. To immediately change from what offset you're consuming messages, you'll want to [seek](#seek), instead.

```javascript
consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
        // Process the message somehow
    },
})

consumer.commitOffsets([
  { topic: 'topic-A', partition: 0, offset: '1' },
  { topic: 'topic-A', partition: 1, offset: '3' },
  { topic: 'topic-B', partition: 0, offset: '2' }
])
```

The `commitOffsets` method accepts an array of `TopicPartitionOffsetAndMetadata`:

| property  | description                          | type     | required |
|-----------|--------------------------------------|----------|----------|
| topic     | Topic name                           | `String` | **Yes** |
| partition | Partition number                     | `Number` | **Yes** |
| offset    | Offset to commit                     | `String` | **Yes** |
| metadata  | Optional metadata string              | `String \| null` | No |

Note that you don't *have* to store consumed offsets in Kafka, but instead store it in a storage mechanism of your own choosing. That's an especially useful approach when the results of consuming a message are written to a datastore that allows atomically writing the consumed offset with it, like for example a SQL database. When possible it can make the consumption fully atomic and give "exactly once" semantics that are stronger than the default "at-least once" semantics you get with Kafka's offset commit functionality.

The usual usage pattern for offsets stored outside of Kafka is as follows:

- Run the consumer with `autoCommit` disabled.
- Store a message's `offset + 1` in the store together with the results of processing. `1` is added to prevent that same message from being consumed again.
- Use the externally stored offset on restart to [seek](#seek) the consumer to it.

## <a name="from-beginning"></a> fromBeginning

The consumer group will use the latest committed offset when starting to fetch messages. If the offset is invalid or not defined, `fromBeginning` defines the behavior of the consumer group. This can be configured when subscribing to a topic:

```javascript
await consumer.subscribe({ topics: ['test-topic'], fromBeginning: true })
await consumer.subscribe({ topics: ['other-topic'], fromBeginning: false })
```

When `fromBeginning` is `true`, the group will use the earliest offset. If set to `false`, it will use the latest offset. The default is `false`.

## <a name="pause-resume"></a> Pause & Resume

In order to pause and resume consuming from one or more topics, the `Consumer` provides the methods `pause` and `resume`. It also provides the `paused` method to get the list of all paused topics. Note that pausing a topic means that it won't be fetched in the next cycle and subsequent messages within the current batch won't be passed to an `eachMessage` handler.

Calling `pause` with a topic that the consumer is not subscribed to is a no-op, calling `resume` with a topic that is not paused is also a no-op.

> Note: Calling `resume` or `pause` while the consumer is not running will throw an error.

Example: A situation where this could be useful is when an external dependency used by the consumer is under too much load. Here we want to `pause` consumption from a topic when this happens, and after a predefined interval we `resume` again:

```javascript
await consumer.connect()
await consumer.subscribe({ topics: ['jobs'] })

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

For finer-grained control, specific partitions of topics can also be paused, rather than the whole topic. The ability to pause and resume on a per-partition basis, means it can be used to isolate the consuming (and processing) of messages.

Example: in combination with [consuming messages per partition concurrently](#concurrent-processing), it can prevent having to stop processing all partitions because of a slow process in one of the other partitions.

```javascript
consumer.run({
    partitionsConsumedConcurrently: 3, // Default: 1
    eachMessage: async ({ topic, partition, message }) => {
      // This will be called up to 3 times concurrently
        try {
            await sendToDependency(message)
        } catch (e) {
            if (e instanceof TooManyRequestsError) {
                consumer.pause([{ topic, partitions: [partition] }])
                // Other partitions will keep fetching and processing, until if / when
                // they also get throttled
                setTimeout(() => {
                    consumer.resume([{ topic, partitions: [partition] }])
                    // Other partitions that are paused will continue to be paused
                }, e.retryAfter * 1000)
            }

            throw e
        }
    },
})
```

As a convenience, the `eachMessage` callback provides a `pause` function to pause the specific topic-partition of the message currently being processed.

```javascript
await consumer.connect()
await consumer.subscribe({ topics: ['jobs'] })

await consumer.run({ eachMessage: async ({ topic, message, pause }) => {
    try {
        await sendToDependency(message)
    } catch (e) {
        if (e instanceof TooManyRequestsError) {
            const resumeThisPartition = pause()
            // Other partitions that are paused will continue to be paused
            setTimeout(resumeThisPartition, e.retryAfter * 1000)
        }

        throw e
    }
}})
```

It's possible to access the list of paused topic partitions using the `paused` method.

```javascript
const pausedTopicPartitions = consumer.paused()

for (const topicPartitions of pausedTopicPartitions) {
  const { topic, partitions } = topicPartitions
  console.log({ topic, partitions })
}
```

### Pause/Resume API

| method   | description                                                          | signature |
|----------|----------------------------------------------------------------------|-----------|
| pause    | Pause consuming from topics/partitions                                | `pause(topics: Array<{ topic: string; partitions?: number[] }>): void` |
| resume   | Resume consuming from paused topics/partitions                        | `resume(topics: Array<{ topic: string; partitions?: number[] }>): void` |
| paused   | Get list of all paused topic partitions                               | `paused(): Array<{ topic: string; partitions: number[] }>` |

## <a name="seek"></a> Seek

To move the offset position in a topic/partition the `Consumer` provides the method `seek`. This method has to be called after the consumer is initialized and is running (after consumer#run).

```javascript
await consumer.connect()
await consumer.subscribe({ topics: ['example'] })

// you don't need to await consumer#run
consumer.run({ eachMessage: async ({ topic, message }) => true })
consumer.seek({ topic: 'example', partition: 0, offset: 12384 })
```

Upon seeking to an offset, any messages in active batches are marked as stale and discarded, making sure the next message read for the partition is from the offset sought to. Make sure to check `isStale()` before processing a message using [the `eachBatch` interface](#each-batch) of `consumer.run`.

By default, the consumer will commit the offset seeked. To disable this, set the [`autoCommit`](#auto-commit) option to `false` on the consumer.

```javascript
consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, message }) => true
})
// This will now only resolve the previous offset, not commit it
consumer.seek({ topic: 'example', partition: 0, offset: "12384" })
```

| property  | description                          | type     | required |
|-----------|--------------------------------------|----------|----------|
| topic     | Topic name                           | `String` | **Yes** |
| partition | Partition number                     | `Number` | **Yes** |
| offset    | Offset to seek to                    | `String \| Number` | **Yes** |

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

The assigner factory function receives:

| argument | description                 | type     |
|----------|-----------------------------|----------|
| cluster  | The cluster instance         | `Cluster` |
| groupId  | The consumer group ID        | `String` |
| logger   | The logger instance          | `Logger` |

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
//      groupInstanceId: 'instance-1', // present with static membership
//    },
//  ],
//  protocol: 'RoundRobinAssigner',
//  protocolType: 'consumer',
//  state: 'Stable',
// },
```

## <a name="compression"></a> Compression

KafkaJS only support GZIP natively, but [other codecs can be supported](Producing.md#compression-other).

## <a name="dead-letter-queue"></a> Dead Letter Queue

KafkaJS provides a `deadLetterQueue` utility that automatically retries failed messages and sends them to a dead letter topic after a configurable number of retries.

```javascript
const { Kafka, deadLetterQueue } = require('kafkajs')

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'my-app',
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'order-processing-group' })

const withDLQ = deadLetterQueue({
  producer,
  topic: 'orders.dlq',
  maxRetries: 3,
  onOriginalMessageFailed: async ({ topic, partition, message }, error) => {
    console.warn('Message sent to DLQ', {
      topic,
      partition,
      offset: message.offset,
      error: error.message,
    })
  },
})

const run = async () => {
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topics: ['orders'], fromBeginning: true })
  await consumer.run({
    eachMessage: withDLQ(async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString())
      if (!order.id) {
        throw new Error('Order is missing an ID')
      }
      console.log('Order processed', { orderId: order.id })
    }),
  })
}

run().catch(e => console.error(e.message, e))
```

The `deadLetterQueue` function accepts the following options:

| option                   | description                                                                          | default | type       | required |
| -------------------------| ------------------------------------------------------------------------------------ | ------- | ---------- | -------- |
| producer                 | A connected KafkaJS producer instance to send failed messages                        |         | `Producer` | **Yes** |
| topic                    | The dead letter topic name                                                           |         | `String`   | **Yes** |
| maxRetries               | Number of retries before sending to the DLQ                                          | `3`     | `Number`   | No |
| onOriginalMessageFailed  | Async callback invoked when a message is sent to the DLQ                             |         | `Function` | No |
| createDLQMessage         | Custom function to create the DLQ message. Receives `(error, eachMessagePayload)` and returns a `Message` object | automatic | `Function` | No |

> **New in v3.0.0:** The `createDLQMessage` option allows you to customize the message that gets sent to the DLQ topic. By default, the original message is forwarded as-is. Use this to add error details as headers, modify the value, etc.

```javascript
const withDLQ = deadLetterQueue({
  producer,
  topic: 'orders.dlq',
  maxRetries: 3,
  createDLQMessage: (error, { topic, partition, message }) => ({
    key: message.key,
    value: message.value,
    headers: {
      ...message.headers,
      'dlq-original-topic': topic,
      'dlq-original-partition': String(partition),
      'dlq-error-message': error.message,
    },
  }),
})
```

## <a name="cdc"></a> Change Data Capture (CDC)

KafkaJS can be used to consume Change Data Capture events from databases using connectors like [Debezium](https://debezium.io/). CDC events follow a standard envelope format with `before`, `after`, `op`, and `source` fields.

### PostgreSQL CDC Example

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'cdc-postgres-consumer',
})

const consumer = kafka.consumer({ groupId: 'cdc-postgres-handler' })

// Debezium topics follow: {topic.prefix}.{schema}.{table}
const topics = ['dbserver1.public.customers', 'dbserver1.public.orders']

const parseCDCEvent = message => {
  const value = message.value ? JSON.parse(message.value.toString()) : null
  const key = message.key ? JSON.parse(message.key.toString()) : null

  if (!value) {
    return { operation: 'tombstone', key, before: null, after: null }
  }

  const operationMap = { c: 'CREATE', u: 'UPDATE', d: 'DELETE', r: 'READ' }

  return {
    operation: operationMap[value.op] || value.op,
    before: value.before,
    after: value.after,
    source: value.source,
    timestamp: value.ts_ms,
    key,
  }
}

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topics, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = parseCDCEvent(message)
      const tableName = topic.split('.').pop()

      switch (event.operation) {
        case 'CREATE':
        case 'READ':
          console.log(`[${tableName}] INSERT`, { data: event.after })
          break
        case 'UPDATE':
          console.log(`[${tableName}] UPDATE`, { before: event.before, after: event.after })
          break
        case 'DELETE':
          console.log(`[${tableName}] DELETE`, { deleted: event.before })
          break
      }
    },
  })
}

run().catch(e => console.error(e.message, e))
```

Debezium supports PostgreSQL, MySQL, MongoDB, and other databases. See the [Debezium documentation](https://debezium.io/documentation/) for connector setup.

## <a name="instrumentation-events"></a> Instrumentation Events

The consumer emits instrumentation events for monitoring and debugging:

```javascript
const {
  HEARTBEAT,
  COMMIT_OFFSETS,
  GROUP_JOIN,
  FETCH_START,
  FETCH,
  START_BATCH_PROCESS,
  END_BATCH_PROCESS,
  CONNECT,
  DISCONNECT,
  STOP,
  CRASH,
  REBALANCING,
  RECEIVED_UNSUBSCRIBED_TOPICS,
  REQUEST,
  REQUEST_TIMEOUT,
  REQUEST_QUEUE_SIZE,
} = consumer.events

consumer.on(CRASH, e => console.error('Consumer crash', e.payload))
consumer.on(GROUP_JOIN, e => console.log('Joined group', e.payload))
```

The `on` method returns a function to remove the listener:

```javascript
const removeListener = consumer.on(consumer.events.CRASH, e => {})
removeListener() // stop listening
```

## <a name="follower-fetching"></a> Follower Fetching

KafkaJS supports "follower fetching", where the consumer tries to fetch data preferentially from a broker in the same "rack", rather than always going to the leader. This can considerably reduce operational costs if data transfer across "racks" is metered. There may also be performance benefits if the network speed between these "racks" is limited.

The meaning of "rack" is very flexible, and can be used to model setups such as data centers, regions/availability zones, or other topologies.

```javascript
const consumer = kafka.consumer({
  groupId: 'my-group',
  rackId: 'us-east-1a', // matches broker's broker.rack setting
})
```

See also [this blog post](https://www.confluent.io/blog/multi-region-data-replication/) for the bigger context.

## Consumer Methods Summary

| method           | description                                              | signature |
|------------------|----------------------------------------------------------|-----------|
| connect()        | Connect the consumer to the cluster                       | `() => Promise<void>` |
| disconnect()     | Disconnect the consumer                                   | `() => Promise<void>` |
| subscribe()      | Subscribe to topics                                       | `(subscription) => Promise<void>` |
| run()            | Start consuming messages                                  | `(config?) => Promise<void>` |
| stop()           | Stop consuming messages                                   | `() => Promise<void>` |
| seek()           | Seek to a specific offset                                 | `(topicPartitionOffset) => void` |
| pause()          | Pause topic-partitions                                    | `(topics) => void` |
| resume()         | Resume paused topic-partitions                            | `(topics) => void` |
| paused()         | Get paused topic-partitions                               | `() => TopicPartitions[]` |
| commitOffsets()  | Manually commit offsets                                   | `(offsets) => Promise<void>` |
| describeGroup()  | Get consumer group metadata                               | `() => Promise<GroupDescription>` |
| on()             | Listen to instrumentation events                          | `(event, listener) => RemoveListener` |
| logger()         | Get the consumer's logger                                 | `() => Logger` |
