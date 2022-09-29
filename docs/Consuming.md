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

Be aware that the `eachMessage` handler should not block for longer than the configured [session timeout](#options) or else the consumer will be removed from the group. If your workload involves very slow processing times for individual messages then you should either increase the session timeout or make periodic use of the `heartbeat` function exposed in the handler payload.
The `pause` function is a convenience for `consumer.pause({ topic, partitions: [partition] })`. It will pause the current topic-partition and returns a function that allows you to resume consuming later.

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

* `eachBatchAutoResolve` configures auto-resolve of batch processing. If set to true, KafkaJS will automatically commit the last offset of the batch if `eachBatch` doesn't throw an error. Default: true.
* `batch.highWatermark` is the last committed offset within the topic partition. It can be useful for calculating lag.
* `resolveOffset()` is used to mark a message in the batch as processed. In case of errors, the consumer will automatically commit the resolved offsets.
* `heartbeat(): Promise<void>` can be used to send heartbeat to the broker according to the set `heartbeatInterval` value in consumer [configuration](#options), which means if you invoke `heartbeat()` sooner than `heartbeatInterval` it will be ignored.
* `commitOffsetsIfNecessary(offsets?): Promise<void>` is used to commit offsets based on the autoCommit configurations (`autoCommitInterval` and `autoCommitThreshold`). Note that auto commit won't happen in `eachBatch` if `commitOffsetsIfNecessary` is not invoked. Take a look at [autoCommit](#auto-commit) for more information.
* `uncommittedOffsets()` returns all offsets by topic-partition which have not yet been committed.
* `isRunning()` returns true if consumer is in running state, else it returns false.
* `isStale()` returns whether the messages in the batch have been rendered stale through some other operation and should be discarded. For example, when calling [`consumer.seek`](#seek) the messages in the batch should be discarded, as they are not at the offset we seeked to.
* `pause()` can be used to pause the consumer for the current topic-partition. All offsets resolved up to that point will be committed (subject to `eachBatchAutoResolve` and [autoCommit](#auto-commit)). Throw an error to pause in the middle of the batch without resolving the current offset. Alternatively, disable `eachBatchAutoResolve`. The returned function can be used to resume processing of the topic-partition. See [Pause & Resume](#pause-resume) for more information about this feature.

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

## <a name="options"></a> Options

```javascript
kafka.consumer({
  groupId: <String>,
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
  maxInFlightRequests: <Number>,
  rackId: <String>
})
```

| option                 | description                                                                                                                                                                                                                                                                                                                                        | default                           |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| partitionAssigners     | List of partition assigners                                                                                                                                                                                                                                                                                                                        | `[PartitionAssigners.roundRobin]` |
| sessionTimeout         | Timeout in milliseconds used to detect failures. The consumer sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, then the broker will remove this consumer from the group and initiate a rebalance                                       | `30000`                           |
| rebalanceTimeout       | The maximum time that the coordinator will wait for each member to rejoin when rebalancing the group                                                                                                                                                                                                                                               | `60000`                           |
| heartbeatInterval      | The expected time in milliseconds between heartbeats to the consumer coordinator. Heartbeats are used to ensure that the consumer's session stays active. The value must be set lower than session timeout                                                                                                                                         | `3000`                            |
| metadataMaxAge         | The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions                                                                                                                                                       | `300000` (5 minutes)              |
| allowAutoTopicCreation | Allow topic creation when querying metadata for non-existent topics                                                                                                                                                                                                                                                                                | `true`                            |
| maxBytesPerPartition   | The maximum amount of data per-partition the server will return. This size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large message on a certain partition | `1048576` (1MB)                   |
| minBytes               | Minimum amount of data the server should return for a fetch request, otherwise wait up to `maxWaitTimeInMs` for more data to accumulate.                                                                                                                                                                                                           | `1`                               |
| maxBytes               | Maximum amount of bytes to accumulate in the response. Supported by Kafka >= `0.10.1.0`                                                                                                                                                                                                                                                            | `10485760` (10MB)                 |
| maxWaitTimeInMs        | The maximum amount of time in milliseconds the server will block before answering the fetch request if there isn’t sufficient data to immediately satisfy the requirement given by `minBytes`                                                                                                                                                      | `5000`                            |
| retry                  | See [retry](Configuration.md#retry) for more information                                                                                                                                                                                                                                                                                           | `{ retries: 5 }`                 |
| readUncommitted        | Configures the consumer isolation level. If `false` (default), the consumer will not return any transactional messages which were not committed.                                                                                                                                                                                                   | `false`                           |
| maxInFlightRequests | Max number of requests that may be in progress at any time. If falsey then no limit.                                    | `null` _(no limit)_ |
| rackId                 | Configure the "rack" in which the consumer resides to enable [follower fetching](#follower-fetching)                 | `null` _(fetch from the leader always)_ |

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

## <a name="follower-fetching"></a> Follower Fetching

KafkaJS supports "follower fetching", where the consumer tries to fetch data preferentially from a broker in the same "rack", rather than always going to the leader. This can considerably reduce operational costs if data transfer across "racks" is metered. There may also be performance benefits if the network speed between these "racks" is limited.

The meaning of "rack" is very flexible, and can be used to model setups such as data centers, regions/availability zones, or other topologies.

See also [this blog post](https://www.confluent.io/blog/multi-region-data-replication/) for the bigger context.
