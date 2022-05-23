---
id: version-2.0.0-migration-guide-v2.0.0
title: Migrating to v2.0.0
original_id: migration-guide-v2.0.0
---

v2.0.0 is the first major version of KafkaJS released since 2018. For most users, the required changes in order to upgrade from 1.x.x are very minor, but it is still important to read through the list of changes to know what, if any, changes need to be made.

## Producer: New default partitioner

> 🚨&nbsp; **Important!** 🚨
> 
> Not selecting the right partitioner will cause messages to be produced to different partitions than in versions previous to 2.0.0.

The default partitioner distributes messages consistently based on a hash of the message `key`. v1.8.0 introduced a new partitioner called `JavaCompatiblePartitioner` that behaves the same way, but fixes a bug where in some circumstances a message with the same key would be distributed to different partitions when produced with KafkaJS and the Java client.

**In v2.0.0 the following changes have been made**:

* `JavaCompatiblePartitioner` is renamed `DefaultPartitioner`
* The partitioner previously called `JavaCompatiblePartitioner` is selected as the default partitioner if no partitioner is configured.
* The old `DefaultPartitioner` is renamed `LegacyPartitioner`

If no partitioner is selected when creating the producer, a warning will be logged. This warning can be silenced either by specifying a partitioner to use or by setting the environment variable `KAFKAJS_NO_PARTITIONER_WARNING`. This warning will be removed in a future version.

### What do I need to do?

What you need to do depends on what partitioner you were previously using and whether or not co-partitioning is important to you.

#### "I was previously using the default partitioner and I want to keep the same behavior"

Import the `LegacyPartitioner` and configure your producer to use it:

```js
const { Partitioners } = require('kafkajs')
kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner })
```

#### "I was previously using the `JavaCompatiblePartitioner` and I want to keep that behavior"

The new `DefaultPartitioner` is re-exported as `JavaCompatiblePartitioner`, so existing code will continue to work. However, that export will be removed in a future version, so it's recommended to either remove the partitioner from the configuration or explicitly configure it to use what is now the default partitioner:

```js
// Rely on the default partitioner being compatible with the Java partitioner
kafka.producer()

// Or explicitly use the default partitioner
const { Partitioners } = require('kafkajs')
kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })
```

#### "I use a custom partitioner"

No need to do anything unless you are using either of the two built-in partitioners.

#### "It's not important to me that messages with the same key end up in the same partition as in previous versions"

Use the new default partitioner.

```js
kafka.producer()
```

## Request timeouts enabled

v1.5.1 added a request timeout mechanism. Due to some issues with the initial implementation, this was not enabled by default, but could be enabled using the undocumented `enforceRequestTimeout` flag. The issues have long since been ironed out and request timeout enforcement is is now enabled by default in v2.0.0.

The request timeout mechanism can be disabled like so:

```javascript
new Kafka({ enforceRequestTimeout: false })
```

See [Request Timeout](/docs/2.0.0/configuration#request-timeout) for more details.

## Consumer: Supporting duplicate header keys

If a message has more than one header value for the same key, previous versions of KafkaJS would discard all but one of the values. Now, it instead returns each value as an array.

```js
/**
 * Given a message like this:
 * {
 *   headers: {
 *     event: "birthday",
 *     participants: "Alice",
 *     participants: "Bob"
 *   }
 * }
 */

// Before
> message.headers
{
    event: <Buffer 62 69 72 74 68 64 61 79>,
    participants: <Buffer 42 6f 62>
}

// After
> message.headers
{
    event: <Buffer 62 69 72 74 68 64 61 79>,
    participants: [
        <Buffer 41 6c 69 63 65>,
        <Buffer 42 6f 62>
    ]
}
```

Adapt your code by handling header values potentially being arrays:

```js
// Before
const participants = message.headers["participants"].toString()

// After
const participants = Array.isArray(message.headers["participants"])
    ? message.headers["participants"].map(participant => participant.toString()).join(", ")
    : message.headers["participants"].toString()
```

## Admin: `getTopicMetadata` removed

The `getTopicMetadata` method of the admin client has been replaced by `fetchTopicMetadata`. `getTopicMetadata` had limitations that did not allow it to get metadata for all topics in the cluster.

See [Fetch Topic Metadata](/docs/2.0.0/admin#a-name-fetch-topic-metadata-a-fetch-topic-metadata) for details.

## Admin: `fetchOffsets` accepts `topics` instead of `topic`

`fetchOffsets` used to only be able to fetch offsets for a single topic, but now it can fetch for multiple topics.

To adapt your current code, pass in an array of `topics` instead of a single `topic` string, and handle the promise resolving to an array with each item being an object with a topic and an array of partition-offsets.

```js
// Before
const partitions = await admin.fetchOffsets({ groupId, topic: 'topic-a' })
for (const { partition, offset } of partitions) {
    admin.logger().info(`${groupId} is at offset ${offset} of partition ${partition}`)
}

// After
const topics = await admin.fetchOffsets({ groupId, topics: ['topic-a', 'topic-b'] })
for (const topic of topics) {
    for (const { partition, offset } of partitions) {
        admin.logger().info(`${groupId} is at offset ${offset} of ${topic}:${partition}`)
    }
}
```

## Admin: `createTopics` respects cluster settings `num.partitions` and `default.replication.factor`

Previously, `admin.createTopics` would default to creating topics with 1 partition and a replication factor of 1. It will now instead respect the cluster settings for partition count and replication factor. If the cluster does not have a default partition count or replication factor, topic creation will fail with an error.

To continue to create topics with 1 partition and a replication factor of 1, either configure the cluster with those defaults or provide them when creating the topic:

```js
await admin.createTopics({
  topics: [{ topic: 'topic-name', numPartitions: 1, replicationFactor: 1 }]
})
```

## Removed support for Node 10 and 12

KafkaJS supports all currently supported versions of NodeJS. If you are currently using NodeJS 10 or 12, you will get a warning when installing KafkaJS, and there is no guarantee that it will function. We **strongly** encourage you to upgrade to a supported, secure version of NodeJS.

## `originalError` property replaced with `cause`

Some errors that are triggered by other errors, such as `KafkaJSNumberOfRetriesExceeded`, used to have a property called `originalError` that contained a reference to the cause. This has been renamed `cause` to closer align with the [Error Cause](https://tc39.es/proposal-error-cause/) specification.

## Typescript: `ResourceTypes` replaced by `AclResourceTypes` and `ConfigResourceTypes`

The `ResourceTypes` enum has been split into `AclResourceTypes` and `ConfigResourceTypes`. The enum values happened to be the same for the two, even though they were actually unrelated to each other.

To migrate, simply import `ConfigResourceTypes` instead of `ResourceTypes` when operating on configs, and `AclResourceTypes` when operating on ACLs.

```ts
// Before
import { ResourceTypes } from 'kafkajs'
await admin.describeConfigs({
  includeSynonyms: false,
  resources: [
    {
      type: ResourceTypes.TOPIC,
      name: 'topic-name'
    }
  ]
})

// After
const { ConfigResourceTypes } = require('kafkajs')

await admin.describeConfigs({
  includeSynonyms: false,
  resources: [
    {
      type: ConfigResourceTypes.TOPIC,
      name: 'topic-name'
    }
  ]
})
```

## Typescript: `TopicPartitionOffsetAndMedata` removed

Use `TopicPartitionOffsetAndMetadata` instead.