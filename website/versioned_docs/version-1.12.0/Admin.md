---
id: admin
title: Admin Client
---

The admin client hosts all the cluster operations, such as: `createTopics`, `createPartitions`, etc.

```javascript
const kafka = new Kafka(...)
const admin = kafka.admin()

// remember to connect and disconnect when you are done
await admin.connect()
await admin.disconnect()
```

The option `retry` can be used to customize the configuration for the admin.

Take a look at [Retry](Configuration.md#default-retry) for more information.

## <a name="list-topics"></a> List topics

`listTopics` lists the names of all existing topics, and returns an array of strings.
The method will throw exceptions in case of errors.

```javascript
await admin.listTopics()
// [ 'topic-1', 'topic-2', 'topic-3', ... ]
```

## <a name="create-topics"></a> Create topics

`createTopics` will resolve to `true` if the topic was created successfully or `false` if it already exists. The method will throw exceptions in case of errors.

```javascript
await admin.createTopics({
    validateOnly: <boolean>,
    waitForLeaders: <boolean>
    timeout: <Number>,
    topics: <ITopicConfig[]>,
})
```

`ITopicConfig` structure:

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

## <a name="delete-topics"></a> Delete topics

```javascript
await admin.deleteTopics({
    topics: <String[]>,
    timeout: <Number>,
})
```

Topic deletion is disabled by default in Apache Kafka versions prior to `1.0.0`. To enable it set the server config.

```yml
delete.topic.enable=true
```

## <a name="create-partitions"></a> Create partitions

`createPartitions` will resolve in case of success. The method will throw exceptions in case of errors.

```javascript
await admin.createPartitions({
    validateOnly: <boolean>,
    timeout: <Number>,
    topicPartitions: <TopicPartition[]>,
})
```

`TopicPartition` structure:

```javascript
{
    topic: <String>,
    count: <Number>,     // partition count
    assignments: <Array<Array<Number>>> // Example: [[0,1],[1,2],[2,0]] 
}
```

| property       | description                                                                                           | default |
| -------------- | ----------------------------------------------------------------------------------------------------- | ------- |
| topicPartitions| Topic partition definition                                                                                      |         |
| validateOnly   | If this is `true`, the request will be validated, but the topic won't be created.                     | false   |
| timeout        | The time in ms to wait for a topic to be completely created on the controller node                    | 5000    |
| count          | New partition count, mandatory                                                                                   |         |
| assignments    | Assigned brokers for each new partition                                                               | null    |

## <a name="get-topic-metadata"></a> Get topic metadata

Deprecated, see [Fetch topic metadata](#fetch-topic-metadata)

## <a name="fetch-topic-metadata"></a> Fetch topic metadata

```javascript
await admin.fetchTopicMetadata({ topics: <Array<String>> })
```

`TopicsMetadata` structure:

```javascript
{
    topics: <Array<TopicMetadata>>,
}
```

`TopicMetadata` structure:

```javascript
{
    topic: <String>,
    partitions: <Array<PartitionMetadata>> // default: 1
}
```

`PartitionMetadata` structure:

```javascript
{
    partitionErrorCode: <Number>, // default: 0
    partitionId: <Number>,
    leader: <Number>,
    replicas: <Array<Number>>,
    isr: <Array<Number>>,
}
```

The admin client will throw an exception if any of the provided topics do not already exist.

If you omit the `topics` argument the admin client will fetch metadata for all topics:

```javascript
await admin.fetchTopicMetadata()
```

## <a name="fetch-topic-offsets"></a> Fetch topic offsets

`fetchTopicOffsets` returns most recent offset for a topic.

```javascript
await admin.fetchTopicOffsets(topic)
// [
//   { partition: 0, offset: '31004', high: '31004', low: '421' },
//   { partition: 1, offset: '54312', high: '54312', low: '3102' },
//   { partition: 2, offset: '32103', high: '32103', low: '518' },
//   { partition: 3, offset: '28', high: '28', low: '0' },
// ]
```

## <a name="fetch-topic-offsets-by-timestamp"></a> Fetch topic offsets by timestamp

Specify a `timestamp` to get the earliest offset on each partition where the message's timestamp is greater than or equal to the given timestamp.

```javascript
await admin.fetchTopicOffsetsByTimestamp(topic, timestamp)
// [
//   { partition: 0, offset: '3244' },
//   { partition: 1, offset: '3113' },
// ]
```

## <a name="fetch-offsets"></a> Fetch consumer group offsets

`fetchOffsets` returns the consumer group offset for a topic.

```javascript
await admin.fetchOffsets({ groupId, topic })
// [
//   { partition: 0, offset: '31004' },
//   { partition: 1, offset: '54312' },
//   { partition: 2, offset: '32103' },
//   { partition: 3, offset: '28' },
// ]
```

## <a name="reset-offsets"></a> Reset consumer group offsets

`resetOffsets` resets the consumer group offset to the earliest or latest offset (latest by default).
The consumer group must have no running instances when performing the reset. Otherwise, the command will be rejected.

```javascript
await admin.resetOffsets({ groupId, topic }) // latest by default
// await admin.resetOffsets({ groupId, topic, earliest: true })
```

## <a name="set-offsets"></a> Set consumer group offsets

`setOffsets` allows you to set the consumer group offset to any value.

```javascript
await admin.setOffsets({
    groupId: <String>,
    topic: <String>,
    partitions: <SeekEntry[]>,
})
```

`SeekEntry` structure:

```javascript
{
    partition: <Number>,
    offset: <String>,
}
```

Example:

```javascript
await admin.setOffsets({
    groupId: 'my-consumer-group',
    topic: 'custom-topic',
    partitions: [
        { partition: 0, offset: '35' },
        { partition: 3, offset: '19' },
    ]
})
```

## <a name="reset-offsets-by-timestamp"></a> Reset consumer group offsets by timestamp

Combining `fetchTopicOffsetsByTimestamp` and `setOffsets` can reset a consumer group's offsets on each partition to the earliest offset whose timestamp is greater than or equal to the given timestamp.
The consumer group must have no running instances when performing the reset. Otherwise, the command will be rejected.

```javascript
await admin.setOffsets({ groupId, topic, partitions: await admin.fetchTopicOffsetsByTimestamp(topic, timestamp) })
```

## <a name="describe-cluster"></a> Describe cluster

Allows you to get information about the broker cluster. This is mostly useful
for monitoring or operations, and is usually not relevant for typical event processing.

```javascript
await admin.describeCluster()
// {
//   brokers: [
//     { nodeId: 0, host: 'localhost', port: 9092 }
//   ],
//   controller: 0,
//   clusterId: 'f8QmWTB8SQSLE6C99G4qzA'
// }
```

## <a name="describe-configs"></a> Describe configs

Get the configuration for the specified resources.

```javascript
await admin.describeConfigs({
  includeSynonyms: <boolean>,
  resources: <ResourceConfigQuery[]>
})
```

`ResourceConfigQuery` structure:

```javascript
{
    type: <ResourceType>,
    name: <String>,
    configNames: <String[]>
}
```

Returning all configs for a given resource:

```javascript
const { ResourceTypes } = require('kafkajs')

await admin.describeConfigs({
  includeSynonyms: false,
  resources: [
    {
      type: ResourceTypes.TOPIC,
      name: 'topic-name'
    }
  ]
})
```

Returning specific configs for a given resource:

```javascript
const { ResourceTypes } = require('kafkajs')

await admin.describeConfigs({
  includeSynonyms: false,
  resources: [
    {
      type: ResourceTypes.TOPIC,
      name: 'topic-name',
      configNames: ['cleanup.policy']
    }
  ]
})
```

Take a look at [resourceTypes](https://github.com/tulios/kafkajs/blob/master/src/protocol/resourceTypes.js) for a complete list of resources.

Example response:

```javascript
{
    resources: [
        {
            configEntries: [{
                configName: 'cleanup.policy',
                configValue: 'delete',
                isDefault: true,
                isSensitive: false,
                readOnly: false
            }],
            errorCode: 0,
            errorMessage: null,
            resourceName: 'topic-name',
            resourceType: 2
        }
    ],
    throttleTime: 0
}
```

## <a name="alter-configs"></a> Alter configs

Update the configuration for the specified resources.

```javascript
await admin.alterConfigs({
    validateOnly: false,
    resources: <ResourceConfig[]>
})
```

`ResourceConfig` structure:

```javascript
{
    type: <ResourceType>,
    name: <String>,
    configEntries: <ResourceConfigEntry[]>
}
```

`ResourceConfigEntry` structure:

```javascript
{
    name: <String>,
    value: <String>
}
```

Example:

```javascript
const { ResourceTypes } = require('kafkajs')

await admin.alterConfigs({
    resources: [{
        type: ResourceTypes.TOPIC,
        name: 'topic-name',
        configEntries: [{ name: 'cleanup.policy', value: 'compact' }]
    }]
})
```

Take a look at [resourceTypes](https://github.com/tulios/kafkajs/blob/master/src/protocol/resourceTypes.js) for a complete list of resources.

Example response:

```javascript
{
    resources: [{
        errorCode: 0,
        errorMessage: null,
        resourceName: 'topic-name',
        resourceType: 2,
    }],
    throttleTime: 0,
}
```

## <a name="list-groups"></a> List groups

List groups available on the broker.

```javascript
await admin.listGroups()
```

Example response:

```javascript
{
    groups: [
        {groupId: 'testgroup', protocolType: 'consumer'}
    ]
}
```

## <a name="describe-groups"></a> Describe groups

Describe consumer groups by `groupId`s. This is similar to [consumer.describeGroup()](Consuming.md#describe-group), except
it allows you to describe multiple groups and does not require you to have a consumer be part of any of those groups.

```js
await admin.describeGroups([ 'testgroup' ])
// {
//   groups: [{
//     errorCode: 0,
//     groupId: 'testgroup',
//     members: [
//       {
//         clientHost: '/172.19.0.1',
//         clientId: 'test-3e93246fe1f4efa7380a',
//         memberAssignment: Buffer,
//         memberId: 'test-3e93246fe1f4efa7380a-ff87d06d-5c87-49b8-a1f1-c4f8e3ffe7eb',
//         memberMetadata: Buffer,
//       },
//     ],
//     protocol: 'RoundRobinAssigner',
//     protocolType: 'consumer',
//     state: 'Stable',
//   }]
// }
```

## <a name="delete-groups"></a> Delete groups

Delete groups by `groupId`.

Note that you can only delete groups with no connected consumers.

```javascript
await admin.deleteGroups([groupId])
```

Example:

```javascript
await admin.deleteGroups(['group-test'])
```

Example response:

```javascript
[
    {groupId: 'testgroup', errorCode: 'consumer'}
]
```

Because this method accepts multiple `groupId`s, it can fail to delete one or more of the provided groups. In case of failure, it will throw an error containing the failed groups:

```javascript
try {
    await admin.deleteGroups(['a', 'b', 'c'])
} catch (error) {
  // error.name 'KafkaJSDeleteGroupsError'
  // error.groups = [{
  //   groupId: a
  //   error: KafkaJSProtocolError
  // }]
}
```
