---
id: version-3.0.0-admin
title: Admin Client
original_id: admin
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

## Admin Configuration

| option | description                            | default         | type     | required |
|--------|----------------------------------------|-----------------|----------|----------|
| retry  | Retry mechanism configuration          | `{ retries: 5 }` | `Object` | No |

## Admin Methods Summary

| method                          | description                                            | Kafka Version |
|---------------------------------|--------------------------------------------------------|---------------|
| listTopics                      | List all topic names                                    | All |
| createTopics                    | Create new topics                                       | All |
| deleteTopics                    | Delete topics                                           | All |
| createPartitions                | Add partitions to existing topics                       | All |
| fetchTopicMetadata              | Get topic metadata                                      | All |
| fetchTopicOffsets               | Get latest offsets for a topic                          | All |
| fetchTopicOffsetsByTimestamp    | Get offsets by timestamp                                | All |
| fetchOffsets                    | Get consumer group offsets                              | All |
| resetOffsets                    | Reset consumer group offsets                            | All |
| setOffsets                      | Set consumer group offsets                              | All |
| describeCluster                 | Get cluster information                                 | All |
| describeConfigs                 | Get resource configurations                             | All |
| alterConfigs                    | Replace resource configurations                         | All |
| incrementalAlterConfigs         | Incrementally update resource configurations            | 2.3+ |
| listGroups                      | List consumer groups                                    | All |
| describeGroups                  | Describe consumer groups                                | All |
| deleteGroups                    | Delete consumer groups                                  | All |
| deleteTopicRecords              | Delete records from topic partitions                    | All |
| createAcls                      | Create ACL entries                                      | All |
| deleteAcls                      | Delete ACL entries                                      | All |
| describeAcls                    | Describe ACL entries                                    | All |
| alterPartitionReassignments     | Reassign partition replicas                             | All |
| listPartitionReassignments      | List ongoing partition reassignments                    | All |
| electLeaders                    | Trigger leader election                                 | 2.4+ |
| deleteOffsets                   | Delete consumer group offsets                           | All |
| describeLogDirs                 | Get log directory information                           | All |
| describeProducers               | Describe active producers                               | All |
| describeTransactions            | Describe active transactions                            | All |
| listTransactions                | List active transactions                                | All |
| shareGroupDescribe              | Describe Share Groups (KIP-932)                         | **4.0+** |

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
    numPartitions: <Number>,     // default: -1 (uses broker `num.partitions` configuration)
    replicationFactor: <Number>, // default: -1 (uses broker `default.replication.factor` configuration)
    replicaAssignment: <Array>,  // Example: [{ partition: 0, replicas: [0,1,2] }] - default: []
    configEntries: <Array>       // Example: [{ name: 'cleanup.policy', value: 'compact' }] - default: []
}
```

| property       | description                                                                                           | default | type | required |
| -------------- | ----------------------------------------------------------------------------------------------------- | ------- | ---- | -------- |
| topics         | Topic definitions array                                                                               |         | `ITopicConfig[]` | **Yes** |
| validateOnly   | If `true`, the request will be validated, but the topic won't be created                              | `false` | `Boolean` | No |
| timeout        | The time in ms to wait for a topic to be completely created on the controller node                    | `5000`  | `Number` | No |
| waitForLeaders | If `true` it will wait until metadata for the new topics doesn't throw `LEADER_NOT_AVAILABLE`         | `true`  | `Boolean` | No |

## <a name="delete-topics"></a> Delete topics

```javascript
await admin.deleteTopics({
    topics: <String[]>,
    timeout: <Number>, // default: 5000
})
```

| property | description                        | default | type       | required |
|----------|------------------------------------|---------|------------|----------|
| topics   | Array of topic names to delete     |         | `String[]` | **Yes** |
| timeout  | Timeout in ms                      | `5000`  | `Number`   | No |

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

| property       | description                                                                           | default | type | required |
| -------------- | ------------------------------------------------------------------------------------- | ------- | ---- | -------- |
| topicPartitions| Topic partition definitions                                                           |         | `ITopicPartitionConfig[]` | **Yes** |
| validateOnly   | If `true`, the request will be validated, but the partitions won't be created         | `false` | `Boolean` | No |
| timeout        | The time in ms to wait for completion                                                 | `5000`  | `Number` | No |

## <a name="fetch-topic-metadata"></a> Fetch topic metadata

```javascript
await admin.fetchTopicMetadata({ topics: <Array<String>> })
```

| property | description                                           | default | type       | required |
|----------|-------------------------------------------------------|---------|------------|----------|
| topics   | Array of topic names. Omit to fetch all topics        |         | `String[]` | No |

`TopicMetadata` structure:

```javascript
{
    name: <String>,
    partitions: <Array<PartitionMetadata>>
}
```

`PartitionMetadata` structure:

```javascript
{
    partitionErrorCode: <Number>, // default: 0
    partitionId: <Number>,
    leader: <Number>,
    leaderEpoch: <Number>,        // Available with Kafka 2.4+ (Metadata v7+)
    replicas: <Array<Number>>,
    isr: <Array<Number>>,
    offlineReplicas: <Array<Number>>, // Available when broker reports it
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

| parameter | description                              | type     | required |
|-----------|------------------------------------------|----------|----------|
| topic     | Topic name                               | `String` | **Yes** |
| timestamp | Timestamp in ms (epoch). Omit for latest | `Number` | No |

## <a name="fetch-offsets"></a> Fetch consumer group offsets

`fetchOffsets` returns the consumer group offset for a list of topics.

```javascript
await admin.fetchOffsets({ groupId, topics: ['topic1', 'topic2'] })
// [
//   {
//     topic: 'topic1',
//     partitions: [
//       { partition: 0, offset: '31004', metadata: null },
//       { partition: 1, offset: '54312', metadata: null },
//     ],
//   },
// ]
```

| property       | description                                                              | default | type       | required |
|----------------|--------------------------------------------------------------------------|---------|------------|----------|
| groupId        | Consumer group ID                                                         |         | `String`   | **Yes** |
| topics         | Array of topic names. Omit to get offsets for all committed topics        |         | `String[]` | No |
| resolveOffsets | Resolve offsets to real values (useful after reset)                        | `false` | `Boolean`  | No |

Include the optional `resolveOffsets` flag to resolve the offsets without having to start a consumer, useful when fetching directly after calling [resetOffsets](#a-name-reset-offsets-a-reset-consumer-group-offsets):

```javascript
await admin.resetOffsets({ groupId, topic })
await admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: false })
// [
//   { partition: 0, offset: '-1' },
//   { partition: 1, offset: '-1' },
// ]

await admin.resetOffsets({ groupId, topic })
await admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: true })
// [
//   { partition: 0, offset: '31004' },
//   { partition: 1, offset: '54312' },
// ]
```

## <a name="reset-offsets"></a> Reset consumer group offsets

`resetOffsets` resets the consumer group offset to the earliest or latest offset (latest by default).
The consumer group must have no running instances when performing the reset. Otherwise, the command will be rejected.

```javascript
await admin.resetOffsets({ groupId, topic }) // latest by default
// await admin.resetOffsets({ groupId, topic, earliest: true })
```

| property | description                                     | default | type      | required |
|----------|-------------------------------------------------|---------|-----------|----------|
| groupId  | Consumer group ID                                |         | `String`  | **Yes** |
| topic    | Topic name                                       |         | `String`  | **Yes** |
| earliest | If `true`, reset to earliest offset              | `false` | `Boolean` | No |

## <a name="set-offsets"></a> Set consumer group offsets

`setOffsets` allows you to set the consumer group offset to any value.

```javascript
await admin.setOffsets({
    groupId: <String>,
    topic: <String>,
    partitions: <SeekEntry[]>,
})
```

| property   | description                        | type     | required |
|------------|------------------------------------|----------|----------|
| groupId    | Consumer group ID                  | `String` | **Yes** |
| topic      | Topic name                         | `String` | **Yes** |
| partitions | Array of partition-offset entries   | `SeekEntry[]` | **Yes** |

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
//     { nodeId: 0, host: 'localhost', port: 9092, rack: 'us-east-1a' }
//   ],
//   controller: 0,
//   clusterId: 'f8QmWTB8SQSLE6C99G4qzA',
//   clusterAuthorizedOperations: 0
// }
```

Response fields:

| field                        | description                                            | type |
|------------------------------|--------------------------------------------------------|------|
| brokers                      | Array of broker info with `nodeId`, `host`, `port`, `rack` | `ClusterBroker[]` |
| controller                   | Node ID of the controller broker (or `null`)           | `Number \| null` |
| clusterId                    | Cluster identifier string                               | `String` |
| clusterAuthorizedOperations  | Bitmask of authorized operations                        | `Number` |

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
    type: <ConfigResourceTypes>,
    name: <String>,
    configNames: <String[]>  // optional - omit to get all configs
}
```

| property        | description                              | default | type | required |
|-----------------|------------------------------------------|---------|------|----------|
| resources       | Resources to describe                    |         | `ResourceConfigQuery[]` | **Yes** |
| includeSynonyms | Include config synonyms in response      | `false` | `Boolean` | No |

Available `ConfigResourceTypes`:

| Type           | Value | Description |
|----------------|-------|-------------|
| `UNKNOWN`      | `0`   | Unknown resource type |
| `TOPIC`        | `2`   | Topic configuration |
| `BROKER`       | `4`   | Broker configuration |
| `BROKER_LOGGER`| `8`   | Broker logger configuration |

Returning all configs for a given resource:

```javascript
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

Returning specific configs for a given resource:

```javascript
const { ConfigResourceTypes } = require('kafkajs')

await admin.describeConfigs({
  includeSynonyms: false,
  resources: [
    {
      type: ConfigResourceTypes.TOPIC,
      name: 'topic-name',
      configNames: ['cleanup.policy']
    }
  ]
})
```

Example response:

```javascript
{
    resources: [
        {
            configEntries: [{
                configName: 'cleanup.policy',
                configValue: 'delete',
                isDefault: true,
                configSource: 5,
                isSensitive: false,
                readOnly: false,
                configSynonyms: []
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

Update the configuration for the specified resources. **Warning:** This replaces ALL configs for the resource. Use [incrementalAlterConfigs](#incremental-alter-configs) instead if you only want to change specific entries.

```javascript
await admin.alterConfigs({
    validateOnly: false,
    resources: <ResourceConfig[]>
})
```

| property     | description                                      | default | type | required |
|-------------|--------------------------------------------------|---------|------|----------|
| resources   | Resources to alter                               |         | `IResourceConfig[]` | **Yes** |
| validateOnly | Validate only, don't apply changes              | `false` | `Boolean` | No |

`ResourceConfig` structure:

```javascript
{
    type: <ConfigResourceType>,
    name: <String>,
    configEntries: <ResourceConfigEntry[]>
}
```

Example:

```javascript
const { ConfigResourceTypes } = require('kafkajs')

await admin.alterConfigs({
    resources: [{
        type: ConfigResourceTypes.TOPIC,
        name: 'topic-name',
        configEntries: [{ name: 'cleanup.policy', value: 'compact' }]
    }]
})
```

## <a name="incremental-alter-configs"></a> Incremental Alter Configs

> **New in v3.0.0 docs.** Requires Kafka 2.3+.

Incrementally update the configuration for the specified resources. Unlike `alterConfigs`, this method only changes the specified config entries without replacing all configs. **This is the recommended way to modify configs.**

```javascript
await admin.incrementalAlterConfigs({
    validateOnly: false,
    resources: <IncrementalAlterConfigsResource[]>
})
```

| property     | description                           | default | type | required |
|-------------|---------------------------------------|---------|------|----------|
| resources   | Resources to alter                    |         | `IncrementalAlterConfigsResource[]` | **Yes** |
| validateOnly | Validate only, don't apply changes   | `false` | `Boolean` | No |

`IncrementalAlterConfigEntry` structure:

```javascript
{
    name: <String>,
    configOperation: <ConfigOperationTypes>,
    value: <String>
}
```

Config operation types (`ConfigOperationTypes`):

| Operation  | Value | Description |
|------------|-------|-------------|
| `SET`      | `0`   | Set the value of the config entry |
| `DELETE`   | `1`   | Reset the config entry to its default value |
| `APPEND`   | `2`   | Append the value to the existing config (for list-type configs) |
| `SUBTRACT` | `3`   | Remove the value from the existing config (for list-type configs) |

Example:

```javascript
const { ConfigResourceTypes, ConfigOperationTypes } = require('kafkajs')

await admin.incrementalAlterConfigs({
    resources: [{
        type: ConfigResourceTypes.TOPIC,
        name: 'topic-name',
        configEntries: [
            { name: 'cleanup.policy', configOperation: ConfigOperationTypes.SET, value: 'compact' },
            { name: 'max.message.bytes', configOperation: ConfigOperationTypes.DELETE, value: '' },
        ]
    }]
})
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
//         groupInstanceId: 'instance-1', // present with static membership
//       },
//     ],
//     protocol: 'RoundRobinAssigner',
//     protocolType: 'consumer',
//     state: 'Stable',
//   }]
// }
```

Consumer group state values:

| State                 | Description |
|-----------------------|-------------|
| `Unknown`             | The group state is unknown |
| `PreparingRebalance`  | The group is preparing to rebalance |
| `CompletingRebalance` | The group is completing rebalance |
| `Stable`              | The group is stable and consuming |
| `Dead`                | The group has been deleted |
| `Empty`               | The group has no active members |

Helper function to decode `memberMetadata` and `memberAssignment` is available in `AssignerProtocol`

Example:

`const memberMetadata = AssignerProtocol.MemberMetadata.decode(memberMetadata)`

`const memberAssignment = AssignerProtocol.MemberAssignment.decode(memberAssignment)`


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

## <a name="delete-topic-records"></a> Delete Topic Records

Delete records for a selected topic. This will delete all records from the earliest offset up to - but not including - the provided target offset for the given partition(s). To delete all records in a partition, use a target offset of `-1`.

Note that you cannot delete records in an arbitrary range (it will always be from the earliest available offset)

```javascript
await admin.deleteTopicRecords({
    topic: <String>,
    partitions: <SeekEntry[]>,
})
```

| property   | description                            | type     | required |
|------------|----------------------------------------|----------|----------|
| topic      | Topic name                             | `String` | **Yes** |
| partitions | Array of partition-offset entries       | `SeekEntry[]` | **Yes** |

Example:

```javascript
await admin.deleteTopicRecords({
    topic: 'custom-topic',
    partitions: [
        { partition: 0, offset: '30' }, // delete up to and including offset 29
        { partition: 3, offset: '-1' }, // delete all available records on this partition
    ]
})
```

## <a name="create-acl"></a> Create ACL

```javascript
const {
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('kafkajs')

const acl = [
  {
    resourceType: AclResourceTypes.TOPIC,
    resourceName: 'topic-name',
    resourcePatternType: ResourcePatternTypes.LITERAL,
    principal: 'User:bob',
    host: '*',
    operation: AclOperationTypes.ALL,
    permissionType: AclPermissionTypes.DENY,
  },
  {
    resourceType: AclResourceTypes.TOPIC,
    resourceName: 'topic-name',
    resourcePatternType: ResourcePatternTypes.LITERAL,
    principal: 'User:alice',
    host: '*',
    operation: AclOperationTypes.ALL,
    permissionType: AclPermissionTypes.ALLOW,
  },
]

await admin.createAcls({ acl })
```

### ACL Enum Reference

**AclResourceTypes:**

| Name                | Value |
|---------------------|-------|
| UNKNOWN             | 0     |
| ANY                 | 1     |
| TOPIC               | 2     |
| GROUP               | 3     |
| CLUSTER             | 4     |
| TRANSACTIONAL_ID    | 5     |
| DELEGATION_TOKEN    | 6     |

**AclOperationTypes:**

| Name              | Value |
|-------------------|-------|
| UNKNOWN           | 0     |
| ANY               | 1     |
| ALL               | 2     |
| READ              | 3     |
| WRITE             | 4     |
| CREATE            | 5     |
| DELETE            | 6     |
| ALTER             | 7     |
| DESCRIBE          | 8     |
| CLUSTER_ACTION    | 9     |
| DESCRIBE_CONFIGS  | 10    |
| ALTER_CONFIGS     | 11    |
| IDEMPOTENT_WRITE  | 12    |

**AclPermissionTypes:**

| Name    | Value |
|---------|-------|
| UNKNOWN | 0     |
| ANY     | 1     |
| DENY    | 2     |
| ALLOW   | 3     |

**ResourcePatternTypes:**

| Name     | Value |
|----------|-------|
| UNKNOWN  | 0     |
| ANY      | 1     |
| MATCH    | 2     |
| LITERAL  | 3     |
| PREFIXED | 4     |

Be aware that the security features might be disabled in your cluster. In that case, the operation will throw an error:

```sh
KafkaJSProtocolError: Security features are disabled
```

## <a name="delete-acl"></a> Delete ACL

```javascript
const {
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('kafkajs')

const acl = {
  resourceName: 'topic-name',
  resourceType: AclResourceTypes.TOPIC,
  host: '*',
  permissionType: AclPermissionTypes.ALLOW,
  operation: AclOperationTypes.ANY,
  resourcePatternType: ResourcePatternTypes.LITERAL,
}

await admin.deleteAcls({ filters: [acl] })
```

## <a name="describe-acl"></a> Describe ACL

```javascript
const {
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('kafkajs')

await admin.describeAcls({
  resourceName: 'topic-name',
  resourceType: AclResourceTypes.TOPIC,
  host: '*',
  permissionType: AclPermissionTypes.ALLOW,
  operation: AclOperationTypes.ANY,
  resourcePatternTypeFilter: ResourcePatternTypes.LITERAL,
})
```

## <a name="alter-partition-reassignments"></a> Alter Partition Reassignments

This is used to reassign the replicas that partitions are on. This method will throw exceptions in the case of errors.

```typescript
await admin.alterPartitionReassignments({
  topics: <PartitionReassignment[]>,
  timeout: <Number> // optional - 5000 default
})
```

| property | description                     | default | type | required |
|----------|---------------------------------|---------|------|----------|
| topics   | Partition reassignment array    |         | `PartitionReassignment[]` | **Yes** |
| timeout  | Timeout in ms                   | `5000`  | `Number` | No |

PartitionReassignment Structure:
```typescript
{
  topic: <String>,
  partitionAssignment: <Number[]> // Example: [{ partition: 0, replicas: [0,1,2] }]
}
```

## <a name="list-partition-reassignments"></a> List Partition Reassignments

This is used to list current partition reassignments in progress.

```javascript
await admin.listPartitionReassignments({
  topics: <TopicPartitions[]>, // optional, if null then all topics will be returned.
  timeout: <Number> // optional - 5000 default
})
```

| property | description                                         | default | type | required |
|----------|-----------------------------------------------------|---------|------|----------|
| topics   | Topic partitions to query. Omit for all topics      |         | `TopicPartitions[]` | No |
| timeout  | Timeout in ms                                       | `5000`  | `Number` | No |

Response includes `OngoingPartitionReassignment`:

| field             | description                                 | type |
|-------------------|---------------------------------------------|------|
| partitionIndex    | Partition number                             | `Number` |
| replicas          | Current replica set                          | `Number[]` |
| addingReplicas    | Replicas being added (empty if not ongoing)  | `Number[]` |
| removingReplicas  | Replicas being removed (empty if not ongoing)| `Number[]` |

## <a name="elect-leaders"></a> Elect Leaders

> Requires Kafka 2.4+

Triggers a leader election for one or more partitions.

```javascript
await admin.electLeaders({
  electionType: 0, // 0 = PREFERRED, 1 = UNCLEAN
  topicPartitions: [{ topic: 'my-topic', partitions: [0, 1] }], // null = all partitions
  timeout: 30000,
})
```

| property        | description                                         | default | type | required |
|-----------------|-----------------------------------------------------|---------|------|----------|
| electionType    | `0` = PREFERRED, `1` = UNCLEAN                      | `0`     | `Number` | No |
| topicPartitions | Topics and partitions. `null` for all                | `null`  | `Array \| null` | No |
| timeout         | Timeout in ms                                        | `30000` | `Number` | No |

Election types (`ElectionType` enum):

| Type       | Value | Description |
|------------|-------|-------------|
| `PREFERRED`| `0`   | Elect the preferred (first) replica as leader |
| `UNCLEAN`  | `1`   | Elect any available replica as leader (may lose data) |

## <a name="delete-offsets"></a> Delete Offsets

Deletes consumer group offsets for specific topic partitions. The consumer group must not have any active members.

```javascript
await admin.deleteOffsets({
  groupId: 'my-group',
  topic: 'my-topic',
  partitions: [{ partition: 0 }, { partition: 1 }],
})
```

| property   | description                            | type     | required |
|------------|----------------------------------------|----------|----------|
| groupId    | Consumer group ID                      | `String` | **Yes** |
| topic      | Topic name                             | `String` | **Yes** |
| partitions | Partitions to delete offsets for       | `Array<{ partition: number }>` | **Yes** |

## <a name="describe-log-dirs"></a> Describe Log Dirs

Returns information about log directories on all brokers.

```javascript
const logDirs = await admin.describeLogDirs({ topics: ['my-topic'] })
```

| property | description                                         | default | type | required |
|----------|-----------------------------------------------------|---------|------|----------|
| topics   | Topics and partitions to describe. `null` for all    | `null`  | `Array \| null` | No |

Response includes per-broker log directory info with topic partition sizes and offset lags.

## <a name="describe-producers"></a> Describe Producers

Returns information about active producers for the specified topic partitions.

```javascript
const result = await admin.describeProducers({
  topics: [{ topic: 'my-topic', partitions: [0, 1] }],
})
```

Response includes `ActiveProducer`:

| field                  | description                          | type |
|-----------------------|--------------------------------------|------|
| producerId            | Producer ID                           | `String` |
| producerEpoch         | Producer epoch                        | `Number` |
| lastSequence          | Last sequence number                  | `Number` |
| lastTimestamp         | Last message timestamp                | `String` |
| coordinatorEpoch      | Transaction coordinator epoch         | `Number` |
| currentTxnStartOffset | Start offset of current transaction   | `String` |

## <a name="describe-transactions"></a> Describe Transactions

Returns details about active transactions.

```javascript
const result = await admin.describeTransactions({
  transactionalIds: ['my-txn-id-1', 'my-txn-id-2'],
})
```

| property         | description                    | type       | required |
|------------------|--------------------------------|------------|----------|
| transactionalIds | Transaction IDs to describe    | `String[]` | **Yes** |

Response includes `TransactionState`:

| field                   | description                  | type |
|------------------------|------------------------------|------|
| transactionalId        | Transaction ID                | `String` |
| state                  | Transaction state             | `String` |
| producerId             | Producer ID                   | `String` |
| producerEpoch          | Producer epoch                | `Number` |
| transactionTimeoutMs   | Transaction timeout           | `Number` |
| transactionStartTimeMs | Transaction start time        | `String` |
| topics                 | Topics involved in transaction| `Array` |

## <a name="list-transactions"></a> List Transactions

Lists active transactions, optionally filtering by state or producer ID.

```javascript
const result = await admin.listTransactions({
  stateFilters: ['Ongoing'],
  producerIdFilters: [],
})
```

| property           | description                        | default | type       | required |
|--------------------|------------------------------------|---------|------------|----------|
| stateFilters       | Filter by transaction state        | `[]`    | `String[]` | No |
| producerIdFilters  | Filter by producer ID              | `[]`    | `Number[]` | No |

## <a name="share-group-describe"></a> Describe Share Groups

> **New in v3.0.0. Requires Kafka 4.0+**

Describes one or more Share Groups (KIP-932). Share Groups provide queue-like consumption where multiple consumers process records from the same partitions concurrently with per-record acknowledgement.

```javascript
const result = await admin.shareGroupDescribe({
  groupIds: ['my-share-group'],
  includeAuthorizedOperations: false,
})
```

| property                     | description                       | default | type       | required |
|------------------------------|-----------------------------------|---------|------------|----------|
| groupIds                     | Share Group IDs to describe       |         | `String[]` | **Yes** |
| includeAuthorizedOperations  | Include authorized ops in response| `false` | `Boolean`  | No |

Response includes `ShareGroupDescribeGroup`:

| field            | description                        | type |
|------------------|------------------------------------|------|
| groupId          | Share group ID                      | `String` |
| groupState       | Group state                         | `String` |
| groupEpoch       | Group epoch                         | `Number` |
| assignmentEpoch  | Assignment epoch                    | `Number` |
| assignorName     | Name of the assignor                | `String` |
| topics           | Topic assignments with partitions   | `Array` |
| members          | Group members with assignments      | `ShareGroupMember[]` |

## Instrumentation Events

The admin client emits instrumentation events for monitoring:

```javascript
const { CONNECT, DISCONNECT, REQUEST, REQUEST_TIMEOUT, REQUEST_QUEUE_SIZE } = admin.events

admin.on(CONNECT, e => console.log('Admin connected'))
admin.on(DISCONNECT, e => console.log('Admin disconnected'))
admin.on(REQUEST, e => console.log('Request', e.payload))
```

The `on` method returns a function to remove the listener:

```javascript
const removeListener = admin.on(admin.events.REQUEST, e => {})
removeListener() // stop listening
```
