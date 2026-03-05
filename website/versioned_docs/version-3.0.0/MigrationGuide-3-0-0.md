---
id: version-3.0.0-migration-guide-v3.0.0
title: Migrating to v3.0.0
original_id: migration-guide-v3.0.0
---

v3.0.0 adds full support for Apache Kafka 4.2.0 with KRaft-native architecture. For most users upgrading from 2.x.x, the migration is straightforward — there are no breaking changes to existing producer/consumer APIs. The only breaking change is the Node.js version requirement.

## Breaking Changes

### Dropped support for Node.js < 20

KafkaJS 3.0.0 requires **Node.js 20 or later**. If you are currently using Node.js 14, 16, or 18, you must upgrade before installing KafkaJS 3.0.0.

| KafkaJS Version | Node.js  | npm     | Kafka Broker Versions |
|-----------------|----------|---------|-----------------------|
| 3.0.0+          | >= 20.0  | >= 10.0 | 1.0.0 - 4.2.0        |
| 2.0.0 - 2.2.4   | >= 12.0  | >= 6.0  | 0.11.0 - 3.x         |

### Kafka version support expanded

v3.0.0 now supports Kafka brokers from 1.0.0 to 4.2.0. Support for Kafka 0.11.x has been dropped.

## What's New

### Kafka 4.2.0 and KRaft Mode

Apache Kafka 4.0 removed ZooKeeper entirely. All Kafka 4.x clusters run in **KRaft mode**. KafkaJS connects directly to brokers (not ZooKeeper), so no client code changes are required. The client works identically with KRaft and ZooKeeper-based clusters.

### Share Groups (KIP-932) — Kafka 4.0+

Share Groups provide queue-like consumption where multiple consumers process records from the same partitions concurrently with per-record acknowledgement. Full protocol support is included for:

- **ShareGroupHeartbeat** — Member lifecycle management
- **ShareGroupDescribe** — Inspect share group state and assignments
- **ShareFetch** — Fetch records with per-record acknowledgement tracking
- **ShareAcknowledge** — Accept, release, or reject individual records

```javascript
// Admin: describe share groups
const result = await admin.shareGroupDescribe({
  groupIds: ['my-share-group'],
  includeAuthorizedOperations: false,
})
```

### Dead Letter Queue (DLQ) Support

A new `deadLetterQueue` utility function for automatic message retry with DLQ fallback:

```javascript
const { deadLetterQueue } = require('kafkajs')

await consumer.run({
  eachMessage: deadLetterQueue({
    producer,
    topic: 'my-dlq-topic',
    maxRetries: 3,
    onFailed: (message, error) => {
      console.error('Message sent to DLQ:', message, error)
    },
    handler: async ({ topic, partition, message }) => {
      // your message processing logic
      await processMessage(message)
    },
  }),
})
```

### Kerberos / GSSAPI Authentication

v3.0.0 adds support for Kerberos (GSSAPI) SASL authentication. This requires the optional `kerberos` npm package:

```sh
npm install kerberos
```

```javascript
new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092'],
  ssl: true,
  sasl: {
    mechanism: 'gssapi',
    serviceName: 'kafka',
    principal: 'kafkajs@EXAMPLE.COM',
    keytab: '/path/to/keytab',
  },
})
```

### Consumer: Static Membership (KIP-345)

Static membership allows consumers to maintain their group assignment across restarts by providing a `groupInstanceId`. This reduces unnecessary rebalances when consumers restart.

```javascript
// Before (v2.x.x) - no static membership support
const consumer = kafka.consumer({ groupId: 'my-group' })

// After (v3.0.0) - optional groupInstanceId
const consumer = kafka.consumer({
  groupId: 'my-group',
  groupInstanceId: 'instance-1',  // Unique per consumer instance
})
```

When a consumer with a `groupInstanceId` disconnects, the broker waits for `session.timeout.ms` before triggering a rebalance. If the consumer reconnects with the same `groupInstanceId` within that window, it resumes its previous assignment without rebalancing.

No changes are required for existing consumers. This is an opt-in feature.

### KRaft Voter Management — Kafka 4.1+

Dynamic voter management for KRaft clusters:

- **AddRaftVoter** — Add a voter to the KRaft quorum
- **RemoveRaftVoter** — Remove a voter from the KRaft quorum
- **UpdateRaftVoter** — Update voter endpoints or supported versions

## New Admin Methods

Several new admin methods have been added in v3.0.0. These are all additive and do not affect existing code.

### `incrementalAlterConfigs`

A new method for incremental config changes, recommended over `alterConfigs` for Kafka 2.3+ brokers. Unlike `alterConfigs`, this only changes the specified config entries without replacing all configs.

```javascript
const { ConfigResourceTypes, ConfigOperationTypes } = require('kafkajs')

await admin.incrementalAlterConfigs({
  resources: [{
    type: ConfigResourceTypes.TOPIC,
    name: 'my-topic',
    configEntries: [
      { name: 'cleanup.policy', configOperation: ConfigOperationTypes.SET, value: 'compact' },
      { name: 'max.message.bytes', configOperation: ConfigOperationTypes.DELETE, value: '' },
    ],
  }],
})
```

### `electLeaders`

Trigger preferred or unclean leader election (requires Kafka 2.4+).

```javascript
await admin.electLeaders({
  electionType: 0, // 0 = PREFERRED, 1 = UNCLEAN
  topicPartitions: [{ topic: 'my-topic', partitions: [0, 1] }],
  timeout: 30000,
})
```

### `deleteOffsets`

Delete consumer group offsets for specific topic partitions. The consumer group must not have any active members.

```javascript
await admin.deleteOffsets({
  groupId: 'my-group',
  topic: 'my-topic',
  partitions: [{ partition: 0 }, { partition: 1 }],
})
```

### `describeLogDirs`

Returns information about log directories on all brokers.

```javascript
const logDirs = await admin.describeLogDirs({ topics: ['my-topic'] })
```

### `describeProducers`

Returns information about active producers for the specified topic partitions.

```javascript
const result = await admin.describeProducers({
  topics: [{ topic: 'my-topic', partitions: [0, 1] }],
})
```

### `describeTransactions` / `listTransactions`

Inspect and list active transactions.

```javascript
const result = await admin.describeTransactions({
  transactionalIds: ['my-txn-id-1'],
})

const list = await admin.listTransactions({
  stateFilters: ['Ongoing'],
  producerIdFilters: [],
})
```

### `alterPartitionReassignments` / `listPartitionReassignments`

Manage partition reassignments programmatically.

```javascript
await admin.alterPartitionReassignments({
  topics: [{
    topic: 'my-topic',
    partitionAssignment: [{ partition: 0, replicas: [0, 1, 2] }],
  }],
})

const reassignments = await admin.listPartitionReassignments({
  topics: [{ topic: 'my-topic', partitions: [0] }],
})
```

### `shareGroupDescribe` — Kafka 4.0+

Describes Share Groups (KIP-932), a new consumption model providing queue-like semantics with per-record acknowledgement.

```javascript
const result = await admin.shareGroupDescribe({
  groupIds: ['my-share-group'],
  includeAuthorizedOperations: false,
})
// result.groups[0]:
// {
//   groupId: 'my-share-group',
//   groupState: 'Stable',
//   groupEpoch: 1,
//   assignorName: 'uniform',
//   topics: [{ topicId, topicName, partitions: [...] }],
//   members: [{ memberId, rackId, memberEpoch, clientId, clientHost, ... }],
// }
```

## Protocol Version Upgrades

KafkaJS automatically negotiates protocol versions with the broker. v3.0.0 supports newer API versions with flexible encoding (KIP-482) for all major APIs. No manual configuration is needed.

Key protocol upgrades include:

| API              | v2.x Version | v3.0.0 Version | Notable Changes                            |
|------------------|--------------|----------------|--------------------------------------------|
| ApiVersions      | v2           | v3             | Client software name/version               |
| Produce          | v7           | v9             | Flexible versions, RecordErrors             |
| Fetch            | v11          | v12            | Flexible versions, LastFetchedEpoch         |
| Metadata         | v6           | v7             | LeaderEpoch in partition metadata           |
| ListOffsets      | v3           | v4             | CurrentLeaderEpoch per partition            |
| JoinGroup        | v5           | v6             | Flexible versions                           |
| OffsetCommit     | v5           | v7             | CommittedLeaderEpoch, GroupInstanceId        |
| OffsetFetch      | v4           | v5             | CommittedLeaderEpoch                        |
| CreateTopics     | v3           | v5             | Flexible versions                           |
| DescribeConfigs  | v2           | v4             | includeDocumentation                        |
| DescribeGroups   | v2           | v5             | authorizedOperations                        |
| ListGroups       | v2           | v4             | statesFilter                                |
| InitProducerId   | v1           | v3             | ProducerId/ProducerEpoch                    |

## New Error Codes

v3.0.0 recognizes all Kafka error codes through 4.2.0 (codes 89–127), including:

- **Kafka 2.6+**: `THROTTLING_QUOTA_EXCEEDED`, `PRODUCER_FENCED`, `RESOURCE_NOT_FOUND`, `DUPLICATE_RESOURCE`
- **Kafka 3.0+**: `UNKNOWN_TOPIC_ID`, `INCONSISTENT_TOPIC_ID`, `TRANSACTIONAL_ID_NOT_FOUND`
- **Kafka 3.6+**: `FENCED_MEMBER_EPOCH`, `UNRELEASED_INSTANCE_ID`, `UNSUPPORTED_ASSIGNOR`
- **Kafka 4.0+**: `SHARE_SESSION_NOT_FOUND`, `INVALID_SHARE_SESSION_EPOCH`, `FENCED_STATE_EPOCH`
- **Kafka 4.1+**: `INVALID_VOTER_KEY`, `DUPLICATE_VOTER`, `VOTER_NOT_FOUND`
- **Kafka 4.2+**: `INVALID_REGULAR_EXPRESSION`, `UNKNOWN_SHARE_GROUP_STATE`

## TypeScript: New types and exports

v3.0.0 adds several new TypeScript exports:

**New Enums:**

```typescript
import {
  ConfigOperationTypes,  // SET, DELETE, APPEND, SUBTRACT
  ShareAcknowledgeType,  // ACCEPT, RELEASE, REJECT, GAP
} from 'kafkajs'
```

**Updated Interfaces:**

- `ConsumerConfig` now includes optional `groupInstanceId: string`
- `PartitionMetadata` now includes optional `leaderEpoch: number` (Kafka 2.4+)

**New Interfaces:** Types for all new admin methods including `IncrementalAlterConfigsResource`, `ShareGroupDescribeRequest`, `ElectLeadersRequest`, `DeleteOffsetsRequest`, `DescribeLogDirsRequest`, `DescribeProducersRequest`, `DescribeTransactionsRequest`, `ListTransactionsRequest`, and their corresponding response types.

## Backward Compatibility

All existing producer, consumer, and admin APIs work without changes:

- All `producer.send()` and `producer.sendBatch()` calls are unchanged
- All `consumer.run()` with `eachMessage` / `eachBatch` handlers are unchanged
- All existing admin methods (`createTopics`, `fetchOffsets`, `describeConfigs`, etc.) are unchanged
- Partition assignment, offset management, and pause/resume behavior are unchanged

## Migration Steps

1. **Upgrade Node.js** to version 20 or later.
2. **Update KafkaJS**:
   ```sh
   npm install kafkajs@3
   # or
   yarn add kafkajs@3
   ```
3. **Test your application** — Existing producer, consumer, and admin code should work without changes.
4. **Adopt new features** (optional):
   - Use `groupInstanceId` for static membership to reduce rebalances during deployments.
   - Use `incrementalAlterConfigs` instead of `alterConfigs` on Kafka 2.3+ brokers.
   - Add `deadLetterQueue` for error handling in consumers.
   - Configure Kerberos authentication with `mechanism: 'gssapi'` if needed.
   - Explore Share Groups if running Kafka 4.0+.
