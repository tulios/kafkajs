# Upgrading KafkaJS for Apache Kafka 4.2.0

This guide covers upgrading KafkaJS to work with Apache Kafka 4.2.0, released February 17, 2026. It includes changes across the Kafka ecosystem from Kafka 3.x through 4.2.0.

## Table of Contents

- [Quick Start](#quick-start)
- [Major Changes in Kafka 4.x](#major-changes-in-kafka-4x)
- [Kafka Broker & Controller Changes](#kafka-broker--controller-changes)
- [Producer Changes](#producer-changes)
- [Consumer Changes](#consumer-changes)
- [Share Groups / Kafka Queues (KIP-932)](#share-groups--kafka-queues-kip-932)
- [Admin Client Changes](#admin-client-changes)
- [Kafka Connect Changes](#kafka-connect-changes)
- [Kafka Streams Changes](#kafka-streams-changes)
- [New Error Codes](#new-error-codes)
- [New API Keys](#new-api-keys)
- [Configuration Changes](#configuration-changes)
- [Deprecations & Removals](#deprecations--removals)
- [Docker Compose (KRaft Mode)](#docker-compose-kraft-mode)
- [Running Tests](#running-tests)
- [Examples](#examples)
- [Migration Checklist](#migration-checklist)
- [References](#references)

---

## Quick Start

KafkaJS uses automatic protocol version negotiation. When connecting to a Kafka 4.2.0 broker, the client will automatically negotiate the highest mutually supported API version for each request type. No manual version configuration is required.

```javascript
const { Kafka } = require('kafkajs')

// KafkaJS automatically negotiates protocol versions with Kafka 4.2.0 brokers
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
})
```

**Key point:** KafkaJS 3.x continues to work with Kafka 4.2.0 brokers through backward-compatible protocol negotiation. The broker will use the highest API version that both the client and server support.

---

## Major Changes in Kafka 4.x

### ZooKeeper Removal (Kafka 4.0)

Apache Kafka 4.0 **completely removed ZooKeeper** support. All clusters must run in **KRaft (Kafka Raft)** mode. If you are upgrading from a ZooKeeper-based cluster:

1. Migrate your cluster to KRaft mode **before** upgrading to Kafka 4.0+
2. KafkaJS client connections are unaffected - clients connect to brokers, not ZooKeeper
3. No KafkaJS code changes are needed for the ZooKeeper-to-KRaft migration

### KRaft-Native Architecture

Kafka 4.x is KRaft-native with improved:
- **Controller observability** - New `AvgIdleRatio` metrics for controller and MetadataLoader (KIP-1190, KIP-1229)
- **Combined mode metrics** - Fixed `RequestHandlerAvgIdlePercent` metric in KRaft combined mode (KIP-1207)
- **Feature level metrics** - New generic metrics displaying finalized, minimum, and maximum supported feature levels (KIP-1180)
- **Metric naming convention** - Corrected to `kafka.COMPONENT` convention, deprecating `org.apache.kafka.COMPONENT` format (KIP-1100)

---

## Kafka Broker & Controller Changes

### Kafka 4.2.0 Broker Improvements

| KIP | Description |
|-----|-------------|
| KIP-1186 | `AddRaftVoterRequest` adds boolean `AckWhenCommitted` flag for immediate response after local writes |
| KIP-1197 | `TopicBasedRemoteLogMetadataManager` introduces `BrokerReadyCallback` interface, delaying initialization until broker readiness |
| KIP-1179 | New dynamic configuration `remote.log.manager.follower.thread.pool.size` |
| KIP-1161 | LIST-type configuration validation standardized - rejects null/empty values, ignores duplicates |
| KIP-1205 | `RecordHeader` thread-safety improved via double-checked locking with volatile fields |
| KIP-1228 | `WriteTxnMarkersRequest` adds `TransactionVersion` field for stricter epoch validation |

### Controller Changes

- **KIP-1190**: `AvgIdleRatio` metric for `ControllerEventManager` thread idle time
- **KIP-1229**: `MetadataLoader` now includes `AvgIdleRatio` metric
- **KIP-1207**: Separate broker and controller `RequestHandlerAvgIdlePercent` metrics in combined mode
- **KIP-1180**: Generic feature level metrics for all feature flags

---

## Producer Changes

### Idempotent Producer Improvements

KafkaJS already supports idempotent producers (`idempotent: true`). Kafka 4.2.0 adds:

- **KIP-1228**: Stricter epoch validation in `WriteTxnMarkersRequest` with a new `TransactionVersion` field
- **KIP-1175**: Typo fix - `PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG` is deprecated; use `PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG`

### Producer Configuration

```javascript
const producer = kafka.producer({
  idempotent: true,          // Recommended for exactly-once semantics
  transactionalId: 'my-txn', // Required for transactions
  maxInFlightRequests: 5,    // Max 5 for idempotent producers
})
```

### Thread-Safe RecordHeader (KIP-1205)

Record headers are now thread-safe on the broker side. KafkaJS headers continue to work as before:

```javascript
await producer.send({
  topic: 'my-topic',
  messages: [{
    value: 'Hello',
    headers: {
      'correlation-id': '12345',
      'source': 'my-service',
    },
  }],
})
```

---

## Consumer Changes

### Consumer Group Protocol

Kafka 4.2.0 continues to support the classic consumer group protocol that KafkaJS uses. The new server-side consumer group protocol (KIP-848) is available but KafkaJS currently uses the classic protocol, which remains fully supported.

### Consumer Configuration

```javascript
const consumer = kafka.consumer({
  groupId: 'my-group',
  sessionTimeout: 30000,
  rebalanceTimeout: 60000,
  heartbeatInterval: 3000,
  maxBytesPerPartition: 1048576,  // 1MB
  readUncommitted: false,         // READ_COMMITTED isolation level
  rackId: 'rack-1',              // Rack-aware consumption
})
```

### Rack-Aware Consumption

KafkaJS already supports `rackId` for rack-aware fetch. Kafka 4.2.0 adds:
- **KIP-1227**: Rack ID exposed in `MemberDescription` and `ShareMemberDescription` via the Admin API

---

## Share Groups / Kafka Queues (KIP-932)

**Kafka 4.2.0 makes Share Groups production-ready.** Share Groups introduce a queue-like consumption model alongside traditional consumer groups.

### What Are Share Groups?

Share Groups (also called "Kafka Queues") allow multiple consumers to cooperatively consume records from the same partitions without partition assignment. Key features:

- **Per-record acknowledgement** - Each record is individually acknowledged
- **Delivery attempt tracking** - Tracks how many times a record has been delivered
- **Cooperative consumption** - Multiple consumers process records from the same partitions concurrently

### New Share Group Features in 4.2.0

| KIP | Feature |
|-----|---------|
| KIP-932 | Share Groups core functionality (production-ready) |
| KIP-1222 | RENEW acknowledgement type for extending acquisition lock timeouts |
| KIP-1224 | Adaptive batching for share coordinators |
| KIP-1226 | Share partition lag metrics |
| KIP-1206 | `ShareAcquireMode` - "batch_optimized" (soft) and "record_limit" (strict) enforcement |

### Share Group API Keys

KafkaJS v3.0.0 includes full protocol implementations for all Share Group APIs:

- `ShareGroupHeartbeat` (API key 76) - Member heartbeats with topic/partition assignment
- `ShareGroupDescribe` (API key 77) - Describe share group state, members, and assignments
- `ShareFetch` (API key 78) - Fetch records in share group mode with per-record acknowledgement
- `ShareAcknowledge` (API key 79) - Acknowledge, release, or reject individual records
- `ReadShareGroupStateSummary` (API key 83) - Read share group partition state summary

These APIs use the flexible version wire format (KIP-482) and UUID-based topic identifiers.

### Share Group Admin Methods

```javascript
const admin = kafka.admin()
await admin.connect()

// Describe share groups
const result = await admin.shareGroupDescribe({
  groupIds: ['my-share-group'],
  includeAuthorizedOperations: false,
})
// result.groups[0].groupState, .members, .topics, etc.

await admin.disconnect()
```

---

## Admin Client Changes

### Current Admin Client Capabilities

KafkaJS admin client supports:
- Topic management (create, delete, list, describe)
- ACL management (create, delete, describe)
- Configuration management (describe, alter)
- Consumer group management (list, describe, delete)
- Partition management (create partitions, reassign)

### New in Kafka 4.2.0

- **KIP-1227**: Rack ID now available in `MemberDescription` for consumer and share group members
- **KIP-1160**: `describeFeatures` accepts optional `--node-id` for querying specific brokers
- **KIP-1147**: Standardized CLI arguments (`--bootstrap-server`, `--command-config`) across all tools

### Admin API Usage

```javascript
const admin = kafka.admin()
await admin.connect()

// List topics
const topics = await admin.listTopics()

// Describe cluster
const cluster = await admin.describeCluster()

// List consumer groups
const groups = await admin.listGroups()

// Describe consumer group (now includes rack ID in Kafka 4.2.0)
const groupDescription = await admin.describeGroups(['my-group'])

await admin.disconnect()
```

---

## Kafka Connect Changes

### Kafka 4.2.0 Connect Improvements

| KIP | Description |
|-----|-------------|
| KIP-1054 | `JsonConverter` accepts optional `schema.content` config for external schemas, reducing message sizes |
| KIP-1188 | New "Allowlist" connector client config override policy for explicit security control |
| KIP-1120 | AppInfo metrics gain `client-id` tag for Kafka Worker and MirrorMaker 2 |

### External Schema Support (KIP-1054)

Connect's `JsonConverter` now supports specifying schemas externally rather than embedding them in every JSON message. This significantly reduces message sizes in schema-heavy workloads.

### Security: Allowlist Policy (KIP-1188)

The new `AllowlistConnectorClientConfigOverridePolicy` explicitly specifies which configurations connectors can override, replacing the deprecated `PrincipalConnectorClientConfigOverridePolicy`.

---

## Kafka Streams Changes

### Kafka 4.2.0 Streams Improvements

| KIP | Description |
|-----|-------------|
| KIP-1071 | Server-side Streams rebalance protocol reaches GA (limited feature set) |
| KIP-1034 | Dead Letter Queue (DLQ) support in exception handlers |
| KIP-1146 | Anchored wall-clock punctuation with optional `startTime` parameter |
| KIP-1153 | New `CloseOptions` with `GroupMembershipOperation` enum for shutdown control |
| KIP-1216 | Thread-level latency metrics for rebalance callbacks |
| KIP-1221 | `application-id` tag added to `client-state` JMX metric |
| KIP-1230 | Optional `allow.os.group.write.access` for state directory permissions |

### Server-Side Rebalance Protocol (KIP-1071)

Kafka Streams now has a GA server-side group management protocol, enabling broker-side task assignment. This improves rebalance stability and reduces the number of rebalances.

### Dead Letter Queue Support (KIP-1034)

Exception handlers now support dead letter queues through a new `Response` class with DLQ records and raw source record bytes in error contexts. This enables cleaner recovery from malformed input and transient failures.

### Anchored Punctuation (KIP-1146)

Wall-clock punctuation now supports an optional `startTime` parameter for deterministic scheduling:

```
// Example: Punctuation triggers exactly at the start of every hour
// rather than relative to when the punctuation was registered
```

---

## Protocol Version Upgrades

KafkaJS now implements newer protocol versions for all major APIs, enabling flexible versions (compact encoding), leader epochs, and other Kafka 4.x features.

### Core APIs

| API | Previous Max | New Max | Key Changes |
|-----|-------------|---------|-------------|
| ApiVersions | v2 | v3 | Flexible versions, sends client software name/version |
| Produce | v7 | v9 | v8: RecordErrors per partition. v9: Flexible versions (compact encoding) |
| Fetch | v11 | v12 | Flexible versions, LastFetchedEpoch for epoch-based truncation |
| Metadata | v6 | v7 | Adds LeaderEpoch to partition metadata |
| ListOffsets | v3 | v4 | Adds CurrentLeaderEpoch per partition |

### Consumer Group APIs

| API | Previous Max | New Max | Key Changes |
|-----|-------------|---------|-------------|
| JoinGroup | v5 | v6 | Flexible versions |
| Heartbeat | v3 | v4 | Flexible versions |
| LeaveGroup | v3 | v4 | Flexible versions |
| SyncGroup | v3 | v4 | Flexible versions |
| OffsetCommit | v5 | v7 | v6: Flexible versions. v7: CommittedLeaderEpoch, GroupInstanceId |
| OffsetFetch | v4 | v5 | Flexible versions, CommittedLeaderEpoch |
| FindCoordinator | v2 | v3 | Flexible versions |

### Producer/Transaction APIs

| API | Previous Max | New Max | Key Changes |
|-----|-------------|---------|-------------|
| InitProducerId | v1 | v3 | v2: Flexible versions. v3: ProducerId/ProducerEpoch for epoch bumping |

### Admin APIs

| API | Previous Max | New Max | Key Changes |
|-----|-------------|---------|-------------|
| CreateTopics | v3 | v5 | Flexible versions |
| DeleteTopics | v1 | v4 | Flexible versions |
| DescribeConfigs | v2 | v4 | Flexible versions, includeDocumentation option |
| AlterConfigs | v1 | v2 | Flexible versions |
| **IncrementalAlterConfigs** | *not implemented* | v1 | **New API!** Incremental config changes (SET/DELETE/APPEND/SUBTRACT) |
| DescribeGroups | v2 | v5 | Flexible versions, authorizedOperations |
| ListGroups | v2 | v4 | Flexible versions, statesFilter |
| DeleteGroups | v1 | v2 | Flexible versions |
| SaslAuthenticate | v1 | v2 | Flexible versions |

### What Are Flexible Versions?

Flexible versions (KIP-482) use a more compact wire format:
- Strings use UVarInt length prefix instead of Int16 (saves bytes)
- Arrays use UVarInt length prefix instead of Int32
- Tagged fields allow adding optional fields without breaking compatibility

KafkaJS automatically negotiates the best version with the broker. No configuration needed.

### IncrementalAlterConfigs (New API)

The `incrementalAlterConfigs` admin method allows changing individual config entries without replacing all configs:

```javascript
const { ConfigResourceTypes, ConfigOperationTypes } = require('kafkajs')

const admin = kafka.admin()
await admin.connect()

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

await admin.disconnect()
```

Config operations: `0 = SET`, `1 = DELETE`, `2 = APPEND`, `3 = SUBTRACT`

### TypeScript Updates

New types added:
- `ConfigOperationTypes` enum (SET, DELETE, APPEND, SUBTRACT)
- `IncrementalAlterConfigsResource` and `IncrementalAlterConfigEntry` interfaces
- `IncrementalAlterConfigsResponse` interface
- `groupInstanceId` on `ConsumerConfig` and `MemberDescription`
- `leaderEpoch` on `PartitionMetadata`

---

## New Error Codes

KafkaJS now includes error codes through Kafka 4.2.0. Key additions:

### Kafka 2.6+ Error Codes
| Code | Type | Retriable | Description |
|------|------|-----------|-------------|
| 89 | `THROTTLING_QUOTA_EXCEEDED` | Yes | Throttling quota exceeded |
| 90 | `PRODUCER_FENCED` | No | Newer producer with same transactionalId fences current one |
| 91 | `RESOURCE_NOT_FOUND` | No | Request referred to a non-existent resource |
| 92 | `DUPLICATE_RESOURCE` | No | Request referred to the same resource twice |

### Kafka 3.0+ Error Codes
| Code | Type | Retriable | Description |
|------|------|-----------|-------------|
| 100 | `UNKNOWN_TOPIC_ID` | Yes | Server does not host this topic ID |
| 103 | `INCONSISTENT_TOPIC_ID` | Yes | Log topic ID mismatch |
| 105 | `TRANSACTIONAL_ID_NOT_FOUND` | No | TransactionalId not found |

### Kafka 3.6+ Error Codes (Consumer Group Protocol)
| Code | Type | Retriable | Description |
|------|------|-----------|-------------|
| 110 | `FENCED_MEMBER_EPOCH` | No | Member epoch fenced by group coordinator |
| 111 | `UNRELEASED_INSTANCE_ID` | No | Instance ID still used by another member |
| 112 | `UNSUPPORTED_ASSIGNOR` | No | Assignor not supported by the consumer group |

### Kafka 4.0+ Error Codes (Share Groups)
| Code | Type | Retriable | Description |
|------|------|-----------|-------------|
| 120 | `SHARE_SESSION_NOT_FOUND` | Yes | Share session was not found |
| 121 | `INVALID_SHARE_SESSION_EPOCH` | Yes | Share session epoch is invalid |
| 122 | `FENCED_STATE_EPOCH` | Yes | Share-group state epoch mismatch |

### Kafka 4.2+ Error Codes
| Code | Type | Retriable | Description |
|------|------|-----------|-------------|
| 126 | `INVALID_REGULAR_EXPRESSION` | No | Regular expression is invalid |
| 127 | `UNKNOWN_SHARE_GROUP_STATE` | No | Share group state is unknown |

---

## New API Keys

KafkaJS now registers API keys through Kafka 4.2.0 for proper protocol version negotiation:

### KRaft API Keys (Kafka 3.0+)
| API Key | Name | Description |
|---------|------|-------------|
| 47 | `OffsetDelete` | Delete consumer group offsets |
| 52-54 | `Vote`, `BeginQuorumEpoch`, `EndQuorumEpoch` | KRaft consensus |
| 55 | `DescribeQuorum` | Describe KRaft quorum |
| 57 | `UpdateFeatures` | Update broker feature flags |
| 60 | `DescribeCluster` | Describe cluster metadata |
| 61 | `DescribeProducers` | Describe active producers |
| 65-66 | `DescribeTransactions`, `ListTransactions` | Transaction inspection |

### Consumer Group Protocol API Keys (Kafka 3.3+)
| API Key | Name | Description |
|---------|------|-------------|
| 68 | `ConsumerGroupHeartbeat` | New consumer group heartbeat |
| 69 | `ConsumerGroupDescribe` | Describe new-protocol consumer groups |
| 71-72 | `GetTelemetrySubscriptions`, `PushTelemetry` | Client telemetry |
| 75 | `DescribeTopicPartitions` | Paginated topic partition descriptions |

### Share Group API Keys (Kafka 4.0+, KIP-932)
| API Key | Name | Description |
|---------|------|-------------|
| 76 | `ShareGroupHeartbeat` | Share group member heartbeat |
| 77 | `ShareGroupDescribe` | Describe share groups |
| 78 | `ShareFetch` | Fetch records in share group mode |
| 79 | `ShareAcknowledge` | Acknowledge/release share group records |

### Kafka 4.1-4.2 API Keys
| API Key | Name | Description |
|---------|------|-------------|
| 80-82 | `AddRaftVoter`, `RemoveRaftVoter`, `UpdateRaftVoter` | Dynamic KRaft voter management |
| 83 | `ReadShareGroupStateSummary` | Read share group state summary |

---

## Configuration Changes

### New Broker Configurations in Kafka 4.2.0

| Configuration | Description |
|---------------|-------------|
| `remote.log.manager.follower.thread.pool.size` | Dynamic configuration for remote log manager thread pool (KIP-1179) |
| `controller.quorum.auto.join.enable` | Allows KRaft controllers to auto-join voter set (default: false) |
| `connector.client.config.override.allowlist` | Allowlist for connector client config overrides (KIP-1188) |

### Deprecated Configurations

| Deprecated | Replacement |
|------------|-------------|
| `org.apache.kafka.disallowed.login.modules` | `org.apache.kafka.allowed.login.modules` |
| `remote.log.manager.thread.pool.size` | `remote.log.manager.follower.thread.pool.size` |
| `--max-partition-memory-bytes` (console producer) | `--batch-size` |

---

## Deprecations & Removals

### Deprecated in Kafka 4.2.0 (Removal in Kafka 5.0)

- **KIP-1136**: `ConsumerGroupMetadata` constructors deprecated
- **KIP-1193**: MX4j support deprecated
- **KIP-1195**: `BrokerNotFoundException` deprecated
- **KIP-1100**: `org.apache.kafka.COMPONENT` metric naming deprecated (use `kafka.COMPONENT`)
- **PrincipalConnectorClientConfigOverridePolicy** deprecated (use `AllowlistConnectorClientConfigOverridePolicy`)

### Removed in Kafka 4.0

- **ZooKeeper**: Completely removed. All clusters must use KRaft mode
- **Old consumer group protocol**: Legacy assignment protocols removed from server defaults

---

## Docker Compose (KRaft Mode)

Kafka 4.2 runs exclusively in KRaft mode (no ZooKeeper). A new Docker Compose file is provided:

```bash
# Start a 3-broker KRaft cluster + dedicated controller
docker-compose -f docker-compose.4_2.yml up -d
```

### Architecture

The `docker-compose.4_2.yml` file uses the official `apache/kafka:4.2.0` image with:

- **1 Controller node** (`controller`) - Handles KRaft metadata consensus
- **3 Broker nodes** (`kafka1`, `kafka2`, `kafka3`) - Handle client traffic
- **No ZooKeeper** - Metadata managed via KRaft consensus protocol

### Key Differences from ZooKeeper-based Setup

| Feature | ZooKeeper (2.x) | KRaft (4.2) |
|---------|-----------------|-------------|
| Metadata | ZooKeeper ensemble | KRaft controller quorum |
| Image | `confluentinc/cp-kafka` | `apache/kafka:4.2.0` |
| Controller | Elected from brokers | Dedicated controller process |
| Configuration | `KAFKA_ZOOKEEPER_CONNECT` | `KAFKA_CONTROLLER_QUORUM_VOTERS` |
| Authorizer | `kafka.security.auth.SimpleAclAuthorizer` | `org.apache.kafka.metadata.authorizer.StandardAuthorizer` |
| SCRAM setup | `kafka-configs --zookeeper` | `kafka-configs --bootstrap-server` |
| Topic management | `kafka-topics --zookeeper` | `kafka-topics --bootstrap-server` |

### Listeners Configuration

Each broker exposes 3 listener types (same ports as ZooKeeper-based setup):

| Broker | Plaintext | SSL | SASL_SSL |
|--------|-----------|-----|----------|
| kafka1 | 9092 | 9093 | 9094 |
| kafka2 | 9095 | 9096 | 9097 |
| kafka3 | 9098 | 9099 | 9100 |

### Starting the Cluster

```bash
# Start KRaft cluster
docker-compose -f docker-compose.4_2.yml up -d

# Verify all nodes are running
docker-compose -f docker-compose.4_2.yml ps

# Check broker logs
docker-compose -f docker-compose.4_2.yml logs kafka1

# Create SCRAM credentials (KRaft mode)
./scripts/createScramCredentialsKRaft.sh

# Tear down
docker-compose -f docker-compose.4_2.yml down --remove-orphans
```

---

## Running Tests

### Test Scripts

| Script | Description |
|--------|-------------|
| `npm run test:kraft` | Full test suite against Kafka 4.2 KRaft cluster |
| `npm run test:kraft:local` | Local tests with KAFKA_VERSION=4.2 |
| `npm run test:kraft:broker:ci` | Broker tests against KRaft cluster |
| `npm run test:kraft:admin:ci` | Admin tests against KRaft cluster |
| `npm run test:kraft:producer:ci` | Producer tests against KRaft cluster |
| `npm run test:kraft:consumer:ci` | Consumer tests against KRaft cluster |

### Running Individual Test Groups

```bash
# Run all tests with KRaft
KAFKA_VERSION=4.2 COMPOSE_FILE=docker-compose.4_2.yml ./scripts/testWithKafka.sh 'npx jest --forceExit'

# Run only broker tests
KAFKA_VERSION=4.2 COMPOSE_FILE=docker-compose.4_2.yml ./scripts/testWithKafka.sh 'npx jest --forceExit --testPathPattern src/broker/.*'

# Run unit tests (no Docker needed)
KAFKA_VERSION=4.2 npx jest src/protocol/kafka42.spec.js
```

### Version-Conditional Tests

Use the test helpers to write tests that only run on Kafka 4.x:

```javascript
const { testIfKafkaAtLeast_4_0_0, testIfKafkaAtLeast_4_2_0 } = require('testHelpers')

// Only runs when KAFKA_VERSION >= 4.0
testIfKafkaAtLeast_4_0_0('connects to KRaft broker', async () => {
  // ...
})

// Only runs when KAFKA_VERSION >= 4.2
testIfKafkaAtLeast_4_2_0('uses Share Group features', async () => {
  // ...
})
```

---

## Examples

KRaft-native examples are provided in the `examples/` directory:

| File | Description |
|------|-------------|
| `examples/kraft-producer.js` | Producer with idempotent mode, headers, GZIP compression |
| `examples/kraft-consumer.js` | Consumer with rack-aware configuration |
| `examples/kraft-admin.js` | Admin client: create/delete topics, describe cluster, manage configs |

### Running Examples

```bash
# Start the KRaft cluster first
docker-compose -f docker-compose.4_2.yml up -d

# Run producer
node examples/kraft-producer.js

# Run consumer (in another terminal)
node examples/kraft-consumer.js

# Run admin operations
node examples/kraft-admin.js

# Custom broker list
KAFKA_BROKERS=broker1:9092,broker2:9092 node examples/kraft-producer.js
```

---

## Migration Checklist

### Before Upgrading to Kafka 4.2.0

- [ ] **Verify KRaft mode**: Ensure your Kafka cluster is running in KRaft mode (required since Kafka 4.0)
- [ ] **Update KafkaJS**: Upgrade to the latest KafkaJS version with Kafka 4.2.0 error code and API key support
- [ ] **Review deprecated configs**: Check for deprecated configuration properties (see [Configuration Changes](#configuration-changes))
- [ ] **Test connection**: Verify KafkaJS can connect and negotiate protocol versions with the new brokers
- [ ] **Monitor metrics**: Update monitoring to use `kafka.COMPONENT` metric naming convention

### After Upgrading

- [ ] **Verify protocol negotiation**: Check logs for successful API version negotiation
- [ ] **Test producer/consumer**: Run integration tests with the new broker version
- [ ] **Review error handling**: New error codes (89-127) may surface - ensure your error handling is robust
- [ ] **Plan for Share Groups**: If using queue-style workloads, plan migration to Share Groups for future KafkaJS releases

### KafkaJS-Specific Notes

1. **Automatic version negotiation**: KafkaJS automatically negotiates the best protocol version. No manual version configuration needed.
2. **Backward compatibility**: KafkaJS 3.x works with Kafka 4.2.0 through protocol negotiation. The broker will use mutually supported versions.
3. **New API support**: Share Group APIs (keys 76-79, 83) and KRaft voter management APIs (keys 80-82) are fully implemented with request/response encoding.
4. **Error codes**: All Kafka 4.2.0 error codes are now recognized. Previously unknown error codes from newer brokers would result in `KAFKAJS_UNKNOWN_ERROR_CODE`.

---

## Dependency Upgrades (v3.0.0)

KafkaJS v3.0.0 bumps the package version from 2.2.4 to 3.0.0. The minimum Node.js version is now **20.0.0** (npm >= 10.0.0).

### Updated devDependencies

The following devDependencies are updated within their semver ranges. No breaking changes are expected:

| Package | From | To | Notes |
|---------|------|----|-------|
| All packages | semver range | latest within range | `npm update` applied |

### Major Upgrades NOT Applied

The following packages have new major versions available but are **not** upgraded to avoid breaking changes in the test/build toolchain:

| Package | Current | Latest | Why Not Upgraded |
|---------|---------|--------|------------------|
| jest | ^25.1.0 | 30.x | Major rewrite; requires test config migration |
| eslint | ^6.8.0 | 10.x | New flat config format; requires full eslint config rewrite |
| typescript | ^3.8.3 | 5.x | Used only for type checking; current version works |
| prettier | ^1.18.2 | 3.x | Formatting changes would touch all files |
| husky | ^3.0.1 | 9.x | New setup pattern; requires migration |
| lint-staged | ^9.2.0 | 16.x | Requires husky migration first |
| uuid | ^3.3.2 | 13.x | ESM-only in v9+; would need code changes |
| execa | ^2.0.3 | 9.x | ESM-only in v5+; used in tests |
| glob | ^7.1.4 | 13.x | API changes in v9+ |

These upgrades are deferred to a future major release (v3.0.0) where breaking dev toolchain changes can be bundled together.

### What's New in v3.0.0

- **Kafka 4.2.0 support**: Full protocol negotiation with Kafka 4.2.0 brokers
- **Protocol version upgrades**: Newer API versions with flexible encoding (see [Protocol Version Upgrades](#protocol-version-upgrades))
- **Share Groups (KIP-932)**: Full protocol implementation for ShareGroupHeartbeat, ShareGroupDescribe, ShareFetch, ShareAcknowledge, and ReadShareGroupStateSummary
- **KRaft voter management**: Full protocol implementation for AddRaftVoter, RemoveRaftVoter, and UpdateRaftVoter
- **UUID wire type support**: Encoder/decoder support for 128-bit UUID fields used by Share Groups and KRaft APIs
- **IncrementalAlterConfigs API**: New admin method for incremental config changes
- **KRaft mode**: Docker Compose and test support for Kafka 4.2 KRaft clusters
- **New error codes**: All error codes through Kafka 4.2.0 (codes 89-127)
- **All 84 API keys implemented**: Every Kafka API key through 4.2.0 (keys 0-83) has a protocol handler
- **TypeScript updates**: New types for `ConfigOperationTypes`, `IncrementalAlterConfigs*`, `shareGroupDescribe`, `leaderEpoch`, `groupInstanceId`
- **Leader epochs**: Partition metadata now includes `leaderEpoch` when using Metadata v7+

---

## References

- [Apache Kafka 4.2.0 Release Announcement](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/)
- [Apache Kafka 4.2.0 Upgrade Guide](https://kafka.apache.org/42/getting-started/upgrade/)
- [Confluent Blog: Apache Kafka 4.2 Release](https://www.confluent.io/blog/apache-kafka-4-2-release/)
- [KIP-932: Queues for Kafka (Share Groups)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932)
- [KIP-1071: Server-side Streams Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1071)
- [KafkaJS Migration Guide v2.0.0](./docs/MigrationGuide-2-0-0.md)
- [KafkaJS Documentation](https://kafka.js.org)
