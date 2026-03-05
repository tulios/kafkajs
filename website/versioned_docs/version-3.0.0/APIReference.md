---
id: version-3.0.0-api-reference
title: API Reference & Types
original_id: api-reference
---

# API Reference & Types

This page documents all exported types, enums, interfaces, and utility functions available in KafkaJS v3.0.0.

```javascript
const {
  Kafka,
  CompressionTypes,
  CompressionCodecs,
  Partitioners,
  logLevel,
  AssignerProtocol,
  PartitionAssigners,
  // Enums
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
  ConfigResourceTypes,
  ConfigOperationTypes,
  ConfigSource,
  ElectionType,
  ShareAcknowledgeType,
  // Error classes
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSProtocolError,
  // Utilities
  isRebalancing,
  isKafkaJSError,
  deadLetterQueue,
} = require('kafkajs')
```

---

## Enums

### CompressionTypes

```typescript
enum CompressionTypes {
  None = 0,
  GZIP = 1,
  Snappy = 2,
  LZ4 = 3,
  ZSTD = 4,
}
```

### logLevel

```typescript
enum logLevel {
  NOTHING = 0,
  ERROR = 1,
  WARN = 2,
  INFO = 4,
  DEBUG = 5,
}
```

### AclResourceTypes

```typescript
enum AclResourceTypes {
  UNKNOWN = 0,
  ANY = 1,
  TOPIC = 2,
  GROUP = 3,
  CLUSTER = 4,
  TRANSACTIONAL_ID = 5,
  DELEGATION_TOKEN = 6,
}
```

### AclOperationTypes

```typescript
enum AclOperationTypes {
  UNKNOWN = 0,
  ANY = 1,
  ALL = 2,
  READ = 3,
  WRITE = 4,
  CREATE = 5,
  DELETE = 6,
  ALTER = 7,
  DESCRIBE = 8,
  CLUSTER_ACTION = 9,
  DESCRIBE_CONFIGS = 10,
  ALTER_CONFIGS = 11,
  IDEMPOTENT_WRITE = 12,
}
```

### AclPermissionTypes

```typescript
enum AclPermissionTypes {
  UNKNOWN = 0,
  ANY = 1,
  DENY = 2,
  ALLOW = 3,
}
```

### ResourcePatternTypes

```typescript
enum ResourcePatternTypes {
  UNKNOWN = 0,
  ANY = 1,
  MATCH = 2,
  LITERAL = 3,
  PREFIXED = 4,
}
```

### ConfigResourceTypes

```typescript
enum ConfigResourceTypes {
  UNKNOWN = 0,
  TOPIC = 2,
  BROKER = 4,
  BROKER_LOGGER = 8,
}
```

### ConfigOperationTypes

Used with `incrementalAlterConfigs`.

```typescript
enum ConfigOperationTypes {
  SET = 0,
  DELETE = 1,
  APPEND = 2,
  SUBTRACT = 3,
}
```

### ConfigSource

Indicates where a configuration value originates.

```typescript
enum ConfigSource {
  UNKNOWN = 0,
  TOPIC_CONFIG = 1,
  DYNAMIC_BROKER_CONFIG = 2,
  DYNAMIC_DEFAULT_BROKER_CONFIG = 3,
  STATIC_BROKER_CONFIG = 4,
  DEFAULT_CONFIG = 5,
  DYNAMIC_BROKER_LOGGER_CONFIG = 6,
}
```

### ElectionType

Used with `admin.electLeaders()`.

```typescript
enum ElectionType {
  PREFERRED = 0,
  UNCLEAN = 1,
}
```

### ShareAcknowledgeType <small>(Kafka 4.0+)</small>

Used with Share Groups (KIP-932) for per-record acknowledgement.

```typescript
enum ShareAcknowledgeType {
  ACCEPT = 1,
  RELEASE = 2,
  REJECT = 3,
  GAP = 4,
}
```

---

## Core Interfaces

### KafkaConfig

```typescript
interface KafkaConfig {
  brokers: string[] | BrokersFunction
  ssl?: tls.ConnectionOptions | boolean
  sasl?: SASLOptions | Mechanism
  clientId?: string
  connectionTimeout?: number
  authenticationTimeout?: number
  reauthenticationThreshold?: number
  requestTimeout?: number
  enforceRequestTimeout?: boolean
  retry?: RetryOptions
  socketFactory?: ISocketFactory
  logLevel?: logLevel
  logCreator?: logCreator
}
```

### SASL Options

Supported SASL mechanisms and their options:

```typescript
type SASLMechanismOptionsMap = {
  plain: { username: string; password: string }
  'scram-sha-256': { username: string; password: string }
  'scram-sha-512': { username: string; password: string }
  aws: {
    authorizationIdentity: string
    accessKeyId: string
    secretAccessKey: string
    sessionToken?: string
  }
  oauthbearer: {
    oauthBearerProvider: () => Promise<OauthbearerProviderResponse>
  }
  gssapi: {
    serviceName?: string
    principal?: string
    keytab?: string
    kerberosServicePrincipal?: string
  }
}
```

### ProducerConfig

```typescript
interface ProducerConfig {
  createPartitioner?: ICustomPartitioner
  retry?: RetryOptions
  metadataMaxAge?: number
  allowAutoTopicCreation?: boolean
  idempotent?: boolean
  transactionalId?: string
  transactionTimeout?: number
  maxInFlightRequests?: number
}
```

### ConsumerConfig

```typescript
interface ConsumerConfig {
  groupId: string
  groupInstanceId?: string           // Static Membership (KIP-345)
  partitionAssigners?: PartitionAssigner[]
  metadataMaxAge?: number
  sessionTimeout?: number
  rebalanceTimeout?: number
  heartbeatInterval?: number
  maxBytesPerPartition?: number
  minBytes?: number
  maxBytes?: number
  maxWaitTimeInMs?: number
  retry?: RetryOptions & { restartOnFailure?: (err: Error) => Promise<boolean> }
  allowAutoTopicCreation?: boolean
  maxInFlightRequests?: number
  readUncommitted?: boolean
  rackId?: string                    // Follower Fetching
}
```

### RetryOptions

```typescript
interface RetryOptions {
  maxRetryTime?: number
  initialRetryTime?: number
  factor?: number
  multiplier?: number
  retries?: number
  restartOnFailure?: (e: Error) => Promise<boolean>
}
```

---

## Message Types

### Message

```typescript
interface Message {
  key?: Buffer | string | null
  value: Buffer | string | null
  partition?: number
  headers?: IHeaders
  timestamp?: string
}
```

### IHeaders

```typescript
interface IHeaders {
  [key: string]: Buffer | string | (Buffer | string)[] | undefined
}
```

### KafkaMessage

The type received when consuming messages:

```typescript
type KafkaMessage = MessageSetEntry | RecordBatchEntry

interface RecordBatchEntry {
  key: Buffer | null
  value: Buffer | null
  timestamp: string
  attributes: number
  offset: string
  headers: IHeaders
}
```

### ProducerRecord

```typescript
interface ProducerRecord {
  topic: string
  messages: Message[]
  acks?: number
  timeout?: number
  compression?: CompressionTypes
}
```

### RecordMetadata

Returned by `producer.send()`:

```typescript
type RecordMetadata = {
  topicName: string
  partition: number
  errorCode: number
  offset?: string
  timestamp?: string
  baseOffset?: string
  logAppendTime?: string
  logStartOffset?: string
}
```

---

## Consumer Types

### ConsumerRunConfig

```typescript
type ConsumerRunConfig = {
  autoCommit?: boolean
  autoCommitInterval?: number | null
  autoCommitThreshold?: number | null
  eachBatchAutoResolve?: boolean
  partitionsConsumedConcurrently?: number
  eachBatch?: EachBatchHandler
  eachMessage?: EachMessageHandler
}
```

### EachMessagePayload

```typescript
interface EachMessagePayload {
  topic: string
  partition: number
  message: KafkaMessage
  heartbeat(): Promise<void>
  pause(): () => void
}
```

### EachBatchPayload

```typescript
interface EachBatchPayload {
  batch: Batch
  resolveOffset(offset: string): void
  heartbeat(): Promise<void>
  pause(): () => void
  commitOffsetsIfNecessary(offsets?: Offsets): Promise<void>
  uncommittedOffsets(): OffsetsByTopicPartition
  isRunning(): boolean
  isStale(): boolean
}
```

### Batch

```typescript
type Batch = {
  topic: string
  partition: number
  highWatermark: string
  messages: KafkaMessage[]
  isEmpty(): boolean
  firstOffset(): string | null
  lastOffset(): string
  offsetLag(): string
  offsetLagLow(): string
}
```

### ConsumerSubscribeTopics

```typescript
type ConsumerSubscribeTopics = {
  topics: (string | RegExp)[]
  fromBeginning?: boolean
}
```

---

## Dead Letter Queue

### DeadLetterQueueConfig

```typescript
interface DeadLetterQueueConfig {
  producer: Producer
  topic: string
  maxRetries?: number
  onOriginalMessageFailed?(payload: EachMessagePayload, error: Error): Promise<void>
  createDLQMessage?(error: Error, payload: EachMessagePayload): Message
}
```

### deadLetterQueue()

```typescript
function deadLetterQueue(
  config: DeadLetterQueueConfig
): (handler: EachMessageHandler) => EachMessageHandler
```

Usage:

```javascript
const { deadLetterQueue } = require('kafkajs')

await consumer.run({
  eachMessage: deadLetterQueue({
    producer,
    topic: 'my-dlq-topic',
    maxRetries: 3,
  })(async ({ topic, partition, message }) => {
    // process message — failures auto-retry then forward to DLQ
  }),
})
```

---

## Admin Client Methods

The admin client exposes the following methods. See [Admin Client](Admin.md) for detailed usage.

### Topic Management

| Method | Description |
|--------|-------------|
| `listTopics()` | List all topic names |
| `createTopics(options)` | Create topics |
| `deleteTopics(options)` | Delete topics |
| `createPartitions(options)` | Add partitions to existing topics |
| `fetchTopicMetadata(options?)` | Get topic metadata (partitions, leaders, ISR) |
| `fetchTopicOffsets(topic)` | Get latest offsets for a topic |
| `fetchTopicOffsetsByTimestamp(topic, timestamp)` | Get offsets by timestamp |
| `deleteTopicRecords(options)` | Delete records up to a given offset |

### Consumer Group Management

| Method | Description |
|--------|-------------|
| `listGroups()` | List all consumer groups |
| `describeGroups(groupIds)` | Describe consumer groups |
| `deleteGroups(groupIds)` | Delete consumer groups |
| `fetchOffsets(options)` | Fetch consumer group offsets |
| `setOffsets(options)` | Set consumer group offsets |
| `resetOffsets(options)` | Reset offsets to earliest/latest |
| `deleteOffsets(options)` | Delete consumer group offsets for specific partitions |

### Cluster Operations

| Method | Description |
|--------|-------------|
| `describeCluster()` | Get broker and controller info |
| `describeConfigs(options)` | Get resource configurations |
| `alterConfigs(options)` | Replace resource configurations |
| `incrementalAlterConfigs(options)` | Incrementally modify configs (Kafka 2.3+) |
| `electLeaders(options?)` | Trigger leader election (Kafka 2.4+) |
| `describeLogDirs(options?)` | Get log directory info |

### Partition Reassignment

| Method | Description |
|--------|-------------|
| `alterPartitionReassignments(request)` | Reassign partition replicas |
| `listPartitionReassignments(request)` | List ongoing reassignments |

### ACL Management

| Method | Description |
|--------|-------------|
| `createAcls(options)` | Create access control lists |
| `deleteAcls(options)` | Delete ACLs matching filters |
| `describeAcls(options)` | Describe ACLs matching filter |

### Transaction Inspection

| Method | Description |
|--------|-------------|
| `describeProducers(options)` | Get active producer info for partitions |
| `describeTransactions(options)` | Describe active transactions by ID |
| `listTransactions(options?)` | List active transactions |

### Share Groups <small>(Kafka 4.0+)</small>

| Method | Description |
|--------|-------------|
| `shareGroupDescribe(options)` | Describe Share Groups (KIP-932) |

---

## Share Group Types <small>(Kafka 4.0+)</small>

Share Groups (KIP-932) provide queue-like consumption where multiple consumers process records concurrently with per-record acknowledgement.

### ShareGroupDescribeRequest

```typescript
interface ShareGroupDescribeRequest {
  groupIds: string[]
  includeAuthorizedOperations?: boolean
}
```

### ShareGroupDescribeResponse

```typescript
interface ShareGroupDescribeResponse {
  throttleTime: number
  groups: ShareGroupDescribeGroup[]
}

interface ShareGroupDescribeGroup {
  errorCode: number
  errorMessage: string | null
  groupId: string
  groupState: string
  groupEpoch: number
  assignmentEpoch: number
  assignorName: string
  topics: Array<{
    topicId: KafkaUUID
    topicName: string
    partitions: Array<{
      partitionIndex: number
      startOffset: string
      stateEpoch: number
      leaderEpoch: number
    }>
  }>
  members: Array<ShareGroupMember>
  authorizedOperations: number
}

interface ShareGroupMember {
  memberId: string
  rackId: string | null
  memberEpoch: number
  clientId: string
  clientHost: string
  subscribedTopicNames: string[]
  assignment: Array<{ topicId: KafkaUUID; partitions: number[] }>
}
```

### ShareFetchRequest / ShareFetchResponse

```typescript
interface ShareFetchRequest {
  groupId: string
  memberId: string
  memberEpoch: number
  maxWaitMs?: number
  minBytes?: number
  maxBytes?: number
  topics?: ShareFetchTopicRequest[]
  forgottenTopicsData?: Array<{ topicId: KafkaUUID; partitions: number[] }>
}

interface ShareFetchResponse {
  throttleTime: number
  errorCode: number
  errorMessage: string | null
  responses: Array<{
    topicId: KafkaUUID
    partitions: Array<{
      partitionIndex: number
      errorCode: number
      errorMessage: string | null
      currentLeader: { leaderId: number; leaderEpoch: number }
      acquiredRecords: Array<{
        firstOffset: string
        lastOffset: string
        deliveryCount: number
      }>
    }>
  }>
}
```

### ShareAcknowledgeRequest / ShareAcknowledgeResponse

```typescript
interface ShareAcknowledgeRequest {
  groupId: string
  memberId: string
  memberEpoch: number
  topics?: Array<{
    topicId: KafkaUUID
    partitions: Array<{
      partitionIndex: number
      acknowledgementBatches: Array<{
        firstOffset: string
        lastOffset: string
        acknowledgeTypes: ShareAcknowledgeType[]
      }>
    }>
  }>
}

interface ShareAcknowledgeResponse {
  throttleTime: number
  errorCode: number
  errorMessage: string | null
  responses: Array<{
    topicId: KafkaUUID
    partitions: Array<{
      partitionIndex: number
      errorCode: number
      errorMessage: string | null
      currentLeader: { leaderId: number; leaderEpoch: number }
    }>
  }>
}
```

---

## KRaft Voter Management Types <small>(Kafka 4.0+)</small>

### AddRaftVoterRequest / Response

```typescript
interface AddRaftVoterRequest {
  clusterId?: string | null
  timeoutMs?: number
  voterId: number
  voterDirectoryId: KafkaUUID
  listeners?: RaftVoterListener[]
}

interface AddRaftVoterResponse {
  throttleTime: number
  errorCode: number
  errorMessage: string | null
  currentLeader: RaftVoterCurrentLeader
}
```

### RemoveRaftVoterRequest / Response

```typescript
interface RemoveRaftVoterRequest {
  clusterId?: string | null
  timeoutMs?: number
  voterId: number
  voterDirectoryId: KafkaUUID
}
```

### UpdateRaftVoterRequest / Response

```typescript
interface UpdateRaftVoterRequest {
  clusterId?: string | null
  currentLeaderEpoch: number
  voterId: number
  voterDirectoryId: KafkaUUID
  listeners?: RaftVoterListener[]
  kRaftVersionFeature?: {
    minSupportedVersion: number
    maxSupportedVersion: number
  } | null
}
```

### Supporting Types

```typescript
interface RaftVoterListener {
  name: string
  host: string
  port: number
  securityProtocol?: number
}

interface RaftVoterCurrentLeader {
  leaderId: number
  host: string
  port: number
}
```

---

## Cluster & Broker Types

### DescribeClusterResponse

```typescript
interface DescribeClusterResponse {
  brokers: ClusterBroker[]
  controller: number | null
  clusterId: string
  clusterAuthorizedOperations?: number
}

interface ClusterBroker {
  nodeId: number
  host: string
  port: number
  rack?: string | null
}
```

### PartitionMetadata

```typescript
type PartitionMetadata = {
  partitionErrorCode: number
  partitionId: number
  leader: number
  leaderEpoch?: number    // Available with Kafka 2.4+
  replicas: number[]
  isr: number[]
  offlineReplicas?: number[]
}
```

### DescribeLogDirsResponse

```typescript
interface DescribeLogDirsResponse {
  brokers: Array<{
    brokerId: number
    throttleTime: number
    results: Array<{
      errorCode: number
      logDir: string
      topics: Array<{
        name: string
        partitions: Array<{
          partitionIndex: number
          partitionSize: string
          offsetLag: string
          isFutureKey: boolean
        }>
      }>
    }>
  }>
}
```

---

## Transaction Types

### DescribeProducersResponse

```typescript
interface DescribeProducersResponse {
  topics: Array<{
    name: string
    partitions: Array<{
      partitionIndex: number
      errorCode: number
      activeProducers: ActiveProducer[]
    }>
  }>
}

interface ActiveProducer {
  producerId: string
  producerEpoch: number
  lastSequence: number
  lastTimestamp: string
  coordinatorEpoch: number
  currentTxnStartOffset: string
}
```

### DescribeTransactionsResponse

```typescript
interface DescribeTransactionsResponse {
  transactionStates: TransactionState[]
}

interface TransactionState {
  errorCode: number
  transactionalId: string
  state: string
  producerId: string
  producerEpoch: number
  transactionTimeoutMs: number
  transactionStartTimeMs: string
  topics: Array<{ topic: string; partitions: number[] }>
}
```

### ListTransactionsResponse

```typescript
interface ListTransactionsResponse {
  transactionStates: TransactionListing[]
}

interface TransactionListing {
  transactionalId: string
  producerId: string
  transactionState: string
}
```

---

## Election Types

### ElectLeadersRequest / Response

```typescript
interface ElectLeadersRequest {
  electionType?: ElectionType | number  // 0=PREFERRED, 1=UNCLEAN
  topicPartitions?: Array<{ topic: string; partitions: number[] }> | null
  timeout?: number
}

interface ElectLeadersResponse {
  throttleTime: number
  errorCode: number
  replicaElectionResults: Array<{
    topic: string
    partitionResults: Array<{
      partitionId: number
      errorCode: number
      errorMessage: string | null
    }>
  }>
}
```

---

## Incremental Config Types

### IncrementalAlterConfigsResource

```typescript
interface IncrementalAlterConfigsResource {
  type: ConfigResourceTypes
  name: string
  configEntries: IncrementalAlterConfigEntry[]
}

interface IncrementalAlterConfigEntry {
  name: string
  configOperation: ConfigOperationTypes  // SET, DELETE, APPEND, SUBTRACT
  value: string
}
```

---

## Instrumentation Events

### Producer Events

```typescript
type ProducerEvents = {
  CONNECT: 'producer.connect'
  DISCONNECT: 'producer.disconnect'
  REQUEST: 'producer.network.request'
  REQUEST_TIMEOUT: 'producer.network.request_timeout'
  REQUEST_QUEUE_SIZE: 'producer.network.request_queue_size'
}
```

### Consumer Events

```typescript
type ConsumerEvents = {
  HEARTBEAT: 'consumer.heartbeat'
  COMMIT_OFFSETS: 'consumer.commit_offsets'
  GROUP_JOIN: 'consumer.group_join'
  FETCH_START: 'consumer.fetch_start'
  FETCH: 'consumer.fetch'
  START_BATCH_PROCESS: 'consumer.start_batch_process'
  END_BATCH_PROCESS: 'consumer.end_batch_process'
  CONNECT: 'consumer.connect'
  DISCONNECT: 'consumer.disconnect'
  STOP: 'consumer.stop'
  CRASH: 'consumer.crash'
  REBALANCING: 'consumer.rebalancing'
  RECEIVED_UNSUBSCRIBED_TOPICS: 'consumer.received_unsubscribed_topics'
  REQUEST: 'consumer.network.request'
  REQUEST_TIMEOUT: 'consumer.network.request_timeout'
  REQUEST_QUEUE_SIZE: 'consumer.network.request_queue_size'
}
```

### Admin Events

```typescript
type AdminEvents = {
  CONNECT: 'admin.connect'
  DISCONNECT: 'admin.disconnect'
  REQUEST: 'admin.network.request'
  REQUEST_TIMEOUT: 'admin.network.request_timeout'
  REQUEST_QUEUE_SIZE: 'admin.network.request_queue_size'
}
```

---

## Error Classes

KafkaJS provides a hierarchy of error classes:

| Error Class | Retriable | Description |
|-------------|-----------|-------------|
| `KafkaJSError` | configurable | Base error class |
| `KafkaJSNonRetriableError` | no | Errors that should not be retried |
| `KafkaJSProtocolError` | varies | Kafka protocol errors with `code` and `type` |
| `KafkaJSAggregateError` | - | Wraps multiple errors |
| `KafkaJSOffsetOutOfRange` | no | Offset is out of range |
| `KafkaJSNumberOfRetriesExceeded` | no | Max retries exceeded |
| `KafkaJSConnectionError` | yes | Connection failures |
| `KafkaJSConnectionClosedError` | yes | Connection unexpectedly closed |
| `KafkaJSRequestTimeoutError` | yes | Request timed out |
| `KafkaJSMetadataNotLoaded` | yes | Metadata not yet available |
| `KafkaJSTopicMetadataNotLoaded` | yes | Topic metadata not loaded |
| `KafkaJSStaleTopicMetadataAssignment` | yes | Partition assignment is stale |
| `KafkaJSServerDoesNotSupportApiKey` | no | Broker doesn't support API |
| `KafkaJSBrokerNotFound` | yes | Broker not found |
| `KafkaJSSASLAuthenticationError` | no | SASL auth failed |
| `KafkaJSGroupCoordinatorNotFound` | yes | Group coordinator unavailable |
| `KafkaJSNotImplemented` | no | Feature not implemented |
| `KafkaJSTimeout` | yes | Generic timeout |
| `KafkaJSLockTimeout` | yes | Lock acquisition timeout |
| `KafkaJSDeleteGroupsError` | no | Partial group deletion failure |
| `KafkaJSDeleteTopicRecordsError` | no | Partial record deletion failure |
| `KafkaJSCreateTopicError` | no | Topic creation failed |
| `KafkaJSMemberIdRequired` | no | Member ID required for join |
| `KafkaJSNoBrokerAvailableError` | yes | No broker available |
| `KafkaJSAlterPartitionReassignmentsError` | no | Reassignment failed |
| `KafkaJSFetcherRebalanceError` | yes | Rebalance during fetch |

### Utility Functions

```javascript
const { isKafkaJSError, isRebalancing } = require('kafkajs')

try {
  await consumer.run({ eachMessage: handler })
} catch (error) {
  if (isRebalancing(error)) {
    // consumer group is rebalancing
  }
  if (isKafkaJSError(error)) {
    // it's a KafkaJS error
  }
}
```

---

## Partitioners

```javascript
const { Partitioners } = require('kafkajs')

// Default partitioner (Java-compatible, murmur2 hash)
kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

// Legacy partitioner (pre-v2.0.0 behavior)
kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner })
```

### Custom Partitioner

```typescript
type ICustomPartitioner = () => (args: PartitionerArgs) => number

interface PartitionerArgs {
  topic: string
  partitionMetadata: PartitionMetadata[]
  message: Message
}
```

---

## Partition Assigners

```javascript
const { PartitionAssigners: { roundRobin } } = require('kafkajs')

kafka.consumer({
  groupId: 'my-group',
  partitionAssigners: [roundRobin],
})
```

### AssignerProtocol

Helpers for encoding/decoding member metadata and assignments:

```javascript
const { AssignerProtocol } = require('kafkajs')

const metadata = AssignerProtocol.MemberMetadata.decode(buffer)
// { version: number, topics: string[], userData: Buffer }

const assignment = AssignerProtocol.MemberAssignment.decode(buffer)
// { version: number, assignment: { [topic]: number[] }, userData: Buffer }
```
