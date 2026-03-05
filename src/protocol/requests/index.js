const apiKeys = require('./apiKeys')
const { KafkaJSServerDoesNotSupportApiKey, KafkaJSNotImplemented } = require('../../errors')

/**
 * @typedef {(options?: Object) => { request: any, response: any, logResponseErrors?: boolean }} Request
 */

/**
 * @typedef {Object} RequestDefinitions
 * @property {string[]} versions
 * @property {({ version: number }) => Request} protocol
 */

/**
 * @typedef {(apiKey: number, definitions: RequestDefinitions) => Request} Lookup
 */

/** @type {RequestDefinitions} */
const noImplementedRequestDefinitions = {
  versions: [],
  protocol: () => {
    throw new KafkaJSNotImplemented()
  },
}

/**
 * @type {{[apiName: string]: RequestDefinitions}}
 */
const requests = {
  Produce: require('./produce'),
  Fetch: require('./fetch'),
  ListOffsets: require('./listOffsets'),
  Metadata: require('./metadata'),
  LeaderAndIsr: noImplementedRequestDefinitions,
  StopReplica: noImplementedRequestDefinitions,
  UpdateMetadata: noImplementedRequestDefinitions,
  ControlledShutdown: noImplementedRequestDefinitions,
  OffsetCommit: require('./offsetCommit'),
  OffsetFetch: require('./offsetFetch'),
  GroupCoordinator: require('./findCoordinator'),
  JoinGroup: require('./joinGroup'),
  Heartbeat: require('./heartbeat'),
  LeaveGroup: require('./leaveGroup'),
  SyncGroup: require('./syncGroup'),
  DescribeGroups: require('./describeGroups'),
  ListGroups: require('./listGroups'),
  SaslHandshake: require('./saslHandshake'),
  ApiVersions: require('./apiVersions'),
  CreateTopics: require('./createTopics'),
  DeleteTopics: require('./deleteTopics'),
  DeleteRecords: require('./deleteRecords'),
  InitProducerId: require('./initProducerId'),
  OffsetForLeaderEpoch: require('./offsetForLeaderEpoch'),
  AddPartitionsToTxn: require('./addPartitionsToTxn'),
  AddOffsetsToTxn: require('./addOffsetsToTxn'),
  EndTxn: require('./endTxn'),
  WriteTxnMarkers: noImplementedRequestDefinitions,
  TxnOffsetCommit: require('./txnOffsetCommit'),
  DescribeAcls: require('./describeAcls'),
  CreateAcls: require('./createAcls'),
  DeleteAcls: require('./deleteAcls'),
  DescribeConfigs: require('./describeConfigs'),
  AlterConfigs: require('./alterConfigs'),
  AlterReplicaLogDirs: noImplementedRequestDefinitions,
  DescribeLogDirs: require('./describeLogDirs'),
  SaslAuthenticate: require('./saslAuthenticate'),
  CreatePartitions: require('./createPartitions'),
  CreateDelegationToken: noImplementedRequestDefinitions,
  RenewDelegationToken: noImplementedRequestDefinitions,
  ExpireDelegationToken: noImplementedRequestDefinitions,
  DescribeDelegationToken: noImplementedRequestDefinitions,
  DeleteGroups: require('./deleteGroups'),
  ElectLeaders: require('./electLeaders'),
  IncrementalAlterConfigs: require('./incrementalAlterConfigs'),
  AlterPartitionReassignments: require('./alterPartitionReassignments'),
  ListPartitionReassignments: require('./listPartitionReassignments'),
  // Kafka 3.0+ (placeholders for version negotiation)
  OffsetDelete: require('./offsetDelete'),
  DescribeClientQuotas: noImplementedRequestDefinitions,
  AlterClientQuotas: noImplementedRequestDefinitions,
  DescribeUserScramCredentials: noImplementedRequestDefinitions,
  AlterUserScramCredentials: noImplementedRequestDefinitions,
  Vote: noImplementedRequestDefinitions,
  BeginQuorumEpoch: noImplementedRequestDefinitions,
  EndQuorumEpoch: noImplementedRequestDefinitions,
  DescribeQuorum: noImplementedRequestDefinitions,
  AlterPartition: noImplementedRequestDefinitions,
  UpdateFeatures: noImplementedRequestDefinitions,
  Envelope: noImplementedRequestDefinitions,
  FetchSnapshot: noImplementedRequestDefinitions,
  DescribeCluster: require('./describeCluster'),
  DescribeProducers: require('./describeProducers'),
  BrokerRegistration: noImplementedRequestDefinitions,
  BrokerHeartbeat: noImplementedRequestDefinitions,
  UnregisterBroker: noImplementedRequestDefinitions,
  DescribeTransactions: require('./describeTransactions'),
  ListTransactions: require('./listTransactions'),
  AllocateProducerIds: noImplementedRequestDefinitions,
  // Kafka 3.3+ KRaft
  ConsumerGroupHeartbeat: noImplementedRequestDefinitions,
  ConsumerGroupDescribe: noImplementedRequestDefinitions,
  ControllerRegistration: noImplementedRequestDefinitions,
  GetTelemetrySubscriptions: noImplementedRequestDefinitions,
  PushTelemetry: noImplementedRequestDefinitions,
  AssignReplicasToDirs: noImplementedRequestDefinitions,
  ListClientMetricsResources: noImplementedRequestDefinitions,
  DescribeTopicPartitions: noImplementedRequestDefinitions,
  // Kafka 4.0+ Share Groups (KIP-932)
  ShareGroupHeartbeat: require('./shareGroupHeartbeat'),
  ShareGroupDescribe: require('./shareGroupDescribe'),
  ShareFetch: require('./shareFetch'),
  ShareAcknowledge: require('./shareAcknowledge'),
  // Kafka 4.1+
  AddRaftVoter: require('./addRaftVoter'),
  RemoveRaftVoter: require('./removeRaftVoter'),
  // Kafka 4.2+
  UpdateRaftVoter: require('./updateRaftVoter'),
  ReadShareGroupStateSummary: require('./readShareGroupStateSummary'),
}

const names = Object.keys(apiKeys)
const keys = Object.values(apiKeys)
const findApiName = apiKey => names[keys.indexOf(apiKey)]

/**
 * @param {import("../../../types").ApiVersions} versions
 * @returns {Lookup}
 */
const lookup = versions => (apiKey, definition) => {
  const version = versions[apiKey]
  const availableVersions = definition.versions.map(Number)
  const bestImplementedVersion = Math.max(...availableVersions)

  if (!version || version.maxVersion == null) {
    throw new KafkaJSServerDoesNotSupportApiKey(
      `The Kafka server does not support the requested API version`,
      { apiKey, apiName: findApiName(apiKey) }
    )
  }

  const bestSupportedVersion = Math.min(bestImplementedVersion, version.maxVersion)
  return definition.protocol({ version: bestSupportedVersion })
}

module.exports = {
  requests,
  lookup,
}
