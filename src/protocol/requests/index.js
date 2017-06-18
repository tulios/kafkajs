const requests = {
  Produce: {},
  Fetch: {},
  Offsets: {},
  Metadata: require('./metadata'),
  LeaderAndIsr: {},
  StopReplica: {},
  UpdateMetadata: {},
  ControlledShutdown: {},
  OffsetCommit: {},
  OffsetFetch: {},
  GroupCoordinator: {},
  JoinGroup: {},
  Heartbeat: {},
  LeaveGroup: {},
  SyncGroup: {},
  DescribeGroups: {},
  ListGroups: {},
  SaslHandshake: {},
  ApiVersions: require('./apiVersions'),
  CreateTopics: {},
  DeleteTopics: {},
}

const lookup = versions => (apiKey, definition) => {
  const version = versions[apiKey]
  const bestImplementedVersion = Math.max.apply(this, definition.versions)
  const bestSupportedVersion = Math.min(bestImplementedVersion, version.maxVersion)
  return definition.protocol({ version: bestSupportedVersion })
}

module.exports = {
  requests,
  lookup,
}
