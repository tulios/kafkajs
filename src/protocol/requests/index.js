const requests = {
  Produce: require('./produce'),
  Fetch: require('./fetch'),
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
  SaslHandshake: require('./saslHandshake'),
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
