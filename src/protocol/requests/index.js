const requests = {
  Produce: require('./produce'),
  Fetch: require('./fetch'),
  Offsets: require('./offsets'),
  Metadata: require('./metadata'),
  LeaderAndIsr: {},
  StopReplica: {},
  UpdateMetadata: {},
  ControlledShutdown: {},
  OffsetCommit: require('./offsetCommit'),
  OffsetFetch: require('./offsetFetch'),
  GroupCoordinator: require('./findCoordinator'),
  JoinGroup: require('./joinGroup'),
  Heartbeat: {},
  LeaveGroup: {},
  SyncGroup: require('./syncGroup'),
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
