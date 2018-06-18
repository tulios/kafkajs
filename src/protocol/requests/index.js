const apiKeys = require('./apiKeys')

const requests = {
  Produce: require('./produce'),
  Fetch: require('./fetch'),
  ListOffsets: require('./listOffsets'),
  Metadata: require('./metadata'),
  LeaderAndIsr: {},
  StopReplica: {},
  UpdateMetadata: {},
  ControlledShutdown: {},
  OffsetCommit: require('./offsetCommit'),
  OffsetFetch: require('./offsetFetch'),
  GroupCoordinator: require('./findCoordinator'),
  JoinGroup: require('./joinGroup'),
  Heartbeat: require('./heartbeat'),
  LeaveGroup: require('./leaveGroup'),
  SyncGroup: require('./syncGroup'),
  DescribeGroups: require('./describeGroups'),
  ListGroups: {},
  SaslHandshake: require('./saslHandshake'),
  ApiVersions: require('./apiVersions'),
  CreateTopics: require('./createTopics'),
  DeleteTopics: {},
}

const BEGIN_EXPERIMENTAL_V011_REQUEST_VERSION = {
  [apiKeys.Produce]: 3,
  [apiKeys.Fetch]: 4,
}

const lookup = (versions, allowExperimentalV011) => (apiKey, definition) => {
  const version = versions[apiKey]
  const allowedVersions = allowExperimentalV011
    ? definition.versions
    : definition.versions.filter(
        version =>
          !BEGIN_EXPERIMENTAL_V011_REQUEST_VERSION[apiKey] ||
          version < BEGIN_EXPERIMENTAL_V011_REQUEST_VERSION[apiKey]
      )
  const bestImplementedVersion = Math.max.apply(this, allowedVersions)
  const bestSupportedVersion = Math.min(bestImplementedVersion, version.maxVersion)
  return definition.protocol({ version: bestSupportedVersion })
}

module.exports = {
  requests,
  lookup,
}
