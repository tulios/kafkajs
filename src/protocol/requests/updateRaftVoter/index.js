const versions = {
  0: ({
    clusterId,
    currentLeaderEpoch,
    voterId,
    voterDirectoryId,
    listeners,
    kRaftVersionFeature,
  }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return {
      request: request({
        clusterId,
        currentLeaderEpoch,
        voterId,
        voterDirectoryId,
        listeners,
        kRaftVersionFeature,
      }),
      response,
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
