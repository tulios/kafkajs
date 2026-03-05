const versions = {
  0: ({ clusterId, timeoutMs, voterId, voterDirectoryId, listeners }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return {
      request: request({ clusterId, timeoutMs, voterId, voterDirectoryId, listeners }),
      response,
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
