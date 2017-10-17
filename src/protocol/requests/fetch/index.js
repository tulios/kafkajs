const versions = {
  0: ({ replicaId, maxWaitTime, minBytes, topics }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ replicaId, maxWaitTime, minBytes, topics }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
