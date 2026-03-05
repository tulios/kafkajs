const versions = {
  0: ({ topics }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ topics }), response }
  },
  2: ({ replicaId, topics }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ replicaId, topics }), response }
  },
  3: ({ replicaId, topics }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ replicaId, topics }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
