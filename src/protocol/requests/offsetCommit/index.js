const versions = {
  0: ({ groupId, topics }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ groupId, topics }), response }
  },
  1: ({ groupId, groupGenerationId, memberId, topics }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ groupId, groupGenerationId, memberId, topics }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
