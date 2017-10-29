const versions = {
  1: ({ groupId, topics }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ groupId, topics }), response }
  },
  2: ({ groupId, topics }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ groupId, topics }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
