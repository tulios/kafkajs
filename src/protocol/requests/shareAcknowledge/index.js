const versions = {
  0: ({ groupId, memberId, memberEpoch, topics }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ groupId, memberId, memberEpoch, topics }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
