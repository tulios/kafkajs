const versions = {
  0: ({ groupId }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ groupId }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
