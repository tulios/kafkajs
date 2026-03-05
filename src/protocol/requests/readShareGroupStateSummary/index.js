const versions = {
  0: ({ topics }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ topics }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
