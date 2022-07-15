const versions = {
  0: ({ timeout, topics }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ timeout, topics }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
