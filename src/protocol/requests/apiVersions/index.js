const versions = {
  0: () => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request(), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
