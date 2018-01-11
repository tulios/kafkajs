const versions = {
  0: ({ groupIds }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ groupIds }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
