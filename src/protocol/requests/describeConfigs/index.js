const versions = {
  0: ({ resources }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ resources }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
