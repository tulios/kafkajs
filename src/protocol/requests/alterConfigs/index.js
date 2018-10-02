const versions = {
  0: ({ resources, validateOnly }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ resources, validateOnly }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
