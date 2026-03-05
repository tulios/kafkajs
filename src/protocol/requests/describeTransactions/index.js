const versions = {
  0: ({ transactionalIds }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ transactionalIds }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
