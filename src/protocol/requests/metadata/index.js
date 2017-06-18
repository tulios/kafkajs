const versions = {
  0: topics => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request(topics), response }
  },
  1: topics => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request(topics), response }
  },
  2: topics => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request(topics), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
