const versions = {
  0: ({ groupIds }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ groupIds }), response }
  },
  1: ({ groupIds }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ groupIds }), response }
  },
  2: ({ groupIds }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ groupIds }), response }
  },
  3: ({ groupIds }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ groupIds }), response }
  },
  4: ({ groupIds }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ groupIds }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
