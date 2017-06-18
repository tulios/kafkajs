module.exports = ({ version }) => topics => {
  switch (version) {
    case 0: {
      const request = require('./v0/request')
      const response = require('./v0/response')
      return { request: request(topics), response }
    }
    case 1: {
      const request = require('./v1/request')
      const response = require('./v1/response')
      return { request: request(topics), response }
    }
    case 2: {
      const request = require('./v2/request')
      const response = require('./v2/response')
      return { request: request(topics), response }
    }
    default:
      throw new Error(`Version ${version} of metadata not implemented`)
  }
}
