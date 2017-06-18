module.exports = ({ version }) => () => {
  switch (version) {
    case 0: {
      const request = require('./v0/request')
      const response = require('./v0/response')
      return { request: request(), response }
    }
    default:
      throw new Error(`Version ${version} of apiVersions not implemented`)
  }
}
