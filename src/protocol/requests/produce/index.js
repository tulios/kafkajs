const versions = {
  0: ({ acks, timeout, topicData }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ acks, timeout, topicData }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
