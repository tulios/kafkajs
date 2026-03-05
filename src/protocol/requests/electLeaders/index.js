const versions = {
  0: ({ electionType, topicPartitions, timeout }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ electionType, topicPartitions, timeout }), response }
  },
  1: ({ electionType, topicPartitions, timeout }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ electionType, topicPartitions, timeout }), response }
  },
  2: ({ electionType, topicPartitions, timeout }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ electionType, topicPartitions, timeout }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
