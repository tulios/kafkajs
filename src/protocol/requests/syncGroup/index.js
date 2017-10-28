const versions = {
  0: ({ groupId, generationId, memberId, groupAssignment }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return {
      request: request({ groupId, generationId, memberId, groupAssignment }),
      response,
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
