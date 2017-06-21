const { failure, KafkaProtocolError } = require('./protocol/error')
const { requests, lookup } = require('./protocol/requests')
const apiKeys = require('./protocol/requests/apiKeys')

const loadVersions = async connection => {
  const apiVersions = requests.ApiVersions.protocol({ version: 0 })
  const response = await connection.send(apiVersions())
  return response.apiVersions.reduce(
    (obj, version) =>
      Object.assign(obj, {
        [version.apiKey]: {
          minVersion: version.minVersion,
          maxVersion: version.maxVersion,
        },
      }),
    {}
  )
}

class API {
  constructor(connection, lookupRequest) {
    this.connection = connection
    this.lookupRequest = lookupRequest
  }

  async metadata(topics) {
    const metadata = this.lookupRequest(apiKeys.Metadata, requests.Metadata)
    return await this.connection.send(metadata(topics))
  }

  async produce({ acks = -1, timeout = 30000, topicData }) {
    const produce = this.lookupRequest(apiKeys.Produce, requests.Produce)
    return await this.connection.send(produce({ acks, timeout, topicData }))
  }
}

module.exports = async connection => {
  const versions = await loadVersions(connection)
  const lookupRequest = lookup(versions)
  return new API(connection, lookupRequest)
}
