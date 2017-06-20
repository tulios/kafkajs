const { failure, KafkaProtocolError } = require('./protocol/error')
const { requests, lookup } = require('./protocol/requests')
const apiKeys = require('./protocol/requests/apiKeys')

module.exports = class API {
  constructor(connection, opts = {}) {
    this.connection = connection
    this.lookupRequest = null
  }

  async load() {
    const apiVersions = requests.ApiVersions.protocol({ version: 0 })
    const response = await this.connection.send(apiVersions())
    const versions = response.apiVersions.reduce(
      (obj, version) =>
        Object.assign(obj, {
          [version.apiKey]: {
            minVersion: version.minVersion,
            maxVersion: version.maxVersion,
          },
        }),
      {}
    )

    this.lookupRequest = lookup(versions)
  }

  async metadata(topics) {
    const metadata = this.lookupRequest(apiKeys.Metadata, requests.Metadata)
    return await this.connection.send(metadata(topics))
  }
}
