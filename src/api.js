const { SUCCESS_CODE, KafkaProtocolError } = require('./protocol/error')
const { requests, lookup } = require('./protocol/requests')
const apiKeys = require('./protocol/requests/apiKeys')

const success = code => code === SUCCESS_CODE

module.exports = class API {
  constructor(connection) {
    this.connection = connection
    this.lookupRequest = null
  }

  async load() {
    const apiVersions = requests.ApiVersions.protocol({ version: 0 })
    const response = await this.connection.send(apiVersions())
    if (!success(response.errorCode)) {
      throw new KafkaProtocolError(response.errorCode)
    }

    const versions = response.apiVersions.reduce((obj, version) =>
      Object.assign(obj, {
        [version.apiKey]: {
          minVersion: version.minVersion,
          maxVersion: version.maxVersion,
        },
      })
    )

    this.lookupRequest = lookup(versions)
  }

  async metadata(topics) {
    const metadata = this.lookupRequest(apiKeys.Metadata, requests.Metadata)
    const response = await this.connection.send(metadata(topics))
    console.log(JSON.stringify(response))
  }
}
