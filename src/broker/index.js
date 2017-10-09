const { failure, KafkaProtocolError } = require('../protocol/error')
const { Types: Compression } = require('../protocol/message/compression')
const { requests, lookup } = require('../protocol/requests')
const apiKeys = require('../protocol/requests/apiKeys')
const SASLAuthenticator = require('./saslAuthenticator')

module.exports = class Broker {
  constructor(connection, versions = null) {
    this.connection = connection
    this.versions = versions
  }

  async connect() {
    await this.connection.connect()

    if (!this.versions) {
      this.versions = await this.apiVersions()
    }

    this.lookupRequest = lookup(this.versions)

    if (this.connection.sasl) {
      await new SASLAuthenticator(this.connection, this.versions).authenticate()
    }
  }

  async disconnect() {
    // Connection#disconnect is not async but Broker will enforce
    // async apis
    this.connection.disconnect()
  }

  async apiVersions() {
    const apiVersions = requests.ApiVersions.protocol({ version: 0 })
    const response = await this.connection.send(apiVersions())
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

  async metadata(topics = []) {
    if (!this.lookupRequest) throw new Error('Broker not connected')
    const metadata = this.lookupRequest(apiKeys.Metadata, requests.Metadata)
    return await this.connection.send(metadata(topics))
  }

  async produce({ acks = -1, timeout = 30000, compression = Compression.None, topicData }) {
    if (!this.lookupRequest) throw new Error('Broker not connected')
    const produce = this.lookupRequest(apiKeys.Produce, requests.Produce)
    return await this.connection.send(produce({ acks, timeout, compression, topicData }))
  }
}
