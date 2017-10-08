const { failure, KafkaProtocolError } = require('../protocol/error')
const { Types: Compression } = require('../protocol/message/compression')
const { requests, lookup } = require('../protocol/requests')
const apiKeys = require('../protocol/requests/apiKeys')

module.exports = class Broker {
  constructor(connection, versions) {
    this.connection = connection
    this.versions = versions
    this.lookupRequest = lookup(this.versions)
  }

  async connect() {
    await this.connection.connect()
  }

  async disconnect() {
    await this.connection.disconnect()
  }

  async metadata(topics = []) {
    const metadata = this.lookupRequest(apiKeys.Metadata, requests.Metadata)
    return await this.connection.send(metadata(topics))
  }

  async produce({ acks = -1, timeout = 30000, compression = Compression.None, topicData }) {
    const produce = this.lookupRequest(apiKeys.Produce, requests.Produce)
    return await this.connection.send(produce({ acks, timeout, compression, topicData }))
  }
}
