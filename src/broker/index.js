const { failure, KafkaProtocolError } = require('../protocol/error')
const { Types: Compression } = require('../protocol/message/compression')
const { requests, lookup } = require('../protocol/requests')
const apiKeys = require('../protocol/requests/apiKeys')
const SASLAuthenticator = require('./saslAuthenticator')

/**
 * Each node in a Kafka cluster is called broker. This class contains
 * the high-level operations a node can perform.
 *
 * @param {Connection} connection
 * @param {Object} [versions=null] The object with all available versions and APIs
 *                                 supported by this cluster. The output of broker#apiVersions
 */
module.exports = class Broker {
  constructor(connection, versions = null) {
    this.connection = connection
    this.versions = versions
    this.authenticated = false
    this.lookupRequest = () => {
      throw new Error('Broker not connected')
    }
  }

  /**
   * @public
   * @returns {Promise}
   */
  async connect() {
    await this.connection.connect()

    if (!this.versions) {
      this.versions = await this.apiVersions()
    }

    this.lookupRequest = lookup(this.versions)

    if (!this.authenticated && this.connection.sasl) {
      await new SASLAuthenticator(this.connection, this.versions).authenticate()
      this.authenticated = true
    }

    return true
  }

  /**
   * @public
   * @returns {Promise}
   */
  async disconnect() {
    this.authenticated = false
    await this.connection.disconnect()
  }

  /**
   * @public
   * @returns {Promise}
   */
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

  /**
   * @public
   * @param {Array} [topics=[]] An array of topics to fetch metadata for.
   *                            If no topics are specified fetch metadata for all topics
   * @returns {Promise}
   */
  async metadata(topics = []) {
    const metadata = this.lookupRequest(apiKeys.Metadata, requests.Metadata)
    return await this.connection.send(metadata(topics))
  }

  /**
   * @public
   * @param {Array} topicData An array of messages per topic and per partition, example:
   *                          [
   *                            {
   *                              topic: 'test-topic-1',
   *                              partitions: [
   *                                {
   *                                  partition: 0,
   *                                  messages: [
   *                                    { key: '1', value: 'A' },
   *                                    { key: '2', value: 'B' },
   *                                  ]
   *                                },
   *                                {
   *                                  partition: 1,
   *                                  messages: [
   *                                    { key: '3', value: 'C' },
   *                                  ]
   *                                }
   *                              ]
   *                            },
   *                            {
   *                              topic: 'test-topic-2',
   *                              partitions: [
   *                                {
   *                                  partition: 4,
   *                                  messages: [
   *                                    { key: '32', value: 'E' },
   *                                  ]
   *                                },
   *                              ]
   *                            },
   *                          ]
   * @param {number} [acks=-1] Control the number of required acks.
   *                           -1 = all replicas must acknowledge
   *                            0 = no acknowledgments
   *                            1 = only waits for the leader to acknowledge
   * @param {number} [timeout=30000] The time to await a response in ms
   * @param {Compression.Types} [compression=Compression.Types.None] Compression codec
   * @returns {Promise}
   */
  async produce({ topicData, acks = -1, timeout = 30000, compression = Compression.None }) {
    const produce = this.lookupRequest(apiKeys.Produce, requests.Produce)
    return await this.connection.send(produce({ acks, timeout, compression, topicData }))
  }
}
