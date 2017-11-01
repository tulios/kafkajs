const Broker = require('../broker')
const connectionBuilder = require('./connectionBuilder')
const { KafkaJSError, KafkaJSBrokerNotFound } = require('../errors')

/**
 * @param {string} host
 * @param {number} port
 * @param {Object} ssl
 * @param {Object} sasl
 * @param {string} clientId
 * @param {number} connectionTimeout
 * @param {Object} retry
 * @param {Object} logger
 */
module.exports = class Cluster {
  constructor({ host, port, ssl, sasl, clientId, connectionTimeout, retry, logger }) {
    this.retry = retry
    this.logger = logger
    this.connectionBuilder = connectionBuilder({
      host,
      port,
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      retry,
      logger,
    })

    this.seedBroker = new Broker(this.connectionBuilder.build())
    this.targetTopics = new Set()
    this.brokerPool = {}

    this.metadata = null
    this.versions = null
  }

  /**
   * @public
   * @returns {boolean}
   */
  isConnected() {
    return this.seedBroker.isConnected()
  }

  /**
   * @public
   * @returns {Promise}
   */
  async connect() {
    await this.seedBroker.connect()
    this.versions = this.seedBroker.versions
  }

  /**
   * @public
   * @returns {Promise}
   */
  async disconnect() {
    await this.seedBroker.disconnect()
    await Promise.all(Object.values(this.brokerPool).map(broker => broker.disconnect()))
    this.brokerPool = {}
    this.metadata = null
  }

  /**
   * @public
   * @returns {Promise}
   */
  async refreshMetadata() {
    this.metadata = await this.seedBroker.metadata(Array.from(this.targetTopics))
    this.brokerPool = this.metadata.brokers.reduce((result, { nodeId, host, port, rack }) => {
      if (result[nodeId]) {
        return result
      }

      const { host: seedHost, port: seedPort } = this.seedBroker.connection

      if (host === seedHost && port === seedPort) {
        return Object.assign(result, {
          [nodeId]: this.seedBroker,
        })
      }

      const connection = this.connectionBuilder.build({ host, port, rack })
      return Object.assign(result, {
        [nodeId]: new Broker(connection, this.versions),
      })
    }, this.brokerPool)
  }

  /**
   * @public
   * @param {string} topic
   * @return {Promise}
   */
  async addTargetTopic(topic) {
    const previousSize = this.targetTopics.size
    this.targetTopics.add(topic)
    const hasChanged = previousSize !== this.targetTopics.size || !this.metadata

    if (hasChanged) {
      await this.refreshMetadata()
    }
  }

  /**
   * @public
   * @param {string} nodeId
   * @returns {Promise<Broker>}
   */
  async findBroker({ nodeId }) {
    const broker = this.brokerPool[nodeId]

    if (!broker) {
      throw new KafkaJSBrokerNotFound(`Broker ${nodeId} not found in the cached metadata`)
    }

    if (!broker.isConnected()) {
      await broker.connect()
    }

    return broker
  }

  /**
   * @public
   * @param {string} topic
   * @returns {Object} Example:
   *                   [{
   *                     isr: [2],
   *                     leader: 2,
   *                     partitionErrorCode: 0,
   *                     partitionId: 0,
   *                     replicas: [2],
   *                   }]
   */
  findTopicPartitionMetadata(topic) {
    if (!this.metadata || !this.metadata.topicMetadata) {
      throw new KafkaJSError('Topic metadata not loaded')
    }

    return this.metadata.topicMetadata.find(t => t.topic === topic).partitionMetadata
  }

  /**
   * @public
   * @param {string} topic
   * @param {Array} partitions
   * @returns {Object} Object with leader and partitions. For partitions 0 and 5
   *                   the result could be:
   *                     { '0': [0], '2': [5] }
   *
   *                   where the key is the nodeId.
   */
  findLeaderForPartitions(topic, partitions) {
    const partitionMetadata = this.findTopicPartitionMetadata(topic)
    return partitions.reduce((result, id) => {
      const partitionId = parseInt(id, 10)
      const { leader } = partitionMetadata.find(p => p.partitionId === partitionId)
      const current = result[leader] || []
      return Object.assign(result, { [leader]: [...current, partitionId] })
    }, {})
  }
}
