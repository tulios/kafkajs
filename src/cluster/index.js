const Broker = require('../broker')
const createRetry = require('../retry')
const connectionBuilder = require('./connectionBuilder')
const { KafkaJSError, KafkaJSBrokerNotFound } = require('../errors')

/**
 * @param {Array<string>} brokers example: ['127.0.0.1:9092', '127.0.0.1:9094']
 * @param {Object} ssl
 * @param {Object} sasl
 * @param {string} clientId
 * @param {number} connectionTimeout
 * @param {Object} retry
 * @param {Object} logger
 */
module.exports = class Cluster {
  constructor({ brokers, ssl, sasl, clientId, connectionTimeout, retry, logger: rootLogger }) {
    this.rootLogger = rootLogger
    this.logger = rootLogger.namespace('Cluster')
    this.retrier = createRetry(Object.assign({}, retry))
    this.connectionBuilder = connectionBuilder({
      logger: rootLogger,
      brokers,
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      retry,
    })

    this.seedBroker = new Broker(this.connectionBuilder.build(), this.rootLogger)
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
    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await this.seedBroker.connect()
        this.versions = this.seedBroker.versions
      } catch (e) {
        this.logger.error(e, { retryCount, retryTime })

        if (e.name === 'KafkaJSConnectionError' || e.type === 'ILLEGAL_SASL_STATE') {
          this.seedBroker = new Broker(this.connectionBuilder.build(), this.rootLogger)
          this.logger.error(
            `Failed to connect to seed broker, trying another broker from the list: ${e.message}`,
            { retryCount, retryTime }
          )
        }

        if (e.retriable) throw e
        bail(e)
      }
    })
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
        [nodeId]: new Broker(connection, this.rootLogger, this.versions),
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

  /**
   * @public
   * @param {string} groupId
   * @returns {Promise<Broker>}
   */
  async findGroupCoordinator({ groupId }) {
    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        const { coordinator } = await this.seedBroker.findGroupCoordinator({ groupId })
        const findCoordinatorBroker = async () => this.findBroker({ nodeId: coordinator.nodeId })

        return await findCoordinatorBroker()
      } catch (e) {
        // A new broker can join the cluster before we have the chance
        // to refresh metadata
        if (e.name === 'KafkaJSBrokerNotFound' || e.type === 'GROUP_COORDINATOR_NOT_AVAILABLE') {
          this.logger.debug(`${e.message}, refreshing metadata and trying again...`, {
            groupId,
            retryCount,
            retryTime,
          })

          await this.refreshMetadata()
          throw e
        }

        bail(e)
      }
    })
  }
}
