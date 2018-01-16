const BrokerPool = require('./brokerPool')
const createRetry = require('../retry')
const connectionBuilder = require('./connectionBuilder')
const flatten = require('../utils/flatten')
const { KafkaJSError, KafkaJSGroupCoordinatorNotFound } = require('../errors')

const { keys, assign } = Object

const EARLIEST_OFFSET = -2
const LATEST_OFFSET = -1

const mergeTopics = (obj, { topic, partitions }) =>
  assign(obj, {
    [topic]: [...(obj[topic] || []), ...partitions],
  })

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
    this.retrier = createRetry(assign({}, retry))
    this.connectionBuilder = connectionBuilder({
      logger: rootLogger,
      brokers,
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      retry,
    })

    this.targetTopics = new Set()
    this.brokerPool = new BrokerPool({
      connectionBuilder: this.connectionBuilder,
      logger: this.rootLogger,
      retry,
    })
  }

  isConnected() {
    return this.brokerPool.hasConnectedBrokers()
  }

  /**
   * @public
   * @returns {Promise<null>}
   */
  async connect() {
    await this.brokerPool.connect()
  }

  /**
   * @public
   * @returns {Promise<null>}
   */
  async disconnect() {
    await this.brokerPool.disconnect()
  }

  /**
   * @public
   * @returns {Promise<null>}
   */
  async refreshMetadata() {
    await this.brokerPool.refreshMetadata(Array.from(this.targetTopics))
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
    return await this.brokerPool.findBroker({ nodeId })
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
    const { metadata } = this.brokerPool
    if (!metadata || !metadata.topicMetadata) {
      throw new KafkaJSError('Topic metadata not loaded')
    }

    return metadata.topicMetadata.find(t => t.topic === topic).partitionMetadata
  }

  /**
   * @public
   * @param {string} topic
   * @param {Array<number>} partitions
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
      const metadata = partitionMetadata.find(p => p.partitionId === partitionId)
      if (metadata.leader === null || metadata.leader === undefined) {
        throw new KafkaJSError('Invalid partition metadata', { topic, partitionId, metadata })
      }

      const { leader } = metadata
      const current = result[leader] || []
      return assign(result, { [leader]: [...current, partitionId] })
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
        const { coordinator } = await this.findGroupCoordinatorMetadata({ groupId })
        return await this.findBroker({ nodeId: coordinator.nodeId })
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

  /**
   * @public
   * @param {string} groupId
   * @returns {Promise<Object>}
   */
  async findGroupCoordinatorMetadata({ groupId }) {
    const brokerMetadata = await this.brokerPool.withBroker(async ({ nodeId, broker }) => {
      return await this.retrier(async (bail, retryCount, retryTime) => {
        try {
          const brokerMetadata = await broker.findGroupCoordinator({ groupId })
          this.logger.debug('Found group coordinator', {
            broker: brokerMetadata.host,
            nodeId: brokerMetadata.coordinator.nodeId,
          })
          return brokerMetadata
        } catch (e) {
          this.logger.debug('Tried to find group coordinator', {
            nodeId,
            error: e,
          })

          if (e.type === 'GROUP_COORDINATOR_NOT_AVAILABLE') {
            this.logger.debug('Group coordinator not available, retrying...', {
              nodeId,
              retryCount,
              retryTime,
            })

            throw e
          }

          bail(e)
        }
      })
    })

    if (brokerMetadata) {
      return brokerMetadata
    }

    throw new KafkaJSGroupCoordinatorNotFound('Failed to find group coordinator')
  }

  /**
   * @param {string} groupId
   * @param {Array} topicsWithPartitions
   * @returns {Array} example:
   *                    [
   *                      {
   *                        topic: 'my-topic',
   *                        partitions: [
   *                          { offset: '-1', partition: 0 },
   *                          { offset: '-1', partition: 1 },
   *                          { offset: '-1', partition: 2 },
   *                        ],
   *                      },
   *                    ]
   */
  async fetchGroupOffset({ groupId, topicsWithPartitions }) {
    const coordinator = await this.findGroupCoordinator({ groupId })

    return this.retrier(async () => {
      const { responses: offsets } = await coordinator.offsetFetch({
        groupId,
        topics: topicsWithPartitions,
      })

      return offsets
    })
  }

  /**
   * @param {object} topicConfiguration
   * @returns {number}
   */
  defaultOffset({ fromBeginning }) {
    return fromBeginning ? EARLIEST_OFFSET : LATEST_OFFSET
  }

  /**
   * @public
   * @param {Array<Object>} topics
   *                          [
   *                            {
   *                              topic: 'my-topic-name',
   *                              partitions: [{ partition: 0 }],
   *                              fromBeginning: false
   *                            }
   *                          ]
   * @returns {Promise<Array>} example:
   *                          [
   *                            {
   *                              topic: 'my-topic-name',
   *                              partitions: [
   *                                { partition: 0, offset: '1' },
   *                                { partition: 1, offset: '2' },
   *                                { partition: 2, offset: '1' },
   *                              ],
   *                            },
   *                          ]
   */
  async fetchTopicsOffset(topics) {
    const partitionsPerBroker = {}
    const topicConfigurations = {}

    const addDefaultOffset = topic => partition => {
      const { fromBeginning } = topicConfigurations[topic]
      return Object.assign({}, partition, { timestamp: this.defaultOffset({ fromBeginning }) })
    }

    // Index all topics and partitions per leader (nodeId)
    for (let topicData of topics) {
      const { topic, partitions, fromBeginning } = topicData
      const partitionsPerLeader = this.findLeaderForPartitions(
        topic,
        partitions.map(p => p.partition)
      )

      topicConfigurations[topic] = { fromBeginning }

      keys(partitionsPerLeader).map(nodeId => {
        partitionsPerBroker[nodeId] = partitionsPerBroker[nodeId] || {}
        partitionsPerBroker[nodeId][topic] = partitions.filter(p =>
          partitionsPerLeader[nodeId].includes(p.partition)
        )
      })
    }

    // Create a list of requests to fetch the offset of all partitions
    const requests = keys(partitionsPerBroker).map(async nodeId => {
      const broker = await this.findBroker({ nodeId })
      const partitions = partitionsPerBroker[nodeId]

      const { responses: topicOffsets } = await broker.listOffsets({
        topics: keys(partitions).map(topic => ({
          topic,
          partitions: partitions[topic].map(addDefaultOffset(topic)),
        })),
      })

      return topicOffsets
    })

    // Execute all requests, merge and normalize the responses
    const responses = await Promise.all(requests)
    const partitionsPerTopic = flatten(responses).reduce(mergeTopics, {})

    return keys(partitionsPerTopic).map(topic => ({
      topic,
      partitions: partitionsPerTopic[topic].map(({ partition, offsets }) => ({
        partition,
        offset: offsets.pop(),
      })),
    }))
  }
}
