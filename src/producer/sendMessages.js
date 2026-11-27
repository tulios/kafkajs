const { KafkaJSMetadataNotLoaded } = require('../errors')
const { staleMetadata } = require('../protocol/error')
const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const createTopicData = require('./createTopicData')
const responseSerializer = require('./responseSerializer')

const { keys } = Object

/**
 * @param {Object} options
 * @param {import("../../types").Logger} options.logger
 * @param {import("../../types").Cluster} options.cluster
 * @param {ReturnType<import("../../types").ICustomPartitioner>} options.partitioner
 * @param {import("./eosManager").EosManager} options.eosManager
 * @param {import("../retry").Retrier} options.retrier
 */
module.exports = ({ logger, cluster, partitioner, eosManager, retrier }) => {
  return async ({ acks, timeout, compression, topicMessages }) => {
    /**
     * Messages of each topic, assigned to their partitions on the first attempt that has the
     * topic's metadata.
     * @type {Map<string, Object<string, any[]>>}
     */
    const messagesPerPartitionPerTopic = new Map()

    /**
     * Produced partitions, keyed by `topic:partition`, with their response (null with acks 0).
     * @type {Map<string, any>}
     */
    const responsePerPartition = new Map()
    const partitionKey = (topic, partition) => `${topic}:${partition}`

    const createProducerRequests = async () => {
      const topicMetadata = new Map()
      const brokers = new Map()

      await cluster.refreshMetadataIfNecessary()

      for (const { topic, messages } of topicMessages) {
        const partitionMetadata = cluster.findTopicPartitionMetadata(topic)

        if (partitionMetadata.length === 0) {
          logger.debug('Producing to topic without metadata', {
            topic,
            targetTopics: Array.from(cluster.targetTopics),
          })

          throw new KafkaJSMetadataNotLoaded('Producing to topic without metadata')
        }

        if (!messagesPerPartitionPerTopic.has(topic)) {
          messagesPerPartitionPerTopic.set(
            topic,
            groupMessagesPerPartition({ topic, partitionMetadata, messages, partitioner })
          )
        }
        const messagesPerPartition = messagesPerPartitionPerTopic.get(topic)

        const pendingPartitions = keys(messagesPerPartition).filter(
          partition => !responsePerPartition.has(partitionKey(topic, partition))
        )
        const partitionsPerLeader = cluster.findLeaderForPartitions(topic, pendingPartitions)

        const ledPartitions = Object.values(partitionsPerLeader).flat()
        if (ledPartitions.length < pendingPartitions.length) {
          throw new KafkaJSMetadataNotLoaded('Producing to a partition missing from the metadata')
        }

        topicMetadata.set(topic, {
          partitionsPerLeader,
          messagesPerPartition,
        })

        for (const nodeId of keys(partitionsPerLeader)) {
          if (!brokers.has(nodeId)) {
            brokers.set(nodeId, await cluster.findBroker({ nodeId }))
          }
        }
      }

      return Array.from(brokers.values()).map(async broker => {
        const entries = Array.from(topicMetadata.entries())
        const topicDataForBroker = entries
          .filter(([_, { partitionsPerLeader }]) => !!partitionsPerLeader[broker.nodeId])
          .map(([topic, { partitionsPerLeader, messagesPerPartition, sequencePerPartition }]) => ({
            topic,
            partitions: partitionsPerLeader[broker.nodeId],
            messagesPerPartition,
          }))

        const topicData = createTopicData(topicDataForBroker)

        await eosManager.acquireBrokerLock(broker)
        try {
          if (eosManager.isTransactional()) {
            await eosManager.addPartitionsToTransaction(topicData)
          }

          topicData.forEach(({ topic, partitions }) => {
            partitions.forEach(entry => {
              entry['firstSequence'] = eosManager.getSequence(topic, entry.partition)
              eosManager.updateSequence(topic, entry.partition, entry.messages.length)
            })
          })

          let response
          try {
            response = await broker.produce({
              transactionalId: eosManager.isTransactional()
                ? eosManager.getTransactionalId()
                : undefined,
              producerId: eosManager.getProducerId(),
              producerEpoch: eosManager.getProducerEpoch(),
              acks,
              timeout,
              compression,
              topicData,
            })
          } catch (e) {
            topicData.forEach(({ topic, partitions }) => {
              partitions.forEach(entry => {
                eosManager.updateSequence(topic, entry.partition, -entry.messages.length)
              })
            })
            throw e
          }

          const expectResponse = acks !== 0
          const formattedResponse = expectResponse ? responseSerializer(response) : []

          topicData.forEach(({ topic, partitions }) => {
            partitions.forEach(({ partition }) => {
              responsePerPartition.set(partitionKey(topic, partition), null)
            })
          })
          formattedResponse.forEach(partitionResponse => {
            const { topicName, partition } = partitionResponse
            responsePerPartition.set(partitionKey(topicName, partition), partitionResponse)
          })
        } finally {
          await eosManager.releaseBrokerLock(broker)
        }
      })
    }

    return retrier(async (bail, retryCount, retryTime) => {
      const topics = topicMessages.map(({ topic }) => topic)
      await cluster.addMultipleTargetTopics(topics)

      try {
        const requests = await createProducerRequests()
        await Promise.all(requests)
        return Array.from(responsePerPartition.values()).filter(Boolean)
      } catch (e) {
        if (e.name === 'KafkaJSConnectionClosedError') {
          cluster.removeBroker({ host: e.host, port: e.port })
        }

        if (!cluster.isConnected()) {
          logger.debug(`Cluster has disconnected, reconnecting: ${e.message}`, {
            retryCount,
            retryTime,
          })
          await cluster.connect()
          await cluster.refreshMetadata()
          throw e
        }

        // This is necessary in case the metadata is stale and the number of partitions
        // for this topic has increased in the meantime
        if (
          staleMetadata(e) ||
          e.name === 'KafkaJSMetadataNotLoaded' ||
          e.name === 'KafkaJSConnectionError' ||
          e.name === 'KafkaJSConnectionClosedError' ||
          (e.name === 'KafkaJSProtocolError' && e.retriable)
        ) {
          logger.error(`Failed to send messages: ${e.message}`, { retryCount, retryTime })
          await cluster.refreshMetadata()
          throw e
        }

        logger.error(`${e.message}`, { retryCount, retryTime })
        if (e.retriable) throw e
        bail(e)
      }
    })
  }
}
