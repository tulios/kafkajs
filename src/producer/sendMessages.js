const createRetry = require('../retry')
const flatten = require('../utils/flatten')
const { KafkaJSMetadataNotLoaded } = require('../errors')
const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const createTopicData = require('./createTopicData')
const responseSerializer = require('./responseSerializer')

const { keys } = Object
const TOTAL_INDIVIDUAL_ATTEMPTS = 5
const staleMetadata = e =>
  ['UNKNOWN_TOPIC_OR_PARTITION', 'LEADER_NOT_AVAILABLE', 'NOT_LEADER_FOR_PARTITION'].includes(
    e.type
  )

module.exports = ({ logger, cluster, partitioner, eosManager }) => {
  const retrier = createRetry({ retries: TOTAL_INDIVIDUAL_ATTEMPTS })

  return async ({ acks, timeout, compression, topicMessages }) => {
    const responsePerBroker = new Map()

    for (const { topic } of topicMessages) {
      await cluster.addTargetTopic(topic)
    }

    const createProducerRequests = async responsePerBroker => {
      const topicMetadata = new Map()

      await cluster.refreshMetadataIfNecessary()

      for (const { topic, messages } of topicMessages) {
        const partitionMetadata = cluster.findTopicPartitionMetadata(topic)

        if (keys(partitionMetadata).length === 0) {
          logger.debug('Producing to topic without metadata', {
            topic,
            targetTopics: Array.from(cluster.targetTopics),
          })

          throw new KafkaJSMetadataNotLoaded('Producing to topic without metadata')
        }

        const messagesPerPartition = groupMessagesPerPartition({
          topic,
          partitionMetadata,
          messages,
          partitioner,
        })

        const partitions = keys(messagesPerPartition)
        const sequencePerPartition = partitions.reduce((result, partition) => {
          result[partition] = eosManager.getSequence(topic, partition)
          return result
        }, {})

        const partitionsPerLeader = cluster.findLeaderForPartitions(topic, partitions)
        const leaders = keys(partitionsPerLeader)

        topicMetadata.set(topic, {
          partitionsPerLeader,
          messagesPerPartition,
          sequencePerPartition,
        })

        for (const nodeId of leaders) {
          const broker = await cluster.findBroker({ nodeId })
          if (!responsePerBroker.has(broker)) {
            responsePerBroker.set(broker, null)
          }
        }
      }

      const brokers = Array.from(responsePerBroker.keys())
      const brokersWithoutResponse = brokers.filter(broker => !responsePerBroker.get(broker))

      return brokersWithoutResponse.map(async broker => {
        const entries = Array.from(topicMetadata.entries())
        const topicDataForBroker = entries
          .filter(([_, { partitionsPerLeader }]) => !!partitionsPerLeader[broker.nodeId])
          .map(([topic, { partitionsPerLeader, messagesPerPartition, sequencePerPartition }]) => ({
            topic,
            partitions: partitionsPerLeader[broker.nodeId],
            sequencePerPartition,
            messagesPerPartition,
          }))

        const topicData = createTopicData(topicDataForBroker)

        try {
          if (eosManager.isTransactional()) {
            await eosManager.addPartitionsToTransaction(topicData)
          }

          const response = await broker.produce({
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

          const expectResponse = acks !== 0
          const formattedResponse = expectResponse ? responseSerializer(response) : []

          formattedResponse.forEach(({ topicName, partition }) => {
            const increment = topicMetadata.get(topicName).messagesPerPartition[partition].length

            eosManager.updateSequence(topicName, partition, increment)
          })

          responsePerBroker.set(broker, formattedResponse)
        } catch (e) {
          responsePerBroker.delete(broker)
          throw e
        }
      })
    }

    const makeRequests = async (bail, retryCount, retryTime) => {
      try {
        const requests = await createProducerRequests(responsePerBroker)
        await Promise.all(requests)
        const responses = Array.from(responsePerBroker.values())
        return flatten(responses)
      } catch (e) {
        if (staleMetadata(e) || e.name === 'KafkaJSMetadataNotLoaded') {
          await cluster.refreshMetadata()
        }

        throw e
      }
    }

    return retrier(makeRequests).catch(e => {
      throw e.originalError || e
    })
  }
}
