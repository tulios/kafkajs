const createRetry = require('../retry')
const flatten = require('../utils/flatten')
const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const createTopicData = require('./createTopicData')
const responseSerializer = require('./responseSerializer')

const { keys } = Object
const TOTAL_INDIVIDUAL_ATTEMPTS = 5
const staleMetadata = e =>
  ['UNKNOWN_TOPIC_OR_PARTITION', 'LEADER_NOT_AVAILABLE', 'NOT_LEADER_FOR_PARTITION'].includes(
    e.type
  )

module.exports = ({ logger, cluster, partitioner }) => {
  const retrier = createRetry({ retries: TOTAL_INDIVIDUAL_ATTEMPTS })

  return async ({ acks, timeout, compression, topicMessages }) => {
    const responsePerBroker = new Map()

    for (let { topic } of topicMessages) {
      await cluster.addTargetTopic(topic)
    }

    const createProducerRequests = async responsePerBroker => {
      const topicMetadata = new Map()

      for (let { topic, messages } of topicMessages) {
        const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
        const messagesPerPartition = groupMessagesPerPartition({
          topic,
          partitionMetadata,
          messages,
          partitioner,
        })

        const partitions = keys(messagesPerPartition)
        const partitionsPerLeader = cluster.findLeaderForPartitions(topic, partitions)
        const leaders = keys(partitionsPerLeader)
        topicMetadata.set(topic, { partitionsPerLeader, messagesPerPartition })

        for (let nodeId of leaders) {
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
          .map(([topic, { partitionsPerLeader, messagesPerPartition }]) => ({
            topic,
            partitions: partitionsPerLeader[broker.nodeId],
            messagesPerPartition,
          }))

        const topicData = createTopicData(topicDataForBroker)

        try {
          const response = await broker.produce({ acks, timeout, compression, topicData })
          responsePerBroker.set(broker, responseSerializer(response))
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
        if (staleMetadata(e)) {
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
