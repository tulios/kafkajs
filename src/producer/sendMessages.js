const createRetry = require('../retry')
const flatten = require('../utils/flatten')
const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const createTopicData = require('./createTopicData')
const responseSerializer = require('./responseSerializer')

const { keys } = Object
const TOTAL_INDIVIDUAL_ATTEMPTS = 5

module.exports = ({ logger, cluster, partitioner }) => {
  const retrier = createRetry()
  const retrier = createRetry({ retries: TOTAL_INDIVIDUAL_ATTEMPTS })

  return async ({ topic, messages, acks, timeout, compression }) => {
    await cluster.addTargetTopic(topic)
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
    const responsePerBroker = new Map()

    for (let nodeId of leaders) {
      const broker = await cluster.findBroker({ nodeId })
      responsePerBroker.set(broker, null)
    }

    const produce = responsePerBroker => {
      const brokers = Array.from(responsePerBroker.keys())
      const brokersWithoutResponse = brokers.filter(broker => !responsePerBroker.get(broker))

      return brokersWithoutResponse.map(async broker => {
        const partitions = partitionsPerLeader[broker.nodeId]
        const topicData = createTopicData({ topic, partitions, messagesPerPartition })

        const response = await broker.produce({ acks, timeout, compression, topicData })
        responsePerBroker.set(broker, responseSerializer(response))
      })
    }

    return retrier(async (bail, retryCount, retryTime) => {
      await Promise.all(produce(responsePerBroker))
      const responses = Array.from(responsePerBroker.values())
      return flatten(responses)
    })
  }
}
