const createRetry = require('../retry')
const flatten = require('../utils/flatten')
const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const createTopicData = require('./createTopicData')
const responseSerializer = require('./responseSerializer')

module.exports = ({ logger, cluster, partitioner }) => {
  const retrier = createRetry()

  return async ({ topic, messages, acks, timeout, compression }) => {
    await cluster.addTargetTopic(topic)
    const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
    const messagesPerPartition = groupMessagesPerPartition({
      topic,
      partitionMetadata,
      messages,
      partitioner,
    })

    const partitionsPerLeader = cluster.findLeaderForPartitions(
      topic,
      Object.keys(messagesPerPartition)
    )

    const responsePerBroker = new Map()
    for (let nodeId of Object.keys(partitionsPerLeader)) {
      const broker = await cluster.findBroker({ nodeId })
      responsePerBroker.set(broker, null)
    }

    const produce = responsePerBroker => {
      return Array.from(responsePerBroker.keys()).map(async broker => {
        if (responsePerBroker.get(broker)) {
          return
        }

        const partitions = partitionsPerLeader[broker.nodeId]
        const topicData = createTopicData({ topic, partitions, messagesPerPartition })

        const response = await broker.produce({ acks, timeout, compression, topicData })
        responsePerBroker.set(broker, responseSerializer(response))
      })
    }

    return retrier(async (bail, retryCount, retryTime) => {
      await Promise.all(produce(responsePerBroker))
      return flatten(Array.from(responsePerBroker.values()))
    })
  }
}
