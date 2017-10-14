const flatten = require('../utils/flatten')
const createDefaultPartitioner = require('./partitioners/default')
const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const createTopicData = require('./createTopicData')
const responseSerializer = require('./responseSerializer')

module.exports = ({ cluster, createPartitioner = createDefaultPartitioner }) => {
  const partitioner = createPartitioner()

  return {
    connect: async () => await cluster.connect(),
    disconnect: async () => await cluster.disconnect(),
    send: async ({ topic, messages, acks, timeout, compression }) => {
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

      const requests = Object.keys(partitionsPerLeader).map(async nodeId => {
        const partitions = partitionsPerLeader[nodeId]
        const topicData = createTopicData({ topic, partitions, messagesPerPartition })
        const broker = await cluster.findBroker({ nodeId })
        const response = await broker.produce({ acks, timeout, compression, topicData })
        return responseSerializer(response)
      })

      return flatten(await Promise.all(requests))
    },
  }
}
