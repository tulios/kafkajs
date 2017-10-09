const Cluster = require('../cluster')
const flatten = require('../utils/flatten')
const createDefaultPartitioner = require('./partitioners/default')
const groupMessagesPerPartition = require('./groupMessagesPerPartition')

const createTopicData = ({ topic, partitions, messagesPerPartition }) => [
  {
    topic,
    partitions: partitions.map(partition => ({
      partition,
      messages: messagesPerPartition[partition],
    })),
  },
]

const responseSerializer = ({ topics }) => {
  const partitions = topics.map(({ topicName, partitions }) =>
    partitions.map(partition => Object.assign({ topicName }, partition))
  )
  return flatten(partitions)
}

module.exports = ({
  host,
  port,
  ssl,
  sasl,
  logger,
  createPartitioner = createDefaultPartitioner,
}) => {
  const cluster = new Cluster({ host, port, ssl, sasl, logger })
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
