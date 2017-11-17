const Long = require('long')
const { keys, assign } = Object

const indexPartitions = (obj, { partition, offset }) => assign(obj, { [partition]: offset })
const shouldInitializeOffset = offset => Long.fromValue(offset).equals(-1)

module.exports = (consumerOffsets, topicOffsets) => {
  const indexTopics = (obj, { topic, partitions }) =>
    assign(obj, { [topic]: partitions.reduce(indexPartitions, {}) })

  const indexedConsumerOffsets = consumerOffsets.reduce(indexTopics, {})
  const indexedTopicOffsets = topicOffsets.reduce(indexTopics, {})

  return keys(indexedConsumerOffsets).map(topic => {
    const partitions = indexedConsumerOffsets[topic]
    return {
      topic,
      partitions: keys(partitions).map(partition => {
        const offset = partitions[partition]
        const resolvedOffset = shouldInitializeOffset(offset)
          ? indexedTopicOffsets[topic][partition]
          : offset

        return { partition: Number(partition), offset: resolvedOffset }
      }),
    }
  })
}
