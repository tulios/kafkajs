const Long = require('long')

module.exports = (consumerOffsets, topicOffsets) => {
  const indexPartitions = (obj, { partition, offset }) =>
    Object.assign(obj, { [partition]: offset })

  const indexTopics = (obj, { topic, partitions }) =>
    Object.assign(obj, { [topic]: partitions.reduce(indexPartitions, {}) })

  const shouldInitializeOffset = offset => Long.fromValue(offset).equals(-1)

  const indexedConsumerOffsets = consumerOffsets.reduce(indexTopics, {})
  const indexedTopicOffsets = topicOffsets.reduce(indexTopics, {})

  return Object.keys(indexedConsumerOffsets).map(topic => {
    return {
      topic,
      partitions: Object.keys(indexedConsumerOffsets[topic]).map(partition => {
        const offset = indexedConsumerOffsets[topic][partition]
        const resolvedOffset = shouldInitializeOffset(offset)
          ? indexedTopicOffsets[topic][partition]
          : offset

        return { partition: Number(partition), offset: resolvedOffset }
      }),
    }
  })
}
