const { keys, assign } = Object

const indexPartitions = (obj, { partition, offset }) => assign(obj, { [partition]: offset })
const indexTopics = (obj, { topic, partitions }) =>
  assign(obj, { [topic]: partitions.reduce(indexPartitions, {}) })

module.exports = topicOffsets => {
  const indexedTopicOffsets = topicOffsets.reduce(indexTopics, {})

  return keys(indexedTopicOffsets).map(topic => {
    const partitions = indexedTopicOffsets[topic]
    return {
      topic,
      partitions: keys(partitions).map(partition => {
        const resolvedOffset = indexedTopicOffsets[topic][partition]
        return { partition: Number(partition), offset: resolvedOffset }
      }),
    }
  })
}
