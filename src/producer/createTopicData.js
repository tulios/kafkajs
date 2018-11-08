module.exports = topicDataForBroker => {
  return topicDataForBroker.map(
    ({ topic, partitions, messagesPerPartition, sequencePerPartition }) => ({
      topic,
      partitions: partitions.map(partition => ({
        partition,
        firstSequence: sequencePerPartition[partition],
        messages: messagesPerPartition[partition],
      })),
    })
  )
}
